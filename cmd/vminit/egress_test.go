//go:build linux

package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type testTrustCA struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	pem         []byte
}

func newTestTrustCA(t *testing.T, name string) testTrustCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testTrustCA{certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca testTrustCA) leaf(t *testing.T) []byte {
	t.Helper()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), DNSNames: []string{"test.beam.cloud"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &ca.key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func trustTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func TestEgressTrustPreservesRootsAndRotatesWithoutRehash(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Fatal("openssl is required to verify guest trust")
	}
	root := t.TempDir()
	custom := newTestTrustCA(t, "Customer root")
	// A customer CA may have the same subject/hash as Beam's CA. Keep it.
	collision := newTestTrustCA(t, "Beam container egress CA")
	first := newTestTrustCA(t, "Beam container egress CA")
	second := newTestTrustCA(t, "Beam container egress CA")
	bundlePath := filepath.Join(root, "etc/ssl/certs/ca-certificates.crt")
	certDir := filepath.Dir(bundlePath)
	original := append(append([]byte{}, custom.pem...), collision.pem...)
	trustTestFile(t, bundlePath, original)
	command := exec.Command("openssl", "x509", "-subject_hash", "-noout")
	command.Stdin = bytes.NewReader(collision.pem)
	hash, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	collisionPath := filepath.Join(certDir, strings.TrimSpace(string(hash))+".0")
	trustTestFile(t, collisionPath, collision.pem)
	// A full rebuild must never execute, even when the distro tool exists.
	trustTestFile(t, filepath.Join(root, "usr/sbin/update-ca-certificates"), []byte("must not run"))
	for _, ca := range []testTrustCA{first, second, second} {
		trustTestFile(t, filepath.Join(root, "etc/beam/ca-certificates.crt"), ca.pem)
		start := time.Now()
		if err := installEgressCAAt(root); err != nil {
			t.Fatal(err)
		}
		t.Logf("direct CA installation: %s", time.Since(start))
		bundle, err := os.ReadFile(bundlePath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(bundle, original) || bytes.Count(bundle, []byte("BEGIN CERTIFICATE")) != 3 {
			t.Fatal("guest roots changed or managed CAs accumulated")
		}
		for _, trusted := range []testTrustCA{custom, ca} {
			leafPath := filepath.Join(root, "leaf.pem")
			trustTestFile(t, leafPath, trusted.leaf(t))
			if output, err := exec.Command("openssl", "verify", "-CAfile", bundlePath, "-verify_hostname", "test.beam.cloud", leafPath).CombinedOutput(); err != nil {
				t.Fatalf("bundle trust failed: %v: %s", err, output)
			}
		}
		for _, trusted := range []testTrustCA{collision, ca} {
			leafPath := filepath.Join(root, "leaf.pem")
			trustTestFile(t, leafPath, trusted.leaf(t))
			if output, err := exec.Command("openssl", "verify", "-no-CAfile", "-CApath", certDir, "-verify_hostname", "test.beam.cloud", leafPath).CombinedOutput(); err != nil {
				t.Fatalf("CApath trust failed: %v: %s", err, output)
			}
		}
	}
	for _, args := range [][]string{{"-CAfile", bundlePath}, {"-no-CAfile", "-CApath", certDir}} {
		leafPath := filepath.Join(root, "wrong-host-leaf.pem")
		trustTestFile(t, leafPath, second.leaf(t))
		wrongHost := append(append([]string{"verify"}, args...), "-verify_hostname", "other.beam.cloud", leafPath)
		if output, err := exec.Command("openssl", wrongHost...).CombinedOutput(); err == nil {
			t.Fatalf("trusted CA bypassed hostname verification: %s", output)
		}
	}
	for _, args := range [][]string{{"-CAfile", bundlePath}, {"-no-CAfile", "-CApath", certDir}} {
		leafPath := filepath.Join(root, "old-leaf.pem")
		trustTestFile(t, leafPath, first.leaf(t))
		if output, err := exec.Command("openssl", append(append([]string{"verify"}, args...), leafPath)...).CombinedOutput(); err == nil {
			t.Fatalf("previous worker CA still trusted: %s", output)
		}
	}
	stored, err := os.ReadFile(collisionPath)
	if err != nil || !bytes.Equal(stored, collision.pem) {
		t.Fatal("customer's colliding hash entry was overwritten")
	}
}

func TestEgressTrustCompletesInterruptedRotation(t *testing.T) {
	root := t.TempDir()
	custom := newTestTrustCA(t, "Customer root")
	previous := newTestTrustCA(t, "Beam container egress CA")
	current := newTestTrustCA(t, "Beam container egress CA")
	bundlePath := filepath.Join(root, "etc/ssl/certs/ca-certificates.crt")
	caPath := filepath.Join(root, "usr/local/share/ca-certificates/beam-egress.crt")
	// The bundle rename committed, but the previous source and unfinished link
	// survived an interrupted install. A retry must complete without a reboot.
	trustTestFile(t, bundlePath, append(append([]byte{}, custom.pem...), current.pem...))
	trustTestFile(t, caPath, previous.pem)
	trustTestFile(t, filepath.Join(root, "etc/beam/ca-certificates.crt"), current.pem)
	linkPath := filepath.Join(filepath.Dir(bundlePath), "beam-egress.pem")
	if err := os.Symlink("interrupted.pem", linkPath+".beam-tmp"); err != nil {
		t.Fatal(err)
	}
	if err := installEgressCAAt(root); err != nil {
		t.Fatal(err)
	}
	bundle, err := os.ReadFile(bundlePath)
	if err != nil || !bytes.Contains(bundle, custom.pem) || !bytes.Contains(bundle, current.pem) || bytes.Contains(bundle, previous.pem) || bytes.Count(bundle, []byte("BEGIN CERTIFICATE")) != 2 {
		t.Fatal("interrupted rotation lost guest roots or retained the previous CA")
	}
	stored, err := os.ReadFile(linkPath)
	if err != nil || !bytes.Equal(stored, current.pem) {
		t.Fatal("managed link did not resolve to the current CA")
	}
	if _, err := os.Lstat(linkPath + ".beam-tmp"); !os.IsNotExist(err) {
		t.Fatal("interrupted symlink was not retired")
	}
}

func TestEgressTrustHooksObserveInstalledRotation(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "etc/ca-certificates/update.d")
	bundlePath := filepath.Join(root, "etc/ssl/certs/ca-certificates.crt")
	linkPath := filepath.Join(filepath.Dir(bundlePath), "beam-egress.pem")
	valid := []byte(`#!/bin/sh
set -eu
directory=${0%/*}
root=${directory%/etc/ca-certificates/update.d}
cmp -s "$root/expected-ca.crt" "$root/usr/local/share/ca-certificates/beam-egress.crt"
cmp -s "$root/expected-ca.crt" "$root/etc/ssl/certs/beam-egress.pem"
cmp -s "$root/expected-bundle.crt" "$root/etc/ssl/certs/ca-certificates.crt"
cat >> "$root/hook-inputs"
`)
	ignored := []byte(`#!/bin/sh
directory=${0%/*}
root=${directory%/etc/ca-certificates/update.d}
touch "$root/unexpected-hook"
`)
	for name, script := range map[string][]byte{
		"managed-test": valid,
		"ignored.sh":   ignored,
		"nonexec":      ignored,
		"a-failing":    []byte("#!/bin/sh\nexit 7\n"),
	} {
		path := filepath.Join(directory, name)
		trustTestFile(t, path, script)
		if name != "nonexec" {
			if err := os.Chmod(path, 0755); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.Mkdir(filepath.Join(directory, "directory-hook"), 0755); err != nil {
		t.Fatal(err)
	}
	custom := newTestTrustCA(t, "Customer root")
	trustTestFile(t, bundlePath, custom.pem)
	for _, ca := range []testTrustCA{newTestTrustCA(t, "Beam container egress CA"), newTestTrustCA(t, "Beam container egress CA")} {
		trustTestFile(t, filepath.Join(root, "etc/beam/ca-certificates.crt"), ca.pem)
		trustTestFile(t, filepath.Join(root, "expected-ca.crt"), ca.pem)
		trustTestFile(t, filepath.Join(root, "expected-bundle.crt"), append(append([]byte{}, custom.pem...), ca.pem...))
		if err := installEgressCAAt(root); err != nil {
			t.Fatal(err)
		}
	}
	input, err := os.ReadFile(filepath.Join(root, "hook-inputs"))
	if err != nil || string(input) != strings.Repeat("+"+linkPath+"\n", 2) {
		t.Fatalf("hook did not receive exactly one installed CA per rotation: %q, %v", input, err)
	}
	if _, err := os.Stat(filepath.Join(root, "unexpected-hook")); !os.IsNotExist(err) {
		t.Fatal("invalid or nonexecutable hook ran")
	}
}

func TestEgressTrustMissingBundleAndAbsentCA(t *testing.T) {
	root := t.TempDir()
	if err := installEgressCAAt(root); err != nil {
		t.Fatal(err)
	}
	custom := newTestTrustCA(t, "Customer root")
	input := filepath.Join(root, "etc/beam/ca-certificates.crt")
	trustTestFile(t, input, custom.pem)
	if err := installEgressCAAt(root); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(root, "etc/ssl/certs/ca-certificates.crt")
	if _, err := os.Stat(bundle); !os.IsNotExist(err) {
		t.Fatal("trust was changed without a managed CA")
	}
	ca := newTestTrustCA(t, "Beam container egress CA")
	trustTestFile(t, input, append(custom.pem, ca.pem...))
	if err := installEgressCAAt(root); err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(bundle)
	if err != nil || !bytes.Contains(stored, custom.pem) || bytes.Count(stored, ca.pem) != 1 {
		t.Fatal("fallback bundle did not retain roots and one managed CA")
	}
}

func TestEgressTrustBeforeSystemdSetsPATH(t *testing.T) {
	root := t.TempDir()
	ca := newTestTrustCA(t, "Beam container egress CA")
	trustTestFile(t, filepath.Join(root, "etc/beam/ca-certificates.crt"), ca.pem)
	t.Setenv("PATH", "")
	if err := installEgressCAAt(root); err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(filepath.Join(root, "etc/ssl/certs/beam-egress.pem"))
	if err != nil || !bytes.Equal(stored, ca.pem) {
		t.Fatal("PID 1 did not install trust without PATH")
	}
}
