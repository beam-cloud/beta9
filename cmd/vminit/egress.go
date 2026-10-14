//go:build linux

package main

import (
	"bytes"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func installEgressCA() error {
	return installEgressCAAt("/")
}

func installEgressCAAt(root string) error {
	data, err := os.ReadFile(filepath.Join(root, "etc/beam/ca-certificates.crt"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for remaining := data; len(remaining) > 0; {
		block, rest := pem.Decode(remaining)
		if block == nil {
			break
		}
		remaining = rest
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return err
		}
		if !cert.IsCA || cert.Subject.CommonName != "Beam container egress CA" {
			continue
		}
		return installSystemCA(root, pem.EncodeToMemory(block), data)
	}
	return nil
}

// Install only Beam's certificate. update-ca-certificates scans every root and
// runs distro hooks, adding seconds to each boot on a lazy filesystem. Preserve
// the guest's own roots and provide both the PEM bundle and OpenSSL CApath entry.
// The source file also lets a later distro update retain the managed CA.
func installSystemCA(root string, certificate, fallbackBundle []byte) error {
	caPath := filepath.Join(root, "usr/local/share/ca-certificates/beam-egress.crt")
	bundlePath := filepath.Join(root, "etc/ssl/certs/ca-certificates.crt")
	previous, err := os.ReadFile(caPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	bundle, err := os.ReadFile(bundlePath)
	if os.IsNotExist(err) {
		bundle = fallbackBundle
	} else if err != nil {
		return err
	}

	// Ask OpenSSL for this one subject's canonical hash; never rehash the store.
	// PID 1 has no PATH before systemd starts.
	command := exec.Command("/usr/bin/openssl", "x509", "-subject_hash", "-noout")
	command.Stdin = bytes.NewReader(certificate)
	output, err := command.Output()
	if err != nil {
		return fmt.Errorf("hash egress CA: %w", err)
	}
	hash := strings.TrimSpace(string(output))
	if _, err := hex.DecodeString(hash); err != nil || len(hash) != 8 {
		return fmt.Errorf("invalid egress CA subject hash %q", hash)
	}

	// Retire the previous boot/worker's CA, including after a memory restore.
	// Match its certificate bytes, not its name: customer roots remain intact.
	oldBlock, _ := pem.Decode(previous)
	newBlock, _ := pem.Decode(certificate)
	var merged bytes.Buffer
	for len(bundle) > 0 {
		block, rest := pem.Decode(bundle)
		if block == nil {
			merged.Write(bundle)
			break
		}
		if !bytes.Equal(block.Bytes, newBlock.Bytes) && (oldBlock == nil || !bytes.Equal(block.Bytes, oldBlock.Bytes)) {
			merged.Write(bundle[:len(bundle)-len(rest)])
		}
		bundle = rest
	}
	if merged.Len() > 0 && merged.Bytes()[merged.Len()-1] != '\n' {
		merged.WriteByte('\n')
	}
	merged.Write(certificate)
	// Commit the bundle first. If interrupted, the source file still identifies
	// the old CA so the next boot can finish retiring it safely.
	if err := writeTrustFile(bundlePath, merged.Bytes()); err != nil {
		return err
	}
	if err := writeTrustFile(caPath, certificate); err != nil {
		return err
	}
	certDir := filepath.Dir(bundlePath)
	const linkName = "beam-egress.pem"
	if err := replaceTrustLink(filepath.Join(certDir, linkName), "../../../usr/local/share/ca-certificates/beam-egress.crt"); err != nil {
		return err
	}
	for index := 0; ; index++ {
		path := filepath.Join(certDir, fmt.Sprintf("%s.%d", hash, index))
		if target, err := os.Readlink(path); err == nil && target == linkName {
			break
		}
		if _, err := os.Lstat(path); os.IsNotExist(err) {
			if err := os.Symlink(linkName, path); err != nil {
				return err
			}
			break
		} else if err != nil {
			return err
		}
	}
	return runEgressTrustHooks(root, filepath.Join(certDir, linkName))
}

// Preserve distro integrations such as Java's keystore with a one-certificate
// change list. Debian's run-parts accepts executable names without dots and
// reports hook failures without failing the system trust update.
func runEgressTrustHooks(root, certificatePath string) error {
	directory := filepath.Join(root, "etc/ca-certificates/update.d")
	hooks, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, hook := range hooks {
		if hook.IsDir() || strings.IndexFunc(hook.Name(), func(c rune) bool {
			return !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-')
		}) >= 0 {
			continue
		}
		path := filepath.Join(directory, hook.Name())
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			continue
		}
		command := exec.Command(path)
		command.Env = append(os.Environ(), "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
		command.Stdin = strings.NewReader("+" + certificatePath + "\n")
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		if err := command.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "egress CA hook %s: %v\n", hook.Name(), err)
		}
	}
	return nil
}

func writeTrustFile(path string, data []byte) error {
	if existing, err := os.ReadFile(path); err == nil && bytes.Equal(existing, data) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".beam-trust-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, err = file.Write(data)
	if err == nil {
		err = file.Chmod(0644)
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

func replaceTrustLink(path, target string) error {
	if current, err := os.Readlink(path); err == nil && current == target {
		return nil
	}
	// A stable temporary name is safe: guest boot and restore are serialized.
	temporary := path + ".beam-tmp"
	if err := os.Remove(temporary); err != nil && !os.IsNotExist(err) {
		return err
	}
	defer os.Remove(temporary)
	if err := os.Symlink(target, temporary); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}
