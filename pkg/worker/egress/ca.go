package egress

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2"
)

const TrustBundlePath = "/etc/beam/ca-certificates.crt"

type authority struct {
	key    *ecdsa.PrivateKey
	cert   *x509.Certificate
	pem    []byte
	mu     sync.Mutex
	leaves *lru.Cache[string, *tls.Certificate]
}

func newAuthority() (*authority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		Subject:               pkix.Name{CommonName: "Beam container egress CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.AddDate(1, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	leaves, _ := lru.New[string, *tls.Certificate](128)
	return &authority{
		key:    key,
		cert:   cert,
		pem:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		leaves: leaves,
	}, nil
}

func (a *authority) leaf(host string) (*tls.Certificate, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if cert, ok := a.leaves.Get(host); ok && time.Now().Before(cert.Leaf.NotAfter.Add(-time.Minute)) {
		return cert, nil
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		Subject:     pkix.Name{CommonName: host},
		DNSNames:    []string{host},
		NotBefore:   now.Add(-time.Minute),
		NotAfter:    now.Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, &key.PublicKey, a.key)
	if err != nil {
		return nil, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	cert := &tls.Certificate{Certificate: [][]byte{der, a.cert.Raw}, PrivateKey: key, Leaf: leaf}
	a.leaves.Add(host, cert)
	return cert, nil
}

// TrustBundle writes public certificates only; the CA key never leaves memory.
func (f *Firewall) TrustBundle(id, directory string) (string, error) {
	c, err := f.lookup(id)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	if c.ca == nil {
		c.ca, err = newAuthority()
	}
	ca := c.ca
	c.mu.Unlock()
	if err != nil {
		return "", err
	}
	roots := f.deps.RootCertificates
	for _, path := range []string{"/etc/ssl/certs/ca-certificates.crt", "/etc/pki/tls/certs/ca-bundle.crt"} {
		if len(roots) > 0 {
			break
		}
		roots, err = os.ReadFile(path)
		if err == nil {
			break
		}
	}
	if err != nil {
		return "", fmt.Errorf("read worker trust roots: %w", err)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return "", err
	}
	path := filepath.Join(directory, "ca-certificates.crt")
	// Guest init can find its managed CA without parsing every system root.
	bundle := bytes.Join([][]byte{ca.pem, roots}, []byte("\n"))
	if err := os.WriteFile(path, bundle, 0644); err != nil {
		return "", err
	}
	return path, nil
}

func TrustEnvironment() []string {
	env := []string{"NODE_USE_SYSTEM_CA=1"}
	for _, key := range []string{
		"SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "NODE_EXTRA_CA_CERTS",
		"NPM_CONFIG_CAFILE", "PIP_CERT", "GIT_SSL_CAINFO", "AWS_CA_BUNDLE",
		"CARGO_HTTP_CAINFO", "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH",
	} {
		env = append(env, key+"="+TrustBundlePath)
	}
	return env
}
