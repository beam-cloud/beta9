//go:build linux

package main

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

func installEgressCA() error {
	data, err := os.ReadFile("/etc/beam/ca-certificates.crt")
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for len(data) > 0 {
		block, rest := pem.Decode(data)
		if block == nil {
			break
		}
		data = rest
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return err
		}
		if !cert.IsCA || cert.Subject.CommonName != "Beam container egress CA" {
			continue
		}
		path := "/usr/local/share/ca-certificates/beam-egress.crt"
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0644); err != nil {
			return err
		}
		command := exec.Command("/usr/sbin/update-ca-certificates")
		command.Env = append(os.Environ(), "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		if err := command.Run(); err != nil {
			return fmt.Errorf("install egress CA: %w", err)
		}
		return nil
	}
	return nil
}
