//go:build linux

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestKioskCannotFallBackToLegacyDesktop(t *testing.T) {
	root := t.TempDir()
	process := &specs.Process{Env: []string{
		"BEAM_VM_ID=676139cd-f92b-4688-bb36-a0e763ef445c", "BEAM_VM_DESKTOP=true", "BEAM_VM_KIOSK=e30=",
	}}
	if err := writeSystemdBootFiles(root, process); err == nil || !strings.Contains(err.Error(), "kiosk streaming backend") {
		t.Fatalf("old image must fail explicitly, got %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "opt/beam-vm"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "etc"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "opt/beam-vm/kiosk-backend"), []byte("selkies-2.1.0\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := writeSystemdBootFiles(root, process); err != nil {
		t.Fatal(err)
	}
	unit, err := os.Readlink(filepath.Join(root, "run/systemd/system/multi-user.target.wants/beam-desktop.service"))
	if err != nil || unit != "/etc/systemd/system/beam-desktop.service" {
		t.Fatalf("missing supervised application display: %q, %v", unit, err)
	}
}
