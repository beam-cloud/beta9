//go:build linux

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestSystemdBootConfiguration(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "etc"), 0755); err != nil {
		t.Fatal(err)
	}
	p := &specs.Process{Args: []string{"/opt/beam-vm/boot", "/.beam/goproc"}, Env: []string{"BEAM_VM_ID=bad-image-default", "BEAM_VM_ID=676139cd-f92b-4688-bb36-a0e763ef445c", "TOKEN=spaces \"quotes\" \\ and %m\nsecond line"}}
	if err := writeSystemdBootFiles(root, p); err != nil {
		t.Fatal(err)
	}
	id, err := os.ReadFile(filepath.Join(root, "etc/machine-id"))
	if err != nil || string(id) != "676139cdf92b4688bb36a0e763ef445c\n" {
		t.Fatalf("machine identity: %q, %v", id, err)
	}
	processFile := filepath.Join(root, systemdProcessFile)
	stat, err := os.Stat(processFile)
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatalf("process secrets permissions: %v, %v", stat, err)
	}
	data, err := os.ReadFile(processFile)
	if err != nil {
		t.Fatal(err)
	}
	var restored specs.Process
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if strings.Join(restored.Env, "|") != strings.Join(p.Env, "|") {
		t.Fatal("launch environment lost")
	}
	unitDir := filepath.Join(root, "run/systemd/system")
	link, err := os.Readlink(filepath.Join(unitDir, "sysinit.target.wants/beam-guest.service"))
	if err != nil || link != "../beam-guest.service" {
		t.Fatalf("guest service not enabled: %q, %v", link, err)
	}
	for _, name := range []string{"systemd-networkd.service", "systemd-resolved.service", "NetworkManager.service"} {
		link, err := os.Readlink(filepath.Join(unitDir, name))
		if err != nil || link != "/dev/null" {
			t.Fatalf("network manager not masked: %s %q %v", name, link, err)
		}
	}
	managerPath := filepath.Join(root, "run/systemd/system.conf.d/90-beam-vm.conf")
	manager, err := os.ReadFile(managerPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(manager), `"TOKEN=spaces \"quotes\" \\ and %%m\nsecond line"`) {
		t.Fatalf("unsafe systemd environment: %s", manager)
	}
	stat, err = os.Stat(managerPath)
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatal("manager environment must be private")
	}
}

func TestSystemdRejectsInvalidMachineIdentity(t *testing.T) {
	err := writeSystemdBootFiles(t.TempDir(), &specs.Process{Env: []string{"BEAM_VM_ID=not-a-uuid"}})
	if err == nil || !strings.Contains(err.Error(), "invalid VM machine ID") {
		t.Fatalf("got %v", err)
	}
}

func TestGuestAgentStopsAfterOrdinaryServices(t *testing.T) {
	if !strings.Contains(guestAgentUnit, "Before=sysinit.target shutdown.target") || !strings.Contains(guestAgentUnit, "After=local-fs.target systemd-journald.service") || !strings.Contains(guestAgentUnit, "DefaultDependencies=no") {
		t.Fatal("guest agent must outlive services and stop before local filesystems")
	}
}
