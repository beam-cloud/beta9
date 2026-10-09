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
	assertPrivateFile(t, processFile)
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
	assertPrivateFile(t, managerPath)
}

func assertPrivateFile(t *testing.T, path string) {
	t.Helper()
	stat, err := os.Stat(path)
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatalf("%s must be private: %v, %v", path, stat, err)
	}
}

func TestSystemdRejectsInvalidMachineIdentity(t *testing.T) {
	err := writeSystemdBootFiles(t.TempDir(), &specs.Process{Env: []string{"BEAM_VM_ID=not-a-uuid"}})
	if err == nil || !strings.Contains(err.Error(), "invalid VM machine ID") {
		t.Fatalf("got %v", err)
	}
}

func TestSystemdManagedServicesFollowVMFeatures(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		root := t.TempDir()
		if err := os.Mkdir(filepath.Join(root, "etc"), 0755); err != nil {
			t.Fatal(err)
		}
		env := []string{"BEAM_VM_ID=676139cd-f92b-4688-bb36-a0e763ef445c"}
		if enabled {
			env = append(env, "BEAM_VM_SSH=true", "BEAM_VM_DESKTOP=true")
		}
		if err := writeSystemdBootFiles(root, &specs.Process{Env: env}); err != nil {
			t.Fatal(err)
		}
		wants := filepath.Join(root, "run/systemd/system/multi-user.target.wants")
		if _, err := os.Readlink(filepath.Join(wants, "beam-terminal.service")); err != nil {
			t.Fatal(err)
		}
		for _, unit := range []string{"ssh.service", "beam-desktop.service"} {
			_, err := os.Readlink(filepath.Join(wants, unit))
			if enabled && err != nil || !enabled && !os.IsNotExist(err) {
				t.Fatalf("%s enablement=%t: %v", unit, enabled, err)
			}
		}
	}
}

func TestGuestAgentStopsAfterOrdinaryServices(t *testing.T) {
	if !strings.Contains(guestAgentUnit, "Before=sysinit.target shutdown.target") || !strings.Contains(guestAgentUnit, "After=local-fs.target systemd-journald.service") || !strings.Contains(guestAgentUnit, "DefaultDependencies=no") {
		t.Fatal("guest agent must outlive services and stop before local filesystems")
	}
	if !strings.Contains(guestAgentUnit, "TimeoutStopSec=infinity") || !strings.Contains(guestAgentUnit, "OnFailure=poweroff.target") {
		t.Fatal("host owns shutdown deadline, and agent failures must power off the guest")
	}
}

func TestVMIdentityPreservesColdBootKeysAndRotatesForkKeys(t *testing.T) {
	root := t.TempDir()
	sshDir := filepath.Join(root, "etc/ssh")
	if err := os.MkdirAll(sshDir, 0755); err != nil {
		t.Fatal(err)
	}
	parent := &specs.Process{Env: []string{"BEAM_VM_ID=676139cd-f92b-4688-bb36-a0e763ef445c"}}
	if err := prepareVMIdentity(root, parent); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ssh_host_ed25519_key", "ssh_host_ed25519_key.pub", "authorized_keys"} {
		if err := os.WriteFile(filepath.Join(sshDir, name), []byte("existing"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := prepareVMIdentity(root, parent); err != nil {
		t.Fatal(err)
	}
	key := filepath.Join(sshDir, "ssh_host_ed25519_key")
	if data, err := os.ReadFile(key); err != nil || string(data) != "existing" {
		t.Fatalf("cold boot changed host key: %q, %v", data, err)
	}
	fork := &specs.Process{Env: []string{"BEAM_VM_ID=3e975de0-fef8-47d2-9530-a0f563eb6132"}}
	if err := prepareVMIdentity(root, fork); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ssh_host_ed25519_key", "ssh_host_ed25519_key.pub"} {
		if _, err := os.Stat(filepath.Join(sshDir, name)); !os.IsNotExist(err) {
			t.Fatalf("fork retained %s: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(sshDir, "authorized_keys")); err != nil {
		t.Fatalf("changed an unrelated user file: %v", err)
	}
}

func TestSSHPreparationIsOrderedBeforeSSHAndIndependentOfExec(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "etc"), 0755); err != nil {
		t.Fatal(err)
	}
	process := &specs.Process{Args: []string{"/usr/bin/goproc"}, Env: []string{
		"BEAM_VM_ID=676139cd-f92b-4688-bb36-a0e763ef445c", "BEAM_VM_SSH=true", "BEAM_VM_SSH_PUBLIC_KEY=ssh-ed25519 test",
	}}
	if err := writeSystemdBootFiles(root, process); err != nil {
		t.Fatal(err)
	}
	keysPath := filepath.Join(root, "run/beam-vm/authorized_keys")
	assertPrivateFile(t, keysPath)
	if data, err := os.ReadFile(keysPath); err != nil || string(data) != "ssh-ed25519 test\n" {
		t.Fatalf("authorized key: %q, %v", data, err)
	}
	dropin, err := os.ReadFile(filepath.Join(root, "run/systemd/system/ssh.service.d/90-beam-keys.conf"))
	if err != nil || !strings.Contains(string(dropin), "Requires=beam-ssh-keys.service\nAfter=beam-ssh-keys.service") || strings.Contains(string(dropin), "ExecStartPre=") {
		t.Fatalf("keys must precede SSH's existing pre-start hooks: %s, %v", dropin, err)
	}
	if _, err := os.Stat(filepath.Join(root, "etc/ssh/ssh_host_ed25519_key")); !os.IsNotExist(err) {
		t.Fatalf("exec initialization generated keys synchronously: %v", err)
	}
	if process.Cwd != "/workspace" || processEnv(process, "HOME") != "/root" || len(process.Args) != 1 || process.Args[0] != "/usr/bin/goproc" {
		t.Fatalf("unexpected workload: %+v", process)
	}
	if strings.Contains(guestAgentUnit, "beam-ssh-keys") {
		t.Fatal("exec agent must not depend on SSH readiness")
	}
}
