//go:build linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func TestMirroredHomeFlagIsAvailableToInteractiveLogins(t *testing.T) {
	for _, mirrored := range []bool{false, true} {
		root := t.TempDir()
		if err := os.Mkdir(filepath.Join(root, "etc"), 0755); err != nil {
			t.Fatal(err)
		}
		process := &specs.Process{Env: []string{
			"BEAM_VM_ID=676139cd-f92b-4688-bb36-a0e763ef445c",
			fmt.Sprintf("BEAM_VM_MIRRORED_HOME=%t", mirrored),
		}}
		if err := writeSystemdBootFiles(root, process); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(filepath.Join(root, "run/beam-mirrored-home"))
		if mirrored {
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0644 {
				t.Fatalf("desktop users need read access, got %s", info.Mode())
			}
		} else if !os.IsNotExist(err) {
			t.Fatalf("plain VM should lack a mirrored-home marker, got %v", err)
		}
	}
}

// The mirror is the desktop user's, who logs in over SSH to work in it. sshd
// reads the key as the user logging in, and requires that nobody else can
// write it or its directory. A VM without a mirror keeps both to root.
func TestMirroredVMsAcceptSSHLoginsOfTheDesktopUser(t *testing.T) {
	for _, test := range []struct {
		mirrored  bool
		ssh       bool
		keys, dir os.FileMode
	}{
		{mirrored: false, ssh: true, keys: 0600, dir: 0700},
		{mirrored: true, ssh: true, keys: 0644, dir: 0711},
		{mirrored: true, ssh: false, dir: 0711},
	} {
		root := t.TempDir()
		if err := os.Mkdir(filepath.Join(root, "etc"), 0755); err != nil {
			t.Fatal(err)
		}
		process := &specs.Process{Env: []string{
			"BEAM_VM_ID=676139cd-f92b-4688-bb36-a0e763ef445c",
			fmt.Sprintf("BEAM_VM_MIRRORED_HOME=%t", test.mirrored),
			fmt.Sprintf("BEAM_VM_SSH=%t", test.ssh), "BEAM_VM_SSH_PUBLIC_KEY=ssh-ed25519 test",
		}}
		if err := writeSystemdBootFiles(root, process); err != nil {
			t.Fatal(err)
		}
		keys := filepath.Join(root, "run/beam-vm/authorized_keys")
		if info, err := os.Stat(filepath.Dir(keys)); err != nil || info.Mode().Perm() != test.dir {
			t.Fatalf("%+v: directory: %v, %v", test, info, err)
		}
		info, err := os.Stat(keys)
		if !test.ssh {
			if !os.IsNotExist(err) {
				t.Fatalf("%+v: a VM without SSH has a key: %v", test, err)
			}
			continue
		}
		if err != nil || info.Mode().Perm() != test.keys {
			t.Fatalf("%+v: key: %v, %v", test, info, err)
		}
		// The launch's environment beside the key is still root's alone.
		assertPrivateFile(t, filepath.Join(root, systemdProcessFile))
	}
}

// A mirror placed where it was on its source machine is the desktop user's
// home, by the flag and by passwd, on every boot. Root keeps /root.
func TestMirrorHomeIsTheDesktopUsersHome(t *testing.T) {
	const passwd = "root:x:0:0:root:/root:/bin/bash\nubuntu:x:1000:1000:Ubuntu:/home/ubuntu:/bin/bash\nbeam:x:1001:1002::/home/beam:/bin/bash\n"
	moved := strings.Replace(passwd, "::/home/beam:", "::/Users/luke:", 1)
	image := func(t *testing.T, desktopUser string) string {
		root := t.TempDir()
		for _, dir := range []string{"etc", "opt/beam-vm"} {
			if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(root, "etc/passwd"), []byte(passwd), 0644); err != nil {
			t.Fatal(err)
		}
		if desktopUser != "" {
			if err := os.WriteFile(filepath.Join(root, "opt/beam-vm/desktop-user"), []byte(desktopUser+"\n"), 0644); err != nil {
				t.Fatal(err)
			}
		}
		return root
	}
	boot := func(t *testing.T, root string, env ...string) {
		t.Helper()
		// /run is new on every boot.
		if err := os.RemoveAll(filepath.Join(root, "run")); err != nil {
			t.Fatal(err)
		}
		process := &specs.Process{Env: append([]string{"BEAM_VM_ID=676139cd-f92b-4688-bb36-a0e763ef445c"}, env...)}
		if err := writeSystemdBootFiles(root, process); err != nil {
			t.Fatal(err)
		}
	}
	expect := func(t *testing.T, root, flag, accounts string) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, "run/beam-mirrored-home"))
		if err != nil || string(data) != flag {
			t.Fatalf("flag: %q, %v; want %q", data, err, flag)
		}
		info, err := os.Stat(filepath.Join(root, "etc/passwd"))
		if err != nil || info.Mode().Perm() != 0644 {
			t.Fatalf("passwd mode: %v, %v", info, err)
		}
		if data, _ := os.ReadFile(filepath.Join(root, "etc/passwd")); string(data) != accounts {
			t.Fatalf("passwd:\n%s\nwant:\n%s", data, accounts)
		}
		if leftovers, _ := filepath.Glob(filepath.Join(root, "etc/.passwd-*")); len(leftovers) != 0 {
			t.Fatalf("left %v behind", leftovers)
		}
	}
	mirrored := []string{"BEAM_VM_MIRRORED_HOME=true", "BEAM_VM_MIRROR_HOME=/Users/luke"}

	root := image(t, "beam")
	boot(t, root, mirrored...)
	expect(t, root, "/Users/luke\n", moved)
	boot(t, root, mirrored...)
	expect(t, root, "/Users/luke\n", moved)

	// A launch without the layer has no path to point at; a plain VM
	// ignores the variable, as it would one an image set.
	root = image(t, "beam")
	boot(t, root, "BEAM_VM_MIRRORED_HOME=true")
	expect(t, root, "", passwd)
	boot(t, root, "BEAM_VM_MIRRORED_HOME=false", "BEAM_VM_MIRROR_HOME=/Users/luke")
	if _, err := os.Stat(filepath.Join(root, "run/beam-mirrored-home")); !os.IsNotExist(err) {
		t.Fatalf("plain VM has a mirrored-home flag: %v", err)
	}
	if data, _ := os.ReadFile(filepath.Join(root, "etc/passwd")); string(data) != passwd {
		t.Fatalf("plain VM changed passwd:\n%s", data)
	}

	// Nothing that could break passwd is written into it.
	for _, home := range []string{"relative/home", "/Users/luke/../root", "/", "/Users/a:b", "/Users/a\nroot:x:0:0::/:/bin/sh"} {
		root = image(t, "beam")
		boot(t, root, "BEAM_VM_MIRRORED_HOME=true", "BEAM_VM_MIRROR_HOME="+home)
		expect(t, root, "", passwd)
	}

	// Without a desktop user, or with one that is root, only the flag says
	// where the mirror is.
	for _, user := range []string{"", "root", "kiosk"} {
		root = image(t, user)
		boot(t, root, mirrored...)
		expect(t, root, "/Users/luke\n", passwd)
	}
}
