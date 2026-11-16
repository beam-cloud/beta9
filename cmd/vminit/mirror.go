//go:build linux

package main

// What init does for a VM with a mirror. writeSystemdBootFiles calls
// prepareMirroredHome last, and a VM without a mirror is left as it is.
import (
	"os"
	"path/filepath"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// prepareMirroredHome tells logins that the VM has a mirror and where it is,
// makes the mirror the desktop user's home when the worker placed it where it
// was on its source machine, and lets that user log in over SSH.
func prepareMirroredHome(root string, process *specs.Process) error {
	if processEnv(process, "BEAM_VM_MIRRORED_HOME") != "true" {
		return nil
	}
	// Login shells use a boot-time flag even if SSH clears the environment.
	// It holds the mirror's path when the worker placed the tree where it
	// was on its source machine.
	home := mirrorHome(process)
	var flag []byte
	if home != "" {
		flag = []byte(home + "\n")
	}
	if err := os.WriteFile(filepath.Join(root, "run/beam-mirrored-home"), flag, 0644); err != nil {
		return err
	}
	if home != "" {
		if err := setDesktopHome(root, home); err != nil {
			return err
		}
	}
	// sshd reads authorized_keys, a public key, as the user logging in, who
	// must be able to reach it. Every other file beside it stays root's alone.
	keys := filepath.Join(root, "run/beam-vm/authorized_keys")
	if err := os.Chmod(filepath.Dir(keys), 0711); err != nil {
		return err
	}
	if err := os.Chmod(keys, 0644); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// mirrorHome is where the worker placed the mirror for this launch when it
// placed it at a home of its own. It goes into passwd, so it must be a clean
// absolute path without a field or line separator.
func mirrorHome(process *specs.Process) string {
	home := processEnv(process, "BEAM_VM_MIRROR_HOME")
	if !filepath.IsAbs(home) || filepath.Clean(home) != home || home == "/" || strings.ContainsAny(home, ":\n") {
		return ""
	}
	return home
}

// setDesktopHome points the desktop user's home at the mirror, which the
// worker gave that user. Root keeps /root, so that root's processes leave
// nothing of theirs in the user's home. It is checked on every boot: the
// root disk may come from a snapshot of a VM without the mirror.
func setDesktopHome(root, home string) error {
	named, err := os.ReadFile(filepath.Join(root, "opt/beam-vm/desktop-user"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	user := strings.TrimSpace(string(named))
	if user == "" || user == "root" {
		return nil
	}
	passwd := filepath.Join(root, "etc/passwd")
	data, err := os.ReadFile(passwd)
	if err != nil {
		return err
	}
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		fields := strings.Split(line, ":")
		if len(fields) != 7 || fields[0] != user {
			continue
		}
		if fields[5] == home {
			return nil
		}
		fields[5] = home
		lines[i] = strings.Join(fields, ":")
		return replaceFile(passwd, []byte(strings.Join(lines, "\n")))
	}
	return nil
}
