package agent

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestWriteConfigRejectsSymlink exercises the O_NOFOLLOW / Lstat guard in
// writeConfigAtomically. A pre-planted symlink at the target path must
// cause the write to fail rather than clobber whatever the symlink
// points at.
func TestWriteConfigRejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink-TOCTOU guard is POSIX-only")
	}

	tmp := t.TempDir()
	victim := filepath.Join(tmp, "victim.txt")
	if err := os.WriteFile(victim, []byte("do not overwrite"), 0600); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(tmp, "config.yaml")
	if err := os.Symlink(victim, target); err != nil {
		t.Fatal(err)
	}

	err := writeConfigAtomically(target, tmp, []byte("gateway:\n  host: x\n"))
	if err == nil {
		t.Fatal("expected symlink at target to be rejected, got nil error")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Errorf("expected error to mention symlink, got: %v", err)
	}

	// Victim content must be untouched.
	got, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "do not overwrite" {
		t.Errorf("victim was overwritten: %q", string(got))
	}
}

// TestWriteConfigRejectsParentDirNotOwned verifies that checkParentDirOwned
// refuses a parent directory owned by a different uid. We simulate this
// by feeding the helper a directory whose owner uid we know does not
// match Geteuid() — on POSIX the /root directory is typically owned by
// uid 0, so when the test runs as a non-root user the check should fire.
//
// If the test happens to run as root (uid 0 == owner of /root), we skip:
// we only want to exercise the negative path.
func TestWriteConfigRejectsParentDirNotOwned(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("owner check is POSIX-only")
	}
	if os.Geteuid() == 0 {
		t.Skip("running as root, cannot observe owner-mismatch path")
	}

	// Find a world-readable directory not owned by our uid. /root is not
	// always readable, but /etc and /usr are.
	candidates := []string{"/etc", "/usr", "/var"}
	var other string
	for _, c := range candidates {
		info, err := os.Stat(c)
		if err != nil || !info.IsDir() {
			continue
		}
		// Any of these on a stock Linux box is owned by root.
		other = c
		break
	}
	if other == "" {
		t.Skip("no non-owned directory available")
	}

	if err := checkParentDirOwned(other); err == nil {
		t.Fatalf("expected checkParentDirOwned(%q) to fail for non-owner, got nil", other)
	}
}

// TestWriteConfigHappyPath confirms the hardened write still produces a
// regular 0600 file with the expected bytes when nothing malicious is
// planted at the target.
func TestWriteConfigHappyPath(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "config.yaml")
	payload := []byte("gateway:\n  host: 100.1.2.3\n")

	if err := writeConfigAtomically(target, tmp, payload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Errorf("content mismatch: got %q, want %q", got, payload)
	}

	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if perm := info.Mode().Perm(); perm != 0600 {
			t.Errorf("expected 0600, got %o", perm)
		}
	}
}

// TestWriteConfigOverwritesRegularFile confirms that a previous config
// file (plain regular file, same owner) is cleanly replaced — this is
// the main happy-path on repeated `b9agent init` runs.
func TestWriteConfigOverwritesRegularFile(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "config.yaml")
	if err := os.WriteFile(target, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := writeConfigAtomically(target, tmp, []byte("new")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Errorf("expected 'new', got %q", got)
	}
}

// TestWriteConfigRejectsNonRegularFile guards the Lstat-branch that
// refuses to remove anything that is not a plain regular file (e.g. a
// device node or FIFO accidentally sitting at the path).
func TestWriteConfigRejectsNonRegularFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("FIFO semantics don't apply on Windows")
	}
	tmp := t.TempDir()
	target := filepath.Join(tmp, "fifo")
	// A FIFO is the easiest-to-create non-regular, non-symlink file in
	// user space. Fall back to skip if mkfifo isn't available.
	if err := mkfifoForTest(target); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}

	err := writeConfigAtomically(target, tmp, []byte("x"))
	if err == nil {
		t.Fatal("expected error on non-regular target")
	}
	if !strings.Contains(err.Error(), "regular") {
		t.Errorf("expected 'regular' in error, got %v", err)
	}
}
