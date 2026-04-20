package agent

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// TestResolveBinariesCachesResults ensures that the sync.Once gate works:
// the cache should not be re-evaluated after the first call, even if
// the underlying PATH changes.
func TestResolveBinariesCachesResults(t *testing.T) {
	first := resolveBinariesForTest()

	// Mutate PATH. If resolveBinaries is cached, a second call should
	// return the *same* paths, not paths from the new PATH.
	origPath := os.Getenv("PATH")
	t.Cleanup(func() { os.Setenv("PATH", origPath) })
	os.Setenv("PATH", "/definitely/not/a/real/path")

	second := resolveBinaries() // NOT ForTest — must hit the cache.
	if first != second {
		t.Errorf("expected cache hit, got first=%+v second=%+v", first, second)
	}
}

// TestResolveBinariesMissingBinReturnsEmpty ensures that a binary not
// on PATH resolves to empty string (rather than a panic or an absolute
// path that doesn't exist). This underpins the graceful-degradation
// contract at the call sites.
func TestResolveBinariesMissingBinReturnsEmpty(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH manipulation test is POSIX-focused")
	}

	// Point PATH at an empty directory so none of the real binaries
	// resolve. Use t.TempDir() so we get automatic cleanup.
	emptyDir := t.TempDir()
	origPath := os.Getenv("PATH")
	t.Cleanup(func() { os.Setenv("PATH", origPath) })
	os.Setenv("PATH", emptyDir)

	got := resolveBinariesForTest()
	if got.kubectl != "" {
		t.Errorf("expected empty kubectl path, got %q", got.kubectl)
	}
	if got.nvidiaSMI != "" {
		t.Errorf("expected empty nvidia-smi path, got %q", got.nvidiaSMI)
	}
	if got.tailscale != "" {
		t.Errorf("expected empty tailscale path, got %q", got.tailscale)
	}
}

// TestResolveBinariesFindsRealBin places a fake "kubectl" script in a
// known directory and verifies it is resolved to an absolute path — so
// we know the LookPath plumbing is actually exercised, not just the
// failure branch.
func TestResolveBinariesFindsRealBin(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("relies on POSIX exec bit semantics")
	}

	binDir := t.TempDir()
	fake := filepath.Join(binDir, "kubectl")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\necho ok\n"), 0755); err != nil {
		t.Fatal(err)
	}

	origPath := os.Getenv("PATH")
	t.Cleanup(func() { os.Setenv("PATH", origPath) })
	os.Setenv("PATH", binDir)

	got := resolveBinariesForTest()
	if got.kubectl != fake {
		t.Errorf("expected kubectl to resolve to %s, got %q", fake, got.kubectl)
	}

	// Sanity: exec.LookPath agrees.
	if p, err := exec.LookPath("kubectl"); err != nil || p != fake {
		t.Errorf("LookPath returned %q (err=%v), want %q", p, err, fake)
	}
}

// TestKubectlPathAccessor confirms the public helper returns whatever
// the cache holds. Trivial but documents the contract.
func TestKubectlPathAccessor(t *testing.T) {
	_ = resolveBinariesForTest()
	if KubectlPath() != binaryCache.kubectl {
		t.Errorf("accessor drift: KubectlPath()=%q cache=%q", KubectlPath(), binaryCache.kubectl)
	}
	if NvidiaSMIPath() != binaryCache.nvidiaSMI {
		t.Errorf("accessor drift: NvidiaSMIPath()=%q cache=%q", NvidiaSMIPath(), binaryCache.nvidiaSMI)
	}
	if TailscalePath() != binaryCache.tailscale {
		t.Errorf("accessor drift: TailscalePath()=%q cache=%q", TailscalePath(), binaryCache.tailscale)
	}
}
