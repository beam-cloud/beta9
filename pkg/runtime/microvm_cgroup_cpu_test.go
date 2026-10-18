package runtime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestMicroVMApplyCPU(t *testing.T) {
	quota, period := int64(200000), uint64(100000)
	t.Run("empty resources need no controllers", func(t *testing.T) {
		path := t.TempDir()
		require.NoError(t, applyMicroVMCPU(path, nil))
		require.NoError(t, applyMicroVMCPU(path, &specs.LinuxCPU{}))
		entries, err := os.ReadDir(path)
		require.NoError(t, err)
		require.Empty(t, entries)
	})
	t.Run("affinity only preserves quota and memory nodes", func(t *testing.T) {
		path := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(path, "cpuset.cpus"), []byte("\n"), 0600))
		require.NoError(t, os.WriteFile(filepath.Join(path, "cpu.max"), []byte("max 100000\n"), 0600))
		require.NoError(t, os.WriteFile(filepath.Join(path, "cpuset.mems"), []byte("0-2\n"), 0600))
		require.NoError(t, applyMicroVMCPU(path, &specs.LinuxCPU{Cpus: "2-3"}))
		for name, want := range map[string]string{"cpuset.cpus": "2-3", "cpu.max": "max 100000\n", "cpuset.mems": "0-2\n"} {
			got, err := os.ReadFile(filepath.Join(path, name))
			require.NoError(t, err)
			require.Equal(t, want, string(got), name)
		}
	})
	t.Run("quota without affinity needs no cpuset", func(t *testing.T) {
		path := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(path, "cpu.max"), []byte("max 100000\n"), 0600))
		require.NoError(t, applyMicroVMCPU(path, &specs.LinuxCPU{Quota: &quota, Period: &period}))
		got, err := os.ReadFile(filepath.Join(path, "cpu.max"))
		require.NoError(t, err)
		require.Equal(t, "200000 100000", string(got))
		require.NoFileExists(t, filepath.Join(path, "cpuset.cpus"))
	})
	t.Run("affinity and quota both apply", func(t *testing.T) {
		path := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(path, "cpuset.cpus"), []byte("\n"), 0600))
		require.NoError(t, os.WriteFile(filepath.Join(path, "cpu.max"), []byte("max 100000\n"), 0600))
		require.NoError(t, applyMicroVMCPU(path, &specs.LinuxCPU{Cpus: "4-5", Quota: &quota, Period: &period}))
		for name, want := range map[string]string{"cpuset.cpus": "4-5", "cpu.max": "200000 100000"} {
			got, err := os.ReadFile(filepath.Join(path, name))
			require.NoError(t, err)
			require.Equal(t, want, string(got))
		}
	})
	t.Run("missing cpuset fails before changing quota", func(t *testing.T) {
		path := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(path, "cpu.max"), []byte("max 100000\n"), 0600))
		require.ErrorContains(t, applyMicroVMCPU(path, &specs.LinuxCPU{Cpus: "4-5", Quota: &quota}), "enable cpuset in the parent")
		got, err := os.ReadFile(filepath.Join(path, "cpu.max"))
		require.NoError(t, err)
		require.Equal(t, "max 100000\n", string(got))
		require.NoFileExists(t, filepath.Join(path, "cpuset.cpus"))
	})
	for _, oldCPUs := range []string{"\n", "0-191\n"} {
		t.Run("failed quota restores affinity "+oldCPUs, func(t *testing.T) {
			path := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(path, "cpuset.cpus"), []byte(oldCPUs), 0600))
			require.ErrorContains(t, applyMicroVMCPU(path, &specs.LinuxCPU{Cpus: "4-5", Quota: &quota}), "set cpu.max")
			got, err := os.ReadFile(filepath.Join(path, "cpuset.cpus"))
			require.NoError(t, err)
			require.Equal(t, oldCPUs, string(got))
			require.NoFileExists(t, filepath.Join(path, "cpu.max"), "missing controllers must never become regular files")
		})
	}
}
