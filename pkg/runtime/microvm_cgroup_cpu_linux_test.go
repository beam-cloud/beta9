//go:build linux

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestMicroVMUpdateResourcesAppliesAffinityWithoutQuota(t *testing.T) {
	path := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(path, "cpuset.cpus"), []byte("\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(path, "cpu.max"), []byte("max 100000\n"), 0600))
	rt := &MicroVM{vms: map[string]*microVMInstance{"cpu-test": {id: "cpu-test", cgroup: path}}}
	require.NoError(t, rt.UpdateResources(context.Background(), "cpu-test", &specs.LinuxResources{CPU: &specs.LinuxCPU{Cpus: "2-3"}}))
	got, err := os.ReadFile(filepath.Join(path, "cpuset.cpus"))
	require.NoError(t, err)
	require.Equal(t, "2-3", string(got))
	got, err = os.ReadFile(filepath.Join(path, "cpu.max"))
	require.NoError(t, err)
	require.Equal(t, "max 100000\n", string(got))
}
