//go:build linux

package runtime

import (
	"os"
	"path/filepath"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

type numaTestFixture struct {
	topology    microVMTopology
	gpu, cgroup string
}

func newNUMATestFixture(t *testing.T) numaTestFixture {
	t.Helper()
	root := t.TempDir()
	f := numaTestFixture{
		topology: microVMTopology{sysfsRoot: filepath.Join(root, "sys"), statusPath: filepath.Join(root, "status")},
		gpu:      filepath.Join(root, "pci", "0000:41:00.0"),
		cgroup:   filepath.Join(root, "cgroup"),
	}
	for name, value := range map[string]string{
		filepath.Join(f.gpu, "numa_node"):                                        "1\n",
		filepath.Join(f.topology.sysfsRoot, "devices/system/node/node0/cpulist"): "0-3\n",
		filepath.Join(f.topology.sysfsRoot, "devices/system/node/node1/cpulist"): "4-7\n",
		filepath.Join(f.topology.sysfsRoot, "devices/system/node/has_memory"):    "0-1\n",
		filepath.Join(f.topology.sysfsRoot, "devices/system/cpu/online"):         "0-7\n",
		f.topology.statusPath:                            "Name:\tbeam-worker\nCpus_allowed_list:\t0-7\nMems_allowed_list:\t0-1\n",
		filepath.Join(f.cgroup, "cpuset.cpus.effective"): "0-7\n",
		filepath.Join(f.cgroup, "cpuset.mems.effective"): "0-1\n",
	} {
		require.NoError(t, os.MkdirAll(filepath.Dir(name), 0o700))
		require.NoError(t, os.WriteFile(name, []byte(value), 0o600))
	}
	return f
}

func TestMicroVMTopologyPlacement(t *testing.T) {
	for _, node := range []string{"0", "1"} {
		t.Run("node "+node, func(t *testing.T) {
			f := newNUMATestFixture(t)
			require.NoError(t, os.WriteFile(filepath.Join(f.gpu, "numa_node"), []byte(node+"\n"), 0o600))
			placement, err := f.topology.placement(f.gpu, nil, f.cgroup)
			require.NoError(t, err)
			want := &microVMPlacement{node: 0, cpus: []int{0, 1, 2, 3}}
			if node == "1" {
				want = &microVMPlacement{node: 1, cpus: []int{4, 5, 6, 7}}
			}
			require.Equal(t, want, placement)
		})
	}
}

func TestMicroVMTopologyIntersectsCPURestrictions(t *testing.T) {
	f := newNUMATestFixture(t)
	for name, value := range map[string]string{
		filepath.Join(f.topology.sysfsRoot, "devices/system/node/node1/cpulist"): "4-15\n",
		filepath.Join(f.topology.sysfsRoot, "devices/system/cpu/online"):         "0-11,14\n",
		f.topology.statusPath:                            "Cpus_allowed_list:\t0-9,11-14\nMems_allowed_list:\t0-3\n",
		filepath.Join(f.cgroup, "cpuset.cpus.effective"): "0,6-7,10-12,14\n",
		filepath.Join(f.cgroup, "cpuset.mems.effective"): "1,3\n",
	} {
		require.NoError(t, os.WriteFile(name, []byte(value), 0o600))
	}
	spec := &specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{
		CPU: &specs.LinuxCPU{Cpus: "1,5-8,11-15", Mems: "1-2"},
	}}}
	placement, err := f.topology.placement(f.gpu, spec, f.cgroup)
	require.NoError(t, err)
	require.Equal(t, &microVMPlacement{node: 1, cpus: []int{6, 7, 11, 14}}, placement)
}

func TestMicroVMTopologyKeepsSparseOnlineCPUs(t *testing.T) {
	f := newNUMATestFixture(t)
	for name, value := range map[string]string{
		filepath.Join(f.topology.sysfsRoot, "devices/system/node/node1/cpulist"): "2,4,8-10\n",
		filepath.Join(f.topology.sysfsRoot, "devices/system/cpu/online"):         "0-8,10\n",
		f.topology.statusPath:                            "Cpus_allowed_list:\t0-10\nMems_allowed_list:\t0-1\n",
		filepath.Join(f.cgroup, "cpuset.cpus.effective"): "0-10\n",
	} {
		require.NoError(t, os.WriteFile(name, []byte(value), 0o600))
	}
	placement, err := f.topology.placement(f.gpu, nil, f.cgroup)
	require.NoError(t, err)
	require.Equal(t, &microVMPlacement{node: 1, cpus: []int{2, 4, 8, 10}}, placement)
}

func TestMicroVMTopologyUnknownGPUDoesNotReadHostTopology(t *testing.T) {
	for _, node := range []string{"no GPU", "missing", "-1"} {
		t.Run(node, func(t *testing.T) {
			root := t.TempDir()
			host := microVMTopology{sysfsRoot: filepath.Join(root, "missing-sys"), statusPath: filepath.Join(root, "missing-status")}
			gpu := ""
			if node != "no GPU" {
				gpu = filepath.Join(root, "gpu")
				require.NoError(t, os.Mkdir(gpu, 0o700))
				if node == "-1" {
					require.NoError(t, os.WriteFile(filepath.Join(gpu, "numa_node"), []byte("-1\n"), 0o600))
				}
			}
			placement, err := host.placement(gpu, nil, filepath.Join(root, "missing-cgroup"))
			require.NoError(t, err)
			require.Nil(t, placement)
		})
	}
}

func TestMicroVMTopologyWithoutCpusetController(t *testing.T) {
	for _, cgroup := range []bool{false, true} {
		t.Run(map[bool]string{false: "no target cgroup", true: "controller disabled"}[cgroup], func(t *testing.T) {
			f := newNUMATestFixture(t)
			path := ""
			if cgroup {
				path = f.cgroup
				require.NoError(t, os.Remove(filepath.Join(path, "cpuset.cpus.effective")))
				require.NoError(t, os.Remove(filepath.Join(path, "cpuset.mems.effective")))
			}
			placement, err := f.topology.placement(f.gpu, &specs.Spec{}, path)
			require.NoError(t, err)
			require.Equal(t, &microVMPlacement{node: 1, cpus: []int{4, 5, 6, 7}}, placement)
		})
	}
}

func TestMicroVMVFIOLocalityUsesGPUFunction(t *testing.T) {
	host := vfioTestHost(t, map[string][]string{"77": {"0000:41:00.1", "0000:41:00.0"}})
	for device, node := range map[string]string{"0000:41:00.1": "0\n", "0000:41:00.0": "1\n"} {
		require.NoError(t, os.WriteFile(filepath.Join(host.pciRoot, device, "numa_node"), []byte(node), 0o600))
	}
	claim, err := host.acquire("")
	require.NoError(t, err)
	t.Cleanup(claim.close)
	require.Equal(t, filepath.Join(host.pciRoot, "0000:41:00.0"), claim.gpu)
	f := newNUMATestFixture(t)
	placement, err := f.topology.placement(claim.gpu, nil, f.cgroup)
	require.NoError(t, err)
	require.Equal(t, &microVMPlacement{node: 1, cpus: []int{4, 5, 6, 7}}, placement)
}

func TestMicroVMTopologyRejectsInvalidOrUnavailablePlacement(t *testing.T) {
	for _, test := range []struct {
		name, target, value string
		remove, directory   bool
		cpu                 *specs.LinuxCPU
	}{
		{name: "malformed GPU node", target: "gpu", value: "not-a-node\n"},
		{name: "invalid negative GPU node", target: "gpu", value: "-2\n"},
		{name: "empty GPU node", target: "gpu"},
		{name: "GPU node read failure", target: "gpu", directory: true},
		{name: "missing known node CPUs", target: "node", remove: true},
		{name: "malformed node CPUs", target: "node", value: "4-b\n"},
		{name: "empty local CPUs", target: "node"},
		{name: "node CPU read failure", target: "node", directory: true},
		{name: "malformed online CPUs", target: "online", value: "not-cpus\n"},
		{name: "all local CPUs offline", target: "online", value: "0-3\n"},
		{name: "missing process CPU mask", target: "status", value: "Mems_allowed_list:\t0-1\n"},
		{name: "malformed process CPU mask", target: "status", value: "Cpus_allowed_list:\tbad\nMems_allowed_list:\t0-1\n"},
		{name: "process excludes local CPUs", target: "status", value: "Cpus_allowed_list:\t0-3\nMems_allowed_list:\t0-1\n"},
		{name: "missing process memory mask", target: "status", value: "Cpus_allowed_list:\t0-7\n"},
		{name: "process excludes local memory", target: "status", value: "Cpus_allowed_list:\t0-7\nMems_allowed_list:\t0\n"},
		{name: "status read failure", target: "status", directory: true},
		{name: "malformed memory nodes", target: "memory", value: "not-nodes\n"},
		{name: "memoryless node", target: "memory", value: "0\n"},
		{name: "missing memory node metadata", target: "memory", remove: true},
		{name: "OCI excludes local CPUs", cpu: &specs.LinuxCPU{Cpus: "0-3"}},
		{name: "OCI excludes local memory", cpu: &specs.LinuxCPU{Mems: "0"}},
		{name: "malformed OCI CPUs", cpu: &specs.LinuxCPU{Cpus: "bad"}},
		{name: "malformed OCI memory", cpu: &specs.LinuxCPU{Mems: "bad"}},
		{name: "cgroup excludes local CPUs", target: "cgroup CPUs", value: "0-3\n"},
		{name: "cgroup excludes local memory", target: "cgroup memory", value: "0\n"},
		{name: "malformed cgroup CPUs", target: "cgroup CPUs", value: "bad\n"},
		{name: "malformed cgroup memory", target: "cgroup memory", value: "bad\n"},
		{name: "cgroup CPU read failure", target: "cgroup CPUs", directory: true},
		{name: "cgroup memory read failure", target: "cgroup memory", directory: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newNUMATestFixture(t)
			path := map[string]string{
				"gpu":           filepath.Join(f.gpu, "numa_node"),
				"node":          filepath.Join(f.topology.sysfsRoot, "devices/system/node/node1/cpulist"),
				"online":        filepath.Join(f.topology.sysfsRoot, "devices/system/cpu/online"),
				"memory":        filepath.Join(f.topology.sysfsRoot, "devices/system/node/has_memory"),
				"status":        f.topology.statusPath,
				"cgroup CPUs":   filepath.Join(f.cgroup, "cpuset.cpus.effective"),
				"cgroup memory": filepath.Join(f.cgroup, "cpuset.mems.effective"),
			}[test.target]
			if path != "" {
				if test.remove || test.directory {
					require.NoError(t, os.Remove(path))
					if test.directory {
						require.NoError(t, os.Mkdir(path, 0o700))
					}
				} else {
					require.NoError(t, os.WriteFile(path, []byte(test.value), 0o600))
				}
			}
			spec := &specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{CPU: test.cpu}}}
			placement, err := f.topology.placement(f.gpu, spec, f.cgroup)
			require.Error(t, err)
			require.Nil(t, placement, "invalid metadata must not silently select node zero or widen the allowed masks")
		})
	}
}
