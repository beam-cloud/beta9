package runtime

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/opencontainers/runtime-spec/specs-go"
	"k8s.io/utils/cpuset"
)

type microVMTopology struct {
	sysfsRoot  string
	statusPath string
}

// placement binds fresh guest RAM before VFIO pins it; unknown PCI locality
// keeps the existing host policy. CPU-only guests never read host topology.
func (h microVMTopology) placement(gpu string, spec *specs.Spec, cgroup string) (*microVMPlacement, error) {
	if gpu == "" {
		return nil, nil
	}
	data, err := os.ReadFile(filepath.Join(gpu, "numa_node"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	node, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || node < -1 {
		return nil, fmt.Errorf("invalid GPU numa_node %q", strings.TrimSpace(string(data)))
	}
	if node == -1 {
		return nil, nil
	}
	if h.sysfsRoot == "" {
		h.sysfsRoot = "/sys"
	}
	if h.statusPath == "" {
		h.statusPath = "/proc/self/status"
	}
	status, err := os.ReadFile(h.statusPath)
	if err != nil {
		return nil, err
	}
	process := make(map[string]string)
	for _, line := range strings.Split(string(status), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok {
			process[key] = strings.TrimSpace(value)
		}
	}
	if process["Cpus_allowed_list"] == "" || process["Mems_allowed_list"] == "" {
		return nil, fmt.Errorf("process CPU or memory affinity is missing from %s", h.statusPath)
	}
	cpuLists := []string{process["Cpus_allowed_list"]}
	memLists := []string{process["Mems_allowed_list"]}
	for _, mask := range []struct {
		path     string
		optional bool
		lists    *[]string
	}{
		{filepath.Join(h.sysfsRoot, "devices/system/node", fmt.Sprintf("node%d", node), "cpulist"), false, &cpuLists},
		{filepath.Join(h.sysfsRoot, "devices/system/cpu/online"), false, &cpuLists},
		{filepath.Join(h.sysfsRoot, "devices/system/node/has_memory"), false, &memLists},
		{filepath.Join(cgroup, "cpuset.cpus.effective"), true, &cpuLists},
		{filepath.Join(cgroup, "cpuset.mems.effective"), true, &memLists},
	} {
		if mask.optional && cgroup == "" {
			continue
		}
		data, err := os.ReadFile(mask.path)
		if mask.optional && os.IsNotExist(err) {
			continue // The parent need not enable the cpuset controller.
		}
		if err != nil {
			return nil, err
		}
		*mask.lists = append(*mask.lists, strings.TrimSpace(string(data)))
	}
	if spec != nil && spec.Linux != nil && spec.Linux.Resources != nil && spec.Linux.Resources.CPU != nil {
		cpu := spec.Linux.Resources.CPU
		if cpu.Cpus != "" {
			cpuLists = append(cpuLists, cpu.Cpus)
		}
		if cpu.Mems != "" {
			memLists = append(memLists, cpu.Mems)
		}
	}
	cpus, err := intersectCPULists(cpuLists)
	if err != nil {
		return nil, fmt.Errorf("GPU NUMA node %d CPU affinity: %w", node, err)
	}
	if cpus.IsEmpty() {
		return nil, fmt.Errorf("GPU NUMA node %d has no permitted online CPUs", node)
	}
	mems, err := intersectCPULists(memLists)
	if err != nil {
		return nil, fmt.Errorf("GPU NUMA node %d memory affinity: %w", node, err)
	}
	if !mems.Contains(node) {
		return nil, fmt.Errorf("GPU NUMA node %d is not a permitted memory node", node)
	}
	return &microVMPlacement{node: node, cpus: cpus.List()}, nil
}

func intersectCPULists(lists []string) (cpuset.CPUSet, error) {
	var allowed cpuset.CPUSet
	for i, value := range lists {
		set, err := cpuset.Parse(value)
		if err != nil {
			return cpuset.CPUSet{}, fmt.Errorf("invalid CPU/node list %q: %w", value, err)
		}
		if i == 0 {
			allowed = set
		} else {
			allowed = allowed.Intersection(set)
		}
	}
	return allowed, nil
}
