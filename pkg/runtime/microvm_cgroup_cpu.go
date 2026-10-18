package runtime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/opencontainers/runtime-spec/specs-go"
)

// applyMicroVMCPU applies the reserved host CPUs before the hypervisor allocates
// guest RAM. Missing cpuset delegation is an error, not an unpinned fallback.
func applyMicroVMCPU(cgroup string, cpu *specs.LinuxCPU) error {
	if cpu == nil {
		return nil
	}
	cpus := strings.TrimSpace(cpu.Cpus)
	cpusetPath := filepath.Join(cgroup, "cpuset.cpus")
	var previous []byte
	if cpus != "" {
		var err error
		previous, err = os.ReadFile(cpusetPath)
		if err != nil {
			return fmt.Errorf("read cpuset.cpus (enable cpuset in the parent cgroup): %w", err)
		}
		if err := writeMicroVMCgroupFile(cpusetPath, []byte(cpus)); err != nil {
			return fmt.Errorf("set cpuset.cpus: %w", err)
		}
	}
	if max := cpuMaxString(cpu); max != "" {
		if err := writeMicroVMCgroupFile(filepath.Join(cgroup, "cpu.max"), []byte(max)); err != nil {
			err = fmt.Errorf("set cpu.max: %w", err)
			if cpus != "" {
				if rollback := writeMicroVMCgroupFile(cpusetPath, previous); rollback != nil {
					err = errors.Join(err, fmt.Errorf("restore cpuset.cpus: %w", rollback))
				}
			}
			return err
		}
	}
	return nil
}

// Never create a regular file when a cgroup controller has not been delegated.
func writeMicroVMCgroupFile(path string, value []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	_, err = f.Write(value)
	return errors.Join(err, f.Close())
}
