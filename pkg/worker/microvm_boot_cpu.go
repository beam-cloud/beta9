package worker

import (
	"os"
	"strconv"
)

// Native boots defer quota until authenticated guest readiness. Their default
// cgroup weight can still overwhelm the worker when a hundred guests run at
// once. This opt-in preserves an explicit OCI share budget during cold boot;
// the tenant's normal resources are applied at the existing readiness fence.
func microVMBootCPUShares() *uint64 {
	shares, err := strconv.ParseUint(os.Getenv("BETA9_MICROVM_BOOT_CPU_SHARES"), 10, 64)
	if err != nil || shares < 2 || shares > 262144 {
		return nil
	}
	return &shares
}
