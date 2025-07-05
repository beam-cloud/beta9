package worker

import (
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"k8s.io/utils/ptr"
)

const (
	standardCPUShare     uint64  = 1024
	standardCPUPeriod    uint64  = 100_000 // 100ms
	memoryOverheadFactor float64 = 1.25
)

func calculateCPUShares(millicores int64) uint64 {
	return uint64(millicores) * standardCPUShare / 1000
}

func calculateCPUQuota(millicores int64) int64 {
	return millicores * int64(standardCPUPeriod) / 1000
}

func getLinuxCPU(request *types.ContainerRequest) *specs.LinuxCPU {
	return &specs.LinuxCPU{
		Shares: ptr.To(calculateCPUShares(request.Cpu)),
		Quota:  ptr.To(calculateCPUQuota(request.Cpu)),
		Period: ptr.To(standardCPUPeriod),
	}
}

func getLinuxMemory(request *types.ContainerRequest) *specs.LinuxMemory {
	softLimit := request.Memory * 1024 * 1024
	hardLimit := int64(float64(softLimit) * memoryOverheadFactor)
	return &specs.LinuxMemory{
		Reservation: ptr.To(softLimit), // soft limit
		Limit:       ptr.To(hardLimit), // hard limit
		Swap:        ptr.To(hardLimit), // total memory (no swap added)
	}
}
