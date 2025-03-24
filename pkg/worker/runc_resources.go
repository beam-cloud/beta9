package worker

import (
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"k8s.io/utils/ptr"
)

const (
	standardCPUShare     uint64  = 1024
	standardCPUPeriod    int64   = 100_000
	memoryOverheadFactor float64 = 1.25
)

func calculateCPUShares(millicores int64) uint64 {
	shares := uint64(millicores) * standardCPUShare / 1000
	return shares
}

func calculateCPUQuota(millicores int64) int64 {
	quota := millicores * standardCPUPeriod / 1000
	return quota
}

func getLinuxCPU(request *types.ContainerRequest) *specs.LinuxCPU {
	return &specs.LinuxCPU{
		Shares: ptr.To(calculateCPUShares(request.Cpu)),
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
