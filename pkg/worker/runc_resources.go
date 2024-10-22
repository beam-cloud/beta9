package worker

import (
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"k8s.io/utils/ptr"
)

const (
	standardCPUShare  uint64 = 1024
	standardCPUPeriod int64  = 100_000
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
		Quota:  ptr.To(calculateCPUQuota(request.Cpu)),
		Period: ptr.To(uint64(standardCPUPeriod)),
	}
}

func getLinuxMemory(request *types.ContainerRequest) *specs.LinuxMemory {
	return &specs.LinuxMemory{
		Limit: ptr.To(request.Memory * 1024 * 1024),
	}
}
