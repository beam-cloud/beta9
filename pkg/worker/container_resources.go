package worker

import (
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"k8s.io/utils/ptr"
)

type ContainerResources interface {
	GetCPU(request *types.ContainerRequest) *specs.LinuxCPU
	GetMemory(request *types.ContainerRequest) *specs.LinuxMemory
}

type StandardResources struct {
	standardCPUShare     uint64
	standardCPUPeriod    uint64
	memoryOverheadFactor float64
}

func NewStandardResources() *StandardResources {
	return &StandardResources{
		standardCPUShare:     1024,
		standardCPUPeriod:    100_000,
		memoryOverheadFactor: 1.25,
	}
}

func (r *StandardResources) calculateCPUShares(millicores int64) uint64 {
	shares := uint64(millicores) * r.standardCPUShare / 1000
	return shares
}

func (r *StandardResources) calculateCPUQuota(millicores int64) int64 {
	quota := int64(uint64(millicores) * r.standardCPUPeriod / 1000)
	return quota
}

func (r *StandardResources) GetCPU(request *types.ContainerRequest) *specs.LinuxCPU {
	return &specs.LinuxCPU{
		Shares: ptr.To(r.calculateCPUShares(request.Cpu)),
		Quota:  ptr.To(r.calculateCPUQuota(request.Cpu)),
		Period: ptr.To(r.standardCPUPeriod),
	}
}

func (r *StandardResources) GetMemory(request *types.ContainerRequest) *specs.LinuxMemory {
	softLimit := request.Memory * 1024 * 1024
	hardLimit := int64(float64(softLimit) * r.memoryOverheadFactor)
	return &specs.LinuxMemory{
		Reservation: ptr.To(softLimit), // soft limit
		Limit:       ptr.To(hardLimit), // hard limit
		Swap:        ptr.To(hardLimit), // total memory (no swap added)
	}
}

type RuncResources struct {
	*StandardResources
}

func NewRuncResources() *RuncResources {
	return &RuncResources{
		StandardResources: NewStandardResources(),
	}
}

type GvisorResources struct {
	*StandardResources
}

func NewGvisorResources() *GvisorResources {
	return &GvisorResources{
		StandardResources: NewStandardResources(),
	}
}

func (g *GvisorResources) GetCPU(request *types.ContainerRequest) *specs.LinuxCPU {
	return g.StandardResources.GetCPU(request)
}

func (g *GvisorResources) GetMemory(request *types.ContainerRequest) *specs.LinuxMemory {
	return g.StandardResources.GetMemory(request)
}
