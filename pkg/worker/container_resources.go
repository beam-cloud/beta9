package worker

import (
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"k8s.io/utils/ptr"
)

// ContainerResources defines the interface for container resource management
type ContainerResources interface {
	// GetCPU returns the CPU configuration for a container
	GetCPU(request *types.ContainerRequest) *specs.LinuxCPU
	// GetMemory returns the memory configuration for a container
	GetMemory(request *types.ContainerRequest) *specs.LinuxMemory
}

// StandardResources implements ContainerResources with standard calculations
type StandardResources struct {
	standardCPUShare     uint64
	standardCPUPeriod    uint64
	memoryOverheadFactor float64
}

// NewStandardResources creates a new standard resources manager
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

// RuncResources implements ContainerResources for runc
type RuncResources struct {
	*StandardResources
}

func NewRuncResources() *RuncResources {
	return &RuncResources{
		StandardResources: NewStandardResources(),
	}
}

// GvisorResources implements ContainerResources for gvisor
type GvisorResources struct {
	*StandardResources
}

func NewGvisorResources() *GvisorResources {
	return &GvisorResources{
		StandardResources: NewStandardResources(),
	}
}

// Gvisor may need different resource calculations
func (g *GvisorResources) GetCPU(request *types.ContainerRequest) *specs.LinuxCPU {
	// TODO: Implement gvisor-specific CPU resource calculations if needed
	return g.StandardResources.GetCPU(request)
}

func (g *GvisorResources) GetMemory(request *types.ContainerRequest) *specs.LinuxMemory {
	// TODO: Implement gvisor-specific memory resource calculations if needed
	return g.StandardResources.GetMemory(request)
}
