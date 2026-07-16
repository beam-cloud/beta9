package worker

import (
	"os"
	"runtime"
	"sort"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"k8s.io/utils/cpuset"
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

func selectRequestedCPUs(millicores int64, available cpuset.CPUSet, load map[int]int64) string {
	if millicores <= 0 || available.IsEmpty() {
		return ""
	}

	count := int((millicores-1)/1000 + 1)
	cpus := available.List()
	sort.Slice(cpus, func(i, j int) bool {
		if load[cpus[i]] != load[cpus[j]] {
			return load[cpus[i]] < load[cpus[j]]
		}
		return cpus[i] < cpus[j]
	})
	if count < len(cpus) {
		cpus = cpus[:count]
	}
	return cpuset.New(cpus...).String()
}

func processCPUSet(cpuLimit int64) cpuset.CPUSet {
	available := cpuset.New()
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			key, value, ok := strings.Cut(line, ":")
			if !ok || key != "Cpus_allowed_list" {
				continue
			}
			if parsed, err := cpuset.Parse(strings.TrimSpace(value)); err == nil && !parsed.IsEmpty() {
				available = parsed
			}
			break
		}
	}

	if available.IsEmpty() {
		cpus := make([]int, runtime.NumCPU())
		for index := range cpus {
			cpus[index] = index
		}
		available = cpuset.New(cpus...)
	}

	limit := int((cpuLimit + 999) / 1000)
	if limit <= 0 || limit >= available.Size() {
		return available
	}
	return cpuset.New(available.List()[:limit]...)
}

func (s *Worker) allocateContainerCPUSet(request *types.ContainerRequest) string {
	if s == nil || s.containerInstances == nil || request == nil || request.IsBuildRequest() ||
		!s.config.Worker.ContainerResourceLimits.CPUAffinityEnforced {
		return ""
	}

	available := processCPUSet(s.cpuLimit)
	load := make(map[int]int64, available.Size())
	s.containerInstances.Range(func(_ string, instance *ContainerInstance) bool {
		if instance == nil || instance.CPUSet == "" {
			return true
		}
		reserved, err := cpuset.Parse(instance.CPUSet)
		if err != nil || reserved.IsEmpty() {
			return true
		}

		millicores := int64(1000)
		if instance.Request != nil && instance.Request.Cpu > 0 {
			millicores = instance.Request.Cpu
		}
		perCPU := (millicores + int64(reserved.Size()) - 1) / int64(reserved.Size())
		for _, cpu := range reserved.List() {
			load[cpu] += perCPU
		}
		return true
	})

	return selectRequestedCPUs(request.Cpu, available, load)
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
