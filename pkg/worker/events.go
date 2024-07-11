package worker

import (
	"log"
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

func collectAndSendContainerMetrics(eventRepo repository.EventRepository, workerID string, request *types.ContainerRequest, spec *specs.Spec, pidChan <-chan int, done chan bool) {
	gpuEnabled := request.Gpu != ""

	ticker := time.NewTicker(types.ContainerResourceUsageEmissionInterval)
	defer ticker.Stop()

	containerPid := <-pidChan
	if containerPid == 0 {
		return
	}

	containerProc, err := NewProcUtil(containerPid)
	if err != nil {
		log.Printf("Unable to get process for container %s: %v\n", request.ContainerId, err)
		return
	}

	var prevProcCpuTime float64 = 0
	var prevSystemCpuTime float64 = 0
	for {
		select {
		case <-done:
			return

		case <-ticker.C:
			// Calculate GPU memory
			var gpuMemUsed int64
			var gpuMemTotal int64
			if gpuEnabled {
				for _, device := range spec.Linux.Resources.Devices {
					stats, err := GetGpuMemoryUsage(*device.Minor)
					if err == nil {
						gpuMemUsed += stats.UsedCapacity
						gpuMemTotal += stats.TotalCapacity
					}
				}
			}

			// Calculate CPU and memory
			procs, err := containerProc.getAllDescendantProcs()
			if err != nil {
				log.Printf("Unable to get descendant PIDs for container %s: %v\n", request.ContainerId, err)
				continue
			}
			procs = append(procs, containerProc)

			var totalCpuTime float64
			var totalMemUsed int
			for _, p := range procs {
				stat, err := p.Stat()
				if err != nil {
					continue
				}

				totalCpuTime += stat.CPUTime()
				totalMemUsed += stat.ResidentMemory()
			}

			totalSystemCpuTime, err := GetSystemCPU()
			if err != nil {
				log.Printf("Unable to get system CPU time for container %s: %v\n", request.ContainerId, err)
				continue
			}

			cpuUsed := GetProcCurrentCPUMillicores(totalCpuTime, prevProcCpuTime, totalSystemCpuTime, prevSystemCpuTime)

			// Send data to event repo
			eventRepo.PushContainerResourceMetricsEvent(
				workerID,
				request,
				types.EventContainerMetricsData{
					CPUUsed:        uint64(cpuUsed),
					CPUTotal:       uint64(request.Cpu),
					MemoryUsed:     uint64(totalMemUsed),
					MemoryTotal:    uint64(request.Memory * 1024 * 1024),
					GPUMemoryUsed:  uint64(gpuMemUsed),
					GPUMemoryTotal: uint64(gpuMemTotal),
					GPUType:        request.Gpu,
				},
			)

			prevProcCpuTime = totalCpuTime
			prevSystemCpuTime = totalSystemCpuTime
		}
	}
}
