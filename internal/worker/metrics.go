package worker

import (
	"context"
	"time"

	repo "github.com/beam-cloud/beta9/internal/repository"
	types "github.com/beam-cloud/beta9/internal/types"
	"github.com/okteto/okteto/pkg/log"
)

type WorkerMetrics struct {
	workerId          string
	metricsRepo       repo.MetricsStatsdRepository
	workerRepo        repo.WorkerRepository
	metricsStreamRepo repo.MetricsStreamRepository
	ctx               context.Context
}

func NewWorkerMetrics(
	ctx context.Context,
	workerId string,
	metricsRepo repo.MetricsStatsdRepository,
	workerRepo repo.WorkerRepository,
	metricsStreamRepo repo.MetricsStreamRepository,
) *WorkerMetrics {
	return &WorkerMetrics{
		ctx:               ctx,
		workerId:          workerId,
		metricsRepo:       metricsRepo,
		workerRepo:        workerRepo,
		metricsStreamRepo: metricsStreamRepo,
	}
}

func (wm *WorkerMetrics) WorkerStarted() {
	wm.metricsRepo.WorkerStarted(wm.workerId)
}

func (wm *WorkerMetrics) WorkerStopped() {
	wm.metricsRepo.WorkerStopped(wm.workerId)
}

func (wm *WorkerMetrics) ContainerStarted(containerId string) {
	wm.metricsRepo.ContainerStarted(
		containerId,
		wm.workerId,
	)
}

func (wm *WorkerMetrics) ContainerStopped(containerId string) {
	wm.metricsRepo.ContainerStopped(
		containerId,
		wm.workerId,
	)
}

// Periodically send statsd metrics to track worker duration
func (wm *WorkerMetrics) EmitWorkerDuration() {
	cursorTime := time.Now()
	ticker := time.NewTicker(types.WorkerDurationEmissionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wm.metricsRepo.WorkerDuration(wm.workerId, time.Now().UnixNano(), time.Since(cursorTime))
			cursorTime = time.Now()
		case <-wm.ctx.Done():
			// Consolidate any remaining time
			wm.metricsRepo.WorkerDuration(wm.workerId, time.Now().UnixNano(), time.Since(cursorTime))
			return
		}
	}
}

func (wm *WorkerMetrics) EmitResourceUsage(request *types.ContainerRequest, pidChan <-chan int, done chan bool) {
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
		case <-ticker.C:
			var totalCpuTime float64 = 0
			var totalMem int = 0

			var gpuMemUsed int64 = -1
			var gpuMemTotal int64 = -1

			if gpuEnabled {
				stats, err := GetGpuMemoryUsage(0) // TODO: Support multiple GPUs
				if err == nil {
					gpuMemUsed = stats.UsedCapacity
					gpuMemTotal = stats.TotalCapacity
				}
			}

			procs, err := containerProc.getAllDescendantProcs()
			if err != nil {
				log.Printf("Unable to get descendant PIDs for container %s: %v\n", request.ContainerId, err)
				continue
			}

			procs = append(procs, containerProc)
			for _, p := range procs {
				stat, err := p.Stat()
				if err != nil {
					continue
				}

				totalCpuTime += stat.CPUTime()
				totalMem += stat.ResidentMemory()
			}

			totalSystemCpuTime, err := GetSystemCPU()
			if err != nil {
				log.Printf("Unable to get system CPU time for container %s: %v\n", request.ContainerId, err)
				continue
			}

			cpuMillicoreUtilization := GetProcCurrentCPUMillicores(totalCpuTime, prevProcCpuTime, totalSystemCpuTime, prevSystemCpuTime)

			wm.metricsStreamRepo.ContainerResourceUsage(
				types.ContainerResourceUsage{
					ContainerID:       request.ContainerId,
					CpuMillicoresUsed: int64(cpuMillicoreUtilization),
					MemoryUsed:        totalMem,
					GpuMemoryUsed:     gpuMemUsed,
					GpuMemoryTotal:    gpuMemTotal,
					GpuType:           request.Gpu,
				},
			)

			prevProcCpuTime = totalCpuTime
			prevSystemCpuTime = totalSystemCpuTime
		case <-done:
			return
		}
	}
}

// Periodically send metrics to track container duration
func (wm *WorkerMetrics) EmitContainerUsage(request *types.ContainerRequest, done chan bool) {
	cursorTime := time.Now()
	ticker := time.NewTicker(types.ContainerDurationEmissionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wm.metricsRepo.ContainerDuration(request.ContainerId, wm.workerId, time.Now().UnixNano(), time.Since(cursorTime))
			cursorTime = time.Now()
		case <-done:
			// Consolidate any remaining time
			wm.metricsRepo.ContainerDuration(request.ContainerId, wm.workerId, time.Now().UnixNano(), time.Since(cursorTime))
			return
		}
	}
}
