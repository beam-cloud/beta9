package worker

import (
	"context"
	"time"

	repo "github.com/beam-cloud/beta9/pkg/repository"
	metrics "github.com/beam-cloud/beta9/pkg/repository/metrics"

	types "github.com/beam-cloud/beta9/pkg/types"
)

type WorkerMetrics struct {
	workerId    string
	metricsRepo repo.MetricsRepository
	workerRepo  repo.WorkerRepository
	ctx         context.Context
}

func NewWorkerMetrics(
	ctx context.Context,
	workerId string,
	workerRepo repo.WorkerRepository,
	config types.MonitoringConfig,
) (*WorkerMetrics, error) {
	metricsRepo, err := metrics.NewMetrics(config, string(metrics.MetricsSourceWorker))
	if err != nil {
		return nil, err
	}

	return &WorkerMetrics{
		ctx:         ctx,
		workerId:    workerId,
		metricsRepo: metricsRepo,
		workerRepo:  workerRepo,
	}, nil
}

func (wm *WorkerMetrics) metricsContainerDuration(request *types.ContainerRequest, duration time.Duration) {
	wm.metricsRepo.IncrementCounter(types.MetricsWorkerContainerDuration, map[string]interface{}{
		"container_id":   request.ContainerId,
		"worker_id":      wm.workerId,
		"stub_id":        request.StubId,
		"workspace_id":   request.WorkspaceId,
		"cpu_millicores": request.Cpu,
		"mem_mb":         request.Memory,
		"gpu":            request.Gpu,
		"gpu_count":      request.GpuCount,
		"duration_ms":    duration.Milliseconds(),
	}, float64(duration.Milliseconds()))
}

// Periodically send metrics to track container duration
func (wm *WorkerMetrics) EmitContainerUsage(request *types.ContainerRequest, done chan bool) {
	cursorTime := time.Now()
	ticker := time.NewTicker(types.ContainerDurationEmissionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go wm.metricsContainerDuration(request, time.Since(cursorTime))
			cursorTime = time.Now()
		case <-done:
			// Consolidate any remaining time
			go wm.metricsContainerDuration(request, time.Since(cursorTime))
			return
		}
	}
}
