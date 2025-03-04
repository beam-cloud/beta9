package worker

import (
	"context"
	"time"

	repo "github.com/beam-cloud/beta9/pkg/repository"
	usage "github.com/beam-cloud/beta9/pkg/repository/usage"

	types "github.com/beam-cloud/beta9/pkg/types"
)

type WorkerUsage struct {
	workerId  string
	usageRepo repo.UsageRepository
	ctx       context.Context
}

func NewWorkerUsage(
	ctx context.Context,
	workerId string,
	config types.MonitoringConfig,
) (*WorkerUsage, error) {
	metricsRepo, err := usage.NewUsage(config, string(usage.MetricsSourceWorker))
	if err != nil {
		return nil, err
	}

	return &WorkerUsage{
		ctx:       ctx,
		workerId:  workerId,
		usageRepo: metricsRepo,
	}, nil
}

func (wm *WorkerUsage) usageContainerDuration(request *types.ContainerRequest, duration time.Duration) {
	wm.usageRepo.IncrementCounter(types.MetricsWorkerContainerDuration, map[string]interface{}{
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

// Periodically send usage to track container duration
func (wm *WorkerUsage) EmitContainerUsage(ctx context.Context, request *types.ContainerRequest) {
	cursorTime := time.Now()
	ticker := time.NewTicker(types.ContainerDurationEmissionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go wm.usageContainerDuration(request, time.Since(cursorTime))
			cursorTime = time.Now()
		case <-ctx.Done():
			// Consolidate any remaining time
			go wm.usageContainerDuration(request, time.Since(cursorTime))
			return
		}
	}
}
