package worker

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	usage "github.com/beam-cloud/beta9/pkg/repository/usage"
	"github.com/rs/zerolog/log"

	types "github.com/beam-cloud/beta9/pkg/types"
)

type WorkerUsageMetrics struct {
	workerId            string
	metricsRepo         repo.UsageMetricsRepository
	ctx                 context.Context
	containerCostClient *clients.ContainerCostClient
	gpuType             string
}

func NewWorkerUsageMetrics(
	ctx context.Context,
	workerId string,
	config types.MonitoringConfig,
	gpuType string,
) (*WorkerUsageMetrics, error) {
	metricsRepo, err := usage.NewUsageMetricsRepository(config, string(usage.MetricsSourceWorker))
	if err != nil {
		return nil, err
	}

	containerCostClient := clients.NewContainerCostClient(config.ContainerCostHookConfig)

	return &WorkerUsageMetrics{
		ctx:                 ctx,
		workerId:            workerId,
		gpuType:             gpuType,
		metricsRepo:         metricsRepo,
		containerCostClient: containerCostClient,
	}, nil
}

func (wm *WorkerUsageMetrics) metricsContainerDuration(request *types.ContainerRequest, duration time.Duration) {
	wm.metricsRepo.IncrementCounter(types.UsageMetricsWorkerContainerDuration, map[string]interface{}{
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

func (wm *WorkerUsageMetrics) metricsContainerCost(request *types.ContainerRequest, duration time.Duration) {
	wm.metricsRepo.IncrementCounter(types.UsageMetricsWorkerContainerCost, map[string]interface{}{
		"container_id":      request.ContainerId,
		"worker_id":         wm.workerId,
		"stub_id":           request.StubId,
		"workspace_id":      request.WorkspaceId,
		"cpu_millicores":    request.Cpu,
		"mem_mb":            request.Memory,
		"gpu":               request.Gpu,
		"gpu_count":         request.GpuCount,
		"cost_per_ms":       request.CostPerMs,
		"cost_for_duration": request.CostPerMs * float64(duration.Milliseconds()),
		"duration_ms":       duration.Milliseconds(),
	}, request.CostPerMs*float64(duration.Milliseconds()))
}

// Periodically send metrics to track container duration
func (wm *WorkerUsageMetrics) EmitContainerUsage(ctx context.Context, request *types.ContainerRequest) {
	cursorTime := time.Now()
	ticker := time.NewTicker(types.ContainerDurationEmissionInterval)
	defer ticker.Stop()

	request.Gpu = wm.gpuType
	request.CostPerMs = wm.getContainerCostPerMs(request)

	for {
		select {
		case <-ticker.C:
			duration := time.Since(cursorTime)
			wm.metricsContainerDuration(request, duration)
			wm.metricsContainerCost(request, duration)
			cursorTime = time.Now()
		case <-ctx.Done():
			// Consolidate any remaining time
			duration := time.Since(cursorTime)
			wm.metricsContainerDuration(request, duration)
			wm.metricsContainerCost(request, duration)
			return
		}
	}
}

func (wm *WorkerUsageMetrics) getContainerCostPerMs(request *types.ContainerRequest) float64 {
	if wm.containerCostClient == nil {
		return 0
	}

	costPerMs, err := wm.containerCostClient.GetContainerCostPerMs(request)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Err(err).Msg("unable to get container cost per ms")
	}

	return costPerMs
}
