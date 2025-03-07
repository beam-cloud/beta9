package worker

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	metrics "github.com/beam-cloud/beta9/pkg/repository/metrics"
	"github.com/rs/zerolog/log"

	types "github.com/beam-cloud/beta9/pkg/types"
)

type WorkerMetrics struct {
	workerId            string
	metricsRepo         repo.MetricsRepository
	ctx                 context.Context
	containerCostClient *clients.ContainerCostClient
}

func NewWorkerMetrics(
	ctx context.Context,
	workerId string,
	config types.MonitoringConfig,
) (*WorkerMetrics, error) {
	metricsRepo, err := metrics.NewMetrics(config, string(metrics.MetricsSourceWorker))
	if err != nil {
		return nil, err
	}

	containerCostClient := clients.NewContainerCostClient(config.ContainerCostHookConfig)

	return &WorkerMetrics{
		ctx:                 ctx,
		workerId:            workerId,
		metricsRepo:         metricsRepo,
		containerCostClient: containerCostClient,
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

func (wm *WorkerMetrics) metricsContainerCost(request *types.ContainerRequest, duration time.Duration) {
	wm.metricsRepo.IncrementCounter(types.MetricsWorkerContainerCost, map[string]interface{}{
		"container_id":   request.ContainerId,
		"worker_id":      wm.workerId,
		"stub_id":        request.StubId,
		"workspace_id":   request.WorkspaceId,
		"cpu_millicores": request.Cpu,
		"mem_mb":         request.Memory,
		"gpu":            request.Gpu,
		"gpu_count":      request.GpuCount,
		"cost_per_ms":    request.CostPerMs,
		"duration_ms":    duration.Milliseconds(),
	}, request.CostPerMs*float64(duration.Milliseconds()))
}

// Periodically send metrics to track container duration
func (wm *WorkerMetrics) EmitContainerUsage(ctx context.Context, request *types.ContainerRequest) {
	cursorTime := time.Now()
	ticker := time.NewTicker(types.ContainerDurationEmissionInterval)
	defer ticker.Stop()

	wm.addContainerCostPerMs(request)

	for {
		select {
		case <-ticker.C:
			go wm.metricsContainerDuration(request, time.Since(cursorTime))
			go wm.metricsContainerCost(request, time.Since(cursorTime))
			cursorTime = time.Now()
		case <-ctx.Done():
			// Consolidate any remaining time
			go wm.metricsContainerDuration(request, time.Since(cursorTime))
			go wm.metricsContainerCost(request, time.Since(cursorTime))
			return
		}
	}
}

// addContainerCostPerMs adds the container cost per ms to the request if the config provided
// a container cost hook endpoint.
func (wm *WorkerMetrics) addContainerCostPerMs(request *types.ContainerRequest) {
	if wm.containerCostClient == nil {
		return
	}

	costPerMs, err := wm.containerCostClient.GetContainerCostPerMs(request)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Err(err).Msg("unable to get container cost per ms")
	}

	request.CostPerMs = costPerMs
}
