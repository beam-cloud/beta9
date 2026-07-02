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

const usageRecordTimeout = 5 * time.Second

// ContainerUsageRecorder reports billable container usage to an external
// system. Implementations carry their own attribution (who is billed, who is
// paid out); the worker only supplies the container and the interval.
type ContainerUsageRecorder interface {
	RecordContainerUsage(ctx context.Context, request *types.ContainerRequest, start, end time.Time, costCents float64) error
}

type WorkerUsageMetrics struct {
	workerId            string
	metricsRepo         repo.UsageMetricsRepository
	ctx                 context.Context
	containerCostClient *clients.ContainerCostClient
	usageRecorder       ContainerUsageRecorder
	gpuType             string
	poolMode            types.PoolMode
}

func NewWorkerUsageMetrics(
	ctx context.Context,
	workerId string,
	config types.AppConfig,
	gpuType string,
	poolMode types.PoolMode,
	usageRecorder ContainerUsageRecorder,
) (*WorkerUsageMetrics, error) {
	metricsRepo, err := usage.NewUsageMetricsRepository(config.Monitoring, string(usage.MetricsSourceWorker))
	if err != nil {
		return nil, err
	}

	return &WorkerUsageMetrics{
		ctx:                 ctx,
		workerId:            workerId,
		gpuType:             gpuType,
		poolMode:            poolMode,
		metricsRepo:         metricsRepo,
		containerCostClient: clients.NewContainerCostClient(config.Monitoring.ContainerCostHookConfig),
		usageRecorder:       usageRecorder,
	}, nil
}

func (wm *WorkerUsageMetrics) metricsContainerDuration(request *types.ContainerRequest, duration time.Duration) {
	wm.metricsRepo.IncrementCounter(
		types.UsageMetricsWorkerContainerDuration,
		wm.containerMetricLabels(request, duration),
		float64(duration.Milliseconds()),
	)
}

func (wm *WorkerUsageMetrics) metricsContainerCost(request *types.ContainerRequest, duration time.Duration) {
	cost := request.CostPerMs * float64(duration.Milliseconds())
	labels := wm.containerMetricLabels(request, duration)
	labels["cost_per_ms"] = request.CostPerMs
	labels["cost_for_duration"] = cost
	wm.metricsRepo.IncrementCounter(
		types.UsageMetricsWorkerContainerCost,
		labels,
		cost,
	)
}

// Periodically send metrics to track container duration
func (wm *WorkerUsageMetrics) EmitContainerUsage(ctx context.Context, request *types.ContainerRequest) {
	if wm == nil || wm.poolMode == types.PoolModePrivate {
		return
	}
	cursorTime := time.Now()
	ticker := time.NewTicker(types.ContainerDurationEmissionInterval)
	defer ticker.Stop()

	request.Gpu = wm.gpuType
	request.CostPerMs = wm.getContainerCostPerMs(request)

	for {
		select {
		case <-ticker.C:
			end := time.Now()
			wm.emitContainerUsageInterval(request, cursorTime, end)
			cursorTime = end
		case <-ctx.Done():
			// Consolidate any remaining time
			wm.emitContainerUsageInterval(request, cursorTime, time.Now())
			return
		}
	}
}

func (wm *WorkerUsageMetrics) emitContainerUsageInterval(request *types.ContainerRequest, start, end time.Time) {
	if request == nil || end.Before(start) {
		return
	}
	duration := end.Sub(start)
	wm.metricsContainerDuration(request, duration)
	wm.metricsContainerCost(request, duration)
	wm.recordExternalUsage(request, start, end, duration)
}

// recordExternalUsage forwards the interval to the configured usage recorder,
// if any. Attribution is the recorder's concern, not the worker's.
func (wm *WorkerUsageMetrics) recordExternalUsage(request *types.ContainerRequest, start, end time.Time, duration time.Duration) {
	if wm.usageRecorder == nil {
		return
	}

	cost := request.CostPerMs * float64(duration.Milliseconds())
	recordCtx, cancel := context.WithTimeout(context.Background(), usageRecordTimeout)
	defer cancel()
	if err := wm.usageRecorder.RecordContainerUsage(recordCtx, request, start, end, cost); err != nil {
		log.Warn().Err(err).Str("container_id", request.ContainerId).Msg("failed to record container usage")
	}
}

func (wm *WorkerUsageMetrics) containerMetricLabels(request *types.ContainerRequest, duration time.Duration) map[string]interface{} {
	return map[string]interface{}{
		"container_id":   request.ContainerId,
		"worker_id":      wm.workerId,
		"stub_id":        request.StubId,
		"app_id":         request.AppId,
		"workspace_id":   request.WorkspaceId,
		"cpu_millicores": request.Cpu,
		"mem_mb":         request.Memory,
		"gpu":            request.Gpu,
		"gpu_count":      request.GpuCount,
		"duration_ms":    duration.Milliseconds(),
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
