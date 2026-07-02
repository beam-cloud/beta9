package worker

import (
	"context"
	"math"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	usage "github.com/beam-cloud/beta9/pkg/repository/usage"
	"github.com/rs/zerolog/log"

	types "github.com/beam-cloud/beta9/pkg/types"
)

const (
	marketplaceUsageKindOnDemand   = "on_demand"
	marketplaceUsageKindServerless = "serverless"
	marketplaceMetadataWorkerID    = "worker_id"
	marketplaceMetadataPoolName    = "pool_name"
	marketplaceRecordTimeout       = 5 * time.Second
)

var marketplaceServerlessStubKinds = map[string]struct{}{
	types.StubTypeFunction:     {},
	types.StubTypeEndpoint:     {},
	types.StubTypeTaskQueue:    {},
	types.StubTypeBot:          {},
	types.StubTypeScheduledJob: {},
}

type WorkerUsageMetrics struct {
	workerId            string
	metricsRepo         repo.UsageMetricsRepository
	ctx                 context.Context
	containerCostClient *clients.ContainerCostClient
	marketplaceClient   *clients.MarketplaceUsageClient
	gpuType             string
	poolMode            types.PoolMode
	sellerPayoutRate    float64
}

type containerUsageInterval struct {
	start    time.Time
	end      time.Time
	duration time.Duration
	cost     float64
}

func NewWorkerUsageMetrics(
	ctx context.Context,
	workerId string,
	config types.AppConfig,
	gpuType string,
	poolMode types.PoolMode,
) (*WorkerUsageMetrics, error) {
	metricsRepo, err := usage.NewUsageMetricsRepository(config.Monitoring, string(usage.MetricsSourceWorker))
	if err != nil {
		return nil, err
	}

	containerCostClient := clients.NewContainerCostClient(config.Monitoring.ContainerCostHookConfig)
	sellerPayoutRate := 1 - config.ManagedCompute.BillableMarginPctOrDefault()
	if sellerPayoutRate < 0 {
		sellerPayoutRate = 0
	}

	return &WorkerUsageMetrics{
		ctx:                 ctx,
		workerId:            workerId,
		gpuType:             gpuType,
		poolMode:            poolMode,
		metricsRepo:         metricsRepo,
		containerCostClient: containerCostClient,
		marketplaceClient:   clients.NewMarketplaceUsageClient(config.ManagedCompute.Billing),
		sellerPayoutRate:    sellerPayoutRate,
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
	interval := newContainerUsageInterval(request, start, end)
	wm.metricsContainerDuration(request, interval.duration)
	wm.metricsContainerCost(request, interval.duration)
	wm.recordMarketplaceUsage(request, interval)
}

func (wm *WorkerUsageMetrics) containerMetricLabels(request *types.ContainerRequest, duration time.Duration) map[string]interface{} {
	return map[string]interface{}{
		"container_id":                    request.ContainerId,
		"worker_id":                       wm.workerId,
		"stub_id":                         request.StubId,
		"app_id":                          request.AppId,
		"workspace_id":                    request.WorkspaceId,
		"cpu_millicores":                  request.Cpu,
		"mem_mb":                          request.Memory,
		"gpu":                             request.Gpu,
		"gpu_count":                       request.GpuCount,
		"duration_ms":                     duration.Milliseconds(),
		"pool_mode":                       string(wm.poolMode),
		"marketplace_listing_id":          request.MarketplaceListingID,
		"marketplace_seller_workspace_id": request.MarketplaceSellerID,
		"marketplace_pool_name":           request.MarketplacePoolName,
		"marketplace_machine_id":          request.MarketplaceMachineID,
	}
}

func newContainerUsageInterval(request *types.ContainerRequest, start, end time.Time) containerUsageInterval {
	duration := end.Sub(start)
	return containerUsageInterval{
		start:    start,
		end:      end,
		duration: duration,
		cost:     request.CostPerMs * float64(duration.Milliseconds()),
	}
}

func (wm *WorkerUsageMetrics) recordMarketplaceUsage(request *types.ContainerRequest, interval containerUsageInterval) {
	if wm == nil || wm.poolMode != types.PoolModeMarketplace || wm.marketplaceClient == nil || request == nil {
		return
	}
	if !request.AllowMarketplace || request.MarketplaceListingID == "" || request.MarketplaceSellerID == "" || request.MarketplaceMachineID == "" {
		return
	}

	recordCtx, cancel := context.WithTimeout(context.Background(), marketplaceRecordTimeout)
	defer cancel()
	if err := wm.marketplaceClient.Record(recordCtx, wm.marketplaceUsageRequest(request, interval)); err != nil {
		log.Warn().Err(err).Str("container_id", request.ContainerId).Msg("failed to record marketplace usage")
	}
}

func (wm *WorkerUsageMetrics) marketplaceUsageRequest(request *types.ContainerRequest, interval containerUsageInterval) clients.MarketplaceUsageRequest {
	return clients.MarketplaceUsageRequest{
		BuyerWorkspaceID:          request.WorkspaceId,
		SellerWorkspaceID:         request.MarketplaceSellerID,
		ListingID:                 request.MarketplaceListingID,
		MachineID:                 request.MarketplaceMachineID,
		ContainerID:               request.ContainerId,
		StubID:                    request.StubId,
		AppID:                     request.AppId,
		UsageKind:                 marketplaceUsageKind(request),
		Runtime:                   marketplaceRuntime(request),
		GPU:                       request.Gpu,
		GPUCount:                  request.GpuCount,
		DurationSeconds:           interval.duration.Seconds(),
		BuyerCostCents:            interval.cost,
		SellerPayoutEstimateCents: math.Max(0, interval.cost*wm.sellerPayoutRate),
		StartAt:                   interval.start.UTC(),
		EndAt:                     interval.end.UTC(),
		ContainerType:             request.Stub.Type.Kind(),
		RuntimeMetadata: map[string]interface{}{
			marketplaceMetadataWorkerID: wm.workerId,
			marketplaceMetadataPoolName: request.MarketplacePoolName,
		},
	}
}

func marketplaceRuntime(request *types.ContainerRequest) string {
	if request != nil && request.MarketplaceRuntime != "" {
		return request.MarketplaceRuntime
	}
	return types.ContainerRuntimeGvisor.String()
}

func marketplaceUsageKind(request *types.ContainerRequest) string {
	if request == nil {
		return marketplaceUsageKindOnDemand
	}
	if _, ok := marketplaceServerlessStubKinds[request.Stub.Type.Kind()]; ok {
		return marketplaceUsageKindServerless
	}
	return marketplaceUsageKindOnDemand
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
