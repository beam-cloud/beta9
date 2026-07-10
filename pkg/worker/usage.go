package worker

import (
	"context"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	usage "github.com/beam-cloud/beta9/pkg/repository/usage"
	"github.com/rs/zerolog/log"

	types "github.com/beam-cloud/beta9/pkg/types"
)

const usageRecordTimeout = 5 * time.Second

// ContainerUsageRecorder reports billable container usage to an external
// system. Implementations carry their own attribution; the worker supplies
// the frozen container interval and an optional quoted cost.
type ContainerUsageRecorder interface {
	RecordContainerUsage(ctx context.Context, request *types.ContainerRequest, start, end time.Time, costCents *float64) error
}

type WorkerUsageMetrics struct {
	workerId            string
	metricsRepo         repo.UsageMetricsRepository
	ctx                 context.Context
	containerCostClient *clients.ContainerCostClient
	usageRecorder       ContainerUsageRecorder
	gpuType             string
	poolMode            types.PoolMode
	openMeterMetadata   bool
	now                 func() time.Time
	newTicker           func(time.Duration) (<-chan time.Time, func())
	quoteProvider       func(context.Context, *types.ContainerRequest) (clients.ContainerCostQuote, error)
}

type containerUsageInterval struct {
	request  types.ContainerRequest
	start    time.Time
	end      time.Time
	duration time.Duration
	quote    clients.ContainerCostQuote
	cost     *float64
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
		openMeterMetadata:   config.Monitoring.MetricsCollector == string(types.MetricsCollectorOpenMeter),
		now:                 time.Now,
		newTicker:           usageTicker,
	}, nil
}

// EmitContainerUsage binds a quote to each interval start. Once an end is
// captured, a bounded quote refresh cannot change the accounted duration.
// Any effective-date boundary inside [start,end) becomes a separate segment.
func (wm *WorkerUsageMetrics) EmitContainerUsage(ctx context.Context, request *types.ContainerRequest) {
	if wm == nil || request == nil || wm.poolMode == types.PoolModePrivate {
		return
	}

	requestSnapshot := *request
	requestSnapshot.Gpu = wm.gpuType
	requestSnapshot.CostPerMs = 0
	start := wm.currentTime()
	ticker, stopTicker := wm.usageTicker(types.ContainerDurationEmissionInterval)
	defer stopTicker()

	cancelledAt := make(chan time.Time, 1)
	go func() {
		<-ctx.Done()
		cancelledAt <- wm.currentTime()
	}()

	currentQuote := wm.getContainerCostQuote(&requestSnapshot)
	for {
		var end time.Time
		final := false
		select {
		case end = <-ticker:
			if ctx.Err() != nil {
				end = <-cancelledAt
				final = true
			}
		case end = <-cancelledAt:
			final = true
		}

		// Duration is authoritative and never waits on the optional quote.
		wm.emitContainerDurationSegments(requestSnapshot, start, end, currentQuote)

		// Resolve after freezing end. Quote latency can delay cost delivery, but
		// it can never lengthen the interval or hold its duration event.
		nextQuote := wm.getContainerCostQuote(&requestSnapshot)
		wm.emitContainerPriceSegments(requestSnapshot, start, end, currentQuote, nextQuote)
		if final {
			return
		}
		currentQuote = quoteAt(nextQuote, currentQuote, end)
		start = end
	}
}

func containerUsageSegments(request types.ContainerRequest, start, end time.Time, extraBoundaries ...time.Time) []containerUsageInterval {
	if !end.After(start) {
		return nil
	}
	boundaries := []time.Time{start, end}
	addBoundary := func(at time.Time) {
		if at.After(start) && at.Before(end) {
			boundaries = append(boundaries, at)
		}
	}
	for _, boundary := range extraBoundaries {
		addBoundary(boundary)
	}
	for midnight := nextUTCMidnight(start); midnight.Before(end); midnight = midnight.AddDate(0, 0, 1) {
		addBoundary(midnight)
	}

	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i].Before(boundaries[j]) })
	remainingMs := end.Sub(start).Milliseconds()
	segments := make([]containerUsageInterval, 0, len(boundaries)-1)
	for i := 1; i < len(boundaries); i++ {
		if boundaries[i].Equal(boundaries[i-1]) {
			continue
		}
		durationMs := boundaries[i].Sub(boundaries[i-1]).Milliseconds()
		if i == len(boundaries)-1 {
			durationMs = remainingMs
		}
		remainingMs -= durationMs
		segments = append(segments, containerUsageInterval{
			request:  request,
			start:    boundaries[i-1].UTC(),
			end:      boundaries[i].UTC(),
			duration: time.Duration(durationMs) * time.Millisecond,
		})
	}
	return segments
}

func (wm *WorkerUsageMetrics) emitContainerDurationSegments(request types.ContainerRequest, start, end time.Time, quote clients.ContainerCostQuote) {
	for _, interval := range containerUsageSegments(request, start, end) {
		interval.quote = quoteAt(quote, clients.ContainerCostQuote{}, interval.start)
		wm.metricsContainerDuration(interval)
	}
}

func (wm *WorkerUsageMetrics) emitContainerPriceSegments(request types.ContainerRequest, start, end time.Time, current, next clients.ContainerCostQuote) {
	boundaries := []time.Time{
		current.EffectiveAt, current.ValidUntil,
		next.EffectiveAt, next.ValidUntil,
	}
	for _, interval := range containerUsageSegments(request, start, end, boundaries...) {
		interval.quote = quoteAt(next, current, interval.start)
		if interval.quote.Valid {
			interval.request.CostPerMs = interval.quote.CostPerMs
			cost := interval.quote.CostPerMs * float64(interval.duration.Milliseconds())
			interval.cost = &cost
			wm.metricsContainerCost(interval)
		}
		wm.recordExternalUsage(interval)
	}
}

func quoteAt(preferred, fallback clients.ContainerCostQuote, at time.Time) clients.ContainerCostQuote {
	if quoteCovers(preferred, at) {
		return preferred
	}
	if quoteCovers(fallback, at) {
		return fallback
	}
	return clients.ContainerCostQuote{}
}

func quoteCovers(quote clients.ContainerCostQuote, at time.Time) bool {
	if !quote.Valid || (!quote.EffectiveAt.IsZero() && quote.EffectiveAt.After(at)) {
		return false
	}
	return quote.ValidUntil.IsZero() || quote.ValidUntil.After(at)
}

func nextUTCMidnight(at time.Time) time.Time {
	at = at.UTC()
	return time.Date(at.Year(), at.Month(), at.Day()+1, 0, 0, 0, 0, time.UTC)
}

func (wm *WorkerUsageMetrics) metricsContainerDuration(interval containerUsageInterval) {
	if err := wm.metricsRepo.IncrementCounter(
		types.UsageMetricsWorkerContainerDuration,
		wm.containerMetricLabels(interval),
		float64(interval.duration.Milliseconds()),
	); err != nil {
		log.Warn().Err(err).Str("container_id", interval.request.ContainerId).Msg("failed to emit container duration")
	}
}

func (wm *WorkerUsageMetrics) metricsContainerCost(interval containerUsageInterval) {
	labels := wm.containerMetricLabels(interval)
	labels["cost_per_ms"] = interval.quote.CostPerMs
	labels["cost_for_duration"] = *interval.cost
	if err := wm.metricsRepo.IncrementCounter(types.UsageMetricsWorkerContainerCost, labels, *interval.cost); err != nil {
		log.Warn().Err(err).Str("container_id", interval.request.ContainerId).Msg("failed to emit container cost")
	}
}

func (wm *WorkerUsageMetrics) recordExternalUsage(interval containerUsageInterval) {
	if wm.usageRecorder == nil {
		return
	}
	recordCtx, cancel := context.WithTimeout(context.Background(), usageRecordTimeout)
	defer cancel()
	if err := wm.usageRecorder.RecordContainerUsage(recordCtx, &interval.request, interval.start, interval.end, interval.cost); err != nil {
		log.Warn().Err(err).Str("container_id", interval.request.ContainerId).Msg("failed to record container usage")
	}
}

func (wm *WorkerUsageMetrics) containerMetricLabels(interval containerUsageInterval) map[string]interface{} {
	labels := map[string]interface{}{
		"container_id":    interval.request.ContainerId,
		"worker_id":       wm.workerId,
		"stub_id":         interval.request.StubId,
		"app_id":          interval.request.AppId,
		"workspace_id":    interval.request.WorkspaceId,
		"cpu_millicores":  interval.request.Cpu,
		"mem_mb":          interval.request.Memory,
		"gpu":             interval.request.Gpu,
		"gpu_count":       interval.request.GpuCount,
		"duration_ms":     interval.duration.Milliseconds(),
		"pricing_version": interval.quote.PricingVersion,
	}
	if wm.openMeterMetadata {
		labels["interval_start"] = interval.start.Format(time.RFC3339Nano)
		labels["interval_end"] = interval.end.Format(time.RFC3339Nano)
		if !interval.quote.EffectiveAt.IsZero() {
			labels["pricing_effective_at"] = interval.quote.EffectiveAt.Format(time.RFC3339Nano)
		}
		if !interval.quote.ValidUntil.IsZero() {
			labels["pricing_valid_until"] = interval.quote.ValidUntil.Format(time.RFC3339Nano)
		}
	}
	return labels
}

func (wm *WorkerUsageMetrics) currentTime() time.Time {
	if wm.now != nil {
		return wm.now()
	}
	return time.Now()
}

func (wm *WorkerUsageMetrics) usageTicker(interval time.Duration) (<-chan time.Time, func()) {
	if wm.newTicker != nil {
		return wm.newTicker(interval)
	}
	return usageTicker(interval)
}

func usageTicker(interval time.Duration) (<-chan time.Time, func()) {
	ticker := time.NewTicker(interval)
	return ticker.C, ticker.Stop
}

func (wm *WorkerUsageMetrics) getContainerCostQuote(request *types.ContainerRequest) clients.ContainerCostQuote {
	if wm.containerCostClient == nil && wm.quoteProvider == nil {
		return clients.ContainerCostQuote{}
	}
	ctx := wm.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	var quote clients.ContainerCostQuote
	var err error
	if wm.quoteProvider != nil {
		quote, err = wm.quoteProvider(ctx, request)
	} else {
		quote, err = wm.containerCostClient.GetContainerCostQuote(ctx, request)
	}
	if err != nil {
		logger := log.Error()
		if quote.Valid {
			logger = log.Warn()
		}
		logger.Str("container_id", request.ContainerId).Err(err).Msg("unable to refresh container cost quote")
	}
	return quote
}
