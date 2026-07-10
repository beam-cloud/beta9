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
	newBoundaryTimer    func(time.Time) (<-chan time.Time, func())
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

type activeContainerUsageInterval struct {
	request      types.ContainerRequest
	start        time.Time
	quote        clients.ContainerCostQuote
	quoteResult  <-chan clients.ContainerCostQuote
	cancelQuote  context.CancelFunc
	boundary     <-chan time.Time
	boundaryAt   time.Time
	stopBoundary func()
	nextQuote    *clients.ContainerCostQuote
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
		newBoundaryTimer:    usageBoundaryTimer,
	}, nil
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
	if err := wm.metricsRepo.IncrementCounter(
		types.UsageMetricsWorkerContainerCost,
		labels,
		*interval.cost,
	); err != nil {
		log.Warn().Err(err).Str("container_id", interval.request.ContainerId).Msg("failed to emit container cost")
	}
}

// Periodically send metrics to track container duration
func (wm *WorkerUsageMetrics) EmitContainerUsage(ctx context.Context, request *types.ContainerRequest) {
	if wm == nil || request == nil || wm.poolMode == types.PoolModePrivate {
		return
	}
	cursorTime := wm.currentTime()
	ticker, stopTicker := wm.usageTicker(types.ContainerDurationEmissionInterval)
	defer stopTicker()
	interval := wm.startContainerUsageInterval(request, cursorTime, nil)

	// Capture cancellation independently of quote/event delivery. Otherwise a
	// slow synchronous retry can postpone observing ctx.Done and inflate the
	// final authoritative duration by the retry latency.
	cancelledAt := make(chan time.Time, 1)
	go func() {
		<-ctx.Done()
		cancelledAt <- wm.currentTime()
	}()

	for {
		select {
		case quote := <-interval.quoteResult:
			interval.quoteResult = nil
			interval.stopQuoteResolution()
			wm.bindContainerCostQuote(&interval, quote)
		case <-interval.boundary:
			end := interval.boundaryAt
			nextQuote := interval.nextQuote
			interval.stopBoundaryTimer()
			interval.stopQuoteResolution()
			wm.flushActiveContainerUsageInterval(interval, end)
			interval = wm.startContainerUsageInterval(request, end, nextQuote)
		case end := <-ticker:
			// If cancellation raced with a buffered tick, prefer the captured
			// cancellation boundary and finish without billing past it.
			if ctx.Err() != nil {
				end = <-cancelledAt
				wm.bindReadyContainerCostQuote(&interval)
				interval.stopQuoteResolution()
				wm.flushActiveContainerUsageIntervalThrough(request, interval, end)
				return
			}
			wm.bindReadyContainerCostQuote(&interval)
			wm.flushActiveContainerUsageIntervalThrough(request, interval, end)
			cursorTime = end
			interval = wm.startContainerUsageInterval(request, cursorTime, nil)
		case end := <-cancelledAt:
			// Consolidate any remaining time
			wm.bindReadyContainerCostQuote(&interval)
			interval.stopQuoteResolution()
			wm.flushActiveContainerUsageIntervalThrough(request, interval, end)
			return
		}
	}
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

func (wm *WorkerUsageMetrics) boundaryTimer(at time.Time) (<-chan time.Time, func()) {
	if wm.newBoundaryTimer != nil {
		return wm.newBoundaryTimer(at)
	}
	return usageBoundaryTimer(at)
}

func usageBoundaryTimer(at time.Time) (<-chan time.Time, func()) {
	delay := time.Until(at)
	if delay < 0 {
		delay = 0
	}
	timer := time.NewTimer(delay)
	return timer.C, func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

func (wm *WorkerUsageMetrics) newActiveContainerUsageInterval(request *types.ContainerRequest, start time.Time) activeContainerUsageInterval {
	requestSnapshot := *request
	requestSnapshot.Gpu = wm.gpuType
	requestSnapshot.CostPerMs = 0
	return activeContainerUsageInterval{request: requestSnapshot, start: start}
}

func (wm *WorkerUsageMetrics) startContainerUsageInterval(request *types.ContainerRequest, start time.Time, quote *clients.ContainerCostQuote) activeContainerUsageInterval {
	interval := wm.newActiveContainerUsageInterval(request, start)
	if quote != nil {
		wm.bindContainerCostQuote(&interval, *quote)
		return interval
	}

	result := make(chan clients.ContainerCostQuote, 1)
	interval.quoteResult = result
	ctx := wm.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	quoteCtx, cancel := context.WithCancel(ctx)
	interval.cancelQuote = cancel
	go func(request types.ContainerRequest) {
		result <- wm.getContainerCostQuoteWithContext(quoteCtx, &request, true)
	}(interval.request)
	return interval
}

func (wm *WorkerUsageMetrics) bindReadyContainerCostQuote(interval *activeContainerUsageInterval) {
	if interval.quoteResult == nil {
		return
	}
	select {
	case quote := <-interval.quoteResult:
		interval.quoteResult = nil
		interval.stopQuoteResolution()
		wm.bindContainerCostQuote(interval, quote)
	default:
	}
}

func (wm *WorkerUsageMetrics) bindContainerCostQuote(interval *activeContainerUsageInterval, quote clients.ContainerCostQuote) {
	if !quote.Valid {
		return
	}

	if !quote.EffectiveAt.IsZero() && quote.EffectiveAt.After(interval.start) {
		nextQuote := quote
		interval.nextQuote = &nextQuote
		interval.setBoundaryTimer(wm, quote.EffectiveAt)
		return
	}

	interval.quote = quote
	if quote.ValidUntil.After(interval.start) {
		interval.setBoundaryTimer(wm, quote.ValidUntil)
	}
}

func (interval *activeContainerUsageInterval) setBoundaryTimer(wm *WorkerUsageMetrics, at time.Time) {
	if !interval.boundaryAt.IsZero() && !at.Before(interval.boundaryAt) {
		return
	}
	interval.stopBoundaryTimer()
	interval.boundaryAt = at
	interval.boundary, interval.stopBoundary = wm.boundaryTimer(at)
}

func (interval *activeContainerUsageInterval) stopBoundaryTimer() {
	if interval.stopBoundary != nil {
		interval.stopBoundary()
	}
	interval.boundary = nil
	interval.stopBoundary = nil
}

func (interval *activeContainerUsageInterval) stopQuoteResolution() {
	if interval.cancelQuote != nil {
		interval.cancelQuote()
	}
	interval.cancelQuote = nil
	interval.quoteResult = nil
}

func (wm *WorkerUsageMetrics) flushActiveContainerUsageInterval(interval activeContainerUsageInterval, end time.Time) {
	if end.Before(interval.start) {
		return
	}
	completed := containerUsageInterval{
		request:  interval.request,
		start:    interval.start.UTC(),
		end:      end.UTC(),
		duration: end.Sub(interval.start),
		quote:    interval.quote,
	}
	wm.emitCompletedContainerUsageInterval(completed)
}

func (wm *WorkerUsageMetrics) flushActiveContainerUsageIntervalThrough(request *types.ContainerRequest, interval activeContainerUsageInterval, end time.Time) {
	for !interval.boundaryAt.IsZero() && !interval.boundaryAt.After(end) {
		boundary := interval.boundaryAt
		nextQuote := interval.nextQuote
		interval.stopBoundaryTimer()
		wm.flushActiveContainerUsageInterval(interval, boundary)

		interval = wm.newActiveContainerUsageInterval(request, boundary)
		if nextQuote != nil {
			wm.bindContainerCostQuote(&interval, *nextQuote)
		}
	}
	interval.stopBoundaryTimer()
	wm.flushActiveContainerUsageInterval(interval, end)
}

func (wm *WorkerUsageMetrics) emitContainerUsageInterval(request *types.ContainerRequest, start, end time.Time) {
	if request == nil || end.Before(start) {
		return
	}

	// Freeze all resource and timing fields before resolving the quote so both
	// emitted events (and the optional external recorder) describe one exact
	// interval even if the scheduler-owned request is changed concurrently.
	requestSnapshot := *request
	requestSnapshot.Gpu = wm.gpuType
	requestSnapshot.CostPerMs = 0
	interval := containerUsageInterval{
		request:  requestSnapshot,
		start:    start.UTC(),
		end:      end.UTC(),
		duration: end.Sub(start),
	}

	// Duration is authoritative: send it before any quote HTTP request or
	// price-derived work can delay the signal.
	wm.metricsContainerDuration(interval)
	interval.quote = wm.getContainerCostQuote(&requestSnapshot)
	wm.emitPriceAndExternalUsage(interval)
}

func (wm *WorkerUsageMetrics) emitCompletedContainerUsageInterval(interval containerUsageInterval) {
	wm.metricsContainerDuration(interval)
	wm.emitPriceAndExternalUsage(interval)
}

func (wm *WorkerUsageMetrics) emitPriceAndExternalUsage(interval containerUsageInterval) {
	if interval.quote.Valid {
		interval.request.CostPerMs = interval.quote.CostPerMs
		cost := interval.quote.CostPerMs * float64(interval.duration.Milliseconds())
		interval.cost = &cost
		wm.metricsContainerCost(interval)
	}
	wm.recordExternalUsage(interval)
}

// recordExternalUsage forwards the interval to the configured usage recorder,
// if any. Attribution is the recorder's concern, not the worker's.
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
	// Exact interval timestamps are useful OpenMeter event metadata, but would
	// be unbounded Prometheus label values when that collector is selected.
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

func (wm *WorkerUsageMetrics) getContainerCostQuote(request *types.ContainerRequest) clients.ContainerCostQuote {
	ctx := wm.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	return wm.getContainerCostQuoteWithContext(ctx, request, false)
}

func (wm *WorkerUsageMetrics) getContainerCostQuoteWithContext(ctx context.Context, request *types.ContainerRequest, suppressCanceled bool) clients.ContainerCostQuote {
	if wm.containerCostClient == nil && wm.quoteProvider == nil {
		return clients.ContainerCostQuote{}
	}

	var quote clients.ContainerCostQuote
	var err error
	if wm.quoteProvider != nil {
		quote, err = wm.quoteProvider(ctx, request)
	} else {
		quote, err = wm.containerCostClient.GetContainerCostQuote(ctx, request)
	}
	if err != nil && !(suppressCanceled && ctx.Err() != nil) {
		logger := log.Error()
		if quote.Valid {
			logger = log.Warn()
		}
		logger.Str("container_id", request.ContainerId).Err(err).Msg("unable to refresh container cost quote")
	}

	return quote
}
