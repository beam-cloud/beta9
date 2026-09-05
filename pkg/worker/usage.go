package worker

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
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

	// Usage events the metering endpoint refused, waiting to be resent.
	backlogMu sync.Mutex
	backlog   []pendingUsageEvent
	drainMu   sync.Mutex
}

const usageBacklogDrainInterval = 30 * time.Second

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

	wm := &WorkerUsageMetrics{
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
	}
	if ctx != nil {
		go wm.startBacklogDrain(ctx, usageBacklogDrainInterval)
	}
	return wm, nil
}

// EmitContainerUsage binds a quote to each interval start. Once an end is
// captured, a bounded quote refresh cannot change the accounted duration.
// Any effective-date boundary inside [start,end) becomes a separate segment.
func (wm *WorkerUsageMetrics) EmitContainerUsage(ctx context.Context, request *types.ContainerRequest) {
	// Gateway reservation metering owns private pools. Recording their
	// containers here as well would charge the same machine twice.
	if wm == nil || request == nil || wm.poolMode == types.PoolModePrivate {
		return
	}

	requestSnapshot := *request
	requestSnapshot.Gpu = billedGpu(request, wm.gpuType)
	requestSnapshot.CostPerMs = 0
	start := wm.currentTime()
	ticker, stopTicker := wm.usageTicker(types.ContainerDurationEmissionInterval)
	defer stopTicker()

	cancelledAt := make(chan time.Time, 1)
	go func() {
		<-ctx.Done()
		cancelledAt <- wm.currentTime()
	}()

	// Intervals that ran before this container ever had a quote. They are
	// priced as soon as one arrives; their duration was already reported.
	var unpriced []containerUsageInterval

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

		// Anything OpenMeter refused earlier goes first, so a recovered
		// endpoint receives usage in the order it happened.
		wm.drainBacklog()

		// Duration is authoritative and never waits on the optional quote.
		wm.emitContainerDurationSegments(requestSnapshot, start, end, currentQuote)

		// Resolve after freezing end. Quote latency can delay cost delivery, but
		// it can never lengthen the interval or hold its duration event.
		nextQuote := wm.getContainerCostQuote(&requestSnapshot)
		if nextQuote.Valid && nextQuote.EffectiveAt.IsZero() && currentQuote.Valid {
			nextQuote.EffectiveAt = currentQuote.EffectiveAt
			if !samePricing(nextQuote, currentQuote) {
				// Changed undated pricing is prospective; it cannot reprice elapsed usage.
				nextQuote.EffectiveAt = end.UTC()
			}
		}
		unpriced = append(unpriced, wm.emitContainerPriceSegments(requestSnapshot, start, end, currentQuote, nextQuote)...)
		if nextQuote.Valid && !currentQuote.Valid && len(unpriced) > 0 {
			// First quote this container has ever seen: it is the best price
			// we will get for the intervals that ran without one, and it is
			// far better than never charging for them at all.
			unpriced = wm.priceBacklog(unpriced, nextQuote)
		}
		if len(unpriced) > usageBacklogLimit {
			log.Error().Str("container_id", request.ContainerId).Int("dropped", len(unpriced)-usageBacklogLimit).Msg("dropping oldest unpriced usage intervals")
			unpriced = unpriced[len(unpriced)-usageBacklogLimit:]
		}
		if final {
			wm.drainBacklog()
			return
		}
		currentQuote = quoteAt(nextQuote, currentQuote, end)
		start = end
	}
}

// billedGpu is the GPU a container is charged as: normally the one it runs on.
// When failover placed it on a GPU the customer did not ask for (a T4 request
// served by an RTX 4090 because the T4 pool was full), the customer is charged
// for what they asked for; the substitution was ours, not theirs. Requests for
// "any" GPU, or whose list includes the placed GPU, are charged as placed.
// The GPU actually used is still reported as gpu_placed for reconciliation.
func billedGpu(request *types.ContainerRequest, placed string) string {
	requested := make([]string, 0, len(request.GpuRequest))
	for _, gpu := range request.GpuRequest {
		gpu = strings.TrimSpace(gpu)
		if gpu == "" || gpu == string(types.NO_GPU) {
			continue
		}
		requested = append(requested, gpu)
	}
	if placed == "" || len(requested) == 0 ||
		slices.Contains(requested, placed) || slices.Contains(requested, string(types.GPU_ANY)) {
		return billingGpuAlias(placed)
	}
	return billingGpuAlias(requested[0])
}

// gpuBillingAliases maps GPU types we no longer sell on their own to the GPU
// they are billed as. A10G serverless is retired: grandfathered A10G requests
// are routed to RTX 4090 pools and pay the RTX 4090 rate, whichever hardware
// actually served them.
var gpuBillingAliases = map[string]string{
	string(types.GPU_A10G): string(types.GPU_RTX4090),
}

func billingGpuAlias(gpu string) string {
	if alias, ok := gpuBillingAliases[gpu]; ok {
		return alias
	}
	return gpu
}

// priceBacklog emits cost for intervals that were reported without a quote,
// returning those the quote still cannot cover.
func (wm *WorkerUsageMetrics) priceBacklog(unpriced []containerUsageInterval, quote clients.ContainerCostQuote) []containerUsageInterval {
	remaining := unpriced[:0]
	for _, interval := range unpriced {
		if !quoteCovers(quote, interval.start) {
			remaining = append(remaining, interval)
			continue
		}
		interval.quote = quote
		interval.request.CostPerMs = quote.CostPerMs
		cost := quote.CostPerMs * float64(interval.duration.Milliseconds())
		interval.cost = &cost
		wm.metricsContainerCost(interval)
	}
	return remaining
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
	for _, interval := range containerUsageSegments(request, start, end, quote.EffectiveAt, quote.ValidUntil) {
		interval.quote = quoteAt(quote, clients.ContainerCostQuote{}, interval.start)
		wm.metricsContainerDuration(interval)
	}
}

// emitContainerPriceSegments reports cost for every segment a quote covers and
// returns the segments none did, so the caller can price them later.
func (wm *WorkerUsageMetrics) emitContainerPriceSegments(request types.ContainerRequest, start, end time.Time, current, next clients.ContainerCostQuote) []containerUsageInterval {
	boundaries := []time.Time{
		current.EffectiveAt, current.ValidUntil,
		next.EffectiveAt, next.ValidUntil,
	}
	var unpriced []containerUsageInterval
	for _, interval := range containerUsageSegments(request, start, end, boundaries...) {
		interval.quote = quoteAt(next, current, interval.start)
		if interval.quote.Valid {
			interval.request.CostPerMs = interval.quote.CostPerMs
			cost := interval.quote.CostPerMs * float64(interval.duration.Milliseconds())
			interval.cost = &cost
			wm.metricsContainerCost(interval)
		} else {
			unpriced = append(unpriced, interval)
		}
		wm.recordExternalUsage(interval)
	}
	return unpriced
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

func samePricing(a, b clients.ContainerCostQuote) bool {
	return a.Valid && b.Valid && a.CostPerMs == b.CostPerMs && a.PricingVersion == b.PricingVersion
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
	wm.emitUsage(
		types.UsageMetricsWorkerContainerDuration,
		wm.containerMetricLabels(interval),
		float64(interval.duration.Milliseconds()),
	)
}

func (wm *WorkerUsageMetrics) metricsContainerCost(interval containerUsageInterval) {
	labels := wm.containerMetricLabels(interval)
	labels["cost_per_ms"] = interval.quote.CostPerMs
	labels["cost_for_duration"] = *interval.cost
	wm.emitUsage(types.UsageMetricsWorkerContainerCost, labels, *interval.cost)
}

// Bound on usage events held back for a metering endpoint that is refusing
// them: at two events per five-second interval this is over an hour per
// container, and the backlog is shared by every container on the worker.
const usageBacklogLimit = 4096

type pendingUsageEvent struct {
	name   string
	labels map[string]interface{}
	value  float64
}

// emitUsage sends one usage event, or holds it back if the metering endpoint
// refuses it so a later tick can retry. Events are idempotent (the repository
// derives their ID from the interval), so a retry after an ambiguous failure
// can never double count; losing one, on the other hand, is money gone.
func (wm *WorkerUsageMetrics) emitUsage(name string, labels map[string]interface{}, value float64) {
	event := pendingUsageEvent{name: name, labels: labels, value: value}
	wm.backlogMu.Lock()
	queued := len(wm.backlog) > 0
	wm.backlogMu.Unlock()
	if queued {
		// Keep order: don't overtake events that are still waiting.
		wm.enqueueUsage(event)
		return
	}
	if err := wm.metricsRepo.IncrementCounter(name, labels, value); err != nil {
		log.Warn().Err(err).Str("container_id", fmt.Sprint(labels["container_id"])).Str("metric", name).Msg("metering endpoint refused usage event; will retry")
		wm.enqueueUsage(event)
	}
}

func (wm *WorkerUsageMetrics) enqueueUsage(event pendingUsageEvent) {
	wm.backlogMu.Lock()
	defer wm.backlogMu.Unlock()
	if len(wm.backlog) >= usageBacklogLimit {
		dropped := wm.backlog[0]
		wm.backlog = wm.backlog[1:]
		log.Error().Str("metric", dropped.name).Str("container_id", fmt.Sprint(dropped.labels["container_id"])).Msg("usage backlog full; dropping oldest usage event")
	}
	wm.backlog = append(wm.backlog, event)
}

// drainBacklog resends held-back usage events in order, stopping at the first
// one the endpoint still refuses.
func (wm *WorkerUsageMetrics) drainBacklog() {
	// One drain at a time: two could send the same event (harmless) and then
	// both pop the front (not harmless).
	if !wm.drainMu.TryLock() {
		return
	}
	defer wm.drainMu.Unlock()
	for {
		wm.backlogMu.Lock()
		if len(wm.backlog) == 0 {
			wm.backlogMu.Unlock()
			return
		}
		event := wm.backlog[0]
		wm.backlogMu.Unlock()

		if err := wm.metricsRepo.IncrementCounter(event.name, event.labels, event.value); err != nil {
			return
		}

		wm.backlogMu.Lock()
		wm.backlog = wm.backlog[1:]
		wm.backlogMu.Unlock()
	}
}

// startBacklogDrain keeps retrying held-back usage even while no container is
// ticking, so events from a container that exited during an outage still land.
func (wm *WorkerUsageMetrics) startBacklogDrain(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			wm.drainBacklog()
			return
		case <-ticker.C:
			wm.drainBacklog()
		}
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
		"gpu_placed":      wm.gpuType,
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
