package worker

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestEmitContainerUsageSkipsPrivatePools(t *testing.T) {
	repo := &usageMetricsRecorder{}
	metrics := &WorkerUsageMetrics{
		workerId:    "worker-1",
		metricsRepo: repo,
		poolMode:    types.PoolModePrivate,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	metrics.EmitContainerUsage(ctx, &types.ContainerRequest{ContainerId: "container-1"})

	if repo.count != 0 {
		t.Fatalf("private pool emitted %d usage counters, want 0", repo.count)
	}
}

func TestEmitContainerUsageRecordsNonPrivatePools(t *testing.T) {
	repo := &usageMetricsRecorder{}
	external := &containerUsageRecorder{}
	metrics := &WorkerUsageMetrics{
		workerId:      "worker-1",
		metricsRepo:   repo,
		poolMode:      types.PoolModeLocal,
		usageRecorder: external,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	metrics.EmitContainerUsage(ctx, &types.ContainerRequest{ContainerId: "container-1", CostPerMs: 99})

	if repo.count != 1 {
		t.Fatalf("local pool emitted %d usage counters, want duration only without a quote", repo.count)
	}
	if external.count != 1 || external.unpriced != 1 || external.invalidCost {
		t.Fatalf("external recorder calls/unpriced/invalid = %d/%d/%t, want one authoritative unpriced interval", external.count, external.unpriced, external.invalidCost)
	}
}

func TestWorkerUsageOpenMeterHTTPIntegration(t *testing.T) {
	type capturedEvent struct {
		ID     string                 `json:"id"`
		Source string                 `json:"source"`
		Type   string                 `json:"type"`
		Time   time.Time              `json:"time"`
		Data   map[string]interface{} `json:"data"`
	}

	var mu sync.Mutex
	quoteCalls := make(map[clients.ContainerCostRequest]int)
	events := make([]capturedEvent, 0)
	failedFirstCostAttempt := false

	mux := http.NewServeMux()
	mux.HandleFunc("/quote", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer quote-token" {
			t.Errorf("quote authorization = %q", r.Header.Get("Authorization"))
		}
		var request clients.ContainerCostRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode quote request: %v", err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}

		mu.Lock()
		quoteCalls[request]++
		mu.Unlock()

		if request.Gpu == "T4" {
			if request.Cpu != 1_000 || request.Memory != 1_024 || request.GpuCount != 1 {
				t.Errorf("T4 quote dimensions = %+v, want 1000m CPU/1024MiB/one T4", request)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{
				"cost_per_ms":     "0.00002082",
				"pricing_version": "legacy-gpu",
				"valid_until":     time.Now().Add(time.Hour).Format(time.RFC3339Nano),
			})
			return
		}

		switch request.Cpu {
		case 1_000:
			_ = json.NewEncoder(w).Encode(map[string]string{
				"cost_per_ms":     "0.00000170",
				"pricing_version": "rates-test",
				"valid_until":     time.Now().Add(time.Hour).Format(time.RFC3339Nano),
			})
		case 2_000:
			_, _ = w.Write([]byte(`{"cost_per_ms":"NaN","pricing_version":"broken"}`))
		default:
			http.Error(w, "unknown resources", http.StatusBadRequest)
		}
	})
	mux.HandleFunc("/api/v1/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer openmeter-key" {
			t.Errorf("OpenMeter authorization = %q", r.Header.Get("Authorization"))
		}
		var event capturedEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("decode OpenMeter event: %v", err)
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}

		mu.Lock()
		events = append(events, event)
		shouldFail := event.Type == types.UsageMetricsWorkerContainerCost && !failedFirstCostAttempt
		if shouldFail {
			failedFirstCostAttempt = true
		}
		mu.Unlock()
		if shouldFail {
			http.Error(w, "retry", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	config := types.AppConfig{Monitoring: types.MonitoringConfig{
		MetricsCollector: string(types.MetricsCollectorOpenMeter),
		OpenMeter: types.OpenMeterConfig{
			ServerUrl: server.URL,
			ApiKey:    "openmeter-key",
		},
		ContainerCostHookConfig: types.ContainerCostHookConfig{
			Endpoint: server.URL + "/quote",
			Token:    "quote-token",
		},
	}}
	external := &containerUsageRecorder{}
	metrics, err := NewWorkerUsageMetrics(context.Background(), "worker-1", config, "", types.PoolModeLocal, external)
	if err != nil {
		t.Fatalf("NewWorkerUsageMetrics: %v", err)
	}

	start := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	baseRequest := func(cpu int64) *types.ContainerRequest {
		return &types.ContainerRequest{
			ContainerId: "container-1",
			WorkspaceId: "workspace-1",
			StubId:      "stub-1",
			AppId:       "app-1",
			Cpu:         cpu,
			Memory:      1_024,
		}
	}

	// The first logical cost event gets a transient response. The repository
	// retries the exact CloudEvent, preserving its source and ID.
	emitResolvedContainerUsageInterval(metrics, baseRequest(1_000), start, start.Add(time.Hour))
	mu.Lock()
	firstEvents := append([]capturedEvent(nil), events...)
	mu.Unlock()
	if len(firstEvents) != 3 {
		t.Fatalf("first interval HTTP events = %d, want duration plus two cost attempts", len(firstEvents))
	}
	if firstEvents[1].Type != types.UsageMetricsWorkerContainerCost || firstEvents[2].Type != types.UsageMetricsWorkerContainerCost {
		t.Fatalf("retried event types = %q/%q, want cost", firstEvents[1].Type, firstEvents[2].Type)
	}
	if firstEvents[1].ID == "" || firstEvents[1].ID != firstEvents[2].ID || firstEvents[1].Source != firstEvents[2].Source {
		t.Fatalf("retry identity = %q/%q and %q/%q, want same nonempty source+ID", firstEvents[1].Source, firstEvents[1].ID, firstEvents[2].Source, firstEvents[2].ID)
	}
	if !firstEvents[0].Time.Equal(start) || !firstEvents[1].Time.Equal(start) || !firstEvents[2].Time.Equal(start) {
		t.Fatalf("event times = %v/%v/%v, want interval start %v across retry", firstEvents[0].Time, firstEvents[1].Time, firstEvents[2].Time, start)
	}
	if firstEvents[0].Data["value"] != float64(3_600_000) || math.Abs(firstEvents[1].Data["value"].(float64)-6.12) > 1e-12 {
		t.Fatalf("duration/cost values = %v/%v, want one hour/6.12 cents", firstEvents[0].Data["value"], firstEvents[1].Data["value"])
	}
	if firstEvents[0].Data["interval_start"] != start.Format(time.RFC3339Nano) ||
		firstEvents[0].Data["interval_end"] != start.Add(time.Hour).Format(time.RFC3339Nano) ||
		firstEvents[1].Data["pricing_version"] != "rates-test" {
		t.Fatalf("interval metadata = %+v", firstEvents[0].Data)
	}

	// A GPU worker uses the unchanged GPU-attached aggregate quote and sends
	// the exact T4 resource dimensions on the wire.
	gpuMetrics, err := NewWorkerUsageMetrics(context.Background(), "worker-gpu", config, "T4", types.PoolModeLocal, external)
	if err != nil {
		t.Fatalf("NewWorkerUsageMetrics GPU: %v", err)
	}
	gpuRequest := baseRequest(1_000)
	gpuRequest.GpuCount = 1
	emitResolvedContainerUsageInterval(gpuMetrics, gpuRequest, start, start.Add(time.Hour))

	// An invalid quote emits authoritative duration but no cost. The client
	// tests exercise deterministic negative-cache expiry and recovery.
	mu.Lock()
	before := len(events)
	mu.Unlock()
	emitResolvedContainerUsageInterval(metrics, baseRequest(2_000), start, start.Add(time.Second))
	mu.Lock()
	afterInvalid := append([]capturedEvent(nil), events...)
	mu.Unlock()
	if len(afterInvalid) != before+1 || afterInvalid[len(afterInvalid)-1].Type != types.UsageMetricsWorkerContainerDuration {
		t.Fatalf("invalid quote emitted events %+v, want duration only", afterInvalid[before:])
	}
	missingAgain := baseRequest(2_000)
	missingAgain.ContainerId = "container-2"
	emitResolvedContainerUsageInterval(metrics, missingAgain, start.Add(time.Second), start.Add(2*time.Second))
	// A canceled container context still performs the final flush through the
	// real loop rather than waiting for the periodic ticker.
	finalCtx, cancel := context.WithCancel(context.Background())
	cancel()
	finalRequest := baseRequest(2_000)
	finalRequest.ContainerId = "final-container"
	metrics.EmitContainerUsage(finalCtx, finalRequest)

	mu.Lock()
	allEvents := append([]capturedEvent(nil), events...)
	calls := make(map[clients.ContainerCostRequest]int, len(quoteCalls))
	for request, count := range quoteCalls {
		calls[request] = count
	}
	mu.Unlock()

	var durations, costAttempts int
	var finalDurationSeen, gpuCostSeen bool
	for _, event := range allEvents {
		switch event.Type {
		case types.UsageMetricsWorkerContainerDuration:
			durations++
			if event.Data["container_id"] == "final-container" {
				finalDurationSeen = true
			}
		case types.UsageMetricsWorkerContainerCost:
			costAttempts++
			if event.Data["pricing_version"] == "legacy-gpu" &&
				event.Data["gpu"] == "T4" && event.Data["gpu_count"] == float64(1) &&
				math.Abs(event.Data["value"].(float64)-74.952) < 1e-12 {
				gpuCostSeen = true
			}
		}
	}
	if durations != 5 || costAttempts != 3 {
		t.Fatalf("duration/cost HTTP attempts = %d/%d, want 5/3", durations, costAttempts)
	}
	if !finalDurationSeen || !gpuCostSeen {
		t.Fatalf("final/GPU flags = %t/%t", finalDurationSeen, gpuCostSeen)
	}
	if calls[clients.ContainerCostRequest{Cpu: 1_000, Memory: 1_024}] != 1 ||
		calls[clients.ContainerCostRequest{Cpu: 1_000, Memory: 1_024, Gpu: "T4", GpuCount: 1}] != 1 ||
		calls[clients.ContainerCostRequest{Cpu: 2_000, Memory: 1_024}] != 1 {
		t.Fatalf("quote calls = %+v, want cache, missing retry, refresh, and final flush", calls)
	}
	if external.count != 5 || external.unpriced != 3 || external.invalidCost {
		t.Fatalf("external recorder calls/unpriced/invalid = %d/%d/%t, want five intervals with three quiet unpriced records", external.count, external.unpriced, external.invalidCost)
	}
}

func TestEmitContainerUsageFreezesCancellationBoundaryDuringQuoteLatency(t *testing.T) {
	start := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	tickAt := start.Add(5 * time.Second)
	cancelAt := start.Add(7 * time.Second)
	ticks := make(chan time.Time, 1)
	initialQuote := make(chan struct{})
	nextQuoteStarted := make(chan struct{})
	releaseNextQuote := make(chan struct{})

	cancelCaptured := make(chan struct{})
	var nowCalls atomic.Int32
	var quoteCalls atomic.Int32
	var captureOnce sync.Once
	repository := &usageMetricsRecorder{notify: make(chan struct{}, 4)}
	metrics := &WorkerUsageMetrics{
		ctx:               context.Background(),
		workerId:          "worker-1",
		metricsRepo:       repository,
		poolMode:          types.PoolModeLocal,
		openMeterMetadata: true,
		now: func() time.Time {
			if nowCalls.Add(1) == 1 {
				return start
			}
			captureOnce.Do(func() { close(cancelCaptured) })
			return cancelAt
		},
		newTicker: func(interval time.Duration) (<-chan time.Time, func()) {
			if interval != types.ContainerDurationEmissionInterval {
				t.Errorf("ticker interval = %v", interval)
			}
			return ticks, func() {}
		},
		quoteProvider: func(context.Context, *types.ContainerRequest) (clients.ContainerCostQuote, error) {
			switch quoteCalls.Add(1) {
			case 1:
				close(initialQuote)
			case 2:
				close(nextQuoteStarted)
				<-releaseNextQuote
			}
			return clients.ContainerCostQuote{}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		metrics.EmitContainerUsage(ctx, &types.ContainerRequest{
			ContainerId: "container-1",
			WorkspaceId: "workspace-1",
			Cpu:         1_000,
			Memory:      1_024,
		})
		close(done)
	}()

	select {
	case <-initialQuote:
	case <-time.After(time.Second):
		t.Fatal("initial quote was not resolved")
	}
	ticks <- tickAt
	select {
	case <-nextQuoteStarted:
	case <-time.After(time.Second):
		t.Fatal("post-tick quote did not start")
	}
	repository.mu.Lock()
	metricsBeforeQuote := append([]recordedUsageMetric(nil), repository.events...)
	repository.mu.Unlock()
	if len(metricsBeforeQuote) != 1 || metricsBeforeQuote[0].name != types.UsageMetricsWorkerContainerDuration {
		t.Fatalf("metrics before blocked quote = %+v, want authoritative duration", metricsBeforeQuote)
	}
	cancel()
	select {
	case <-cancelCaptured:
	case <-time.After(time.Second):
		t.Fatal("cancellation boundary was not captured during quote latency")
	}
	close(releaseNextQuote)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("usage loop did not finish after bounded quote release")
	}

	repository.mu.Lock()
	events := append([]recordedUsageMetric(nil), repository.events...)
	repository.mu.Unlock()
	var durations []recordedUsageMetric
	for _, event := range events {
		if event.name == types.UsageMetricsWorkerContainerDuration {
			durations = append(durations, event)
		}
	}
	if len(durations) != 2 {
		t.Fatalf("duration events = %d, want periodic and final", len(durations))
	}
	if durations[0].value != 5_000 || durations[1].value != 2_000 {
		t.Fatalf("duration values = %v/%v, want 5000/2000ms", durations[0].value, durations[1].value)
	}
	if durations[0].labels["interval_end"] != tickAt.Format(time.RFC3339Nano) ||
		durations[1].labels["interval_end"] != cancelAt.Format(time.RFC3339Nano) {
		t.Fatalf("interval ends = %v/%v, want tick/cancellation boundaries", durations[0].labels["interval_end"], durations[1].labels["interval_end"])
	}
}

func TestEmitContainerUsageSplitsAtQuoteValidityBoundary(t *testing.T) {
	transition := time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)
	start := transition.Add(-time.Second)
	end := transition.Add(4 * time.Second)
	ticks := make(chan time.Time, 1)
	initialQuoteProvided := make(chan struct{})

	repository := &usageMetricsRecorder{notify: make(chan struct{}, 16)}
	external := &containerUsageRecorder{}
	var quoteCalls atomic.Int32
	var initialQuoteOnce sync.Once
	var nowCalls atomic.Int32
	metrics := &WorkerUsageMetrics{
		ctx:               context.Background(),
		workerId:          "worker-1",
		metricsRepo:       repository,
		usageRecorder:     external,
		poolMode:          types.PoolModeLocal,
		openMeterMetadata: true,
		now: func() time.Time {
			if nowCalls.Add(1) == 1 {
				return start
			}
			return end
		},
		newTicker: func(time.Duration) (<-chan time.Time, func()) {
			return ticks, func() {}
		},
		quoteProvider: func(context.Context, *types.ContainerRequest) (clients.ContainerCostQuote, error) {
			if quoteCalls.Add(1) == 1 {
				initialQuoteOnce.Do(func() { close(initialQuoteProvided) })
				return clients.ContainerCostQuote{
					CostPerMs:      0.001,
					PricingVersion: "old",
					EffectiveAt:    start.Add(-time.Hour),
					ValidUntil:     transition,
					Valid:          true,
				}, nil
			}
			return clients.ContainerCostQuote{
				CostPerMs:      0.002,
				PricingVersion: "new",
				EffectiveAt:    transition,
				Valid:          true,
			}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		metrics.EmitContainerUsage(ctx, &types.ContainerRequest{
			ContainerId: "container-1",
			WorkspaceId: "workspace-1",
			Cpu:         1_000,
			Memory:      1_024,
		})
		close(done)
	}()

	select {
	case <-initialQuoteProvided:
	case <-time.After(time.Second):
		t.Fatal("old quote was not resolved at interval start")
	}
	ticks <- end
	waitForUsageMetrics(t, repository.notify, 4)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("transition usage loop did not stop")
	}

	repository.mu.Lock()
	events := append([]recordedUsageMetric(nil), repository.events...)
	repository.mu.Unlock()
	var durationTotal, oldCost, newCost float64
	for _, event := range events {
		intervalStart, _ := time.Parse(time.RFC3339Nano, event.labels["interval_start"].(string))
		intervalEnd, _ := time.Parse(time.RFC3339Nano, event.labels["interval_end"].(string))
		version, _ := event.labels["pricing_version"].(string)
		if event.name == types.UsageMetricsWorkerContainerDuration && event.value > 0 {
			durationTotal += event.value
		}
		if event.name != types.UsageMetricsWorkerContainerCost {
			continue
		}
		switch version {
		case "old":
			oldCost += event.value
			if intervalEnd.After(transition) {
				t.Fatalf("old quote crossed transition: %v to %v", intervalStart, intervalEnd)
			}
		case "new":
			newCost += event.value
			if intervalStart.Before(transition) {
				t.Fatalf("new quote applied before transition: %v to %v", intervalStart, intervalEnd)
			}
		}
	}
	if durationTotal != 5_000 || oldCost != 1 || newCost != 8 {
		t.Fatalf("duration/old/new = %v/%v/%v, want 5000ms/1/8 cents", durationTotal, oldCost, newCost)
	}
	if external.count < 2 || external.invalidCost {
		t.Fatalf("external transition records = %d, invalid = %t", external.count, external.invalidCost)
	}
}

func TestEmitContainerDurationSplitsAtKnownQuoteBoundary(t *testing.T) {
	boundary := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	start := boundary.Add(-time.Second)
	end := boundary.Add(4 * time.Second)
	repository := &usageMetricsRecorder{}
	metrics := &WorkerUsageMetrics{
		workerId:          "worker-1",
		metricsRepo:       repository,
		openMeterMetadata: true,
	}
	quote := clients.ContainerCostQuote{
		CostPerMs:      0.001,
		PricingVersion: "old",
		EffectiveAt:    start.Add(-time.Hour),
		ValidUntil:     boundary,
		Valid:          true,
	}

	metrics.emitContainerDurationSegments(types.ContainerRequest{ContainerId: "container-1"}, start, end, quote)

	if len(repository.events) != 2 {
		t.Fatalf("duration events = %d, want 2", len(repository.events))
	}
	first, second := repository.events[0], repository.events[1]
	if first.value != 1_000 || second.value != 4_000 {
		t.Fatalf("duration values = %v/%v, want 1000/4000ms", first.value, second.value)
	}
	if first.labels["interval_end"] != boundary.Format(time.RFC3339Nano) ||
		second.labels["interval_start"] != boundary.Format(time.RFC3339Nano) {
		t.Fatalf("duration boundary metadata = %v/%v, want %v", first.labels["interval_end"], second.labels["interval_start"], boundary)
	}
	if first.labels["pricing_version"] != "old" || second.labels["pricing_version"] != "" {
		t.Fatalf("duration pricing versions = %q/%q, want old/empty after expiry", first.labels["pricing_version"], second.labels["pricing_version"])
	}
}

func TestEmitContainerUsageDoesNotBackdateUndatedRefresh(t *testing.T) {
	start := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	firstEnd := start.Add(time.Second)
	secondEnd := firstEnd.Add(time.Second)
	thirdEnd := secondEnd.Add(time.Second)
	ticks := make(chan time.Time, 3)
	repository := &usageMetricsRecorder{notify: make(chan struct{}, 6)}
	var nowCalls atomic.Int32
	var quoteCalls atomic.Int32
	metrics := &WorkerUsageMetrics{
		workerId:          "worker-1",
		metricsRepo:       repository,
		poolMode:          types.PoolModeLocal,
		openMeterMetadata: true,
		now: func() time.Time {
			if nowCalls.Add(1) == 1 {
				return start
			}
			return thirdEnd
		},
		newTicker: func(time.Duration) (<-chan time.Time, func()) {
			return ticks, func() {}
		},
		quoteProvider: func(context.Context, *types.ContainerRequest) (clients.ContainerCostQuote, error) {
			switch quoteCalls.Add(1) {
			case 1:
				return clients.ContainerCostQuote{
					CostPerMs:      0.001,
					PricingVersion: "old",
					EffectiveAt:    start.Add(-time.Hour),
					Valid:          true,
				}, nil
			case 2:
				return clients.ContainerCostQuote{CostPerMs: 0.001, PricingVersion: "old", Valid: true}, nil
			}
			return clients.ContainerCostQuote{
				CostPerMs:      0.002,
				PricingVersion: "new",
				Valid:          true,
			}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		metrics.EmitContainerUsage(ctx, &types.ContainerRequest{ContainerId: "container-1"})
		close(done)
	}()
	ticks <- firstEnd
	waitForUsageMetrics(t, repository.notify, 2)
	ticks <- secondEnd
	waitForUsageMetrics(t, repository.notify, 2)
	ticks <- thirdEnd
	waitForUsageMetrics(t, repository.notify, 2)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("usage loop did not stop")
	}

	repository.mu.Lock()
	events := append([]recordedUsageMetric(nil), repository.events...)
	repository.mu.Unlock()
	if len(events) != 6 {
		t.Fatalf("usage events = %d, want three duration/cost pairs", len(events))
	}
	for i, want := range []struct {
		name    string
		version string
		value   float64
	}{
		{types.UsageMetricsWorkerContainerDuration, "old", 1_000},
		{types.UsageMetricsWorkerContainerCost, "old", 1},
		{types.UsageMetricsWorkerContainerDuration, "old", 1_000},
		{types.UsageMetricsWorkerContainerCost, "old", 1},
		{types.UsageMetricsWorkerContainerDuration, "new", 1_000},
		{types.UsageMetricsWorkerContainerCost, "new", 2},
	} {
		if events[i].name != want.name || events[i].labels["pricing_version"] != want.version || events[i].value != want.value {
			t.Fatalf("event %d = %s/%v/%v, want %s/%s/%v", i, events[i].name, events[i].labels["pricing_version"], events[i].value, want.name, want.version, want.value)
		}
	}
	if events[2].labels["pricing_effective_at"] != start.Add(-time.Hour).Format(time.RFC3339Nano) {
		t.Fatalf("unchanged quote effective_at advanced to %v", events[2].labels["pricing_effective_at"])
	}
	if events[4].labels["pricing_effective_at"] != secondEnd.Format(time.RFC3339Nano) {
		t.Fatalf("new quote effective_at = %v, want frozen second interval end %v", events[4].labels["pricing_effective_at"], secondEnd)
	}
}

func TestEmitContainerUsageQuoteFailureRemainsDurationOnly(t *testing.T) {
	start := time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)
	end := start.Add(5 * time.Second)
	ticks := make(chan time.Time, 1)
	quoteFailed := make(chan struct{})
	var quoteFailedOnce sync.Once
	repository := &usageMetricsRecorder{notify: make(chan struct{}, 4)}
	metrics := &WorkerUsageMetrics{
		ctx:         context.Background(),
		workerId:    "worker-1",
		metricsRepo: repository,
		poolMode:    types.PoolModeLocal,
		now:         func() time.Time { return start },
		newTicker: func(time.Duration) (<-chan time.Time, func()) {
			return ticks, func() {}
		},
		quoteProvider: func(context.Context, *types.ContainerRequest) (clients.ContainerCostQuote, error) {
			quoteFailedOnce.Do(func() { close(quoteFailed) })
			return clients.ContainerCostQuote{}, context.DeadlineExceeded
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		metrics.EmitContainerUsage(ctx, &types.ContainerRequest{ContainerId: "container-1"})
		close(done)
	}()
	select {
	case <-quoteFailed:
	case <-time.After(time.Second):
		t.Fatal("quote failure was not returned")
	}
	ticks <- end
	waitForUsageMetrics(t, repository.notify, 1)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("failed-quote usage loop did not stop")
	}

	repository.mu.Lock()
	events := append([]recordedUsageMetric(nil), repository.events...)
	repository.mu.Unlock()
	for _, event := range events {
		if event.name == types.UsageMetricsWorkerContainerCost {
			t.Fatalf("failed quote emitted cost event %+v", event)
		}
	}
}

func TestExpiredStaleQuoteDoesNotPriceAfterTransition(t *testing.T) {
	transition := time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)
	start := transition.Add(-time.Second)
	end := transition.Add(4 * time.Second)
	repository := &usageMetricsRecorder{}
	external := &containerUsageRecorder{}
	metrics := &WorkerUsageMetrics{
		workerId:          "worker-1",
		metricsRepo:       repository,
		usageRecorder:     external,
		poolMode:          types.PoolModeLocal,
		openMeterMetadata: true,
	}
	request := types.ContainerRequest{ContainerId: "container-1", Cpu: 1_000, Memory: 1_024}
	stale := clients.ContainerCostQuote{
		CostPerMs:      0.001,
		PricingVersion: "old",
		EffectiveAt:    start.Add(-time.Hour),
		ValidUntil:     transition,
		Valid:          true,
	}

	metrics.emitContainerDurationSegments(request, start, end, stale)
	metrics.emitContainerPriceSegments(request, start, end, stale, stale)

	repository.mu.Lock()
	events := append([]recordedUsageMetric(nil), repository.events...)
	repository.mu.Unlock()
	var duration, cost float64
	var costEvents int
	for _, event := range events {
		if event.name == types.UsageMetricsWorkerContainerDuration {
			duration += event.value
		}
		if event.name == types.UsageMetricsWorkerContainerCost {
			cost += event.value
			costEvents++
		}
	}
	if duration != 5_000 || cost != 1 || costEvents != 1 {
		t.Fatalf("duration/cost/events = %v/%v/%d, want 5000ms/1 cent/one pre-transition cost", duration, cost, costEvents)
	}
	if external.count != 2 || external.unpriced != 1 || external.invalidCost {
		t.Fatalf("external records/unpriced/invalid = %d/%d/%t, want two/one/false", external.count, external.unpriced, external.invalidCost)
	}
}

func TestContainerUsageSegmentsPreserveWholeIntervalMilliseconds(t *testing.T) {
	start := time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)
	middle := start.Add(500 * time.Microsecond)
	segments := containerUsageSegments(
		types.ContainerRequest{}, start, start.Add(time.Millisecond), middle,
	)
	if len(segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(segments))
	}
	var total int64
	for _, segment := range segments {
		total += segment.duration.Milliseconds()
	}
	if total != 1 {
		t.Fatalf("split duration = %dms, want 1ms", total)
	}
}

func waitForUsageMetrics(t *testing.T, notifications <-chan struct{}, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-notifications:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for usage metric %d/%d", i+1, count)
		}
	}
}

func emitResolvedContainerUsageInterval(metrics *WorkerUsageMetrics, request *types.ContainerRequest, start, end time.Time) {
	requestSnapshot := *request
	requestSnapshot.Gpu = metrics.gpuType
	requestSnapshot.CostPerMs = 0
	quote := metrics.getContainerCostQuote(&requestSnapshot)
	metrics.emitContainerDurationSegments(requestSnapshot, start, end, quote)
	metrics.emitContainerPriceSegments(requestSnapshot, start, end, quote, quote)
}

type usageMetricsRecorder struct {
	mu     sync.Mutex
	count  int
	events []recordedUsageMetric
	notify chan struct{}
}

type containerUsageRecorder struct {
	mu          sync.Mutex
	count       int
	unpriced    int
	invalidCost bool
}

type recordedUsageMetric struct {
	name   string
	labels map[string]interface{}
	value  float64
}

func (r *containerUsageRecorder) RecordContainerUsage(_ context.Context, request *types.ContainerRequest, _, _ time.Time, costCents *float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	if costCents == nil {
		r.unpriced++
		if request.CostPerMs != 0 {
			r.invalidCost = true
		}
		return nil
	}
	if request.CostPerMs < 0 || *costCents < 0 {
		r.invalidCost = true
	}
	return nil
}

func (r *usageMetricsRecorder) Init(string) error {
	return nil
}

func (r *usageMetricsRecorder) IncrementCounter(name string, labels map[string]interface{}, value float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	r.events = append(r.events, recordedUsageMetric{name: name, labels: labels, value: value})
	if r.notify != nil {
		select {
		case r.notify <- struct{}{}:
		default:
		}
	}
	return nil
}

func (r *usageMetricsRecorder) SetGauge(string, map[string]interface{}, float64) error {
	return nil
}
