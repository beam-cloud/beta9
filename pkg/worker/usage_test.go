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
				"pricing_version": "cpu-only-202607",
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
	metrics.emitContainerUsageInterval(baseRequest(1_000), start, start.Add(time.Hour))
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
	if firstEvents[0].Data["value"] != float64(3_600_000) || math.Abs(firstEvents[1].Data["value"].(float64)-6.12) > 1e-12 {
		t.Fatalf("duration/cost values = %v/%v, want one hour/6.12 cents", firstEvents[0].Data["value"], firstEvents[1].Data["value"])
	}
	if firstEvents[0].Data["interval_start"] != start.Format(time.RFC3339Nano) ||
		firstEvents[0].Data["interval_end"] != start.Add(time.Hour).Format(time.RFC3339Nano) ||
		firstEvents[1].Data["pricing_version"] != "cpu-only-202607" {
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
	gpuMetrics.emitContainerUsageInterval(gpuRequest, start, start.Add(time.Hour))

	// An invalid quote emits authoritative duration but no cost. The client
	// tests exercise deterministic negative-cache expiry and recovery.
	mu.Lock()
	before := len(events)
	mu.Unlock()
	metrics.emitContainerUsageInterval(baseRequest(2_000), start, start.Add(time.Second))
	mu.Lock()
	afterInvalid := append([]capturedEvent(nil), events...)
	mu.Unlock()
	if len(afterInvalid) != before+1 || afterInvalid[len(afterInvalid)-1].Type != types.UsageMetricsWorkerContainerDuration {
		t.Fatalf("invalid quote emitted events %+v, want duration only", afterInvalid[before:])
	}
	missingAgain := baseRequest(2_000)
	missingAgain.ContainerId = "container-2"
	metrics.emitContainerUsageInterval(missingAgain, start.Add(time.Second), start.Add(2*time.Second))
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
	quoteStarted := make(chan struct{})
	releaseQuote := make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseQuote) }) }

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		startedOnce.Do(func() { close(quoteStarted) })
		<-releaseQuote
		_, _ = w.Write([]byte(`{"cost_per_ms":"0.001","pricing_version":"test"}`))
	}))
	t.Cleanup(func() {
		release()
		server.Close()
	})

	start := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	tickAt := start.Add(5 * time.Second)
	cancelAt := start.Add(7 * time.Second)
	ticks := make(chan time.Time, 1)

	cancelCaptured := make(chan struct{})
	var nowCalls atomic.Int32
	var captureOnce sync.Once
	repository := &usageMetricsRecorder{notify: make(chan struct{}, 4)}
	metrics := &WorkerUsageMetrics{
		ctx:                 context.Background(),
		workerId:            "worker-1",
		metricsRepo:         repository,
		containerCostClient: clients.NewContainerCostClient(types.ContainerCostHookConfig{Endpoint: server.URL, Token: "test"}),
		poolMode:            types.PoolModeLocal,
		openMeterMetadata:   true,
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
	case <-quoteStarted:
	case <-time.After(time.Second):
		t.Fatal("quote request did not start")
	}
	ticks <- tickAt
	select {
	case <-repository.notify:
	case <-time.After(time.Second):
		t.Fatal("duration was not emitted while quote request was blocked")
	}
	repository.mu.Lock()
	eventsBeforeQuoteRelease := append([]recordedUsageMetric(nil), repository.events...)
	repository.mu.Unlock()
	if len(eventsBeforeQuoteRelease) != 1 || eventsBeforeQuoteRelease[0].name != types.UsageMetricsWorkerContainerDuration || eventsBeforeQuoteRelease[0].value != 5_000 {
		t.Fatalf("events while quote blocked = %+v, want authoritative duration first", eventsBeforeQuoteRelease)
	}
	cancel()
	select {
	case <-cancelCaptured:
	case <-time.After(time.Second):
		t.Fatal("cancellation boundary was not captured during quote latency")
	}
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("usage loop did not finish after quote release")
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
