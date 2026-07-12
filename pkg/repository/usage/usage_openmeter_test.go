package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestOpenMeterEventDataUsesRepositoryValue(t *testing.T) {
	metadata := map[string]interface{}{
		"workspace_id": "workspace-1",
		"value":        1,
	}

	data := eventData(metadata, 42)

	if data["value"] != float64(42) {
		t.Fatalf("event value = %v, want 42", data["value"])
	}
	if metadata["value"] != 1 {
		t.Fatalf("eventData mutated caller metadata value = %v", metadata["value"])
	}
}

func TestOpenMeterEventTimeUsesIntervalStart(t *testing.T) {
	start := time.Date(2026, 7, 10, 23, 59, 59, 0, time.UTC)
	transition := start.Add(time.Second)
	preTransition := openMeterEventTime(map[string]interface{}{
		"interval_start": start.Format(time.RFC3339Nano),
	})
	postTransition := openMeterEventTime(map[string]interface{}{
		"interval_start": transition.Format(time.RFC3339Nano),
	})
	if !preTransition.Equal(start) || !preTransition.Before(transition) {
		t.Fatalf("pre-transition event time = %v, want %v before transition", preTransition, start)
	}
	if !postTransition.Equal(transition) || postTransition.Before(transition) {
		t.Fatalf("post-transition event time = %v, want transition %v", postTransition, transition)
	}
}

func TestOpenMeterIntervalEventIDIsIdempotent(t *testing.T) {
	type event struct {
		ID string `json:"id"`
	}
	var events []event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got event
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode event: %v", err)
		}
		events = append(events, got)
		if len(events) == 1 {
			http.Error(w, "retry", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)

	repository := &OpenMeterUsageMetricsRepository{config: types.OpenMeterConfig{
		ServerUrl: server.URL,
		ApiKey:    "test-key",
	}}
	if err := repository.Init("worker"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	interval := map[string]interface{}{
		"container_id":    "container-1",
		"worker_id":       "worker-1",
		"cpu_millicores":  int64(1_000),
		"pricing_version": "price-1",
		"interval_start":  "2026-07-10T12:00:00Z",
		"interval_end":    "2026-07-10T12:00:05Z",
	}
	emit := func(name string, labels map[string]interface{}, value float64) {
		t.Helper()
		if err := repository.IncrementCounter(name, labels, value); err != nil {
			t.Fatalf("IncrementCounter: %v", err)
		}
	}

	emit("container_duration_milliseconds", interval, 5_000)
	replayed := make(map[string]interface{}, len(interval))
	for key, value := range interval {
		replayed[key] = value
	}
	replayed["worker_id"] = "worker-2"
	replayed["pricing_version"] = "price-2"
	emit("container_duration_milliseconds", replayed, 5_000)

	differentResource := make(map[string]interface{}, len(interval))
	for key, value := range interval {
		differentResource[key] = value
	}
	differentResource["cpu_millicores"] = int64(2_000)
	emit("container_duration_milliseconds", differentResource, 5_000)
	emit("container_duration_milliseconds", interval, 4_000)
	emit("container_cost_cents", interval, 1)

	if len(events) != 6 {
		t.Fatalf("events = %d, want retry plus five logical sends", len(events))
	}
	if events[0].ID == "" || events[0].ID != events[1].ID || events[0].ID != events[2].ID {
		t.Fatalf("same interval IDs = %q/%q/%q, want one stable ID", events[0].ID, events[1].ID, events[2].ID)
	}
	for index, got := range events[3:] {
		if got.ID == events[0].ID {
			t.Fatalf("distinct event %d reused interval ID %q", index, got.ID)
		}
	}
}

func TestOpenMeterRejectsNon2xxWithoutRetryingPermanentStatus(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(server.Close)

	repository := &OpenMeterUsageMetricsRepository{config: types.OpenMeterConfig{
		ServerUrl: server.URL,
		ApiKey:    "test-key",
	}}
	if err := repository.Init("worker"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	err := repository.IncrementCounter("container_duration_milliseconds", map[string]interface{}{
		"workspace_id": "workspace-1",
	}, 1)
	if err == nil || !strings.Contains(err.Error(), "302 Found") {
		t.Fatalf("IncrementCounter error = %v, want strict non-2xx status", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("OpenMeter calls = %d, want no retry for permanent 3xx", got)
	}
}
