package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

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
