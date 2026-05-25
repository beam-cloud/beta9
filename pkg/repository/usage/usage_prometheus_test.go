package metrics

import (
	"net/http/httptest"
	"sync"
	"testing"

	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestMetricsHandlerIncludesVictoriaMetricsSet(t *testing.T) {
	repo := NewPrometheusUsageMetricsRepository(types.PrometheusConfig{}).(*PrometheusUsageMetricsRepository)
	require.NoError(t, repo.IncrementCounter("usage_prometheus_test_counter", map[string]interface{}{"source": "test"}, 1))

	vmetrics.GetDefaultSet().GetOrCreateCounter("usage_prometheus_test_victoria_counter_total").Inc()

	request := httptest.NewRequest("GET", "/metrics", nil)
	response := httptest.NewRecorder()

	repo.metricsHandler().ServeHTTP(response, request)

	body := response.Body.String()
	require.Contains(t, body, "usage_prometheus_test_counter")
	require.Contains(t, body, "usage_prometheus_test_victoria_counter_total")
}

func TestConcurrentCounterVecRegistration(t *testing.T) {
	repo := NewPrometheusUsageMetricsRepository(types.PrometheusConfig{}).(*PrometheusUsageMetricsRepository)
	metadata := map[string]interface{}{
		"container_id": "container",
		"worker_id":    "worker",
	}

	errs := make(chan error, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- repo.IncrementCounter("usage_prometheus_concurrent_counter", metadata, 1)
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 1, repo.counterVecs.Len())
}
