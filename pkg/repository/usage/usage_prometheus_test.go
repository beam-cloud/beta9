package metrics

import (
	"net/http/httptest"
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
