package metrics

import (
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

func NewMetrics(config types.MonitoringConfig) (repository.MetricsRepository, error) {
	metricsCollector := NewPrometheusMetricsCollector(config.Prometheus)
	return metricsCollector, nil
}
