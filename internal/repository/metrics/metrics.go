package metrics

import (
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

func NewMetrics(config types.MonitoringConfig) (repository.MetricsRepository, error) {
	var metricsRepo repository.MetricsRepository

	switch config.MetricsCollector {
	case string(types.MetricsCollectorPrometheus):
		metricsRepo = NewPrometheusMetricsRepository(config.Prometheus)
	}

	err := metricsRepo.Init()
	if err != nil {
		return nil, err
	}

	return metricsRepo, nil
}
