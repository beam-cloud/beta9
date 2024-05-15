package metrics

import (
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type MetricsSource string

var (
	MetricsSourceGateway MetricsSource = "gateway"
	MetricsSourceWorker  MetricsSource = "worker"
)

func NewMetrics(config types.MonitoringConfig, source string) (repository.MetricsRepository, error) {
	var metricsRepo repository.MetricsRepository

	switch config.MetricsCollector {
	case string(types.MetricsCollectorPrometheus):
		metricsRepo = NewPrometheusMetricsRepository(config.Prometheus)
	case string(types.MetricsCollectorOpenMeter):
		metricsRepo = NewOpenMeterMetricsRepository(config.OpenMeter)
	}

	err := metricsRepo.Init(source)
	if err != nil {
		return nil, err
	}

	return metricsRepo, nil
}
