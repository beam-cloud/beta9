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

func NewUsageMetricsRepository(config types.MonitoringConfig, source string) (repository.UsageMetricsRepository, error) {
	var usageMetricsRepo repository.UsageMetricsRepository

	switch config.MetricsCollector {
	case string(types.MetricsCollectorPrometheus):
		usageMetricsRepo = NewPrometheusUsageMetricsRepository(config.Prometheus)
	case string(types.MetricsCollectorOpenMeter):
		usageMetricsRepo = NewOpenMeterUsageMetricsRepository(config.OpenMeter)
	}

	err := usageMetricsRepo.Init(source)
	if err != nil {
		return nil, err
	}

	return usageMetricsRepo, nil
}
