package metrics

import (
	"fmt"

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
	case "", string(types.MetricsCollectorNone):
		usageMetricsRepo = noopUsageMetricsRepository{}
	default:
		return nil, fmt.Errorf("unsupported metrics collector %q", config.MetricsCollector)
	}

	err := usageMetricsRepo.Init(source)
	if err != nil {
		return nil, err
	}

	return usageMetricsRepo, nil
}

type noopUsageMetricsRepository struct{}

func (noopUsageMetricsRepository) Init(source string) error {
	return nil
}

func (noopUsageMetricsRepository) IncrementCounter(name string, metadata map[string]interface{}, value float64) error {
	return nil
}

func (noopUsageMetricsRepository) SetGauge(name string, metadata map[string]interface{}, value float64) error {
	return nil
}
