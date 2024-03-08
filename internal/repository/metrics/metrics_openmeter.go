package metrics

import (
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

type OpenMeterMetricsRepository struct {
}

func NewOpenMeterMetricsRepository(promConfig types.OpenMeterConfig) repository.MetricsRepository {
	return &OpenMeterMetricsRepository{}
}

func (o *OpenMeterMetricsRepository) Init() error {
	return nil
}

func (o *OpenMeterMetricsRepository) IncrementGauge(name string, metadata map[string]string) {

}

func (o *OpenMeterMetricsRepository) AddToCounter(name string, metadata map[string]string, value float64) {

}
