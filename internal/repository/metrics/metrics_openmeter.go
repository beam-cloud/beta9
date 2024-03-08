package metrics

import (
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

// Api key
// om_walLWH2mc0y0ZMDULrtGMmCveENWCIuV.80fmyXxfBy7og5M-ZdH3UvXgLZI4i5JX7gd88Uf0hu8

type OpenMeterMetricsRepository struct {
}

func NewOpenMeterMetricsRepository(promConfig types.OpenMeterConfig) repository.MetricsRepository {
	return &OpenMeterMetricsRepository{}
}

func (o *OpenMeterMetricsRepository) Init() error {
	return nil
}

func (o *OpenMeterMetricsRepository) IncrementGauge(name string, labels []string) {

}

func (o *OpenMeterMetricsRepository) AddToCounter(name string, metadata map[string]string, value float64) {

}
