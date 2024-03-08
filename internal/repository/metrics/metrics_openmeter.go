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

func (r *OpenMeterMetricsRepository) Init() error {
	return nil
}
