package scheduler

import (
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

type SchedulerMetrics struct {
	metricsRepo repository.MetricsRepository
}

func NewSchedulerMetrics(metricsRepo repository.MetricsRepository) SchedulerMetrics {
	return SchedulerMetrics{
		metricsRepo: metricsRepo,
	}
}

func (sm *SchedulerMetrics) CounterIncContainerScheduled() {
	sm.metricsRepo.IncrementCounter(types.MetricsSchedulerContainerScheduled, map[string]interface{}{
		"value": 1,
	}, 1.0)
}

func (sm *SchedulerMetrics) CounterIncContainerRequested() {
	sm.metricsRepo.IncrementCounter(types.MetricsSchedulerContainerRequested, map[string]interface{}{
		"value": 1,
	}, 1.0)
}
