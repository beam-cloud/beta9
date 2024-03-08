package scheduler

import (
	"github.com/beam-cloud/beta9/internal/repository"
)

type SchedulerMetrics struct {
	metricsRepo repository.MetricsRepository
}

func NewSchedulerMetrics(metricsRepo repository.MetricsRepository) SchedulerMetrics {
	// metricsRepo.RegisterCounter(
	// 	prometheus.CounterOpts{
	// 		Name: types.MetricsSchedulerContainerScheduled,
	// 	},
	// )
	// metricsRepo.RegisterCounter(
	// 	prometheus.CounterOpts{
	// 		Name: types.MetricsSchedulerContainerRequested,
	// 	},
	// )

	return SchedulerMetrics{
		metricsRepo: metricsRepo,
	}
}

func (sm *SchedulerMetrics) CounterIncContainerScheduled() {
	// if handler := sm.metricsRepo.GetCounterHandler(types.MetricsSchedulerContainerScheduled); handler != nil {
	// 	handler.Inc()
	// }
}

func (sm *SchedulerMetrics) CounterIncContainerRequested() {
	// if handler := sm.metricsRepo.GetCounterHandler(types.MetricsSchedulerContainerRequested); handler != nil {
	// 	handler.Inc()
	// }
}
