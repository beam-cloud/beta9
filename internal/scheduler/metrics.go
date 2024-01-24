package scheduler

import (
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/prometheus/client_golang/prometheus"
)

type SchedulerMetrics struct {
	metricsRepo repository.PrometheusRepository
}

func NewSchedulerMetrics(metricsRepo repository.PrometheusRepository) SchedulerMetrics {
	metricsRepo.RegisterCounter(
		prometheus.CounterOpts{
			Name: types.MetricsSchedulerContainerScheduled,
		},
	)
	metricsRepo.RegisterCounter(
		prometheus.CounterOpts{
			Name: types.MetricsSchedulerContainerRequested,
		},
	)

	return SchedulerMetrics{
		metricsRepo: metricsRepo,
	}
}

func (sm *SchedulerMetrics) ContainerScheduled() {
	if handler := sm.metricsRepo.GetCounterHandler(types.MetricsSchedulerContainerScheduled); handler != nil {
		handler.Add(1)
	}
}

func (sm *SchedulerMetrics) ContainerRequested() {
	if handler := sm.metricsRepo.GetCounterHandler(types.MetricsSchedulerContainerRequested); handler != nil {
		handler.Add(1)
	}
}
