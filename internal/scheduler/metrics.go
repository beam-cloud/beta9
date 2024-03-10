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

func (sm *SchedulerMetrics) CounterIncContainerScheduled(request *types.ContainerRequest) {
	sm.metricsRepo.IncrementCounter(types.MetricsSchedulerContainerScheduled, map[string]interface{}{
		"value":        1,
		"workspace_id": request.WorkspaceId,
		"stub_id":      request.StubId,
		"gpu":          request.Gpu,
	}, 1.0)
}

func (sm *SchedulerMetrics) CounterIncContainerRequested(request *types.ContainerRequest) {
	sm.metricsRepo.IncrementCounter(types.MetricsSchedulerContainerRequested, map[string]interface{}{
		"value":        1,
		"workspace_id": request.WorkspaceId,
		"stub_id":      request.StubId,
		"gpu":          request.Gpu,
	}, 1.0)
}
