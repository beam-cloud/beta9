package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type SchedulerMetrics struct {
	metricsRepo repository.UsageRepository
}

func NewSchedulerMetrics(metricsRepo repository.UsageRepository) SchedulerMetrics {
	return SchedulerMetrics{
		metricsRepo: metricsRepo,
	}
}

func (sm *SchedulerMetrics) CounterIncContainerScheduled(request *types.ContainerRequest) {
	if sm.metricsRepo == nil {
		return
	}

	sm.metricsRepo.IncrementCounter(types.MetricsSchedulerContainerScheduled, map[string]interface{}{
		"value":        1,
		"workspace_id": request.WorkspaceId,
		"stub_id":      request.StubId,
		"gpu":          request.Gpu,
	}, 1.0)
}

func (sm *SchedulerMetrics) CounterIncContainerRequested(request *types.ContainerRequest) {
	if sm.metricsRepo == nil {
		return
	}

	sm.metricsRepo.IncrementCounter(types.MetricsSchedulerContainerRequested, map[string]interface{}{
		"value":        1,
		"workspace_id": request.WorkspaceId,
		"stub_id":      request.StubId,
		"gpu":          request.Gpu,
	}, 1.0)
}
