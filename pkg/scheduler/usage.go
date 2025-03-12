package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type SchedulerUsageMetrics struct {
	UsageRepo repository.UsageMetricsRepository
}

func NewSchedulerUsageMetrics(usageMetricsRepo repository.UsageMetricsRepository) SchedulerUsageMetrics {
	return SchedulerUsageMetrics{
		UsageRepo: usageMetricsRepo,
	}
}

func (sm *SchedulerUsageMetrics) CounterIncContainerScheduled(request *types.ContainerRequest) {
	if sm.UsageRepo == nil {
		return
	}

	sm.UsageRepo.IncrementCounter(types.UsageMetricsSchedulerContainerScheduled, map[string]interface{}{
		"value":        1,
		"workspace_id": request.WorkspaceId,
		"stub_id":      request.StubId,
		"gpu":          request.Gpu,
	}, 1.0)
}

func (sm *SchedulerUsageMetrics) CounterIncContainerRequested(request *types.ContainerRequest) {
	if sm.UsageRepo == nil {
		return
	}

	sm.UsageRepo.IncrementCounter(types.UsageMetricsSchedulerContainerRequested, map[string]interface{}{
		"value":        1,
		"workspace_id": request.WorkspaceId,
		"stub_id":      request.StubId,
		"gpu":          request.Gpu,
	}, 1.0)
}
