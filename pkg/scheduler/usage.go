package scheduler

import (
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

type SchedulerUsage struct {
	UsageRepo repository.UsageMetricsRepository
}

func NewSchedulerUsage(usageRepo repository.UsageMetricsRepository) SchedulerUsage {
	return SchedulerUsage{
		UsageRepo: usageRepo,
	}
}

func (sm *SchedulerUsage) CounterIncContainerScheduled(request *types.ContainerRequest) {
	if sm.UsageRepo == nil {
		return
	}

	sm.UsageRepo.IncrementCounter(types.MetricsSchedulerContainerScheduled, map[string]interface{}{
		"value":        1,
		"workspace_id": request.WorkspaceId,
		"stub_id":      request.StubId,
		"gpu":          request.Gpu,
	}, 1.0)
}

func (sm *SchedulerUsage) CounterIncContainerRequested(request *types.ContainerRequest) {
	if sm.UsageRepo == nil {
		return
	}

	sm.UsageRepo.IncrementCounter(types.MetricsSchedulerContainerRequested, map[string]interface{}{
		"value":        1,
		"workspace_id": request.WorkspaceId,
		"stub_id":      request.StubId,
		"gpu":          request.Gpu,
	}, 1.0)
}
