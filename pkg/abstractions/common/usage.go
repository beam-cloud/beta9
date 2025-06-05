package abstractions

import (
	"time"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TrackTaskCost(duration time.Duration, stub *types.StubWithRelated, pricingConfig *types.PricingPolicy, usageMetricsRepo repository.UsageMetricsRepository, taskId, externalWorkspaceId string) {
	if stub.Workspace.ExternalId == externalWorkspaceId {
		return
	}

	totalCostCents := 0.0
	if pricingConfig.CostModel == string(types.PricingPolicyCostModelTask) {
		totalCostCents = pricingConfig.CostPerTask * 100
	} else if pricingConfig.CostModel == string(types.PricingPolicyCostModelDuration) {
		totalCostCents = pricingConfig.CostPerTaskDurationMs * float64(duration.Milliseconds()) * 100
	}

	usageMetricsRepo.IncrementCounter(types.UsageMetricsPublicTaskCost, map[string]interface{}{
		"stub_id":      stub.ExternalId,
		"app_id":       stub.App.ExternalId,
		"workspace_id": externalWorkspaceId,
		"task_id":      taskId,
		"value":        totalCostCents,
	}, totalCostCents)
}

func TrackTaskCount(stub *types.StubWithRelated, usageMetricsRepo repository.UsageMetricsRepository, taskId, externalWorkspaceId string) {
	if stub.Workspace.ExternalId == externalWorkspaceId {
		return
	}

	usageMetricsRepo.IncrementCounter(types.UsageMetricsPublicTaskCount, map[string]interface{}{
		"stub_id":      stub.ExternalId,
		"app_id":       stub.App.ExternalId,
		"workspace_id": externalWorkspaceId,
		"task_id":      taskId,
		"value":        1,
	}, 1)
}
