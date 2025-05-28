package abstractions

import (
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TrackTaskCost(duration time.Duration, instance *AutoscaledInstance, taskId, externalWorkspaceId string) {
	if instance.Workspace.ExternalId == externalWorkspaceId {
		return
	}

	pricingConfig := instance.StubConfig.Pricing

	totalCostCents := 0.0
	if pricingConfig.CostModel == string(types.PricingPolicyCostModelTask) {
		totalCostCents = pricingConfig.CostPerTask * 100
	} else if pricingConfig.CostModel == string(types.PricingPolicyCostModelDuration) {
		totalCostCents = pricingConfig.CostPerTaskDurationMs * float64(duration.Milliseconds()) * 100
	}

	instance.UsageMetricsRepo.IncrementCounter(types.UsageMetricsPublicTaskCost, map[string]interface{}{
		"stub_id":      instance.Stub.ExternalId,
		"app_id":       instance.Stub.App.ExternalId,
		"workspace_id": externalWorkspaceId,
		"task_id":      taskId,
		"value":        totalCostCents,
	}, totalCostCents)
}
