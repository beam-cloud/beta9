package endpoint

import (
	"fmt"
	"math"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type endpointAutoscalerSample struct {
	TotalRequests     int64
	CurrentContainers int64
}

func endpointSampleFunc(i *endpointInstance) (*endpointAutoscalerSample, error) {
	totalRequests, err := i.TaskRepo.TasksInFlight(i.Ctx, i.Workspace.Name, i.Stub.ExternalId)
	if err != nil {
		totalRequests = -1
	}

	currentContainers := 0
	state, err := i.State()
	if err != nil {
		currentContainers = -1
	}

	currentContainers = state.PendingContainers + state.RunningContainers

	sample := &endpointAutoscalerSample{
		TotalRequests:     int64(totalRequests),
		CurrentContainers: int64(currentContainers),
	}

	return sample, nil
}

func endpointDeploymentScaleFunc(i *endpointInstance, s *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 0

	if s.TotalRequests == 0 {
		desiredContainers = 0
	} else {
		if s.TotalRequests == -1 {
			return &abstractions.AutoscalerResult{
				ResultValid: false,
			}
		}

		tasksPerContainer := int64(1)
		if i.StubConfig.Autoscaler != nil && i.StubConfig.Autoscaler.TasksPerContainer > 0 {
			tasksPerContainer = int64(i.StubConfig.Autoscaler.TasksPerContainer)
		}

		desiredContainers = int(s.TotalRequests / int64(tasksPerContainer))
		if s.TotalRequests%int64(tasksPerContainer) > 0 {
			desiredContainers += 1
		}

		maxContainers := uint(1)
		if i.StubConfig.Autoscaler != nil {
			maxContainers = i.StubConfig.Autoscaler.MaxContainers
		}

		// Limit max replicas to either what was set in autoscaler config, or the limit specified on the gateway config (whichever is lower)
		maxReplicas := math.Min(float64(maxContainers), float64(i.AppConfig.GatewayService.StubLimits.MaxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

func endpointServeScaleFunc(i *endpointInstance, sample *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 1

	lockKey := common.RedisKeys.SchedulerServeLock(i.Workspace.Name, i.Stub.ExternalId)
	exists, err := i.Rdb.Exists(i.Ctx, lockKey).Result()
	if err != nil {
		return &abstractions.AutoscalerResult{
			ResultValid: false,
		}
	}

	if exists == 0 {
		desiredContainers = 0
		recordEndpointScaleDecisionAsync(i, sample, desiredContainers, "SERVE_LOCK_MISSING")
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

func recordEndpointScaleDecisionAsync(i *endpointInstance, sample *endpointAutoscalerSample, desiredContainers int, reason string) {
	if i == nil || i.EventRepo == nil || sample == nil {
		return
	}
	sampleCopy := *sample
	go recordEndpointScaleDecision(i, &sampleCopy, desiredContainers, reason)
}

func recordEndpointScaleDecision(i *endpointInstance, sample *endpointAutoscalerSample, desiredContainers int, reason string) {
	if i == nil || i.EventRepo == nil {
		return
	}

	containers, err := i.ContainerRepo.GetActiveContainersByStubId(i.Stub.ExternalId)
	if err != nil {
		return
	}

	for _, container := range containers {
		appID := ""
		if i.Stub.App != nil {
			appID = i.Stub.App.ExternalId
		}
		i.EventRepo.PushContainerEvent(types.EventContainerEventSchema{
			ID:          types.ContainerEventAutoscalerScaleDecision,
			ContainerID: container.ContainerId,
			StubID:      container.StubId,
			StubType:    string(i.Stub.Type.Kind()),
			WorkspaceID: container.WorkspaceId,
			AppID:       appID,
			WorkerID:    container.WorkerId,
			MachineID:   container.MachineId,
			Reason:      reason,
			Source:      types.EventSourceEndpointAutoscaler.String(),
			Message:     types.EventMessageAutoscalerScaleDecision.String(),
			Attrs: map[string]string{
				types.EventAttrDesiredContainers: fmt.Sprintf("%d", desiredContainers),
				types.EventAttrCurrentContainers: fmt.Sprintf("%d", sample.CurrentContainers),
				types.EventAttrTotalRequests:     fmt.Sprintf("%d", sample.TotalRequests),
			},
		})
	}
}
