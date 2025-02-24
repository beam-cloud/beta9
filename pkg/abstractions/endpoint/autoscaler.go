package endpoint

import (
	"math"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
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

		tasksPerContainer := 1
		if i.StubConfig.Autoscaler != nil && i.StubConfig.Autoscaler.TasksPerContainer > 0 {
			tasksPerContainer = int(i.StubConfig.Autoscaler.TasksPerContainer)
		}

		desiredContainers = int(s.TotalRequests / int64(tasksPerContainer))
		if s.TotalRequests%int64(tasksPerContainer) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or the limit specified on the gateway config (whichever is lower)
		maxReplicas := math.Min(float64(i.StubConfig.Autoscaler.MaxContainers), float64(i.AppConfig.GatewayService.StubLimits.MaxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

func endpointServeScaleFunc(i *endpointInstance, sample *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 1

	timeoutKey := Keys.endpointServeLock(i.Workspace.Name, i.Stub.ExternalId)
	exists, err := i.Rdb.Exists(i.Ctx, timeoutKey).Result()
	if err != nil {
		return &abstractions.AutoscalerResult{
			ResultValid: false,
		}
	}

	if exists == 0 {
		desiredContainers = 0
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
