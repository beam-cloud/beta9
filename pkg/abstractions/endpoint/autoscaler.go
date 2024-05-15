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
		return nil, err
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

func endpointDeploymentScaleFunc(i *endpointInstance, sample *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 0

	if sample.TotalRequests == 0 {
		desiredContainers = 0
	} else {
		desiredContainers = int(sample.TotalRequests / int64(i.StubConfig.Concurrency))
		if sample.TotalRequests%int64(i.StubConfig.Concurrency) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or our default of MaxReplicas (whichever is lower)
		maxReplicas := math.Min(float64(i.StubConfig.MaxContainers), float64(abstractions.MaxReplicas))
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

	if sample.TotalRequests == 0 && exists == 0 {
		desiredContainers = 0
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
