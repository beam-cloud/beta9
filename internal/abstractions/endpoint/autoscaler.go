package endpoint

import (
	"math"

	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
)

type endpointAutoscalerSample struct {
	TotalRequests     int64
	CurrentContainers int64
}

// endpointDeploymentSampleFunc retrieve a sample from the endpoint instance
func endpointDeploymentSampleFunc(i *endpointInstance) (*endpointAutoscalerSample, error) {
	totalRequests := i.buffer.Length()

	currentContainers := 0
	state, err := i.state()
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

// endpointDeploymentSampleFunc computes a scale result for an endpoint deployment
func endpointDeploymentScaleFunc(i *endpointInstance, sample *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 0

	if sample.TotalRequests == 0 {
		desiredContainers = 0
	} else {
		desiredContainers = int(sample.TotalRequests / int64(i.stubConfig.Concurrency))
		if sample.TotalRequests%int64(i.stubConfig.Concurrency) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or our default of MaxReplicas (whichever is lower)
		maxReplicas := math.Min(float64(i.stubConfig.MaxContainers), float64(abstractions.MaxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

func endpointServeSampleFunc(i *endpointInstance) (*endpointAutoscalerSample, error) {
	sample := &endpointAutoscalerSample{}
	return sample, nil
}

func endpointServeScaleFunc(i *endpointInstance, sample *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	// Criteria for serve autoscaling
	//
	// i.rdb.Get(i.ctx, Keys.endpointKeepWarmLock(i.workspace.Name, i.stub.ExternalId, "*"))

	return &abstractions.AutoscalerResult{
		DesiredContainers: 1,
		ResultValid:       true,
	}
}
