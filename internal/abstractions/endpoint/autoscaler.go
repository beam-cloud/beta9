package endpoint

import (
	"math"

	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
)

type endpointAutoscalerSample struct {
	TotalRequests     int64
	CurrentContainers int64
}

// Retrieve a datapoint from the request bucket
func deploymentSampleFunc(i *endpointInstance) (*endpointAutoscalerSample, error) {
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

func deploymentScaleFunc(instance *endpointInstance, sample *endpointAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 0

	if sample.TotalRequests == 0 {
		desiredContainers = 0
	} else {
		desiredContainers = int(sample.TotalRequests / int64(instance.stubConfig.Concurrency))
		if sample.TotalRequests%int64(instance.stubConfig.Concurrency) > 0 {
			desiredContainers += 1
		}

		// Limit max replicas to either what was set in autoscaler config, or our default of MaxReplicas (whichever is lower)
		maxReplicas := math.Min(float64(instance.stubConfig.MaxContainers), float64(abstractions.MaxReplicas))
		desiredContainers = int(math.Min(maxReplicas, float64(desiredContainers)))
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
