package pod

import (
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
)

type podAutoscalerSample struct {
	CurrentContainers int
}

// podAutoscalerSampleFunc retrieves an autoscaling sample from the pod instance
func podAutoscalerSampleFunc(i *podInstance) (*podAutoscalerSample, error) {
	currentContainers := 0
	state, err := i.State()
	if err != nil {
		currentContainers = -1
	}

	currentContainers = state.PendingContainers + state.RunningContainers

	sample := &podAutoscalerSample{
		CurrentContainers: currentContainers,
	}

	return sample, nil
}

// podScaleFunc scales based on the number of items in the queue
func podScaleFunc(i *podInstance, s *podAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 1

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
