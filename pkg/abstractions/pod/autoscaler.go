package pod

import (
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
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
		return nil, err
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

	if i.Stub.Type == types.StubType(types.StubTypePodRun) {
		if s.CurrentContainers == 0 {
			desiredContainers = 0
		}
	}

	if i.Stub.Type == types.StubType(types.StubTypePodDeployment) && i.StubConfig.Autoscaler.MinContainers > 0 {
		desiredContainers = int(i.StubConfig.Autoscaler.MinContainers)
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
