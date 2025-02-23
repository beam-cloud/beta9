package pod

import (
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

type podAutoscalerSample struct {
	CurrentContainers int
	TotalConnections  int64
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
	totalConnections, err := i.Rdb.Get(i.Ctx, Keys.podTotalConnections(i.Workspace.Name, i.Stub.ExternalId)).Int64()
	if err != nil && err != redis.Nil {
		return nil, err
	} else if err == redis.Nil {
		totalConnections = 0
	}

	sample := &podAutoscalerSample{
		CurrentContainers: currentContainers,
		TotalConnections:  totalConnections,
	}

	return sample, nil
}

// podScaleFunc scales based on the number of desired containers
func podScaleFunc(i *podInstance, s *podAutoscalerSample) *abstractions.AutoscalerResult {
	desiredContainers := 1

	if i.Stub.Type == types.StubType(types.StubTypePodRun) {
		if s.CurrentContainers == 0 {
			desiredContainers = 0
		}
	}

	if i.Stub.Type == types.StubType(types.StubTypePodDeployment) {
		desiredContainers = int(i.StubConfig.Autoscaler.MaxContainers)

		if s.TotalConnections == 0 {
			desiredContainers = int(i.StubConfig.Autoscaler.MinContainers)
		}
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}
