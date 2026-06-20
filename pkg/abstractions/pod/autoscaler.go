package pod

import (
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
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
	var totalConnections int64
	if i.Rdb != nil && i.Workspace != nil && i.Stub != nil {
		totalConnections, err = sharedPodTotalConnections(i.Ctx, i.Rdb, i.Workspace.Name, i.Stub.ExternalId)
		if err != nil {
			return nil, err
		}
	} else if i.buffer != nil {
		totalConnections = i.buffer.totalConnectionCount()
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

	if i.Stub.Type == types.StubType(types.StubTypePodRun) || i.Stub.Type == types.StubType(types.StubTypeSandbox) {
		if s.CurrentContainers == 0 {
			desiredContainers = 0
		} else if s.CurrentContainers > 0 && i.StubConfig.KeepWarmSeconds >= 0 {
			desiredContainers = 0
		}
	}

	if i.Stub.Type == types.StubType(types.StubTypePodDeployment) {
		desiredContainers = desiredPodDeploymentContainers(
			i.StubConfig,
			s.TotalConnections,
			i.AppConfig.GatewayService.StubLimits.MaxReplicas,
		)
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

func desiredPodDeploymentContainers(config *types.StubConfigV1, totalConnections int64, maxReplicasLimit uint64) int {
	minContainers := 0
	maxContainers := 1
	tasksPerContainer := int64(1)

	if config != nil && config.Autoscaler != nil {
		minContainers = int(config.Autoscaler.MinContainers)
		maxContainers = int(config.Autoscaler.MaxContainers)
		if config.Autoscaler.TasksPerContainer > 0 {
			tasksPerContainer = int64(config.Autoscaler.TasksPerContainer)
		}
	}

	if maxReplicasLimit > 0 && uint64(maxContainers) > maxReplicasLimit {
		maxContainers = int(maxReplicasLimit)
	}
	if minContainers > maxContainers {
		minContainers = maxContainers
	}

	if totalConnections <= 0 {
		return minContainers
	}

	desiredContainers := int(totalConnections / tasksPerContainer)
	if totalConnections%tasksPerContainer > 0 {
		desiredContainers++
	}

	if desiredContainers < minContainers {
		return minContainers
	}
	if desiredContainers > maxContainers {
		return maxContainers
	}
	return desiredContainers
}
