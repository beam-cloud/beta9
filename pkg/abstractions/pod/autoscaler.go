package pod

import (
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type podAutoscalerSample struct {
	CurrentContainers int
	TotalConnections  int64
	LLMPressure       llmPressureSnapshot
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

	var llmPressure llmPressureSnapshot
	if i.Rdb != nil && i.Workspace != nil && i.Stub != nil {
		llmPressure, err = sharedPodLLMPressure(i.Ctx, i.Rdb, i.Workspace.Name, i.Stub.ExternalId)
		if err != nil {
			return nil, err
		}
	}

	sample := &podAutoscalerSample{
		CurrentContainers: currentContainers,
		TotalConnections:  totalConnections,
		LLMPressure:       llmPressure,
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
		if i.StubConfig != nil && i.StubConfig.Autoscaler != nil && i.StubConfig.Autoscaler.Type == types.LLMTokenPressureAutoscaler {
			desiredContainers = desiredPodLLMDeploymentContainers(
				i.StubConfig,
				s,
				i.AppConfig.GatewayService.StubLimits.MaxReplicas,
			)
		} else {
			desiredContainers = desiredPodDeploymentContainers(
				i.StubConfig,
				s.TotalConnections,
				i.AppConfig.GatewayService.StubLimits.MaxReplicas,
			)
		}
	}

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

func desiredPodLLMDeploymentContainers(config *types.StubConfigV1, sample *podAutoscalerSample, maxReplicasLimit uint64) int {
	if sample == nil {
		return desiredPodDeploymentContainers(config, 0, maxReplicasLimit)
	}

	minContainers, maxContainers, tasksPerContainer := podAutoscalerBounds(config, maxReplicasLimit)
	if sample.TotalConnections <= 0 && sample.LLMPressure.ActiveStreams <= 0 && sample.LLMPressure.TokenPressure <= 0 {
		return minContainers
	}

	desiredByConnections := ceilDiv(maxInt64(sample.TotalConnections, sample.LLMPressure.ActiveStreams), tasksPerContainer)
	tokensPerContainer := llmContextLength(config) * tasksPerContainer
	if tokensPerContainer <= 0 {
		tokensPerContainer = int64(llmDefaultContextLen)
	}
	desiredByTokens := ceilDiv(sample.LLMPressure.TokenPressure, tokensPerContainer)
	desiredContainers := int(maxInt64(desiredByConnections, desiredByTokens))

	if desiredContainers < minContainers {
		return minContainers
	}
	if desiredContainers > maxContainers {
		return maxContainers
	}
	return desiredContainers
}

func desiredPodDeploymentContainers(config *types.StubConfigV1, totalConnections int64, maxReplicasLimit uint64) int {
	minContainers, maxContainers, tasksPerContainer := podAutoscalerBounds(config, maxReplicasLimit)

	if totalConnections <= 0 {
		return minContainers
	}

	desiredContainers := int(ceilDiv(totalConnections, tasksPerContainer))

	if desiredContainers < minContainers {
		return minContainers
	}
	if desiredContainers > maxContainers {
		return maxContainers
	}
	return desiredContainers
}

func podAutoscalerBounds(config *types.StubConfigV1, maxReplicasLimit uint64) (int, int, int64) {
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
	if tasksPerContainer <= 0 {
		tasksPerContainer = 1
	}
	return minContainers, maxContainers, tasksPerContainer
}

func ceilDiv(value, divisor int64) int64 {
	if value <= 0 {
		return 0
	}
	if divisor <= 0 {
		divisor = 1
	}
	return (value + divisor - 1) / divisor
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
