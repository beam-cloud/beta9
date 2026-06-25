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

type podScaleBounds struct {
	min               int
	max               int
	tasksPerContainer int64
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
	desiredContainers := desiredPodContainers(i, s)

	return &abstractions.AutoscalerResult{
		DesiredContainers: desiredContainers,
		ResultValid:       true,
	}
}

func desiredPodContainers(i *podInstance, s *podAutoscalerSample) int {
	if s == nil {
		s = &podAutoscalerSample{}
	}

	if i.Stub.Type == types.StubType(types.StubTypePodRun) || i.Stub.Type == types.StubType(types.StubTypeSandbox) {
		if s.CurrentContainers == 0 {
			return 0
		}
		if i.StubConfig.KeepWarmSeconds >= 0 {
			return 0
		}
		return 1
	}

	if i.Stub.Type == types.StubType(types.StubTypePodDeployment) {
		if i.StubConfig != nil && i.StubConfig.Autoscaler != nil && i.StubConfig.Autoscaler.Type == types.LLMTokenPressureAutoscaler {
			return desiredPodLLMDeploymentContainers(
				i.StubConfig,
				s,
				i.AppConfig.GatewayService.StubLimits.MaxReplicas,
			)
		}
		return desiredPodDeploymentContainers(
			i.StubConfig,
			s.TotalConnections,
			i.AppConfig.GatewayService.StubLimits.MaxReplicas,
		)
	}

	return 1
}

func desiredPodLLMDeploymentContainers(config *types.StubConfigV1, sample *podAutoscalerSample, maxReplicasLimit uint64) int {
	if sample == nil {
		return desiredPodDeploymentContainers(config, 0, maxReplicasLimit)
	}

	bounds := podAutoscalerBounds(config, maxReplicasLimit)
	if !sample.hasLLMLoad() {
		return bounds.min
	}

	desiredByRequests := ceilDiv(maxInt64(sample.TotalConnections, sample.LLMPressure.ActiveStreams), bounds.tasksPerContainer)
	desiredByTokens := ceilDiv(sample.LLMPressure.TokenPressure, llmTokensPerContainer(config, bounds.tasksPerContainer))
	return bounds.clamp(int(maxInt64(desiredByRequests, desiredByTokens)))
}

func desiredPodDeploymentContainers(config *types.StubConfigV1, totalConnections int64, maxReplicasLimit uint64) int {
	bounds := podAutoscalerBounds(config, maxReplicasLimit)
	if totalConnections <= 0 {
		return bounds.min
	}
	return bounds.clamp(int(ceilDiv(totalConnections, bounds.tasksPerContainer)))
}

func (s *podAutoscalerSample) hasLLMLoad() bool {
	return s != nil &&
		(s.TotalConnections > 0 || s.LLMPressure.ActiveStreams > 0 || s.LLMPressure.TokenPressure > 0)
}

func llmTokensPerContainer(config *types.StubConfigV1, tasksPerContainer int64) int64 {
	tokens := llmContextLength(config) * tasksPerContainer
	if tokens <= 0 {
		return int64(llmDefaultContextLen)
	}
	return tokens
}

func podAutoscalerBounds(config *types.StubConfigV1, maxReplicasLimit uint64) podScaleBounds {
	bounds := podScaleBounds{
		min:               0,
		max:               1,
		tasksPerContainer: 1,
	}

	if config != nil && config.Autoscaler != nil {
		bounds.min = int(config.Autoscaler.MinContainers)
		bounds.max = int(config.Autoscaler.MaxContainers)
		if config.Autoscaler.TasksPerContainer > 0 {
			bounds.tasksPerContainer = int64(config.Autoscaler.TasksPerContainer)
		}
	}

	if maxReplicasLimit > 0 && uint64(bounds.max) > maxReplicasLimit {
		bounds.max = int(maxReplicasLimit)
	}
	if bounds.min > bounds.max {
		bounds.min = bounds.max
	}
	if bounds.tasksPerContainer <= 0 {
		bounds.tasksPerContainer = 1
	}
	return bounds
}

func (b podScaleBounds) clamp(desired int) int {
	if desired < b.min {
		return b.min
	}
	if desired > b.max {
		return b.max
	}
	return desired
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
