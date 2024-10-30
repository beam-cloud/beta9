package taskqueue

import (
	"testing"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestDeploymentScaleFuncWithDefaults(t *testing.T) {
	autoscaledInstance := &abstractions.AutoscaledInstance{}
	autoscaledInstance.StubConfig = &types.StubConfigV1{}
	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     1,
		TasksPerContainer: 1,
	}
	autoscaledInstance.AppConfig = types.AppConfig{}
	autoscaledInstance.AppConfig.GatewayService = types.GatewayServiceConfig{
		StubLimits: types.StubLimits{
			MaxReplicas: 10,
		},
	}

	instance := &taskQueueInstance{}
	instance.AutoscaledInstance = autoscaledInstance

	// Check for default scaling up behavior - scale to 1
	sample := &taskQueueAutoscalerSample{
		QueueLength: 10,
	}

	result := taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 1, result.DesiredContainers)

	// Check for default scaling down behavior - scale to 0
	sample = &taskQueueAutoscalerSample{
		QueueLength: 0,
	}

	result = taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 0, result.DesiredContainers)

	// Check for invalid queue depth
	sample = &taskQueueAutoscalerSample{
		QueueLength: -1,
	}

	result = taskQueueScaleFunc(instance, sample)
	assert.Equal(t, false, result.ResultValid)
	assert.Equal(t, 0, result.DesiredContainers)
}

func TestDeploymentScaleFuncWithMaxTasksPerContainer(t *testing.T) {
	autoscaledInstance := &abstractions.AutoscaledInstance{}
	autoscaledInstance.StubConfig = &types.StubConfigV1{}
	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     3,
		TasksPerContainer: 1,
	}
	autoscaledInstance.AppConfig = types.AppConfig{}
	autoscaledInstance.AppConfig.GatewayService = types.GatewayServiceConfig{
		StubLimits: types.StubLimits{
			MaxReplicas: 10,
		},
	}

	// Make sure we scale up to max containers
	instance := &taskQueueInstance{}
	instance.AutoscaledInstance = autoscaledInstance

	sample := &taskQueueAutoscalerSample{
		QueueLength: 3,
	}

	result := taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Ensure we don't exceed max containers
	sample = &taskQueueAutoscalerSample{
		QueueLength: 4,
	}

	result = taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Make sure modulo operator makes sense
	sample = &taskQueueAutoscalerSample{
		QueueLength: 11,
	}

	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     5,
		TasksPerContainer: 5,
	}
	instance.AutoscaledInstance = autoscaledInstance

	result = taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Make sure no modulo case works
	sample = &taskQueueAutoscalerSample{
		QueueLength: 10,
	}

	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     5,
		TasksPerContainer: 5,
	}
	instance.AutoscaledInstance = autoscaledInstance

	result = taskQueueScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 2, result.DesiredContainers)
}
