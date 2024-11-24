package endpoint

import (
	"context"
	"testing"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestDeploymentScaleFuncWithDefaults(t *testing.T) {
	autoscaledInstance := &abstractions.AutoscaledInstance{
		Ctx: context.Background(),
		Stub: &types.StubWithRelated{
			Stub: types.Stub{
				ExternalId: "test",
			},
		},
		AppConfig: types.AppConfig{},
	}
	autoscaledInstance.AppConfig.GatewayService = types.GatewayServiceConfig{
		StubLimits: types.StubLimits{
			MaxReplicas: 10,
		},
	}
	autoscaledInstance.StubConfig = &types.StubConfigV1{}
	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     1,
		TasksPerContainer: 1,
	}

	instance := &endpointInstance{}
	instance.AutoscaledInstance = autoscaledInstance

	// Check for default scaling up behavior - scale to 1
	sample := &endpointAutoscalerSample{
		TotalRequests: 10,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 1, result.DesiredContainers)

	// Check for default scaling down behavior - scale to 0
	sample = &endpointAutoscalerSample{
		TotalRequests: 0,
	}

	result = endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 0, result.DesiredContainers)

	// Check for invalid queue depth
	sample = &endpointAutoscalerSample{
		TotalRequests: -1,
	}

	result = endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, false, result.ResultValid)
	assert.Equal(t, 0, result.DesiredContainers)
}

func TestDeploymentScaleFuncWithMaxTasksPerContainer(t *testing.T) {
	autoscaledInstance := &abstractions.AutoscaledInstance{
		Ctx: context.Background(),
		Stub: &types.StubWithRelated{
			Stub: types.Stub{
				ExternalId: "test",
			},
		},
		AppConfig: types.AppConfig{},
	}

	autoscaledInstance.AppConfig.GatewayService = types.GatewayServiceConfig{
		StubLimits: types.StubLimits{
			MaxReplicas: 10,
		},
	}
	autoscaledInstance.StubConfig = &types.StubConfigV1{}
	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     3,
		TasksPerContainer: 1,
	}

	// Make sure we scale up to max containers
	instance := &endpointInstance{}
	instance.AutoscaledInstance = autoscaledInstance

	sample := &endpointAutoscalerSample{
		TotalRequests: 3,
	}

	result := endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Ensure we don't exceed max containers
	sample = &endpointAutoscalerSample{
		TotalRequests: 4,
	}

	result = endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Make sure modulo operator makes sense
	sample = &endpointAutoscalerSample{
		TotalRequests: 11,
	}

	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     5,
		TasksPerContainer: 5,
	}
	instance.AutoscaledInstance = autoscaledInstance

	result = endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 3, result.DesiredContainers)

	// Make sure no modulo case works
	sample = &endpointAutoscalerSample{
		TotalRequests: 10,
	}

	autoscaledInstance.StubConfig.Autoscaler = &types.Autoscaler{
		Type:              "queue_depth",
		MaxContainers:     5,
		TasksPerContainer: 5,
	}
	instance.AutoscaledInstance = autoscaledInstance

	result = endpointDeploymentScaleFunc(instance, sample)
	assert.Equal(t, true, result.ResultValid)
	assert.Equal(t, 2, result.DesiredContainers)
}
