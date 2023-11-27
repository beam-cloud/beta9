package gateway

import (
	"testing"

	"github.com/beam-cloud/beam/pkg/types"
)

func TestScaleByQueueDepth(t *testing.T) {
	as := &AutoScaler{
		requestBucket: &DeploymentRequestBucket{
			AutoscalerConfig: types.Autoscaler{
				QueueDepth: &types.QueueDepthAutoscaler{
					MaxTasksPerReplica: 5,
					MaxReplicas:        3,
				},
			},
		},
	}

	tests := []struct {
		name          string
		sample        *AutoscalerSample
		expectedScale *AutoscaleResult
	}{
		{
			name: "Zero queue length",
			sample: &AutoscalerSample{
				QueueLength: 0,
			},
			expectedScale: &AutoscaleResult{
				DesiredContainers: 0,
				ResultValid:       true,
			},
		},
		{
			name: "Queue length less than MaxTasksPerReplica",
			sample: &AutoscalerSample{
				QueueLength: 3,
			},
			expectedScale: &AutoscaleResult{
				DesiredContainers: 1,
				ResultValid:       true,
			},
		},
		{
			name: "Queue length equal to MaxTasksPerReplica",
			sample: &AutoscalerSample{
				QueueLength: 5,
			},
			expectedScale: &AutoscaleResult{
				DesiredContainers: 1,
				ResultValid:       true,
			},
		},
		{
			name: "Queue length greater than MaxTasksPerReplica",
			sample: &AutoscalerSample{
				QueueLength: 14,
			},
			expectedScale: &AutoscaleResult{
				DesiredContainers: 3,
				ResultValid:       true,
			},
		},
		{
			name: "Autoscaler result should not exceed max_replicas",
			sample: &AutoscalerSample{
				QueueLength: 40,
			},
			expectedScale: &AutoscaleResult{
				DesiredContainers: 3,
				ResultValid:       true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := as.scaleByQueueDepth(tt.sample)
			if result.DesiredContainers != tt.expectedScale.DesiredContainers || result.ResultValid != tt.expectedScale.ResultValid {
				t.Errorf("Expected %+v, got %+v", tt.expectedScale, result)
			}
		})
	}
}
