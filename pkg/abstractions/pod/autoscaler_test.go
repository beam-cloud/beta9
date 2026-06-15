package pod

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestDesiredPodDeploymentContainersKeepsIdleMinimum(t *testing.T) {
	config := podDeploymentConfig(2, 4, 1)

	got := desiredPodDeploymentContainers(config, 0, 0)

	if got != 2 {
		t.Fatalf("desired containers = %d, want 2", got)
	}
}

func TestDesiredPodDeploymentContainersScalesWithinBounds(t *testing.T) {
	config := podDeploymentConfig(2, 4, 1)

	tests := []struct {
		name        string
		connections int64
		want        int
	}{
		{name: "one connection stays at minimum", connections: 1, want: 2},
		{name: "three connections scale to three", connections: 3, want: 3},
		{name: "many connections cap at maximum", connections: 99, want: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := desiredPodDeploymentContainers(config, tt.connections, 0); got != tt.want {
				t.Fatalf("desired containers = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestDesiredPodDeploymentContainersRespectsTasksPerContainer(t *testing.T) {
	config := podDeploymentConfig(1, 4, 2)

	got := desiredPodDeploymentContainers(config, 3, 0)

	if got != 2 {
		t.Fatalf("desired containers = %d, want 2", got)
	}
}

func TestDesiredPodDeploymentContainersAllowsFixedZeroReplicas(t *testing.T) {
	config := podDeploymentConfig(0, 0, 1)

	got := desiredPodDeploymentContainers(config, 10, 0)

	if got != 0 {
		t.Fatalf("desired containers = %d, want 0", got)
	}
}

func TestDesiredPodDeploymentContainersAppliesGlobalReplicaLimit(t *testing.T) {
	config := podDeploymentConfig(1, 8, 1)

	got := desiredPodDeploymentContainers(config, 8, 3)

	if got != 3 {
		t.Fatalf("desired containers = %d, want 3", got)
	}
}

func podDeploymentConfig(minContainers, maxContainers, tasksPerContainer uint) *types.StubConfigV1 {
	return &types.StubConfigV1{
		Autoscaler: &types.Autoscaler{
			Type:              types.QueueDepthAutoscaler,
			MinContainers:     minContainers,
			MaxContainers:     maxContainers,
			TasksPerContainer: tasksPerContainer,
		},
	}
}
