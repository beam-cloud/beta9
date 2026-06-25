package pod

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
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

func TestDesiredPodLLMDeploymentContainersScalesOnActiveStreams(t *testing.T) {
	config := podLLMDeploymentConfig(1, 5, 2, 4096)
	sample := &podAutoscalerSample{
		TotalConnections: 1,
		LLMPressure:      llmPressureSnapshot{ActiveStreams: 5, TokenPressure: 1024},
	}

	got := desiredPodLLMDeploymentContainers(config, sample, 0)

	if got != 3 {
		t.Fatalf("desired containers = %d, want 3", got)
	}
}

func TestDesiredPodLLMDeploymentContainersScalesOnTokenPressure(t *testing.T) {
	config := podLLMDeploymentConfig(0, 5, 1, 4096)
	sample := &podAutoscalerSample{
		TotalConnections: 1,
		LLMPressure:      llmPressureSnapshot{ActiveStreams: 1, TokenPressure: 9000},
	}

	got := desiredPodLLMDeploymentContainers(config, sample, 0)

	if got != 3 {
		t.Fatalf("desired containers = %d, want 3", got)
	}
}

func TestDesiredPodLLMDeploymentContainersUsesIdleMinimum(t *testing.T) {
	config := podLLMDeploymentConfig(2, 5, 1, 4096)

	got := desiredPodLLMDeploymentContainers(config, &podAutoscalerSample{}, 0)

	if got != 2 {
		t.Fatalf("desired containers = %d, want 2", got)
	}
}

func TestPodAutoscalerSampleUsesSharedConnectionSnapshots(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	workspace := &types.Workspace{Name: "workspace"}
	stub := &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub"}}
	buffer := &PodProxyBuffer{}
	buffer.totalConnections.Store(1)

	if err := rdb.Set(ctx, Keys.podProxyConnections(workspace.Name, stub.ExternalId, "proxy-1", "total"), 2, connectionSnapshotTTL).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.SAdd(ctx, Keys.podProxyConnectionIndex(workspace.Name, stub.ExternalId), "proxy-1").Err(); err != nil {
		t.Fatal(err)
	}

	instance := &podInstance{
		AutoscaledInstance: &abstractions.AutoscaledInstance{
			Ctx:           ctx,
			Rdb:           rdb,
			Workspace:     workspace,
			Stub:          stub,
			ContainerRepo: repository.NewContainerRedisRepositoryForTest(rdb),
		},
		buffer: buffer,
	}

	sample, err := podAutoscalerSampleFunc(instance)
	if err != nil {
		t.Fatal(err)
	}
	if sample.TotalConnections != 2 {
		t.Fatalf("total connections = %d, want shared Redis aggregate", sample.TotalConnections)
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

func podLLMDeploymentConfig(minContainers, maxContainers, tasksPerContainer uint, contextLength int) *types.StubConfigV1 {
	config := podDeploymentConfig(minContainers, maxContainers, tasksPerContainer)
	config.Autoscaler.Type = types.LLMTokenPressureAutoscaler
	config.ServingProtocol = "openai"
	config.LLM = &types.LLMConfig{ContextLength: contextLength}
	return config
}
