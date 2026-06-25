package gatewayservices

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func TestNewGatewayServiceRequiresComputeRepoOrRedis(t *testing.T) {
	_, err := NewGatewayService(&GatewayServiceOpts{})
	if err == nil {
		t.Fatal("expected missing compute repository and redis client to fail")
	}
}

func TestConfigurePoolSelectorNamesReservedPool(t *testing.T) {
	pool := poolConfigFromProto(&pb.PoolConfig{
		Nodes:     1,
		Ttl:       "1h",
		MaxSpend:  2,
		Providers: []string{"shadeform"},
	})

	configurePoolSelector(pool, "workspace-1", "handler")
	if !pool.RequiresReservation() {
		t.Fatal("test setup expected pool to require reservation")
	}
	if got, want := pool.Selector, "private-workspace-1-handler"; got != want {
		t.Fatalf("selector = %q, want %q", got, want)
	}
	if got, want := pool.Name, pool.Selector; got != want {
		t.Fatalf("name = %q, want selector %q", got, want)
	}
}

func TestNormalizeKeepWarmSeconds(t *testing.T) {
	tests := []struct {
		name     string
		raw      float32
		stubType types.StubType
		want     int
	}{
		{
			name:     "zero means immediate scale to zero for pod deployments",
			raw:      0,
			stubType: types.StubType(types.StubTypePodDeployment),
			want:     0,
		},
		{
			name:     "positive values have a small minimum",
			raw:      1,
			stubType: types.StubType(types.StubTypePodDeployment),
			want:     10,
		},
		{
			name:     "negative is preserved for pods",
			raw:      -1,
			stubType: types.StubType(types.StubTypePodDeployment),
			want:     -1,
		},
		{
			name:     "negative is not persisted for non-pod stubs",
			raw:      -1,
			stubType: types.StubType(types.StubTypeEndpointDeployment),
			want:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeKeepWarmSeconds(tt.raw, tt.stubType); got != tt.want {
				t.Fatalf("normalizeKeepWarmSeconds() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestAutoscalerFromProtoDefaultsWhenOmitted(t *testing.T) {
	autoscaler := autoscalerFromProto(nil)

	if got, want := autoscaler.Type, types.QueueDepthAutoscaler; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
	if got, want := autoscaler.MaxContainers, uint(1); got != want {
		t.Fatalf("max containers = %d, want %d", got, want)
	}
	if got, want := autoscaler.TasksPerContainer, uint(1); got != want {
		t.Fatalf("tasks per container = %d, want %d", got, want)
	}
}

func TestConfigurePodDeploymentAutoscalerPreservesReplicaBounds(t *testing.T) {
	autoscaler := &types.Autoscaler{
		Type:              types.QueueDepthAutoscaler,
		MaxContainers:     4,
		MinContainers:     2,
		TasksPerContainer: 1,
	}

	if err := configurePodDeploymentAutoscaler(autoscaler, 0, 0); err != nil {
		t.Fatalf("configurePodDeploymentAutoscaler() error = %v", err)
	}

	if got, want := autoscaler.MinContainers, uint(2); got != want {
		t.Fatalf("min containers = %d, want %d", got, want)
	}
	if got, want := autoscaler.MaxContainers, uint(4); got != want {
		t.Fatalf("max containers = %d, want %d", got, want)
	}
}

func TestConfigurePodDeploymentAutoscalerBackfillsLegacyAlwaysOn(t *testing.T) {
	autoscaler := &types.Autoscaler{
		Type:              types.QueueDepthAutoscaler,
		MaxContainers:     1,
		MinContainers:     0,
		TasksPerContainer: 1,
	}

	if err := configurePodDeploymentAutoscaler(autoscaler, -1, 0); err != nil {
		t.Fatalf("configurePodDeploymentAutoscaler() error = %v", err)
	}

	if got, want := autoscaler.MinContainers, uint(1); got != want {
		t.Fatalf("min containers = %d, want %d", got, want)
	}
	if got, want := autoscaler.MaxContainers, uint(1); got != want {
		t.Fatalf("max containers = %d, want %d", got, want)
	}
}

func TestConfigurePodDeploymentAutoscalerRejectsMinAboveMax(t *testing.T) {
	autoscaler := &types.Autoscaler{
		Type:              types.QueueDepthAutoscaler,
		MaxContainers:     1,
		MinContainers:     3,
		TasksPerContainer: 1,
	}

	if err := configurePodDeploymentAutoscaler(autoscaler, 0, 0); err == nil {
		t.Fatal("configurePodDeploymentAutoscaler() error = nil, want error")
	}
}

func TestConfigurePodDeploymentAutoscalerRejectsMaxAboveLimit(t *testing.T) {
	autoscaler := &types.Autoscaler{
		Type:              types.QueueDepthAutoscaler,
		MaxContainers:     11,
		MinContainers:     1,
		TasksPerContainer: 1,
	}

	if err := configurePodDeploymentAutoscaler(autoscaler, 0, 10); err == nil {
		t.Fatal("configurePodDeploymentAutoscaler() error = nil, want error")
	}
}
