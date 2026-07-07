package gatewayservices

import (
	"context"
	"strings"
	"testing"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type privatePoolPolicyComputeRepo struct {
	repository.ComputeRepository
	pools map[string]*model.PoolState
}

func (r *privatePoolPolicyComputeRepo) GetPoolState(_ context.Context, workspaceID, name string) (*model.PoolState, error) {
	return r.pools[workspaceID+"/"+name], nil
}

func (r *privatePoolPolicyComputeRepo) ListPoolStates(_ context.Context, workspaceID string, _ int) ([]*model.PoolState, error) {
	pools := []*model.PoolState{}
	for key, pool := range r.pools {
		if strings.HasPrefix(key, workspaceID+"/") {
			pools = append(pools, pool)
		}
	}
	return pools, nil
}

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

func TestPrivatePoolPolicyRequiresStrictFallbackForManagedLimitBypass(t *testing.T) {
	gws := &GatewayService{
		appConfig: types.AppConfig{GatewayService: types.GatewayServiceConfig{StubLimits: types.StubLimits{
			Cpu:         1000,
			Memory:      1024,
			MaxGpuCount: 1,
		}}},
		computeRepo: &privatePoolPolicyComputeRepo{pools: map[string]*model.PoolState{
			"workspace-1/large-gpu-pool":      {Name: "large-gpu-pool", Mode: string(types.PoolModePrivate)},
			"workspace-1/large-gpu-pool-fail": {Name: "large-gpu-pool-fail", Mode: string(types.PoolModePrivate), Fallback: types.PrivatePoolFallbackFail},
			"workspace-1/stored-pool":         {Name: "stored-pool", Selector: "selector-only", Mode: string(types.PoolModePrivate), Fallback: types.PrivatePoolFallbackFail},
		}},
	}

	policy, err := gws.stubResourcePolicy(context.Background(), "workspace-1", &pb.PoolConfig{Name: "large-gpu-pool"}, "handler")
	if err != nil {
		t.Fatal(err)
	}
	if !policy.privatePoolTargeted {
		t.Fatal("expected existing private pool to be targeted")
	}
	if policy.privatePoolOnly() {
		t.Fatal("expected internal fallback to keep managed limits enforced")
	}
	if policy.fallback != types.PrivatePoolFallbackInternal {
		t.Fatal("expected omitted fallback to default to internal")
	}
	request := &pb.GetOrCreateStubRequest{Cpu: 2000, Memory: 2048, Gpu: "H100", GpuCount: 8}
	if got := policy.validateManagedLimits(gws, request, &types.Workspace{}); got == "" {
		t.Fatal("expected managed stub limit error")
	}

	failPolicy, err := gws.stubResourcePolicy(context.Background(), "workspace-1", &pb.PoolConfig{Name: "large-gpu-pool-fail"}, "handler")
	if err != nil {
		t.Fatal(err)
	}
	if !failPolicy.privatePoolOnly() {
		t.Fatal("expected fail fallback private pool to bypass managed limits")
	}
	if got := failPolicy.validateManagedLimits(gws, request, &types.Workspace{}); got != "" {
		t.Fatalf("managed limit error = %q, want empty", got)
	}

	selectorPolicy, err := gws.stubResourcePolicy(context.Background(), "workspace-1", &pb.PoolConfig{Selector: "selector-only"}, "handler")
	if err != nil {
		t.Fatal(err)
	}
	if !selectorPolicy.privatePoolOnly() {
		t.Fatal("expected selector-only private pool lookup to bypass managed limits")
	}
	if got := selectorPolicy.validateManagedLimits(gws, request, &types.Workspace{}); got != "" {
		t.Fatalf("selector managed limit error = %q, want empty", got)
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

func TestGpuTypesForStubRequestDefaultsCountOnlyToAnyGPU(t *testing.T) {
	gpus := gpuTypesForStubRequest(&pb.GetOrCreateStubRequest{GpuCount: 1})

	if len(gpus) != 1 || gpus[0] != types.GPU_ANY {
		t.Fatalf("gpus = %#v, want any", gpus)
	}
}

func TestGpuTypesForStubRequestPreservesExplicitGPU(t *testing.T) {
	gpus := gpuTypesForStubRequest(&pb.GetOrCreateStubRequest{Gpu: "L4", GpuCount: 1})

	if len(gpus) != 1 || gpus[0] != types.GPU_L4 {
		t.Fatalf("gpus = %#v, want L4", gpus)
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
