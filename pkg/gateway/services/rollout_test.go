package gatewayservices

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestDeploymentRolloutAppliesToAnyAlwaysOnDeployment(t *testing.T) {
	latestConfig := types.StubConfigV1{
		Autoscaler: &types.Autoscaler{MinContainers: 1},
	}
	configBytes, err := json.Marshal(latestConfig)
	if err != nil {
		t.Fatal(err)
	}

	for _, stubType := range []types.StubType{
		types.StubType(types.StubTypeEndpointDeployment),
		types.StubType(types.StubTypeFunctionDeployment),
		types.StubType(types.StubTypePodDeployment),
	} {
		if !deploymentRolloutApplies(deploymentRolloutInput{
			stub:   &types.StubWithRelated{Stub: types.Stub{Type: stubType}},
			config: &types.StubConfigV1{Autoscaler: &types.Autoscaler{MinContainers: 1}},
			latest: &types.DeploymentWithRelated{
				Deployment: types.Deployment{Active: true},
				Stub:       types.Stub{Type: stubType, Config: string(configBytes)},
			},
		}) {
			t.Fatalf("expected rollout to apply to %s", stubType)
		}
	}
}

func TestExplicitReplaceRolloutStopsActiveWarmDeployment(t *testing.T) {
	oldConfig := types.StubConfigV1{KeepWarmSeconds: 3600}
	oldConfigBytes, err := json.Marshal(oldConfig)
	if err != nil {
		t.Fatal(err)
	}

	workerRepo := &rolloutWorkerRepo{workers: []*types.Worker{{
		Id:            "worker-1",
		PoolName:      "private-gpu",
		Gpu:           string(types.GPU_B200),
		TotalCpu:      16_000,
		FreeCpu:       12_000,
		TotalMemory:   64 * 1024,
		FreeMemory:    48 * 1024,
		TotalGpuCount: 8,
		FreeGpuCount:  3,
		Status:        types.WorkerStatusAvailable,
	}}}
	gws := &GatewayService{
		scheduler: scheduler.NewSchedulerForCapacityChecks(workerRepo, nil, nil),
		containerRepo: &rolloutContainerRepo{containers: []types.ContainerState{{
			ContainerId: "old-container",
			StubId:      "old-stub",
			WorkerId:    "worker-1",
			Cpu:         4_000,
			Memory:      16 * 1024,
			Gpu:         string(types.GPU_B200),
			GpuCount:    5,
			Status:      types.ContainerStatusRunning,
		}}},
	}

	plan, err := gws.planDeploymentRollout(deploymentRolloutInput{
		stub: &types.StubWithRelated{Stub: types.Stub{
			ExternalId: "new-stub",
			Type:       types.StubType(types.StubTypeEndpointDeployment),
		}},
		config: &types.StubConfigV1{
			KeepWarmSeconds: 3600,
			Runtime: types.Runtime{
				Cpu:      4_000,
				Memory:   16 * 1024,
				Gpu:      types.GPU_B200,
				GpuCount: 5,
			},
			Pool: &types.PoolConfig{Name: "private-gpu"},
		},
		latest: &types.DeploymentWithRelated{
			Deployment: types.Deployment{Active: true},
			Stub: types.Stub{
				ExternalId: "old-stub",
				Type:       types.StubType(types.StubTypeEndpointDeployment),
				Config:     string(oldConfigBytes),
			},
		},
		mode: rolloutModeReplace,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.stopLatest || plan.action != rolloutActionReplace {
		t.Fatalf("plan = %+v, want replace with stopLatest", plan)
	}
}

func TestActiveDeploymentsForRolloutUsesExactActiveMatches(t *testing.T) {
	repo := &rolloutDeploymentRepo{deployments: []types.DeploymentWithRelated{
		{Deployment: types.Deployment{ExternalId: "v1", WorkspaceId: 1, Name: "service", Active: true, StubType: string(types.StubTypeEndpointDeployment), Version: 1}},
		{Deployment: types.Deployment{ExternalId: "v2", WorkspaceId: 1, Name: "service", Active: true, StubType: string(types.StubTypeEndpointDeployment), Version: 2}},
		{Deployment: types.Deployment{ExternalId: "inactive", WorkspaceId: 1, Name: "service", Active: false, StubType: string(types.StubTypeEndpointDeployment), Version: 3}},
		{Deployment: types.Deployment{ExternalId: "similar", WorkspaceId: 1, Name: "service-extra", Active: true, StubType: string(types.StubTypeEndpointDeployment), Version: 4}},
		{Deployment: types.Deployment{ExternalId: "wrong-type", WorkspaceId: 1, Name: "service", Active: true, StubType: string(types.StubTypeFunctionDeployment), Version: 5}},
		{Deployment: types.Deployment{ExternalId: "wrong-workspace", WorkspaceId: 2, Name: "service", Active: true, StubType: string(types.StubTypeEndpointDeployment), Version: 6}},
	}}
	gws := &GatewayService{backendRepo: repo}

	active, err := gws.activeDeploymentsForRollout(context.Background(), 1, "service", string(types.StubTypeEndpointDeployment))
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 2 {
		t.Fatalf("active deployments = %d, want 2: %+v", len(active), active)
	}

	latest := latestActiveDeployment(active)
	if latest == nil || latest.ExternalId != "v2" {
		t.Fatalf("latest active = %+v, want v2", latest)
	}
}

type rolloutWorkerRepo struct {
	repository.WorkerRepository
	workers []*types.Worker
}

func (r *rolloutWorkerRepo) GetAllWorkers() ([]*types.Worker, error) {
	return r.workers, nil
}

type rolloutContainerRepo struct {
	repository.ContainerRepository
	containers []types.ContainerState
}

func (r *rolloutContainerRepo) GetActiveContainersByStubId(stubId string) ([]types.ContainerState, error) {
	containers := make([]types.ContainerState, 0, len(r.containers))
	for _, container := range r.containers {
		if container.StubId == stubId {
			containers = append(containers, container)
		}
	}
	return containers, nil
}

type rolloutDeploymentRepo struct {
	repository.BackendRepository
	deployments []types.DeploymentWithRelated
}

func (r *rolloutDeploymentRepo) ListDeploymentsWithRelated(_ context.Context, _ types.DeploymentFilter) ([]types.DeploymentWithRelated, error) {
	return r.deployments, nil
}
