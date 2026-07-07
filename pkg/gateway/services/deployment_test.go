package gatewayservices

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type deploymentLifecycleBackendRepo struct {
	repository.BackendRepository
	deployments map[string]types.DeploymentWithRelated
	stubs       map[string]types.StubWithRelated
	stubConfigs map[uint]*types.StubConfigV1
	snapshots   map[string]*types.DiskSnapshot
	created     []types.Deployment
}

func newDeploymentLifecycleGateway(t *testing.T) (*GatewayService, *deploymentLifecycleBackendRepo) {
	t.Helper()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)

	config := types.StubConfigV1{
		Autoscaler: &types.Autoscaler{
			Type:              types.QueueDepthAutoscaler,
			MinContainers:     0,
			MaxContainers:     1,
			TasksPerContainer: 1,
		},
	}
	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	now := types.Time{Time: time.Now()}
	deployment := types.DeploymentWithRelated{
		Deployment: types.Deployment{
			Id:          10,
			ExternalId:  "deployment-id",
			Name:        "service-probe",
			Active:      true,
			WorkspaceId: 1,
			StubId:      20,
			StubType:    string(types.StubTypePodDeployment),
			Version:     1,
			AppId:       30,
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		Workspace: types.Workspace{
			Id:         1,
			ExternalId: "workspace-id",
			Name:       "workspace",
		},
		Stub: types.Stub{
			Id:         20,
			ExternalId: "stub-id",
			Name:       "pod/deployment",
			Type:       types.StubType(types.StubTypePodDeployment),
			Config:     string(configBytes),
			AppId:      30,
		},
		App: types.App{
			Id:         30,
			ExternalId: "app-id",
			Name:       "app",
		},
	}

	backend := &deploymentLifecycleBackendRepo{
		deployments: map[string]types.DeploymentWithRelated{
			deployment.ExternalId: deployment,
		},
		stubs: map[string]types.StubWithRelated{
			deployment.Stub.ExternalId: {
				Stub:      deployment.Stub,
				Workspace: deployment.Workspace,
				App:       &deployment.App,
			},
		},
		stubConfigs: map[uint]*types.StubConfigV1{},
		snapshots:   map[string]*types.DiskSnapshot{},
	}

	return &GatewayService{
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				Host:       "localhost",
				StubLimits: types.StubLimits{MaxReplicas: 10},
			},
		},
		backendRepo:   backend,
		containerRepo: &deploymentLifecycleContainerRepo{},
		computeRepo: &deploymentLifecycleComputeRepo{states: map[string]*compute.PoolState{
			"private-gpu": {Name: "private-gpu", Mode: string(types.PoolModePrivate)},
		}},
		redisClient: rdb,
	}, backend
}

func (r *deploymentLifecycleBackendRepo) ListDeploymentsWithRelated(_ context.Context, filters types.DeploymentFilter) ([]types.DeploymentWithRelated, error) {
	deployments := make([]types.DeploymentWithRelated, 0, len(r.deployments))
	for _, deployment := range r.deployments {
		if filters.WorkspaceID != 0 && deployment.Deployment.WorkspaceId != filters.WorkspaceID {
			continue
		}
		if filters.Active != nil && deployment.Active != *filters.Active {
			continue
		}
		deployments = append(deployments, deployment)
	}
	return deployments, nil
}

func (r *deploymentLifecycleBackendRepo) GetDeploymentByExternalId(_ context.Context, workspaceID uint, deploymentExternalID string) (*types.DeploymentWithRelated, error) {
	deployment, ok := r.deployments[deploymentExternalID]
	if !ok || deployment.Deployment.WorkspaceId != workspaceID {
		return nil, nil
	}
	return &deployment, nil
}

func (r *deploymentLifecycleBackendRepo) GetLatestDeploymentByName(_ context.Context, workspaceID uint, name string, stubType string, _ bool) (*types.DeploymentWithRelated, error) {
	var latest *types.DeploymentWithRelated
	for _, deployment := range r.deployments {
		if deployment.Deployment.WorkspaceId != workspaceID || deployment.Name != name || deployment.StubType != stubType {
			continue
		}
		if latest == nil || deployment.Version > latest.Version {
			next := deployment
			latest = &next
		}
	}
	return latest, nil
}

func (r *deploymentLifecycleBackendRepo) GetStubByExternalId(_ context.Context, stubExternalID string, _ ...types.QueryFilter) (*types.StubWithRelated, error) {
	stub, ok := r.stubs[stubExternalID]
	if !ok {
		return nil, nil
	}
	return &stub, nil
}

func (r *deploymentLifecycleBackendRepo) CreateDeployment(_ context.Context, workspaceID uint, name string, version uint, stubID uint, stubType string, appID uint) (*types.Deployment, error) {
	deployment := types.Deployment{
		Id:          uint(100 + len(r.created)),
		ExternalId:  fmt.Sprintf("deployment-%d", version),
		Name:        name,
		Active:      true,
		WorkspaceId: workspaceID,
		StubId:      stubID,
		StubType:    stubType,
		Version:     version,
		AppId:       appID,
	}
	r.created = append(r.created, deployment)
	return &deployment, nil
}

func (r *deploymentLifecycleBackendRepo) UpdateDeployment(_ context.Context, deployment types.Deployment) (*types.Deployment, error) {
	existing, ok := r.deployments[deployment.ExternalId]
	if !ok {
		return nil, nil
	}
	existing.Deployment = deployment
	r.deployments[deployment.ExternalId] = existing
	return &deployment, nil
}

func (r *deploymentLifecycleBackendRepo) DeleteDeployment(_ context.Context, deployment types.Deployment) error {
	delete(r.deployments, deployment.ExternalId)
	return nil
}

func (r *deploymentLifecycleBackendRepo) UpdateStubConfig(_ context.Context, stubID uint, config *types.StubConfigV1) error {
	r.stubConfigs[stubID] = config
	deployment := r.deployments["deployment-id"]
	configBytes, err := json.Marshal(config)
	if err != nil {
		return err
	}
	deployment.Stub.Config = string(configBytes)
	r.deployments[deployment.ExternalId] = deployment
	return nil
}

func (r *deploymentLifecycleBackendRepo) GetLatestDiskSnapshot(_ context.Context, workspaceID uint, diskName string) (*types.DiskSnapshot, error) {
	snapshot, ok := r.snapshots[diskName]
	if !ok || snapshot.WorkspaceId != workspaceID {
		return nil, &types.ErrDiskSnapshotNotFound{SnapshotId: diskName}
	}
	return snapshot, nil
}

func setDeploymentStubConfig(t *testing.T, backend *deploymentLifecycleBackendRepo, config types.StubConfigV1) {
	t.Helper()

	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	deployment := backend.deployments["deployment-id"]
	deployment.Stub.Config = string(configBytes)
	backend.deployments[deployment.ExternalId] = deployment
	stub := backend.stubs[deployment.Stub.ExternalId]
	stub.Stub.Config = string(configBytes)
	backend.stubs[deployment.Stub.ExternalId] = stub
}

type deploymentLifecycleComputeRepo struct {
	repository.ComputeRepository
	states map[string]*compute.PoolState
}

func (r *deploymentLifecycleComputeRepo) GetPoolState(_ context.Context, _ string, name string) (*compute.PoolState, error) {
	return r.states[name], nil
}

type deploymentLifecycleContainerRepo struct {
	repository.ContainerRepository
	active []types.ContainerState
}

func (r *deploymentLifecycleContainerRepo) GetActiveContainersByStubId(stubID string) ([]types.ContainerState, error) {
	out := []types.ContainerState{}
	for _, container := range r.active {
		if container.StubId == stubID {
			out = append(out, container)
		}
	}
	return out, nil
}

type deploymentRolloutWorkerRepo struct {
	repository.WorkerRepository
	workers []*types.Worker
}

func (r *deploymentRolloutWorkerRepo) GetAllWorkers() ([]*types.Worker, error) {
	return r.workers, nil
}

func configurePrivateRolloutTest(t *testing.T, gws *GatewayService, backend *deploymentLifecycleBackendRepo, cpu int64, memory int64, workers []*types.Worker, active []types.ContainerState) {
	t.Helper()

	config := types.StubConfigV1{
		Runtime: types.Runtime{
			Cpu:    cpu,
			Memory: memory,
		},
		Autoscaler: &types.Autoscaler{
			Type:              types.QueueDepthAutoscaler,
			MinContainers:     1,
			MaxContainers:     1,
			TasksPerContainer: 1,
		},
		Pool: &types.PoolConfig{Name: "private-gpu", Selector: "private-gpu", Fallback: types.PrivatePoolFallbackFail},
	}
	setDeploymentStubConfig(t, backend, config)

	containerRepo := &deploymentLifecycleContainerRepo{active: active}
	workerRepo := &deploymentRolloutWorkerRepo{workers: workers}
	manager := scheduler.NewWorkerPoolManager(false)
	manager.SetPool("private-gpu", types.WorkerPoolConfig{Mode: types.PoolModePrivate, RequiresPoolSelector: true}, nil)

	gws.containerRepo = containerRepo
	gws.workerRepo = workerRepo
	gws.scheduler = scheduler.NewSchedulerForCapacityChecks(workerRepo, gws.computeRepo, manager)
}

func TestPodDeploymentLifecycleUsesListedDeploymentID(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	ctx := deploymentLifecycleContext(types.TokenTypeWorkspace)

	listResp, err := gws.ListDeployments(ctx, &pb.ListDeploymentsRequest{})
	require.NoError(t, err)
	require.True(t, listResp.Ok)
	require.Len(t, listResp.Deployments, 1)

	deploymentID := listResp.Deployments[0].Id
	require.Equal(t, "deployment-id", deploymentID)

	stopResp, err := gws.StopDeployment(ctx, &pb.StopDeploymentRequest{Id: deploymentID})
	require.NoError(t, err)
	require.True(t, stopResp.Ok, stopResp.ErrMsg)
	require.False(t, backend.deployments[deploymentID].Active)

	deleteResp, err := gws.DeleteDeployment(ctx, &pb.DeleteDeploymentRequest{Id: deploymentID})
	require.NoError(t, err)
	require.True(t, deleteResp.Ok, deleteResp.ErrMsg)
	require.Empty(t, backend.deployments)
}

func TestPrivatePoolRedeployReportsBlueGreenWhenSpareCapacityExists(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	configurePrivateRolloutTest(t, gws, backend, 1000, 1024, []*types.Worker{{
		Id:                   "worker-1",
		PoolName:             "private-gpu",
		Status:               types.WorkerStatusAvailable,
		TotalCpu:             2000,
		FreeCpu:              1000,
		TotalMemory:          4096,
		FreeMemory:           2048,
		RequiresPoolSelector: true,
	}}, []types.ContainerState{{
		StubId:   "stub-id",
		WorkerId: "worker-1",
		Cpu:      1000,
		Memory:   1024,
	}})

	resp, err := gws.DeployStub(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.DeployStubRequest{
		StubId: "stub-id",
		Name:   "service-probe",
	})
	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Equal(t, rolloutActionBlueGreen, resp.RolloutAction)
	require.Empty(t, resp.WarnMsg)
	require.True(t, backend.deployments["deployment-id"].Active)
}

func TestPrivatePoolRedeployWarnsAndReplacesWhenOnlyOldCapacityFits(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	configurePrivateRolloutTest(t, gws, backend, 1000, 1024, []*types.Worker{{
		Id:                   "worker-1",
		PoolName:             "private-gpu",
		Status:               types.WorkerStatusAvailable,
		TotalCpu:             1000,
		FreeCpu:              0,
		TotalMemory:          2048,
		FreeMemory:           768,
		RequiresPoolSelector: true,
	}}, []types.ContainerState{{
		StubId:   "stub-id",
		WorkerId: "worker-1",
		Cpu:      1000,
		Memory:   1280,
	}})

	resp, err := gws.DeployStub(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.DeployStubRequest{
		StubId: "stub-id",
		Name:   "service-probe",
	})
	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Equal(t, rolloutActionReplace, resp.RolloutAction)
	require.Contains(t, resp.WarnMsg, "Expected downtime")
	require.False(t, backend.deployments["deployment-id"].Active)
}

func TestPrivatePoolRedeployBlocksWhenReplacementCannotFit(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	configurePrivateRolloutTest(t, gws, backend, 2000, 1024, []*types.Worker{{
		Id:                   "worker-1",
		PoolName:             "private-gpu",
		Status:               types.WorkerStatusAvailable,
		TotalCpu:             1000,
		FreeCpu:              0,
		TotalMemory:          2048,
		FreeMemory:           768,
		RequiresPoolSelector: true,
	}}, []types.ContainerState{{
		StubId:   "stub-id",
		WorkerId: "worker-1",
		Cpu:      1000,
		Memory:   1280,
	}})

	resp, err := gws.DeployStub(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.DeployStubRequest{
		StubId: "stub-id",
		Name:   "service-probe",
	})
	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrMsg, "not predicted to fit")
	require.Empty(t, backend.created)
	require.True(t, backend.deployments["deployment-id"].Active)
}

func TestPrivatePoolRedeployBlueGreenModeFailsWithoutSpareCapacity(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	configurePrivateRolloutTest(t, gws, backend, 1000, 1024, []*types.Worker{{
		Id:                   "worker-1",
		PoolName:             "private-gpu",
		Status:               types.WorkerStatusAvailable,
		TotalCpu:             1000,
		FreeCpu:              0,
		TotalMemory:          2048,
		FreeMemory:           768,
		RequiresPoolSelector: true,
	}}, []types.ContainerState{{
		StubId:   "stub-id",
		WorkerId: "worker-1",
		Cpu:      1000,
		Memory:   1280,
	}})

	resp, err := gws.DeployStub(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.DeployStubRequest{
		StubId:  "stub-id",
		Name:    "service-probe",
		Rollout: rolloutModeBlueGreen,
	})
	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrMsg, "blue-green rollout requires enough spare capacity")
	require.Empty(t, backend.created)
	require.True(t, backend.deployments["deployment-id"].Active)
}

func TestListDeploymentsIncludesDatabaseMetadata(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	setDeploymentStubConfig(t, backend, types.StubConfigV1{
		Serving: &types.ServingConfig{
			AppKind:         "database",
			ServingProtocol: "redis",
			Database: &types.DatabaseServingConfig{
				Kind:                    "redis",
				ConnectionEnvName:       "REDIS_URL",
				ConnectionURLSecretName: "BETA9_REDIS_TEST_URL",
			},
		},
	})

	resp, err := gws.ListDeployments(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.ListDeploymentsRequest{})

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.Len(t, resp.Deployments, 1)
	require.Equal(t, "redis", resp.Deployments[0].DatabaseKind)
	require.Equal(t, "REDIS_URL", resp.Deployments[0].ConnectionEnvName)
	require.Equal(t, "BETA9_REDIS_TEST_URL", resp.Deployments[0].ConnectionStringSecret)
}

func TestScalePodDeploymentUpdatesFixedReplicaBounds(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)

	resp, err := gws.ScaleDeployment(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.ScaleDeploymentRequest{
		Id:         "deployment-id",
		Containers: 3,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Equal(t, uint(3), backend.stubConfigs[20].Autoscaler.MinContainers)
	require.Equal(t, uint(3), backend.stubConfigs[20].Autoscaler.MaxContainers)
}

func TestScalePodDeploymentZeroEnablesScaleToZero(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)

	resp, err := gws.ScaleDeployment(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.ScaleDeploymentRequest{
		Id:         "deployment-id",
		Containers: 0,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Equal(t, uint(0), backend.stubConfigs[20].Autoscaler.MinContainers)
	require.Equal(t, uint(1), backend.stubConfigs[20].Autoscaler.MaxContainers)
}

func TestScalePodDeploymentPreservesLLMAutoscaler(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	setDeploymentStubConfig(t, backend, types.StubConfigV1{
		Autoscaler: &types.Autoscaler{
			Type:              types.LLMTokenPressureAutoscaler,
			MinContainers:     1,
			MaxContainers:     4,
			TasksPerContainer: 16,
		},
	})

	resp, err := gws.ScaleDeployment(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.ScaleDeploymentRequest{
		Id:         "deployment-id",
		Containers: 2,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Equal(t, types.LLMTokenPressureAutoscaler, backend.stubConfigs[20].Autoscaler.Type)
	require.Equal(t, uint(16), backend.stubConfigs[20].Autoscaler.TasksPerContainer)
	require.Equal(t, uint(2), backend.stubConfigs[20].Autoscaler.MinContainers)
	require.Equal(t, uint(2), backend.stubConfigs[20].Autoscaler.MaxContainers)
}

func TestScalePodDeploymentRestoresDurableDiskToRegularPoolWhenPrivatePoolGone(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	gws.computeRepo = &deploymentLifecycleComputeRepo{states: map[string]*compute.PoolState{
		"pool-a": {Name: "pool-a", Mode: string(types.PoolModePrivate)},
	}}
	backend.snapshots["pg-data"] = &types.DiskSnapshot{
		WorkspaceId: 1,
		DiskName:    "pg-data",
		Format:      types.DiskSnapshotFormatDirV1,
		Status:      types.DiskSnapshotStatusAvailable,
		ManifestKey: "durable-disks/pg-data/snapshots/1/manifest.json",
	}

	config := types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Autoscaler: &types.Autoscaler{
			Type:              types.QueueDepthAutoscaler,
			MinContainers:     0,
			MaxContainers:     1,
			TasksPerContainer: 1,
		},
		Disks: []*pb.DurableDisk{{
			Name:   "pg-data",
			Driver: types.DurableDiskDriverSnapshot,
		}},
	}
	setDeploymentStubConfig(t, backend, config)

	resp, err := gws.ScaleDeployment(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.ScaleDeploymentRequest{
		Id:         "deployment-id",
		Containers: 1,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Nil(t, backend.stubConfigs[20].Pool)
	require.Equal(t, types.DurableDiskDriverSnapshot, backend.stubConfigs[20].Disks[0].Driver)
}

func TestScalePodDeploymentRestoresDurableDiskWhenPrivatePoolRecordIsGone(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	gws.computeRepo = &deploymentLifecycleComputeRepo{states: map[string]*compute.PoolState{}}
	backend.snapshots["pg-data"] = &types.DiskSnapshot{
		WorkspaceId:    1,
		DiskName:       "pg-data",
		Format:         types.DiskSnapshotFormatPostgresWalV1,
		Status:         types.DiskSnapshotStatusAvailable,
		ManifestKey:    "durable-disks/pg-data/snapshots/1/manifest.json",
		SourceWorkerId: "worker-a",
	}

	setDeploymentStubConfig(t, backend, types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Autoscaler: &types.Autoscaler{
			Type:              types.QueueDepthAutoscaler,
			MinContainers:     0,
			MaxContainers:     1,
			TasksPerContainer: 1,
		},
		Disks: []*pb.DurableDisk{{
			Name:   "pg-data",
			Driver: types.DurableDiskDriverSnapshot,
		}},
	})

	resp, err := gws.ScaleDeployment(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.ScaleDeploymentRequest{
		Id:         "deployment-id",
		Containers: 1,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Nil(t, backend.stubConfigs[20].Pool)
}

func TestScalePodDeploymentClearsUnavailablePrivatePoolWithoutDisks(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)

	setDeploymentStubConfig(t, backend, types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Autoscaler: &types.Autoscaler{
			Type:              types.QueueDepthAutoscaler,
			MinContainers:     0,
			MaxContainers:     1,
			TasksPerContainer: 1,
		},
	})

	resp, err := gws.ScaleDeployment(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.ScaleDeploymentRequest{
		Id:         "deployment-id",
		Containers: 1,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Nil(t, backend.stubConfigs[20].Pool)
}

func TestScaleDeploymentRejectsRestrictedToken(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)

	resp, err := gws.ScaleDeployment(deploymentLifecycleContext(types.TokenTypeWorkspaceRestricted), &pb.ScaleDeploymentRequest{
		Id:         "deployment-id",
		Containers: 1,
	})

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Equal(t, "Unauthorized Access", resp.ErrMsg)
	require.Empty(t, backend.stubConfigs)
}

func deploymentLifecycleContext(tokenType string) context.Context {
	workspace := &types.Workspace{
		Id:         1,
		ExternalId: "workspace-id",
		Name:       "workspace",
	}
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: workspace,
		Token: &types.Token{
			TokenType: tokenType,
		},
	})
}
