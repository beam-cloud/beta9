package gatewayservices

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type deploymentLifecycleBackendRepo struct {
	repository.BackendRepository
	deployments map[string]types.DeploymentWithRelated
	stubConfigs map[uint]*types.StubConfigV1
	snapshots   map[string]*types.DiskSnapshot
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
		redisClient:   rdb,
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

type deploymentLifecycleComputeRepo struct {
	repository.ComputeRepository
	states map[string]*compute.PoolState
}

func (r *deploymentLifecycleComputeRepo) GetPoolState(_ context.Context, _ string, name string) (*compute.PoolState, error) {
	return r.states[name], nil
}

type deploymentLifecycleContainerRepo struct {
	repository.ContainerRepository
}

func (r *deploymentLifecycleContainerRepo) GetActiveContainersByStubId(string) ([]types.ContainerState, error) {
	return nil, nil
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
	config := types.StubConfigV1{
		Autoscaler: &types.Autoscaler{
			Type:              types.LLMTokenPressureAutoscaler,
			MinContainers:     1,
			MaxContainers:     4,
			TasksPerContainer: 16,
		},
	}
	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	deployment := backend.deployments["deployment-id"]
	deployment.Stub.Config = string(configBytes)
	backend.deployments["deployment-id"] = deployment

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

func TestScalePodDeploymentRefreshesDevDurableDiskPlacement(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	require.NoError(t, workerRepo.AddWorker(&types.Worker{
		Id:        "worker-b",
		MachineId: "node-b",
		Status:    types.WorkerStatusAvailable,
		PoolName:  "pool-a",
	}))
	gws.workerRepo = workerRepo

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
			Driver: types.DurableDiskDriverDev,
			Replication: &pb.DiskReplication{
				PrimaryWorkerId: "node-a",
			},
		}},
	}
	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	deployment := backend.deployments["deployment-id"]
	deployment.Stub.Config = string(configBytes)
	backend.deployments["deployment-id"] = deployment

	resp, err := gws.ScaleDeployment(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.ScaleDeploymentRequest{
		Id:         "deployment-id",
		Containers: 1,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Equal(t, "node-b", backend.stubConfigs[20].Disks[0].Replication.PrimaryWorkerId)
	require.Equal(t, []string{"node-b"}, backend.stubConfigs[20].Disks[0].Replication.ReplicaWorkerIds)
}

func TestScalePodDeploymentRestoresDevDurableDiskToRegularPoolWhenPrivatePoolGone(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	require.NoError(t, workerRepo.AddWorker(&types.Worker{
		Id:                   "a-private-worker",
		Status:               types.WorkerStatusAvailable,
		PoolName:             "other-private",
		RequiresPoolSelector: true,
	}))
	require.NoError(t, workerRepo.AddWorker(&types.Worker{
		Id:         "z-regular-worker",
		Status:     types.WorkerStatusAvailable,
		PoolName:   "beta9-cpu",
		TotalCpu:   2000,
		FreeCpu:    2000,
		FreeMemory: 2000,
	}))
	gws.workerRepo = workerRepo
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
			Driver: types.DurableDiskDriverDev,
			Replication: &pb.DiskReplication{
				PrimaryWorkerId: "node-a",
			},
		}},
	}
	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	deployment := backend.deployments["deployment-id"]
	deployment.Stub.Config = string(configBytes)
	backend.deployments["deployment-id"] = deployment

	resp, err := gws.ScaleDeployment(deploymentLifecycleContext(types.TokenTypeWorkspace), &pb.ScaleDeploymentRequest{
		Id:         "deployment-id",
		Containers: 1,
	})

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Nil(t, backend.stubConfigs[20].Pool)
	require.Equal(t, "z-regular-worker", backend.stubConfigs[20].Disks[0].Replication.PrimaryWorkerId)
	require.Equal(t, []string{"z-regular-worker"}, backend.stubConfigs[20].Disks[0].Replication.ReplicaWorkerIds)
}

func TestScalePodDeploymentClearsUnavailablePrivatePoolWithoutDisks(t *testing.T) {
	gws, backend := newDeploymentLifecycleGateway(t)
	gws.workerRepo = newDurableDiskPlacementWorkerRepo(t)

	config := types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Autoscaler: &types.Autoscaler{
			Type:              types.QueueDepthAutoscaler,
			MinContainers:     0,
			MaxContainers:     1,
			TasksPerContainer: 1,
		},
	}
	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	deployment := backend.deployments["deployment-id"]
	deployment.Stub.Config = string(configBytes)
	backend.deployments["deployment-id"] = deployment

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
