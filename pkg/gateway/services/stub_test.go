package gatewayservices

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	repositoryservices "github.com/beam-cloud/beta9/pkg/gateway/services/repository"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestConfigureDurableDiskPlacementFillsDRBDReplicaWorkers(t *testing.T) {
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	for _, worker := range []*types.Worker{
		{Id: "worker-c", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-a", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-b", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-disabled", Status: types.WorkerStatusDisabled, PoolName: "pool-a"},
		{Id: "worker-other", Status: types.WorkerStatusAvailable, PoolName: "pool-b"},
	} {
		require.NoError(t, workerRepo.AddWorker(worker))
	}

	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:   "pg-data",
				Driver: types.DurableDiskDriverDRBD,
				Replication: &pb.DiskReplication{
					Replicas: 3,
				},
			},
		},
	}

	gws := &GatewayService{workerRepo: workerRepo}
	require.NoError(t, gws.configureDurableDiskPlacement(context.Background(), nil, config))

	require.Equal(t, "worker-a", config.Disks[0].Replication.PrimaryWorkerId)
	require.Equal(t, []string{"worker-a", "worker-b", "worker-c"}, config.Disks[0].Replication.ReplicaWorkerIds)
	require.Equal(t, types.DurableDiskReplicationModeSync, config.Disks[0].Replication.Mode)
	require.Equal(t, types.DurableDiskReplicationQuorumMajority, config.Disks[0].Replication.Quorum)
}

func TestConfigureDurableDiskPlacementFailsClosedWithoutQuorumWorkers(t *testing.T) {
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	require.NoError(t, workerRepo.AddWorker(&types.Worker{Id: "worker-a", Status: types.WorkerStatusAvailable, PoolName: "pool-a"}))

	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:   "pg-data",
				Driver: types.DurableDiskDriverDRBD,
				Replication: &pb.DiskReplication{
					Replicas: 3,
				},
			},
		},
	}

	gws := &GatewayService{workerRepo: workerRepo}
	err := gws.configureDurableDiskPlacement(context.Background(), nil, config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "needs 3 DRBD storage nodes")
}

func TestConfigureDurableDiskPlacementPinsDevDiskToSingleStorageNode(t *testing.T) {
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	for _, worker := range []*types.Worker{
		{Id: "worker-b", MachineId: "node-b", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-a", MachineId: "node-a", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
	} {
		require.NoError(t, workerRepo.AddWorker(worker))
	}

	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:        "pg-data",
				Driver:      types.DurableDiskDriverDev,
				Replication: &pb.DiskReplication{},
			},
		},
	}

	gws := &GatewayService{workerRepo: workerRepo}
	require.NoError(t, gws.configureDurableDiskPlacement(context.Background(), nil, config))

	require.Equal(t, uint32(0), config.Disks[0].Replication.Replicas)
	require.Equal(t, "node-b", config.Disks[0].Replication.PrimaryWorkerId)
	require.Equal(t, []string{"node-b"}, config.Disks[0].Replication.ReplicaWorkerIds)
}

func TestConfigureDurableDiskPlacementReplacesMissingDevPrimary(t *testing.T) {
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	require.NoError(t, workerRepo.AddWorker(&types.Worker{
		Id:        "worker-b",
		MachineId: "node-b",
		Status:    types.WorkerStatusAvailable,
		PoolName:  "pool-a",
	}))

	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:   "pg-data",
				Driver: types.DurableDiskDriverDev,
				Replication: &pb.DiskReplication{
					PrimaryWorkerId: "node-a",
				},
			},
		},
	}

	gws := &GatewayService{workerRepo: workerRepo}
	require.NoError(t, gws.configureDurableDiskPlacement(context.Background(), nil, config))

	require.Equal(t, "node-b", config.Disks[0].Replication.PrimaryWorkerId)
	require.Equal(t, []string{"node-b"}, config.Disks[0].Replication.ReplicaWorkerIds)
}

func TestConfigureDurableDiskPlacementAllowsDevDiskWithoutRegisteredNode(t *testing.T) {
	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:   "pg-data",
				Driver: types.DurableDiskDriverDev,
				Replication: &pb.DiskReplication{
					PrimaryWorkerId:  "node-a",
					ReplicaWorkerIds: []string{"node-a"},
				},
			},
		},
	}

	gws := &GatewayService{workerRepo: newDurableDiskPlacementWorkerRepo(t)}
	require.NoError(t, gws.configureDurableDiskPlacement(context.Background(), nil, config))
	require.Empty(t, config.Disks[0].Replication.PrimaryWorkerId)
	require.Empty(t, config.Disks[0].Replication.ReplicaWorkerIds)
}

func TestConfigureDurableDiskPlacementPreservesExplicitPrimary(t *testing.T) {
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	for _, worker := range []*types.Worker{
		{Id: "worker-a", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-b", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-c", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
	} {
		require.NoError(t, workerRepo.AddWorker(worker))
	}

	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:   "pg-data",
				Driver: types.DurableDiskDriverDRBD,
				Replication: &pb.DiskReplication{
					Replicas:        3,
					PrimaryWorkerId: "worker-c",
				},
			},
		},
	}

	gws := &GatewayService{workerRepo: workerRepo}
	require.NoError(t, gws.configureDurableDiskPlacement(context.Background(), nil, config))

	require.Equal(t, "worker-c", config.Disks[0].Replication.PrimaryWorkerId)
	require.Equal(t, []string{"worker-c", "worker-a", "worker-b"}, config.Disks[0].Replication.ReplicaWorkerIds)
}

func TestConfigureDurableDiskPlacementUsesStorageNodeIDs(t *testing.T) {
	workerRepo := newDurableDiskPlacementWorkerRepo(t)
	for _, worker := range []*types.Worker{
		{Id: "worker-a-rolled", MachineId: "node-a", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-b", MachineId: "node-b", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-c", MachineId: "node-c", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
	} {
		require.NoError(t, workerRepo.AddWorker(worker))
	}

	config := &types.StubConfigV1{
		Pool: &types.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:   "pg-data",
				Driver: types.DurableDiskDriverDRBD,
				Replication: &pb.DiskReplication{
					Replicas:        3,
					PrimaryWorkerId: "worker-a-rolled",
				},
			},
		},
	}

	gws := &GatewayService{workerRepo: workerRepo}
	require.NoError(t, gws.configureDurableDiskPlacement(context.Background(), nil, config))

	require.Equal(t, "node-a", config.Disks[0].Replication.PrimaryWorkerId)
	require.Equal(t, []string{"node-a", "node-b", "node-c"}, config.Disks[0].Replication.ReplicaWorkerIds)
}

func TestGatewayDurableDiskFlowPublishesPrepareCommandsToStorageNodeStream(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	workerRepo := repository.NewWorkerRedisRepository(rdb, types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: 10 * time.Minute,
	})
	for _, worker := range []*types.Worker{
		{Id: "worker-a", MachineId: "node-a", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-b-old", MachineId: "node-b", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
		{Id: "worker-c", MachineId: "node-c", Status: types.WorkerStatusAvailable, PoolName: "pool-a"},
	} {
		require.NoError(t, workerRepo.AddWorker(worker))
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	backend := &durableDiskGatewayFlowBackend{}
	workerService := repositoryservices.NewWorkerRepositoryService(ctx, workerRepo, nil, backend, rdb, types.AppConfig{}, "")
	waitForMiniredisSubscriber(t, server, common.EventChannelKey(common.EventTypeDurableDiskCommand))

	streamCtx, streamCancel := context.WithCancel(context.Background())
	stream := &durableDiskGatewayFlowStream{
		ctx:  streamCtx,
		sent: make(chan *pb.WorkerEvent, 8),
	}
	streamErrs := make(chan error, 1)
	go func() {
		streamErrs <- workerService.StreamWorkerEvents(&pb.StreamWorkerEventsRequest{
			WorkerId:      "worker-b-rolled",
			StorageNodeId: "node-b",
		}, stream)
	}()
	t.Cleanup(func() {
		streamCancel()
		requireStreamClosed(t, streamErrs)
	})

	gws := &GatewayService{
		appConfig: types.AppConfig{
			GatewayService: types.GatewayServiceConfig{
				StubLimits: types.StubLimits{
					Cpu:         10_000,
					Memory:      65_536,
					MaxGpuCount: 1,
					MaxReplicas: 10,
				},
			},
		},
		backendRepo: backend,
		workerRepo:  workerRepo,
		redisClient: rdb,
	}
	authCtx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 1, ExternalId: "workspace-id", Name: "workspace"},
		Token:     &types.Token{TokenType: types.TokenTypeWorkspace},
	})

	res, err := gws.GetOrCreateStub(authCtx, &pb.GetOrCreateStubRequest{
		Name:     "postgres",
		StubType: types.StubTypePodDeployment,
		ObjectId: "object-id",
		Cpu:      1_000,
		Memory:   1_024,
		ImageId:  "postgres:16",
		Pool:     &pb.PoolConfig{Name: "pool-a"},
		Disks: []*pb.DurableDisk{
			{
				Name:      "pg-data",
				Size:      "10Gi",
				MountPath: "/var/lib/postgresql/data",
				Driver:    types.DurableDiskDriverDRBD,
				Replication: &pb.DiskReplication{
					Replicas: 3,
				},
			},
		},
	})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)

	require.Len(t, backend.stubConfig.Disks, 1)
	replication := backend.stubConfig.Disks[0].Replication
	require.NotNil(t, replication)
	require.Equal(t, "node-a", replication.PrimaryWorkerId)
	require.Equal(t, []string{"node-a", "node-b", "node-c"}, replication.ReplicaWorkerIds)

	event := receiveGatewayFlowWorkerEvent(t, stream.sent)
	disk := event.GetDurableDisk()
	require.NotNil(t, disk)
	require.Equal(t, "node-b", disk.StorageNodeId)
	require.Equal(t, string(types.DurableDiskCommandActionPrepare), disk.Action)
	require.NotNil(t, disk.Mount)
	require.Equal(t, types.StorageModeDurableDisk, disk.Mount.MountType)
	require.Equal(t, "/var/lib/postgresql/data", disk.Mount.MountPath)
	require.Equal(t, "pg-data", disk.Mount.DurableDisk.Name)
	require.Equal(t, "node-a", disk.Mount.DurableDisk.PrimaryWorkerId)
	require.Equal(t, []string{"node-a", "node-b", "node-c"}, disk.Mount.DurableDisk.ReplicaWorkerIDs)
}

func newDurableDiskPlacementWorkerRepo(t *testing.T) repository.WorkerRepository {
	t.Helper()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)

	return repository.NewWorkerRedisRepository(rdb, types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: 10 * time.Minute,
	})
}

type durableDiskGatewayFlowBackend struct {
	repository.BackendRepository
	stubConfig types.StubConfigV1
}

func (b *durableDiskGatewayFlowBackend) GetOrCreateApp(ctx context.Context, workspaceId uint, appName string) (*types.App, error) {
	return &types.App{Id: 30, ExternalId: "app-id", Name: appName, WorkspaceId: workspaceId}, nil
}

func (b *durableDiskGatewayFlowBackend) GetObjectByExternalId(ctx context.Context, externalId string, workspaceId uint) (types.Object, error) {
	return types.Object{Id: 20, ExternalId: externalId, WorkspaceId: workspaceId}, nil
}

func (b *durableDiskGatewayFlowBackend) GetOrCreateStub(ctx context.Context, name, stubType string, config types.StubConfigV1, objectId, workspaceId uint, forceCreate bool, appId uint) (types.Stub, error) {
	b.stubConfig = config
	return types.Stub{
		Id:          40,
		ExternalId:  "stub-id",
		Name:        name,
		Type:        types.StubType(stubType),
		ObjectId:    objectId,
		WorkspaceId: workspaceId,
		AppId:       appId,
	}, nil
}

type durableDiskGatewayFlowStream struct {
	ctx  context.Context
	sent chan *pb.WorkerEvent
}

func (s *durableDiskGatewayFlowStream) Send(event *pb.WorkerEvent) error {
	select {
	case s.sent <- event:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *durableDiskGatewayFlowStream) SetHeader(metadata.MD) error  { return nil }
func (s *durableDiskGatewayFlowStream) SendHeader(metadata.MD) error { return nil }
func (s *durableDiskGatewayFlowStream) SetTrailer(metadata.MD)       {}
func (s *durableDiskGatewayFlowStream) Context() context.Context     { return s.ctx }
func (s *durableDiskGatewayFlowStream) SendMsg(any) error            { return nil }
func (s *durableDiskGatewayFlowStream) RecvMsg(any) error            { return nil }

func waitForMiniredisSubscriber(t *testing.T, server *miniredis.Miniredis, channel string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return server.PubSubNumSub(channel)[channel] > 0
	}, time.Second, 10*time.Millisecond)
}

func receiveGatewayFlowWorkerEvent(t *testing.T, events <-chan *pb.WorkerEvent) *pb.WorkerEvent {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker event")
		return nil
	}
}

func requireStreamClosed(t *testing.T, errs <-chan error) {
	t.Helper()

	select {
	case err := <-errs:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream to close")
	}
}
