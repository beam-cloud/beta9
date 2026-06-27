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
