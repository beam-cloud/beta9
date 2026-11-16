package repository_services

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// placementGateway is a gateway's repository services behind the production
// auth interceptor, on a Redis that counts its commands. The cluster worker
// "worker" calls them with the token in ctx, which Redis has cached.
type placementGateway struct {
	ctx        context.Context
	rdb        *common.RedisClient
	counts     *repository.RedisCommandCounts
	containers repository.ContainerRepository
	workers    repository.WorkerRepository
	worker     *types.Worker
	workerRPC  pb.WorkerRepositoryServiceClient
	container  pb.ContainerRepositoryServiceClient
}

func newPlacementGateway(t *testing.T) *placementGateway {
	t.Helper()
	rdb, counts, err := repository.NewRedisClientForTestWithCounts()
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })

	gateway := &placementGateway{
		rdb:        rdb,
		counts:     counts,
		containers: repository.NewContainerRedisRepositoryForTest(rdb),
		workers:    repository.NewWorkerRedisRepositoryForTest(rdb),
		worker: &types.Worker{
			Id: "worker", PoolName: "pool", Status: types.WorkerStatusAvailable,
			TotalCpu: 1 << 30, FreeCpu: 1 << 30, TotalMemory: 1 << 30, FreeMemory: 1 << 30,
		},
	}
	require.NoError(t, gateway.workers.AddWorker(gateway.worker))

	storageID := uint(1)
	workspaces := repository.NewWorkspaceRedisRepositoryForTest(rdb)
	require.NoError(t, workspaces.SetAuthorizationToken(
		&types.Token{Key: "worker-token", TokenType: types.TokenTypeWorker, ExternalId: "token", Active: true, WorkerID: gateway.worker.Id},
		&types.Workspace{ExternalId: "admin", Storage: &types.WorkspaceStorage{Id: &storageID}},
	))
	gateway.ctx = metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer worker-token")

	listener := bufconn.Listen(1 << 20)
	interceptor := auth.NewAuthInterceptor(types.AppConfig{}, nil, workspaces)
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.Unary()), grpc.StreamInterceptor(interceptor.Stream()))
	pb.RegisterWorkerRepositoryServiceServer(server, &WorkerRepositoryService{ctx: context.Background(), workerRepo: gateway.workers, containerRepo: gateway.containers})
	pb.RegisterContainerRepositoryServiceServer(server, &ContainerRepositoryService{ctx: context.Background(), containerRepo: gateway.containers})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///placement", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	gateway.workerRPC = pb.NewWorkerRepositoryServiceClient(conn)
	gateway.container = pb.NewContainerRepositoryServiceClient(conn)
	return gateway
}

// deliver admits a container, places it on the worker and hands the worker
// its request, as the scheduler and the worker's request stream do.
func (g *placementGateway) deliver(t *testing.T, containerID string) *types.ContainerRequest {
	t.Helper()
	request := &types.ContainerRequest{
		ContainerId: containerID, StubId: "stub", WorkspaceId: "workspace", Cpu: 1000, Memory: 1024,
		Ports: []uint32{8001}, Timestamp: time.Now(),
	}
	require.NoError(t, g.containers.CreateContainerStateWithConcurrencyLimit(nil, request))
	require.NoError(t, g.workers.ScheduleContainerRequests(g.worker, []*types.ContainerRequest{request}))
	delivered, err := g.workers.GetNextContainerRequests(g.worker.Id, 128)
	require.NoError(t, err)
	require.Len(t, delivered, 1)
	return delivered[0]
}

// start makes the calls a worker makes to start a delivered container, in
// their order, and returns what each cost Redis.
func (g *placementGateway) start(t *testing.T, request *types.ContainerRequest) map[string]redisCost {
	t.Helper()
	costs := map[string]redisCost{}
	measure := func(name string, call func()) {
		g.counts.Start()
		call()
		commands, roundTrips := g.counts.Stop()
		costs[name] = redisCost{commands: commands, roundTrips: roundTrips}
	}

	measure("ClaimContainer", func() {
		response, err := g.workerRPC.ClaimContainer(g.ctx, &pb.ClaimContainerRequest{
			WorkerId: g.worker.Id, ContainerId: request.ContainerId, DeliveryToken: request.DeliveryToken,
			WorkerAddress: &pb.SetWorkerAddressRequest{ContainerId: request.ContainerId, Address: "10.0.0.1:1993"},
		})
		require.NoError(t, err)
		require.True(t, response.Ok, response.ErrorMsg)
		require.True(t, response.StartupPrepared)
		require.Equal(t, string(types.ContainerStatusPending), response.State.Status)
		require.Equal(t, g.worker.Id, response.State.WorkerId)
		require.Equal(t, "workspace", response.State.WorkspaceId)
		require.Equal(t, "stub", response.State.StubId)
		require.EqualValues(t, 1000, response.State.Cpu)
		require.EqualValues(t, 1024, response.State.Memory)
		require.NotZero(t, response.State.ScheduledAt)
	})
	measure("SetContainerAddressMap", func() {
		response, err := g.container.SetContainerAddressMap(g.ctx, &pb.SetContainerAddressMapRequest{
			ContainerId: request.ContainerId, AddressMap: map[int32]string{8001: "10.0.0.1:30001"}, PrimaryPort: 8001,
		})
		require.NoError(t, err)
		require.True(t, response.Ok, response.ErrorMsg)
	})
	measure("UpdateContainerStatus", func() {
		response, err := g.container.UpdateContainerStatus(g.ctx, &pb.UpdateContainerStatusRequest{
			ContainerId: request.ContainerId, Status: string(types.ContainerStatusRunning), ExpirySeconds: types.ContainerStateTtlS,
		})
		require.NoError(t, err)
		require.True(t, response.Ok, response.ErrorMsg)
		require.Equal(t, string(types.ContainerStatusRunning), response.Status)
	})
	return costs
}

// redisCost is what a call cost Redis: the commands the server ran, those of
// scripts among them, and the round trips that carried them.
type redisCost struct {
	commands   map[string]int
	roundTrips int
}

// A worker starting a container makes three calls to the gateway. Each is
// authenticated (one GET of the token) and authorized (one HGETALL of the
// container's state). What each costs beyond those two round trips is pinned
// here; before the container's lock was dropped from the status update the
// three calls took 9, 4 and 7 round trips and 7 scripts between them.
func TestRedisCommandsForAWorkerStartingAContainer(t *testing.T) {
	gateway := newPlacementGateway(t)
	// The first container loads the scripts.
	gateway.start(t, gateway.deliver(t, "sandbox-warm"))

	costs := gateway.start(t, gateway.deliver(t, "sandbox-1"))
	for name, cost := range costs {
		t.Logf("%-24s %d round trips, %v", name, cost.roundTrips, cost.commands)
	}
	require.Equal(t, map[string]redisCost{
		// Acknowledge the delivery (a script), renew the pending lease and
		// read the state (a script), publish the worker's address. The
		// delivery is checked for with HEXISTS: reading it to find its token
		// meant parsing the request it holds.
		"ClaimContainer": {roundTrips: 5, commands: map[string]int{
			"GET": 1, "HGETALL": 2, "EVALSHA": 2, "SET": 1,
			"HGET": 5, "HEXISTS": 1, "HSET": 3, "HDEL": 2, "SADD": 1, "EXISTS": 1, "EXPIRE": 1, "ZADD": 1,
		}},
		// The address map and the primary address, sent together.
		"SetContainerAddressMap": {roundTrips: 3, commands: map[string]int{"GET": 1, "HGETALL": 1, "SET": 2}},
		// One script updates the status and returns it.
		"UpdateContainerStatus": {roundTrips: 3, commands: map[string]int{
			"GET": 1, "HGETALL": 2, "EVALSHA": 1, "EXISTS": 1, "HSET": 1, "EXPIRE": 1, "ZADD": 1,
		}},
	}, costs)

	ctx := context.Background()
	state, err := gateway.containers.GetContainerState("sandbox-1")
	require.NoError(t, err)
	require.Equal(t, types.ContainerStatusRunning, state.Status)
	require.InDelta(t, time.Now().Unix(), state.StartedAt, 2)
	requireContainerStateTTL(t, gateway.rdb, "sandbox-1", types.ContainerStateTtlS)
	score, err := gateway.rdb.ZScore(ctx, common.RedisKeys.SchedulerContainerStateIndex(), common.RedisKeys.SchedulerContainerState("sandbox-1")).Result()
	require.NoError(t, err)
	require.InDelta(t, time.Now().Unix()+types.ContainerStateTtlS, score, 2)
	address, err := gateway.containers.GetWorkerAddress(ctx, "sandbox-1")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:1993", address)
	containerAddress, err := gateway.containers.GetContainerAddress("sandbox-1")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:30001", containerAddress)
	addressMap, err := gateway.containers.GetContainerAddressMap("sandbox-1")
	require.NoError(t, err)
	require.Equal(t, map[int32]string{8001: "10.0.0.1:30001"}, addressMap)
}
