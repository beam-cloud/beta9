package repository_services

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

// keepAliveGateway is a gateway with a Redis-backed worker repository and a
// worker connected to it through the production interceptors.
type keepAliveGateway struct {
	ctx     context.Context
	rdb     *common.RedisClient
	workers repository.WorkerRepository
	client  pb.WorkerRepositoryServiceClient
}

func newKeepAliveGateway(t *testing.T) *keepAliveGateway {
	t.Helper()
	broker, _ := newWorkerEventBrokerForTest(t)
	workers := repository.NewWorkerRedisRepositoryForTest(broker.rdb)
	serviceCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	service := &WorkerRepositoryService{ctx: serviceCtx, workerRepo: workers, workerEvents: broker}
	ctx, cancel := context.WithTimeout(repositoryWorkerAuthContext(types.TokenTypeWorker, "worker"), 10*time.Second)
	t.Cleanup(cancel)
	return &keepAliveGateway{
		ctx: ctx, rdb: broker.rdb, workers: workers,
		client: pb.NewWorkerRepositoryServiceClient(repositoryAuthClient(t, service, &ContainerRepositoryService{})),
	}
}

// writePendingRecord writes the record the pool controller creates for a
// machine's worker, both when the machine joins and when the record is missing.
func (g *keepAliveGateway) writePendingRecord(t *testing.T) {
	t.Helper()
	require.NoError(t, g.workers.AddWorker(&types.Worker{
		Id: "worker", PoolName: "pool", MachineId: "machine", Status: types.WorkerStatusPending,
		TotalCpu: 4000, FreeCpu: 4000, TotalMemory: 8000, FreeMemory: 8000,
	}))
}

func (g *keepAliveGateway) workerStatus(t *testing.T) types.WorkerStatus {
	t.Helper()
	worker, err := g.workers.GetWorkerById("worker")
	require.NoError(t, err)
	return worker.Status
}

func (g *keepAliveGateway) keepAlive(t *testing.T) *pb.SetWorkerKeepAliveResponse {
	t.Helper()
	response, err := g.client.SetWorkerKeepAlive(g.ctx, &pb.SetWorkerKeepAliveRequest{WorkerId: "worker", MachineId: "machine"})
	require.NoError(t, err)
	return response
}

func TestWorkerServesAgainOnItsOpenStreamAfterItsRecordIsEvicted(t *testing.T) {
	g := newKeepAliveGateway(t)
	g.writePendingRecord(t)

	// The worker starts and opens its request stream, which marks it available.
	stream, err := g.client.GetNextContainerRequest(g.ctx, &pb.GetNextContainerRequestRequest{WorkerId: "worker"})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return g.workerStatus(t) == types.WorkerStatusAvailable }, 5*time.Second, 10*time.Millisecond)

	// Redis evicts the record and the pool controller recreates it. The
	// worker's stream stays open, so nothing reopens it to mark it available.
	require.NoError(t, g.rdb.Del(g.ctx, common.RedisKeys.SchedulerWorkerState("worker")).Err())
	g.writePendingRecord(t)
	require.Equal(t, types.WorkerStatusPending, g.workerStatus(t))

	// The worker's next keepalive does.
	require.True(t, g.keepAlive(t).Ok)
	require.Equal(t, types.WorkerStatusAvailable, g.workerStatus(t))

	// A sandbox placed on the worker reaches it over the stream it never closed.
	worker, err := g.workers.GetWorkerById("worker")
	require.NoError(t, err)
	sandbox := &types.ContainerRequest{ContainerId: "sandbox", Cpu: 1000, Memory: 800}
	require.NoError(t, g.rdb.HSet(g.ctx, common.RedisKeys.SchedulerContainerState(sandbox.ContainerId),
		"container_id", sandbox.ContainerId, "status", string(types.ContainerStatusPending), "worker_id", "", "machine_id", "").Err())
	require.NoError(t, g.workers.ScheduleContainerRequest(worker, sandbox))
	for {
		response, err := stream.Recv()
		require.NoError(t, err)
		require.True(t, response.Ok)
		if response.ContainerRequest != nil {
			require.Equal(t, sandbox.ContainerId, response.ContainerRequest.ContainerId)
			break
		}
	}
}

func TestKeepAliveFromWorkerWithoutRequestStreamLeavesItPending(t *testing.T) {
	g := newKeepAliveGateway(t)
	g.writePendingRecord(t)

	// A starting worker sends a keepalive before it acknowledges startup.
	require.True(t, g.keepAlive(t).Ok)
	require.Equal(t, types.WorkerStatusPending, g.workerStatus(t))
}

func TestRemovedWorkerWithOpenRequestStreamStaysRemoved(t *testing.T) {
	g := newKeepAliveGateway(t)
	g.writePendingRecord(t)
	_, err := g.client.GetNextContainerRequest(g.ctx, &pb.GetNextContainerRequestRequest{WorkerId: "worker"})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return g.workerStatus(t) == types.WorkerStatusAvailable }, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, g.workers.RemoveWorker("worker"))

	// The worker has not noticed yet and keeps sending keepalives.
	for range 2 {
		response, err := g.client.SetWorkerKeepAlive(g.ctx, &pb.SetWorkerKeepAliveRequest{WorkerId: "worker"})
		require.NoError(t, err)
		require.False(t, response.Ok)
	}
	var notFound *types.ErrWorkerNotFound
	_, err = g.workers.GetWorkerById("worker")
	require.ErrorAs(t, err, &notFound)
}
