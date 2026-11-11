package repository

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// A worker record has a TTL, so Redis can evict it under memory pressure while
// the worker keeps running. The pool controller of an external machine then
// recreates the record as pending. These tests cover what the worker's
// keepalives do with its record after that.

// servingKeepAlive is the keepalive of a worker that has started: the gateway
// that receives it holds the worker's container request stream.
var servingKeepAlive = types.WorkerKeepAlive{MachineId: "machine", RequestStreamOpen: true}

// machineWorkerRecord is the record the pool controller writes for a machine's
// worker, both when the machine joins and when the record has gone missing.
func machineWorkerRecord() *types.Worker {
	return &types.Worker{
		Id: "worker", PoolName: "pool", MachineId: "machine", Status: types.WorkerStatusPending,
		TotalCpu: 4000, FreeCpu: 4000, TotalMemory: 8000, FreeMemory: 8000,
	}
}

func TestServingWorkerIsUsableAgainAfterKeepAliveOnItsRecreatedRecord(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	ctx := context.Background()
	stateKey := common.RedisKeys.SchedulerWorkerState("worker")

	// The worker starts: its request stream opens and marks it available.
	require.NoError(t, repo.AddWorker(machineWorkerRecord()))
	require.NoError(t, repo.ToggleWorkerAvailable("worker", ""))

	// Redis evicts the record. A keepalive finds nothing and creates nothing.
	require.NoError(t, rdb.Del(ctx, stateKey).Err())
	var notFound *types.ErrWorkerNotFound
	require.ErrorAs(t, repo.SetWorkerKeepAlive("worker", servingKeepAlive), &notFound)
	require.Zero(t, rdb.Exists(ctx, stateKey).Val())

	// The pool controller recreates the record. A container that kept running
	// on the worker still holds part of its capacity.
	require.NoError(t, repo.AddWorker(machineWorkerRecord()))
	running := &types.ContainerState{ContainerId: "running", WorkerId: "worker", Status: types.ContainerStatusRunning, Cpu: 1000, Memory: 800}
	runningKey := common.RedisKeys.SchedulerContainerState(running.ContainerId)
	require.NoError(t, rdb.HSet(ctx, runningKey, common.ToSlice(running)).Err())
	require.NoError(t, rdb.SAdd(ctx, common.RedisKeys.SchedulerContainerWorkerIndex("worker"), runningKey).Err())

	worker, err := repo.GetWorkerById("worker")
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusPending, worker.Status)
	sandbox := &types.ContainerRequest{ContainerId: "sandbox", Cpu: 1000, Memory: 800}
	setPendingContainerRequests(t, rdb, sandbox)
	require.Error(t, repo.ScheduleContainerRequest(worker, sandbox), "a pending worker takes no work")

	// The worker's next keepalive makes it available again.
	require.NoError(t, repo.SetWorkerKeepAlive("worker", servingKeepAlive))
	worker, err = repo.GetWorkerById("worker")
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusAvailable, worker.Status)
	require.EqualValues(t, 3000, worker.FreeCpu, "the running container still holds its cpu")

	require.NoError(t, repo.ScheduleContainerRequest(worker, sandbox))
	delivered, err := repo.GetNextContainerRequests("worker", 1)
	require.NoError(t, err)
	require.Len(t, delivered, 1)
	require.Equal(t, sandbox.ContainerId, delivered[0].ContainerId)
}

func TestRemovedWorkerStaysRemovedWhileItKeepsSendingKeepAlives(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)

	require.NoError(t, repo.AddWorker(machineWorkerRecord()))
	require.NoError(t, repo.ToggleWorkerAvailable("worker", ""))
	require.NoError(t, repo.RemoveWorker("worker"))

	var notFound *types.ErrWorkerNotFound
	for range 3 {
		require.ErrorAs(t, repo.SetWorkerKeepAlive("worker", servingKeepAlive), &notFound)
	}
	_, err = repo.GetWorkerById("worker")
	require.ErrorAs(t, err, &notFound)
	workers, err := repo.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)
	workers, err = repo.GetAllWorkersInPool("pool")
	require.NoError(t, err)
	require.Empty(t, workers)
}

func TestKeepAliveDoesNotReturnWorkerThatWasTakenOutOfService(t *testing.T) {
	for name, takeOut := range map[string]func(t *testing.T, repo WorkerRepository){
		"draining": func(t *testing.T, repo WorkerRepository) {
			require.NoError(t, repo.UpdateWorkerStatus("worker", types.WorkerStatusDisabled))
		},
		"cordoned": func(t *testing.T, repo WorkerRepository) {
			require.NoError(t, repo.SetWorkerCordon("worker", true))
		},
		"being replaced by a rollout": func(t *testing.T, repo WorkerRepository) {
			_, err := repo.PrepareWorkerRollout("worker", "next-generation")
			require.NoError(t, err)
		},
		"recreated for a cordoned machine": func(t *testing.T, repo WorkerRepository) {
			require.NoError(t, repo.RemoveWorker("worker"))
			record := machineWorkerRecord()
			record.Status, record.CordonRequested = types.WorkerStatusDisabled, true
			require.NoError(t, repo.AddWorker(record))
		},
		"recreated as pending while cordoned": func(t *testing.T, repo WorkerRepository) {
			require.NoError(t, repo.RemoveWorker("worker"))
			record := machineWorkerRecord()
			record.CordonRequested = true
			require.NoError(t, repo.AddWorker(record))
		},
	} {
		t.Run(name, func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			require.NoError(t, err)
			repo := NewWorkerRedisRepositoryForTest(rdb)
			require.NoError(t, repo.AddWorker(machineWorkerRecord()))
			require.NoError(t, repo.ToggleWorkerAvailable("worker", ""))
			takeOut(t, repo)

			for range 2 {
				require.NoError(t, repo.SetWorkerKeepAlive("worker", servingKeepAlive))
			}
			worker, err := repo.GetWorkerById("worker")
			require.NoError(t, err)
			require.Equal(t, types.WorkerStatusDisabled, worker.Status)
		})
	}
}

func TestKeepAliveFromStartingWorkerLeavesItPending(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	require.NoError(t, repo.AddWorker(machineWorkerRecord()))

	// A starting worker sends a keepalive before it acknowledges startup and
	// before it opens its request stream. Startup still decides availability.
	require.NoError(t, repo.SetWorkerKeepAlive("worker", types.WorkerKeepAlive{MachineId: "machine"}))
	worker, err := repo.GetWorkerById("worker")
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusPending, worker.Status)
}

func TestServingKeepAliveCostsNothingExtraWhenTheRecordWasNotLost(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	require.NoError(t, repo.AddWorker(machineWorkerRecord()))
	require.NoError(t, repo.ToggleWorkerAvailable("worker", ""))
	before, err := repo.GetWorkerById("worker")
	require.NoError(t, err)

	trips := &redisRoundTrips{}
	trips.enabled.Store(true)
	rdb.AddHook(trips)
	roundTrips := func(keepAlive types.WorkerKeepAlive) int64 {
		start := trips.calls.Load()
		require.NoError(t, repo.SetWorkerKeepAlive("worker", keepAlive))
		return trips.calls.Load() - start
	}
	starting := roundTrips(types.WorkerKeepAlive{MachineId: "machine"})
	serving := roundTrips(servingKeepAlive)
	require.Equal(t, starting, serving)
	trips.enabled.Store(false)

	after, err := repo.GetWorkerById("worker")
	require.NoError(t, err)
	require.Equal(t, before, after)
}
