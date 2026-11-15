package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// lostWorkerWithRequests returns a worker whose state has expired with one
// request still on its queue and one it had taken off its queue but not
// claimed. (For one it had claimed, see claimedOnWorker.)
func lostWorkerWithRequests(t *testing.T, rdb *common.RedisClient, repo WorkerRepository) *types.Worker {
	t.Helper()

	worker := &types.Worker{
		Id: "lost-worker", Status: types.WorkerStatusAvailable, PoolName: "default",
		FreeCpu: 10_000, FreeMemory: 10_000, TotalCpu: 10_000, TotalMemory: 10_000,
	}
	require.NoError(t, repo.AddWorker(worker))

	requests := []*types.ContainerRequest{
		{ContainerId: "taken", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
		{ContainerId: "queued", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
	}
	setPendingContainerRequests(t, rdb, requests...)
	require.NoError(t, repo.ScheduleContainerRequests(worker, requests))
	taken, err := repo.GetNextContainerRequests(worker.Id, 1)
	require.NoError(t, err)
	require.Len(t, taken, 1)

	// The worker stops sending heartbeats and its state expires.
	require.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id)).Err())
	return worker
}

func backlogContainerIDs(t *testing.T, rdb *common.RedisClient) []string {
	t.Helper()

	backlog, err := rdb.ZRange(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), 0, -1).Result()
	require.NoError(t, err)
	ids := make([]string, 0, len(backlog))
	for _, raw := range backlog {
		var request types.ContainerRequest
		require.NoError(t, json.Unmarshal([]byte(raw), &request))
		ids = append(ids, request.ContainerId)
	}
	return ids
}

func requireNothingLeftOnWorker(t *testing.T, rdb *common.RedisClient, workerID string) {
	t.Helper()

	ctx := context.TODO()
	require.Zero(t, rdb.LLen(ctx, common.RedisKeys.SchedulerWorkerRequests(workerID)).Val())
	require.Zero(t, rdb.HLen(ctx, common.RedisKeys.SchedulerWorkerPendingRequests(workerID)).Val())
	for _, containerID := range []string{"taken", "queued"} {
		require.Empty(t, rdb.HGet(ctx, common.RedisKeys.SchedulerContainerState(containerID), "worker_id").Val())
	}
}

func TestListingWorkersRequeuesTheRequestsOfALostWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := lostWorkerWithRequests(t, rdb, repo)

	workers, err := repo.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)

	require.ElementsMatch(t, []string{"taken", "queued"}, backlogContainerIDs(t, rdb))
	requireNothingLeftOnWorker(t, rdb, worker.Id)

	// The worker is forgotten, so a second read has nothing to do.
	indexed, err := rdb.SMembers(context.TODO(), common.RedisKeys.SchedulerWorkerIndex()).Result()
	require.NoError(t, err)
	require.Empty(t, indexed)
	_, err = repo.GetAllWorkers()
	require.NoError(t, err)
	require.Len(t, backlogContainerIDs(t, rdb), 2)
}

func TestListingAPoolRequeuesTheRequestsOfALostWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := lostWorkerWithRequests(t, rdb, repo)

	workers, err := repo.GetAllWorkersInPool("default")
	require.NoError(t, err)
	require.Empty(t, workers)

	require.ElementsMatch(t, []string{"taken", "queued"}, backlogContainerIDs(t, rdb))
	requireNothingLeftOnWorker(t, rdb, worker.Id)
}

func TestRemovingALostWorkerRequeuesItsRequests(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := lostWorkerWithRequests(t, rdb, repo)

	// The pool cleaner removes a worker whose Job outlived its state. There
	// is no state left to delete, and the requests still have to move.
	err = repo.RemoveWorker(worker.Id)
	var notFound *types.ErrWorkerNotFound
	require.True(t, errors.As(err, &notFound), "got %v", err)

	require.ElementsMatch(t, []string{"taken", "queued"}, backlogContainerIDs(t, rdb))
	requireNothingLeftOnWorker(t, rdb, worker.Id)
}

func TestALostWorkerCannotStartARequestThatWasRequeued(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)

	worker := &types.Worker{
		Id: "slow-worker", Status: types.WorkerStatusAvailable, PoolName: "default",
		FreeCpu: 10_000, FreeMemory: 10_000, TotalCpu: 10_000, TotalMemory: 10_000,
	}
	require.NoError(t, repo.AddWorker(worker))
	request := &types.ContainerRequest{ContainerId: "taken", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100}
	setPendingContainerRequests(t, rdb, request)
	require.NoError(t, repo.ScheduleContainerRequests(worker, []*types.ContainerRequest{request}))
	taken, err := repo.GetNextContainerRequests(worker.Id, 1)
	require.NoError(t, err)
	require.Len(t, taken, 1)

	require.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id)).Err())
	_, err = repo.GetAllWorkers()
	require.NoError(t, err)
	require.Equal(t, []string{"taken"}, backlogContainerIDs(t, rdb))

	// The worker was only slow. It must not start a container that is
	// already on its way to another worker.
	require.Error(t, repo.AddContainerToWorker(worker.Id, "taken", taken[0].DeliveryToken))
}

// A worker claims a request before it starts the container, and the claim
// takes the request off the worker for good: from then on there is nothing
// to return to the backlog. If the worker is lost before the container has
// started, the container's state says PENDING until its lease runs out, half
// an hour later, and for all that time its owner counts a container that is
// starting. So when the worker is forgotten or removed, the container is
// ended as a stop request would end it, and its owner starts another.

// claimedOnWorker is a worker with four containers on its index:
//
//	claimed  the worker claimed it and has not started it
//	running  the worker started it
//	queued   the worker has not taken it off its queue
//	moved    another worker's by now, and still listed here
type claimedOnWorker struct {
	rdb    *common.RedisClient
	repo   *WorkerRedisRepository
	worker *types.Worker
}

func newClaimedOnWorker(t *testing.T) *claimedOnWorker {
	t.Helper()
	ctx := context.TODO()
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	f := &claimedOnWorker{rdb: rdb, repo: NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)}
	f.worker = &types.Worker{
		Id: "worker", Status: types.WorkerStatusAvailable, PoolName: "default",
		FreeCpu: 10_000, FreeMemory: 10_000, TotalCpu: 10_000, TotalMemory: 10_000,
	}
	require.NoError(t, f.repo.AddWorker(f.worker))

	started := []*types.ContainerRequest{
		{ContainerId: "claimed", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
		{ContainerId: "running", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
	}
	setPendingContainerRequests(t, rdb, started...)
	require.NoError(t, f.repo.ScheduleContainerRequests(f.worker, started))
	taken, err := f.repo.GetNextContainerRequests(f.worker.Id, 2)
	require.NoError(t, err)
	require.Len(t, taken, 2)
	for _, request := range taken {
		require.NoError(t, f.repo.AddContainerToWorker(f.worker.Id, request.ContainerId, request.DeliveryToken))
	}
	require.NoError(t, rdb.HSet(ctx, common.RedisKeys.SchedulerContainerState("running"), "status", string(types.ContainerStatusRunning)).Err())

	queued := &types.ContainerRequest{ContainerId: "queued", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100}
	setPendingContainerRequests(t, rdb, queued)
	require.NoError(t, f.repo.ScheduleContainerRequests(f.worker, []*types.ContainerRequest{queued}))

	moved := common.RedisKeys.SchedulerContainerState("moved")
	require.NoError(t, rdb.HSet(ctx, moved, "container_id", "moved", "status", string(types.ContainerStatusPending), "worker_id", "another-worker").Err())
	require.NoError(t, rdb.SAdd(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(f.worker.Id), moved).Err())

	// Each state has the lease of a container that is starting.
	for _, id := range []string{"claimed", "running", "queued", "moved"} {
		require.NoError(t, rdb.Expire(ctx, common.RedisKeys.SchedulerContainerState(id), time.Duration(types.ContainerStateTtlSWhilePending)*time.Second).Err())
	}
	return f
}

func (f *claimedOnWorker) state(t *testing.T, containerID string) (status types.ContainerStatus, workerID string, lease time.Duration) {
	t.Helper()
	ctx := context.TODO()
	key := common.RedisKeys.SchedulerContainerState(containerID)
	fields, err := f.rdb.HGetAll(ctx, key).Result()
	require.NoError(t, err)
	return types.ContainerStatus(fields["status"]), fields["worker_id"], f.rdb.TTL(ctx, key).Val()
}

// requireClaimedContainerEnded checks what became of each container once the
// worker was forgotten or removed.
func (f *claimedOnWorker) requireClaimedContainerEnded(t *testing.T) {
	t.Helper()
	pendingLease := time.Duration(types.ContainerStateTtlSWhilePending) * time.Second
	stoppingLease := time.Duration(types.ContainerStateTtlSWhileStopping) * time.Second

	status, owner, lease := f.state(t, "claimed")
	require.Equal(t, types.ContainerStatusStopping, status, "its owner no longer counts it as starting")
	require.Equal(t, f.worker.Id, owner)
	require.Equal(t, stoppingLease, lease, "and it is gone once a stopping container's time is up")
	// The index of container states says when each expires.
	expires, err := f.rdb.ZScore(context.TODO(), common.RedisKeys.SchedulerContainerStateIndex(), common.RedisKeys.SchedulerContainerState("claimed")).Result()
	require.NoError(t, err)
	require.InDelta(t, time.Now().Add(stoppingLease).Unix(), expires, 5)

	// A container that had started lives and dies by its own lease.
	status, owner, lease = f.state(t, "running")
	require.Equal(t, types.ContainerStatusRunning, status)
	require.Equal(t, f.worker.Id, owner)
	require.Equal(t, pendingLease, lease)

	// A request the worker never claimed goes back to the backlog.
	status, owner, _ = f.state(t, "queued")
	require.Equal(t, types.ContainerStatusPending, status)
	require.Empty(t, owner)
	require.Equal(t, []string{"queued"}, backlogContainerIDs(t, f.rdb), "what was claimed cannot be placed again: its request is gone")

	// Another worker's container is not this worker's to end.
	status, owner, lease = f.state(t, "moved")
	require.Equal(t, types.ContainerStatusPending, status)
	require.Equal(t, "another-worker", owner)
	require.Equal(t, pendingLease, lease)
}

func TestAContainerClaimedByAWorkerThatIsForgottenIsEnded(t *testing.T) {
	f := newClaimedOnWorker(t)

	// The worker stops sending keepalives and its state expires.
	require.NoError(t, f.rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerState(f.worker.Id)).Err())
	workers, err := f.repo.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)

	f.requireClaimedContainerEnded(t)

	// Looking again changes nothing.
	_, err = f.repo.GetAllWorkers()
	require.NoError(t, err)
	f.requireClaimedContainerEnded(t)
}

func TestAContainerClaimedByAWorkerThatIsRemovedIsEnded(t *testing.T) {
	f := newClaimedOnWorker(t)

	require.NoError(t, f.repo.RemoveWorker(f.worker.Id))

	f.requireClaimedContainerEnded(t)
}

// A worker is forgotten when it is on the index and has no state. Between
// the read that found no state and the forgetting, the worker can have been
// put on record again: a machine's worker is, by its pool, when its record
// has expired. That worker is not lost.
func TestAWorkerThatIsOnRecordAgainIsNotForgotten(t *testing.T) {
	f := newClaimedOnWorker(t)
	ctx := context.TODO()
	stateKey := common.RedisKeys.SchedulerWorkerState(f.worker.Id)

	f.repo.forgetLostWorker(ctx, stateKey, common.RedisKeys.SchedulerWorkerIndex())

	workers, err := f.repo.GetAllWorkers()
	require.NoError(t, err)
	require.Len(t, workers, 1, "the worker is still on the index")
	require.EqualValues(t, 1, f.rdb.LLen(ctx, common.RedisKeys.SchedulerWorkerRequests(f.worker.Id)).Val(), "with the request that is queued on it")
	require.Empty(t, backlogContainerIDs(t, f.rdb))
	status, _, _ := f.state(t, "claimed")
	require.Equal(t, types.ContainerStatusPending, status)
}

// Whoever writes a worker's record holds the worker's lock. A read that
// finds the worker without a state while the lock is held leaves it for the
// next read to look at.
func TestAWorkerIsNotForgottenWhileItsRecordIsBeingWritten(t *testing.T) {
	f := newClaimedOnWorker(t)
	ctx := context.TODO()
	require.NoError(t, f.rdb.Del(ctx, common.RedisKeys.SchedulerWorkerState(f.worker.Id)).Err())

	writer := common.NewRedisLock(f.rdb)
	lockKey := common.RedisKeys.SchedulerWorkerLock(f.worker.Id)
	require.NoError(t, writer.Acquire(ctx, lockKey, schedulerWorkerLockOptions))

	workers, err := f.repo.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)
	require.Empty(t, backlogContainerIDs(t, f.rdb))
	indexed, err := f.rdb.SMembers(ctx, common.RedisKeys.SchedulerWorkerIndex()).Result()
	require.NoError(t, err)
	require.Len(t, indexed, 1)

	// The writer is done and wrote nothing: the worker is lost after all.
	require.NoError(t, writer.Release(lockKey))
	_, err = f.repo.GetAllWorkers()
	require.NoError(t, err)
	f.requireClaimedContainerEnded(t)
	indexed, err = f.rdb.SMembers(ctx, common.RedisKeys.SchedulerWorkerIndex()).Result()
	require.NoError(t, err)
	require.Empty(t, indexed)
}

// The backlog has two lanes. Work that only fills spare capacity waits in a
// lane of its own, which the scheduler takes from after the other, so that it
// never holds up a request that is waiting for capacity of its own. A request
// that comes back from a lost worker belongs in the lane it was taken from.

func laneContainerIDs(t *testing.T, rdb *common.RedisClient, lane string) []string {
	t.Helper()
	members, err := rdb.ZRange(context.TODO(), lane, 0, -1).Result()
	require.NoError(t, err)
	ids := make([]string, 0, len(members))
	for _, raw := range members {
		var request types.ContainerRequest
		require.NoError(t, json.Unmarshal([]byte(raw), &request))
		ids = append(ids, request.ContainerId)
	}
	return ids
}

func TestRequestsOfALostWorkerReturnToTheLaneTheyCameFrom(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id: "lost-worker", Status: types.WorkerStatusAvailable, PoolName: "default",
		FreeCpu: 10_000, FreeMemory: 10_000, TotalCpu: 10_000, TotalMemory: 10_000,
	}
	require.NoError(t, repo.AddWorker(worker))

	requests := []*types.ContainerRequest{
		{ContainerId: "serverless", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
		{ContainerId: "replica", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, Evictable: true},
		{ContainerId: "spare-only", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, OpportunisticOnly: true},
		{ContainerId: "spot", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, Evictable: true, OpportunisticOnly: true},
	}
	for _, request := range requests {
		require.Equal(t, request.ContainerId != "serverless", BacklogLane(request) == common.RedisKeys.SchedulerBackgroundRequests())
	}
	setPendingContainerRequests(t, rdb, requests...)
	require.NoError(t, repo.ScheduleContainerRequests(worker, requests))
	// The worker had taken some of them and started none.
	taken, err := repo.GetNextContainerRequests(worker.Id, 2)
	require.NoError(t, err)
	require.Len(t, taken, 2)

	// Something else waits in each lane already.
	for _, waiting := range []*types.ContainerRequest{{ContainerId: "waiting"}, {ContainerId: "waiting-replica", Evictable: true}} {
		member, err := json.Marshal(waiting)
		require.NoError(t, err)
		require.NoError(t, rdb.ZAdd(context.TODO(), BacklogLane(waiting), redis.Z{Score: 1, Member: member}).Err())
	}

	// The worker stops sending keepalives and its state expires.
	require.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id)).Err())
	workers, err := repo.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)

	require.Equal(t, []string{"waiting", "serverless"}, laneContainerIDs(t, rdb, common.RedisKeys.SchedulerContainerRequests()))
	require.ElementsMatch(t, []string{"waiting-replica", "replica", "spare-only", "spot"}, laneContainerIDs(t, rdb, common.RedisKeys.SchedulerBackgroundRequests()))
	require.EqualValues(t, 6, vmetrics.GetDefaultSet().GetOrCreateGauge("scheduler_backlog_depth", nil).Get(),
		"the backlog's depth is what waits in both lanes")
}

// A worker's requests are moved all together or not at all, and nothing is
// moved into a lane that is not a backlog.
func TestRequestsOfALostWorkerStayOnItWhenALaneCannotTakeThem(t *testing.T) {
	for _, broken := range []string{common.RedisKeys.SchedulerContainerRequests(), common.RedisKeys.SchedulerBackgroundRequests()} {
		t.Run(broken, func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			require.NoError(t, err)
			repo := NewWorkerRedisRepositoryForTest(rdb)
			worker := &types.Worker{
				Id: "lost-worker", Status: types.WorkerStatusAvailable, PoolName: "default",
				FreeCpu: 10_000, FreeMemory: 10_000, TotalCpu: 10_000, TotalMemory: 10_000,
			}
			require.NoError(t, repo.AddWorker(worker))
			requests := []*types.ContainerRequest{
				{ContainerId: "serverless", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
				{ContainerId: "replica", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, Evictable: true},
			}
			setPendingContainerRequests(t, rdb, requests...)
			require.NoError(t, repo.ScheduleContainerRequests(worker, requests))
			require.NoError(t, rdb.Set(context.TODO(), broken, "not a sorted set", 0).Err())

			require.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id)).Err())
			_, err = repo.GetAllWorkers()
			require.NoError(t, err)

			require.EqualValues(t, 2, rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
			indexed, err := rdb.SMembers(context.TODO(), common.RedisKeys.SchedulerWorkerIndex()).Result()
			require.NoError(t, err)
			require.Len(t, indexed, 1, "the worker stays on the index, so the next read tries again")
		})
	}
}

func backlogRequests(t *testing.T, rdb *common.RedisClient) map[string]types.ContainerRequest {
	t.Helper()
	backlog, err := rdb.ZRange(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), 0, -1).Result()
	require.NoError(t, err)
	requests := map[string]types.ContainerRequest{}
	for _, raw := range backlog {
		var request types.ContainerRequest
		require.NoError(t, json.Unmarshal([]byte(raw), &request))
		requests[request.ContainerId] = request
	}
	return requests
}

// A request is stamped with the machine it is placed on. When that machine's
// worker is lost, the request goes back to the backlog free to run anywhere:
// only a machine its caller chose still binds it.
func TestARequeuedRequestIsNotBoundToTheMachineItWasPlacedOn(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)

	worker := &types.Worker{
		Id: "worker-1", MachineId: "machine-1", Status: types.WorkerStatusAvailable, PoolName: "spot-l40s",
		FreeCpu: 10_000, FreeMemory: 10_000, TotalCpu: 10_000, TotalMemory: 10_000,
	}
	require.NoError(t, repo.AddWorker(worker))
	requests := []*types.ContainerRequest{
		{ContainerId: "taken", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
		{ContainerId: "queued", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
		{ContainerId: "pinned", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, MachineId: "machine-1"},
	}
	setPendingContainerRequests(t, rdb, requests...)
	require.NoError(t, repo.ScheduleContainerRequests(worker, requests))

	// On the worker, each request names the machine it runs on.
	taken, err := repo.GetNextContainerRequests(worker.Id, 1)
	require.NoError(t, err)
	require.Len(t, taken, 1)
	require.Equal(t, "taken", taken[0].ContainerId)
	require.Equal(t, "machine-1", taken[0].MachineId)
	require.True(t, taken[0].MachineAssigned)

	// The machine is reclaimed.
	require.NoError(t, repo.RemoveWorker(worker.Id))

	requeued := backlogRequests(t, rdb)
	require.Len(t, requeued, 3)
	for _, id := range []string{"taken", "queued"} {
		require.Empty(t, requeued[id].MachineId, "%s is still bound to the machine that is gone", id)
		require.False(t, requeued[id].MachineAssigned)
	}
	require.Equal(t, "machine-1", requeued["pinned"].MachineId, "its caller chose the machine")
	require.False(t, requeued["pinned"].MachineAssigned)
}
