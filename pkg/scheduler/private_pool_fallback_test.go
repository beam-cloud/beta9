package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/clock"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// A request that names its workspace's own pool falls back to the regular
// pools when its pool has no room for it and its stub allows it. On its own
// pool the request is exempt from the credit gate and from the managed
// concurrency limit. On the regular pools it is not, so the fallback asks
// both before the request leaves its pool.

const (
	fallbackWorkspace = "workspace-private-fallback"
	fallbackOwnPool   = "private-cpu"
)

// fallbackFixture is a scheduler with a workspace's own pool that has no
// room for the request and cannot add a worker, and a regular pool with a
// worker that has room.
type fallbackFixture struct {
	*Scheduler
	t        *testing.T
	clock    *clock.Fake
	request  *types.ContainerRequest
	regular  *types.Worker
	failures <-chan *redis.Message
}

func newFallbackFixture(t *testing.T) *fallbackFixture {
	t.Helper()
	s, err := NewSchedulerForTest()
	require.NoError(t, err)

	f := &fallbackFixture{Scheduler: s, t: t, clock: clock.NewFake(time.Now())}
	s.clock, s.requestBacklog.now, s.spawn = f.clock, f.clock.Now, inline
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f.failures, _ = s.requestBacklog.rdb.Subscribe(ctx, common.EventChannelKey(common.EventTypeContainerSchedulingFailed))

	state := &compute.PoolState{Selector: fallbackOwnPool, Mode: string(types.PoolModePrivate)}
	s.workerPoolManager.SetPoolAt(agentPoolControllerKey(fallbackWorkspace, state), fallbackOwnPool,
		types.WorkerPoolConfig{Mode: types.PoolModePrivate, RequiresPoolSelector: true},
		&LocalWorkerPoolControllerForTest{ctx: ctx, name: fallbackOwnPool, config: s.config, workerRepo: s.workerRepo, requiresSelector: true, cannotAddWorker: true})

	f.regular = &types.Worker{Id: "regular-worker", Status: types.WorkerStatusAvailable, FreeCpu: 2000, FreeMemory: 2000, PoolName: "beta9-cpu"}
	for _, worker := range []*types.Worker{
		{Id: "own-worker", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 2000, PoolName: fallbackOwnPool, RequiresPoolSelector: true},
		f.regular,
	} {
		require.NoError(t, s.workerRepo.AddWorker(worker))
	}

	f.request = &types.ContainerRequest{
		ContainerId: uuid.NewString(), TaskId: uuid.NewString(), WorkspaceId: fallbackWorkspace,
		Cpu: 1000, Memory: 1000, PoolSelector: fallbackOwnPool, Workspace: testWorkspaceWithStorage(),
	}
	return f
}

// submit sends the request to the scheduler. It names its workspace's own
// pool, so Run asks neither for credit nor for the concurrency limit.
func (f *fallbackFixture) submit() {
	f.t.Helper()
	require.NoError(f.t, f.Run(f.request))
}

// pass is one pass of the scheduling loop.
func (f *fallbackFixture) pass() {
	f.t.Helper()
	f.ProcessBacklog()
}

// waiting returns the request as it waits in the backlog, or nil if it does
// not.
func (f *fallbackFixture) waiting() *types.ContainerRequest {
	f.t.Helper()
	members, err := f.requestBacklog.rdb.ZRange(context.Background(), common.RedisKeys.SchedulerContainerRequests(), 0, -1).Result()
	require.NoError(f.t, err)
	for _, member := range members {
		var request types.ContainerRequest
		require.NoError(f.t, json.Unmarshal([]byte(member), &request))
		if request.ContainerId == f.request.ContainerId {
			return &request
		}
	}
	return nil
}

// placedOn reports whether the request is on the worker's queue.
func (f *fallbackFixture) placedOn(worker *types.Worker) bool {
	f.t.Helper()
	queued, err := f.workerRepo.GetNextContainerRequests(worker.Id, 1)
	require.NoError(f.t, err)
	return len(queued) == 1 && queued[0].ContainerId == f.request.ContainerId
}

// failed reports whether the scheduler gave up on the request.
func (f *fallbackFixture) failed() bool {
	status, err := f.containerRepo.GetContainerRequestStatus(f.request.ContainerId)
	return err == nil && status == types.ContainerRequestStatusFailed
}

// failure returns why the scheduler gave up on the request.
func (f *fallbackFixture) failure() types.ContainerSchedulingFailureReason {
	f.t.Helper()
	require.True(f.t, f.failed(), "the scheduler has not given up on the request")
	select {
	case message := <-f.failures:
		event, _, claimed := f.eventBus.Claim(message.Payload)
		require.True(f.t, claimed)
		failure, ok := common.ParseContainerSchedulingFailure(event)
		require.True(f.t, ok)
		return failure.Reason
	case <-time.After(time.Second):
		f.t.Fatal("the scheduler did not say why it gave up on the request")
		return ""
	}
}

// A request that has left its own pool is a request for the regular pools
// like any other: the fallback places nothing, and the next pass plans it.
func TestPrivatePoolFallbackLeavesTheRequestToTheNextPass(t *testing.T) {
	f := newFallbackFixture(t)
	f.submit()
	retries := f.waiting().RetryCount

	f.pass()

	waiting := f.waiting()
	require.NotNil(t, waiting, "the request goes back to the backlog")
	require.Empty(t, waiting.PoolSelector, "without its pool")
	require.Equal(t, retries, waiting.RetryCount, "falling back is not a retry")
	require.False(t, f.placedOn(f.regular))

	f.pass()
	require.Nil(t, f.waiting())
	require.True(t, f.placedOn(f.regular))
}

func TestPrivatePoolFallbackFailsARequestOfAWorkspaceWithoutCredit(t *testing.T) {
	f := newFallbackFixture(t)
	billing := &fakeCreditBackend{decision: creditDecision{OK: false, ErrorCode: "insufficient_credits"}}
	f.creditGate = newCreditGate(types.CreditGateConfig{}, billing, f.requestBacklog.rdb)
	f.submit()

	f.pass()

	require.Equal(t, types.ContainerSchedulingFailureManagedFallbackNoCredit, f.failure())
	require.Nil(t, f.waiting())
	require.False(t, f.placedOn(f.regular))
}

func TestPrivatePoolFallbackRetriesWhileBillingCannotBeAsked(t *testing.T) {
	f := newFallbackFixture(t)
	failOpen := false
	billing := &fakeCreditBackend{err: errors.New("billing is down")}
	f.creditGate = newCreditGate(types.CreditGateConfig{FailOpen: &failOpen}, billing, f.requestBacklog.rdb)
	f.submit()
	retries := f.waiting().RetryCount

	f.pass()

	require.False(t, f.failed(), "no answer is not a refusal")
	waiting := f.waiting()
	require.NotNil(t, waiting)
	require.Equal(t, fallbackOwnPool, waiting.PoolSelector)
	require.Equal(t, retries+1, waiting.RetryCount)

	// Billing answers again, and the request goes on to the regular pools.
	billing.err, billing.decision = nil, creditDecision{OK: true}
	f.clock.Advance(time.Second)
	f.pass()
	f.pass()
	require.True(t, f.placedOn(f.regular))
}

func TestPrivatePoolFallbackFailsARequestOverItsWorkspacesConcurrencyLimit(t *testing.T) {
	f := newFallbackFixture(t)
	f.backendRepo = &BackendRepoConcurrencyLimitsForTest{BackendRepository: f.backendRepo, CPUConcurrencyLimit: uint32(f.request.Cpu) / 2}
	// The workspace has asked for the regular pools before, so what it uses
	// of them is being counted.
	earlier := &types.ContainerRequest{ContainerId: uuid.NewString(), WorkspaceId: fallbackWorkspace, Cpu: 100}
	require.NoError(t, f.CheckConcurrencyLimit(earlier))
	f.submit()

	f.pass()

	require.Equal(t, types.ContainerSchedulingFailureManagedFallbackConcurrencyLimit, f.failure())
	require.Nil(t, f.waiting())
	require.False(t, f.placedOn(f.regular))
}

// unreadableConcurrencyLimits is a database that cannot be reached.
type unreadableConcurrencyLimits struct {
	repo.BackendRepository
}

func (unreadableConcurrencyLimits) GetConcurrencyLimitByWorkspaceId(context.Context, string) (*types.ConcurrencyLimit, error) {
	return nil, errors.New("database unavailable")
}

func TestPrivatePoolFallbackRetriesWhileTheConcurrencyLimitCannotBeRead(t *testing.T) {
	f := newFallbackFixture(t)
	limits := f.backendRepo
	f.backendRepo = unreadableConcurrencyLimits{limits}
	f.submit()
	retries := f.waiting().RetryCount

	f.pass()

	require.False(t, f.failed(), "no answer is not a refusal")
	waiting := f.waiting()
	require.NotNil(t, waiting)
	require.Equal(t, fallbackOwnPool, waiting.PoolSelector)
	require.Equal(t, retries+1, waiting.RetryCount)

	// The database answers again, and the request goes on to the regular
	// pools.
	f.backendRepo = limits
	f.clock.Advance(time.Second)
	f.pass()
	f.pass()
	require.True(t, f.placedOn(f.regular))
}

func TestPrivatePoolFallbackWaitsLikeAnyRequestWhenTheWorkerThatIsStartingIsTaken(t *testing.T) {
	f := newFallbackFixture(t)
	// The only regular pool cannot add a worker. It has one that is
	// starting, with room for one request, and another request waits for it.
	f.workerPoolManager.DeletePool("default")
	pool, ok := f.workerPoolManager.GetPool("beta9-cpu")
	require.True(t, ok)
	pool.Controller.(*LocalWorkerPoolControllerForTest).cannotAddWorker = true
	require.NoError(t, f.workerRepo.RemoveWorker(f.regular.Id))
	starting := &types.Worker{Id: "starting-worker", Status: types.WorkerStatusPending, PoolName: "beta9-cpu",
		TotalCpu: 1000, FreeCpu: 1000, TotalMemory: 2000, FreeMemory: 2000}
	require.NoError(t, f.workerRepo.AddWorker(starting))
	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	other := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1000, Timestamp: f.clock.Now()}
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, workers, other))
	f.submit()

	// The request leaves its pool for the worker that is starting, and then
	// finds no room on it. It waits for room, as the other request does.
	for range 3 {
		f.pass()
		f.clock.Advance(time.Second)
	}

	require.False(t, f.failed())
	waiting := f.waiting()
	require.NotNil(t, waiting)
	require.Empty(t, waiting.PoolSelector)
}
