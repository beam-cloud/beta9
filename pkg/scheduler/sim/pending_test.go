package schedsim

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// These scenarios pin what a container that has not started says it waits
// for (pkg/scheduler/pending.go). A function that asks for a GPU and names
// no pool may be interrupted in production, so the functions here are
// submitted that way: they are the work that capacity which can be taken
// back runs.

// pendingOf is what the container says it waits for, as phase/reason, and
// empty once it runs.
func pendingOf(sim *Sim, id string) string {
	sim.t.Helper()
	state, err := sim.containerRepo.GetContainerState(id)
	require.NoError(sim.t, err)
	if state.PendingReason == "" {
		return string(state.PendingPhase)
	}
	return string(state.PendingPhase) + "/" + string(state.PendingReason)
}

// pendingSince is when the container began to wait for what it waits for.
func pendingSince(sim *Sim, id string) int64 {
	sim.t.Helper()
	state, err := sim.containerRepo.GetContainerState(id)
	require.NoError(sim.t, err)
	return state.PendingSince
}

// A pool with nothing warm adds a worker for the request, and the request
// says that is what it waits for. Nothing is wrong: this is a cold start.
func TestRequestWaitingForAWorkerToStartSaysSo(t *testing.T) {
	sim := New(t, onePool)
	sim.Pool("a10g").BootAfter(2 * time.Minute)

	ids := sim.Submit(Function("A10G").For(time.Hour), 1)
	require.Equal(t, "queued", pendingOf(sim, ids[0]))

	sim.Advance(30 * time.Second)
	require.Equal(t, "provisioning/worker_starting", pendingOf(sim, ids[0]))
	since := pendingSince(sim, ids[0])
	require.NotZero(t, since)

	// It passes several times a second while it waits, and goes on saying
	// what it said: since when does not move.
	sim.Advance(time.Minute)
	require.Equal(t, "provisioning/worker_starting", pendingOf(sim, ids[0]))
	require.Equal(t, since, pendingSince(sim, ids[0]))

	sim.Advance(time.Minute)
	require.Equal(t, []string{"a10g"}, sim.PoolsOf(ids))
	require.Empty(t, pendingOf(sim, ids[0]), "a container that runs waits for nothing")
}

// The same for a pool that buys its machines: one is bought for the request
// and boots.
func TestRequestWaitingForASpotMachineToBootSaysAWorkerIsStarting(t *testing.T) {
	sim := New(t, spotPool)
	sim.Provider("verda").BootAfter(3 * time.Minute)

	ids := sim.Submit(Function("L40S").Preemptable(true).For(time.Hour), 1)
	sim.Advance(time.Minute)

	require.Equal(t, 1, machines(sim, "verda"))
	require.Equal(t, "provisioning/worker_starting", pendingOf(sim, ids[0]))

	sim.Advance(3 * time.Minute)
	require.Equal(t, []string{"spot-l40s"}, sim.PoolsOf(ids))
	require.Empty(t, pendingOf(sim, ids[0]))
}

// spotThenOnDemand is a Kubernetes pool on nodes that can be taken back,
// tried first, and one on nodes that cannot behind it.
const spotThenOnDemand = `
worker:
  pools:
    spot-a10g:
      gpuType: A10G
      priority: 10
      preemptable: true
      stalledAfter: 2m
    a10g:
      gpuType: A10G
`

// When the cluster has no spot node for the worker, the request gives up on
// it after stalledAfter and has the on-demand pool add one. It says that it
// is a backup it waits for, and then runs there.
func TestRequestThatMovesOnFromAStalledSpotPoolSaysItWaitsForABackup(t *testing.T) {
	sim := New(t, spotThenOnDemand)
	sim.Pool("spot-a10g").Stall()
	sim.Pool("a10g").BootAfter(2 * time.Minute)

	ids := sim.Submit(Function("A10G").Preemptable(true).For(time.Hour), 1)
	sim.Advance(90 * time.Second)

	require.Equal(t, "provisioning/worker_starting", pendingOf(sim, ids[0]))
	require.Equal(t, map[string]int{"spot-a10g": 1, "a10g": 0}, started(sim, "spot-a10g", "a10g"))

	sim.Advance(90 * time.Second)
	require.Equal(t, "provisioning/backup_worker_starting", pendingOf(sim, ids[0]))
	require.Equal(t, map[string]int{"spot-a10g": 1, "a10g": 1}, started(sim, "spot-a10g", "a10g"))

	sim.Advance(2 * time.Minute)
	require.Equal(t, []string{"a10g"}, sim.PoolsOf(ids))
	sim.ExpectFailed(0)
}

// Without stalledAfter the request never gives up on the spot pool, however
// long its worker stays Pending: the on-demand pool behind it is not asked.
// What the request says is true and of no use, which is why a spot pool that
// has another pool behind it sets stalledAfter.
func TestRequestOnASpotPoolWithoutStalledAfterNeverReachesTheBackup(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    spot-a10g:
      gpuType: A10G
      priority: 10
      preemptable: true
    a10g:
      gpuType: A10G
`)
	sim.Pool("spot-a10g").Stall()

	ids := sim.Submit(Function("A10G").Preemptable(true).For(time.Hour), 1)
	sim.Advance(8 * time.Minute)

	require.Equal(t, "provisioning/worker_starting", pendingOf(sim, ids[0]))
	require.Zero(t, sim.Pool("a10g").Started())
	sim.ExpectWaiting(1)
}

// buyingSpotThenOnDemand is the same route for pools that buy their
// machines: spot first, on demand behind it.
const buyingSpotThenOnDemand = `
worker:
  pools:
    spot-l40s:
      gpuType: L40S
      priority: 20
      provisioning: {providers: [verda], spot: true, maxNodes: 4}
    l40s:
      gpuType: L40S
      priority: 10
      provisioning: {providers: [nebius], maxNodes: 4}
`

// A pool that buys knows at once that there is no spot machine to buy. The
// request moves on in the same pass, and waits for the on-demand machine.
func TestRequestThatFindsNoSpotMachineToBuySaysItWaitsForABackup(t *testing.T) {
	sim := New(t, buyingSpotThenOnDemand)
	sim.Provider("verda").Stock("L40S", 0)
	sim.Provider("nebius").BootAfter(3 * time.Minute)

	ids := sim.Submit(Function("L40S").Preemptable(true).For(time.Hour), 1)
	sim.Advance(time.Minute)

	require.Zero(t, sim.Provider("verda").Bought())
	require.Equal(t, 1, sim.Provider("nebius").Bought())
	require.Equal(t, "provisioning/backup_worker_starting", pendingOf(sim, ids[0]))

	sim.Advance(3 * time.Minute)
	require.Equal(t, []string{"l40s"}, sim.PoolsOf(ids))
}

// Two pools that the route tells apart by name alone are tried first
// together: the second is not a backup for the first.
func TestPoolsOfEqualStandingAreNotBackupsForEachOther(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    spot-a-l40s:
      gpuType: L40S
      provisioning: {providers: [verda], spot: true, maxNodes: 4}
    spot-b-l40s:
      gpuType: L40S
      provisioning: {providers: [nebius], spot: true, maxNodes: 4}
`)
	sim.Provider("verda").Stock("L40S", 0)
	sim.Provider("nebius").BootAfter(3 * time.Minute)

	ids := sim.Submit(Function("L40S").Preemptable(true).For(time.Hour), 1)
	sim.Advance(time.Minute)

	require.Equal(t, 1, sim.Provider("nebius").Bought())
	require.Equal(t, "provisioning/worker_starting", pendingOf(sim, ids[0]))
}

// When no pool can add a worker the request says so, for as long as that
// lasts: it is not the same as waiting for a machine that is on its way. It
// goes on waiting, and runs once there is a machine to be had.
func TestRequestThatNoPoolCanAddAWorkerForSaysThereIsNoCapacity(t *testing.T) {
	sim := New(t, buyingSpotThenOnDemand)
	sim.Provider("verda").Stock("L40S", 0).BootAfter(2 * time.Minute)
	sim.Provider("nebius").Stock("L40S", 0)

	ids := sim.Submit(Function("L40S").Preemptable(true).For(time.Hour), 1)
	sim.Advance(time.Minute)

	require.Equal(t, "waiting_for_capacity/no_capacity", pendingOf(sim, ids[0]))
	require.Zero(t, sim.Provider("verda").Bought()+sim.Provider("nebius").Bought())
	sim.ExpectWaiting(1)
	sim.ExpectFailed(0)

	sim.Provider("verda").Stock("L40S", 1)
	sim.Advance(time.Minute)
	require.Equal(t, "provisioning/worker_starting", pendingOf(sim, ids[0]))

	sim.Advance(2 * time.Minute)
	require.Equal(t, []string{"spot-l40s"}, sim.PoolsOf(ids))
}

// A request that was placed on a worker which then went away waits again,
// and says what for anew: not that the lost worker is starting it.
func TestRequestThatLosesItsWorkerSaysWhatItWaitsForAgain(t *testing.T) {
	sim := New(t, onePool)
	sim.Warm("a10g")
	sim.Pool("a10g").Workers()[0].Vanish()

	// For up to a minute the scheduler still believes the worker is there
	// and places work on it.
	ids := sim.Submit(Endpoint("A10G").For(time.Hour), 1)
	sim.Advance(5 * time.Second)
	require.Equal(t, "starting", pendingOf(sim, ids[0]))

	// The worker's state expires and the request returns to the backlog.
	// The pool adds a worker for it, which takes a while to start.
	sim.Pool("a10g").BootAfter(10 * time.Minute)
	sim.Advance(5 * time.Minute)
	sim.ExpectRequeued(1)
	require.Equal(t, "provisioning/worker_starting", pendingOf(sim, ids[0]))
}

// failureOf is why the scheduler gave up on a container, as whoever waited
// for it reads it.
func failureOf(sim *Sim, containerID string) types.ContainerRequestFailure {
	sim.t.Helper()
	failure, err := sim.containerRepo.GetContainerRequestFailure(containerID)
	require.NoError(sim.t, err)
	require.NotNil(sim.t, failure, "nothing says why the scheduler gave up on %s", containerID)
	return *failure
}

// A request that waits out the scheduling limit with no capacity to be had
// is given up on, and what it asked for and how long it waited are kept for
// its caller.
func TestRequestThatNeverGetsCapacityIsGivenUpOnWithWhy(t *testing.T) {
	sim := New(t, buyingSpotThenOnDemand)
	sim.Provider("verda").Stock("L40S", 0)
	sim.Provider("nebius").Stock("L40S", 0)

	ids := sim.Submit(Function("L40S").Preemptable(true).For(time.Hour), 1)
	sim.Advance(19 * time.Minute)
	require.Equal(t, "waiting_for_capacity/no_capacity", pendingOf(sim, ids[0]))
	sim.ExpectFailed(0)

	sim.Advance(90 * time.Second)
	sim.ExpectFailed(1)
	failure := failureOf(sim, ids[0])
	require.Equal(t, types.ContainerSchedulingFailureWorkerCapacityTimeout, failure.Reason)
	require.Equal(t, []string{"L40S"}, failure.Gpus)
	require.Empty(t, failure.PoolSelector)
	require.InDelta(t, 20*60, failure.WaitedSeconds, 5)
	require.True(t, failure.NoCapacity())
}

// A pool that cannot start a worker however often it is asked gives the
// request up sooner, and that is a lack of capacity too.
func TestRequestWhoseWorkersNeverStartIsGivenUpOnForWantOfCapacity(t *testing.T) {
	sim := New(t, onePool)
	sim.Pool("a10g").RefuseWorkers()

	ids := sim.Submit(Endpoint("A10G").For(time.Hour), 1)
	sim.Advance(time.Minute)

	sim.ExpectFailed(1)
	failure := failureOf(sim, ids[0])
	require.Equal(t, types.ContainerSchedulingFailureProvisioningLimit, failure.Reason)
	require.Equal(t, []string{"A10G"}, failure.Gpus)
	require.True(t, failure.NoCapacity())
	require.Less(t, failure.WaitedSeconds, int64(60))

	// Why is kept for as long as the request is known to have failed.
	sim.Advance(types.ContainerRequestStatusTTL)
	kept, err := sim.containerRepo.GetContainerRequestFailure(ids[0])
	require.NoError(t, err)
	require.Nil(t, kept)
}
