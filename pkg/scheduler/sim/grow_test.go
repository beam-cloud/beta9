package schedsim_test

import (
	"testing"
	"time"

	schedsim "github.com/beam-cloud/beta9/pkg/scheduler/sim"
	"github.com/stretchr/testify/require"
)

// These scenarios cover which pool adds a worker when nothing in a request's
// route has room.

// growPools gives A10G requests a route of four pools: a10g, a10g-reserve,
// then rtx4090 and l40s, which also serve A10G.
const growPools = `
worker:
  pools:
    a10g:
      gpuType: A10G
    a10g-reserve:
      gpuType: A10G
      priority: -2
    rtx4090:
      gpuType: RTX4090
      priority: 5
      alsoServes: [A10G]
    l40s:
      gpuType: L40S
      priority: 3
      alsoServes: [A10G]
`

func refused(sim *schedsim.Sim, pools ...string) map[string]int {
	out := map[string]int{}
	for _, name := range pools {
		out[name] = sim.Pool(name).Refused()
	}
	return out
}

// Invariant 2, first half: a pool is asked to grow only when no pool in the
// route has room or pending capacity for the request.
func TestPoolGrowsOnlyWhenNothingInTheRouteHasRoomOrIsStarting(t *testing.T) {
	// Each a10g worker has two GPUs, so one worker has room for two requests.
	const pools = `
worker:
  pools:
    a10g:
      gpuType: A10G
      poolSizing:
        defaultWorkerCPU: 4000m
        defaultWorkerMemory: 8Gi
        defaultWorkerGpuCount: "2"
    rtx4090:
      gpuType: RTX4090
      alsoServes: [A10G]
`
	t.Run("a starting worker with room is waited for", func(t *testing.T) {
		sim := schedsim.New(t, pools)

		first := sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 1)
		sim.Advance(5 * time.Second)
		require.Equal(t, 1, sim.Pool("a10g").Started())

		// The worker is still starting. It will have a GPU to spare, so the
		// second request waits for it instead of growing either pool.
		second := sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 1)
		sim.Advance(time.Minute)

		require.Equal(t, []string{"a10g", "a10g"}, sim.PoolsOf(append(first, second...)))
		require.Equal(t, map[string]int{"a10g": 1, "rtx4090": 0}, started(sim, "a10g", "rtx4090"))
	})

	t.Run("a worker with room is used", func(t *testing.T) {
		sim := schedsim.New(t, pools)
		sim.Warm("rtx4090")

		ids := sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 1)
		sim.Advance(time.Minute)

		require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
		require.Equal(t, map[string]int{"a10g": 0, "rtx4090": 1}, started(sim, "a10g", "rtx4090"))
	})

	t.Run("with neither, the first pool in the route grows", func(t *testing.T) {
		sim := schedsim.New(t, pools)

		ids := sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 3)
		sim.Advance(time.Minute)

		// Two workers of two GPUs for three requests, all in the first pool.
		require.Equal(t, []string{"a10g", "a10g", "a10g"}, sim.PoolsOf(ids))
		require.Equal(t, map[string]int{"a10g": 2, "rtx4090": 0}, started(sim, "a10g", "rtx4090"))
	})
}

// The attempt limit is per pool. Counted per request, three pools that
// refuse would use it up and the fourth would never be asked.
func TestRequestReachesTheEndOfALongRouteOfRefusingPools(t *testing.T) {
	sim := schedsim.New(t, growPools)
	sim.Pool("a10g").RefuseWorkers()
	sim.Pool("a10g-reserve").RefuseWorkers()
	sim.Pool("rtx4090").RefuseWorkers()

	ids := sim.Submit(schedsim.Endpoint("A10G"), 1)
	sim.Advance(2 * time.Minute)

	require.Equal(t, []string{"l40s"}, sim.PoolsOf(ids))
	sim.ExpectFailed(0)
}

func TestRequestAsksEachPoolInItsRouteForThreeWorkersThenFails(t *testing.T) {
	sim := schedsim.New(t, growPools)
	pools := []string{"a10g", "a10g-reserve", "rtx4090", "l40s"}
	for _, name := range pools {
		sim.Pool(name).RefuseWorkers()
	}

	sim.Submit(schedsim.Endpoint("A10G"), 1)
	sim.Advance(2 * time.Minute)

	sim.ExpectFailed(1)
	require.Equal(t, map[string]int{"a10g": 3, "a10g-reserve": 3, "rtx4090": 3, "l40s": 3}, refused(sim, pools...))

	// Nothing is asked again once the request has failed.
	sim.Advance(5 * time.Minute)
	require.Equal(t, map[string]int{"a10g": 3, "a10g-reserve": 3, "rtx4090": 3, "l40s": 3}, refused(sim, pools...))
}

// A pool that just failed to add a worker is left alone for a moment, so a
// queue of waiting requests does not hammer a cluster that is refusing Jobs.
func TestRefusingPoolIsNotAskedForEveryWaitingRequestAtOnce(t *testing.T) {
	const pools = `
worker:
  pools:
    t4:
      gpuType: T4
`
	sim := schedsim.New(t, pools)
	sim.Pool("t4").RefuseWorkers()

	sim.Submit(schedsim.Endpoint("T4"), 20)
	sim.Advance(4 * time.Second)

	require.Equal(t, 1, sim.Pool("t4").Refused())
}

// stallPools has an A10G pool that gives up on workers that have not started
// after two minutes, and an RTX4090 pool that A10G requests may overflow to.
const stallPools = `
worker:
  pools:
    a10g:
      gpuType: A10G
      stalledAfter: 2m
    rtx4090:
      gpuType: RTX4090
      alsoServes: [A10G]
`

func TestStalledPoolOverflowsToAPoolThatAlsoServesTheRequest(t *testing.T) {
	sim := schedsim.New(t, stallPools)
	sim.Pool("a10g").Stall()

	first := sim.Submit(schedsim.Endpoint("A10G").For(30*time.Minute), 1)
	sim.Advance(90 * time.Second)

	// Until stalledAfter the request waits for the worker it asked for.
	require.Equal(t, []string{""}, sim.PoolsOf(first))
	require.Equal(t, map[string]int{"a10g": 1, "rtx4090": 0}, started(sim, "a10g", "rtx4090"))

	sim.Advance(90 * time.Second)
	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(first))

	// The pool still has a worker that never started, so the next request
	// passes over it at once instead of waiting two minutes of its own.
	second := sim.Submit(schedsim.Endpoint("A10G").For(30*time.Minute), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(second))
	require.Equal(t, map[string]int{"a10g": 1, "rtx4090": 2}, started(sim, "a10g", "rtx4090"))
}

// Invariant 2, second half: a pool that cannot grow is skipped without
// spending an attempt. The request below passes over its stalled pool on
// every scheduling pass for minutes and is neither failed nor given a second
// worker.
func TestPoolThatCannotGrowIsSkippedWithoutSpendingAnAttempt(t *testing.T) {
	const pools = `
worker:
  pools:
    t4:
      gpuType: T4
      stalledAfter: 1m
`
	sim := schedsim.New(t, pools)
	sim.Pool("t4").Stall()

	ids := sim.Submit(schedsim.Endpoint("T4"), 1)
	sim.Advance(8 * time.Minute)

	sim.ExpectFailed(0)
	sim.ExpectWaiting(1)
	require.Equal(t, 1, sim.Pool("t4").Started())

	// When the cluster finds room the worker starts, and the request that
	// was waiting on the pool runs there.
	sim.Pool("t4").Unstall()
	sim.Advance(10 * time.Second)

	require.Equal(t, []string{"t4"}, sim.PoolsOf(ids))
	require.Equal(t, 1, sim.Pool("t4").Started())
}

func TestPoolGrowsAgainOnceItsWorkersStart(t *testing.T) {
	sim := schedsim.New(t, stallPools)
	sim.Pool("a10g").Stall()

	sim.Submit(schedsim.Endpoint("A10G").For(30*time.Minute), 1)
	sim.Advance(3 * time.Minute)
	sim.ExpectPlaced("rtx4090", 1)

	sim.Pool("a10g").Unstall()
	sim.Advance(10 * time.Second)

	// The worker that was stuck is idle now and takes the next request; the
	// one after that makes the pool add a worker again.
	ids := sim.Submit(schedsim.Endpoint("A10G").For(30*time.Minute), 2)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"a10g", "a10g"}, sim.PoolsOf(ids))
	require.Equal(t, map[string]int{"a10g": 2, "rtx4090": 1}, started(sim, "a10g", "rtx4090"))
}

// How long a worker has been starting is read from its shared state, so a
// scheduler replica that did not add the worker passes over the pool too.
func TestEverySchedulerReplicaSeesAStalledPool(t *testing.T) {
	sim := schedsim.New(t, stallPools)
	sim.AddScheduler()
	sim.Pool("a10g").Stall()

	sim.Submit(schedsim.Endpoint("A10G").For(30*time.Minute), 1)
	sim.Advance(3 * time.Minute)
	sim.ExpectPlaced("rtx4090", 1)

	// Requests alternate between the replicas.
	ids := sim.Submit(schedsim.Endpoint("A10G").For(30*time.Minute), 2)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"rtx4090", "rtx4090"}, sim.PoolsOf(ids))
	require.Equal(t, 1, sim.Pool("a10g").Started())
}

// stalledAfter is opt-in. Without it a pool that cannot get nodes keeps its
// requests until the scheduling limit, as it always has.
func TestPoolWithoutStalledAfterKeepsItsRequests(t *testing.T) {
	const pools = `
worker:
  pools:
    a10g:
      gpuType: A10G
    rtx4090:
      gpuType: RTX4090
      alsoServes: [A10G]
`
	sim := schedsim.New(t, pools)
	sim.Pool("a10g").Stall()

	ids := sim.Submit(schedsim.Endpoint("A10G"), 1)
	sim.Advance(19 * time.Minute)

	require.Equal(t, []string{""}, sim.PoolsOf(ids))
	require.Equal(t, 0, sim.Pool("rtx4090").Started())
	sim.ExpectFailed(0)

	sim.Advance(2 * time.Minute)
	sim.ExpectFailed(1)
}
