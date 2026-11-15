package schedsim_test

import (
	"testing"
	"time"

	schedsim "github.com/beam-cloud/beta9/pkg/scheduler/sim"
	"github.com/stretchr/testify/require"
)

// These scenarios pin where requests go when a GPU type has fallback pools.
// They were written against scheduling.failover.chains, before alsoServes
// replaced it, and are kept as they were: a config that still has chains
// must route as it did.

// chainPools has two A10G pools, and two pools of other GPU types that A10G
// requests may fall back to. The fallback order matches the pools'
// priorities, as it does in production.
const chainPools = `
worker:
  pools:
    a10g:
      gpuType: A10G
      priority: 0
    a10g-reserve:
      gpuType: A10G
      priority: -2
    rtx4090:
      gpuType: RTX4090
      priority: 5
    l40s:
      gpuType: L40S
      priority: 3
      requiresPoolSelector: true
    t4:
      gpuType: T4
scheduling:
  failover:
    enabled: true
    chains:
      A10G:
        pools: [rtx4090, l40s]
`

func started(sim *schedsim.Sim, pools ...string) map[string]int {
	out := map[string]int{}
	for _, name := range pools {
		out[name] = sim.Pool(name).Started()
	}
	return out
}

func TestRequestFillsItsOwnPoolsThenItsFallbacksInOrder(t *testing.T) {
	sim := schedsim.New(t, chainPools)
	sim.Warm("l40s", "rtx4090", "a10g-reserve", "a10g")

	var ids []string
	for i := 0; i < 4; i++ {
		ids = append(ids, sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 1)...)
		sim.Advance(2 * time.Second)
	}

	// Its own GPU type first, by priority, although both fallbacks have a
	// higher priority. Then the fallbacks in their order.
	require.Equal(t, []string{"a10g", "a10g-reserve", "rtx4090", "l40s"}, sim.PoolsOf(ids))
}

func TestRequestsInOneBatchSpreadDownTheRoute(t *testing.T) {
	sim := schedsim.New(t, chainPools)
	sim.Warm("l40s", "rtx4090", "a10g-reserve", "a10g")

	ids := sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 4)
	sim.Advance(2 * time.Second)

	require.Equal(t, []string{"a10g", "a10g-reserve", "rtx4090", "l40s"}, sim.PoolsOf(ids))
}

func TestReadyFallbackCapacityBeatsGrowingTheRequestsOwnPool(t *testing.T) {
	sim := schedsim.New(t, chainPools)
	sim.Warm("rtx4090")

	ids := sim.Submit(schedsim.Endpoint("A10G"), 1)
	sim.Advance(2 * time.Second)

	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
	require.Equal(t, map[string]int{"a10g": 0, "a10g-reserve": 0}, started(sim, "a10g", "a10g-reserve"))
}

func TestRequestGrowsItsOwnPoolFirst(t *testing.T) {
	sim := schedsim.New(t, chainPools)

	ids := sim.Submit(schedsim.Endpoint("A10G"), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"a10g"}, sim.PoolsOf(ids))
	require.Equal(t,
		map[string]int{"a10g": 1, "a10g-reserve": 0, "rtx4090": 0, "l40s": 0},
		started(sim, "a10g", "a10g-reserve", "rtx4090", "l40s"))
}

func TestGrowthMovesDownTheRouteWhenAPoolRefuses(t *testing.T) {
	t.Run("to the next pool of the same GPU type", func(t *testing.T) {
		sim := schedsim.New(t, chainPools)
		sim.Pool("a10g").RefuseWorkers()

		ids := sim.Submit(schedsim.Endpoint("A10G"), 1)
		sim.Advance(2 * time.Minute)

		require.Equal(t, []string{"a10g-reserve"}, sim.PoolsOf(ids))
		require.Equal(t, map[string]int{"rtx4090": 0, "l40s": 0}, started(sim, "rtx4090", "l40s"))
	})

	t.Run("then to the first fallback", func(t *testing.T) {
		sim := schedsim.New(t, chainPools)
		sim.Pool("a10g").RefuseWorkers()
		sim.Pool("a10g-reserve").RefuseWorkers()

		ids := sim.Submit(schedsim.Endpoint("A10G"), 1)
		sim.Advance(2 * time.Minute)

		require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
		require.Equal(t, 0, sim.Pool("l40s").Started())
	})
}

func TestFallbackPoolBehindASelectorTakesOnlyFallbackRequests(t *testing.T) {
	sim := schedsim.New(t, chainPools)
	sim.Warm("l40s", "l40s")

	fallback := sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 1)
	direct := sim.Submit(schedsim.Endpoint("L40S"), 1)
	named := sim.Submit(schedsim.Endpoint("L40S").InPool("l40s"), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"l40s"}, sim.PoolsOf(fallback))
	require.Equal(t, []string{"l40s"}, sim.PoolsOf(named))
	// A request for the pool's own GPU type still has to name the pool.
	require.Equal(t, []string{""}, sim.PoolsOf(direct))
	sim.ExpectFailed(1)
}

func TestRequestThatNamesAPoolStaysThere(t *testing.T) {
	sim := schedsim.New(t, chainPools)
	sim.Warm("rtx4090", "a10g-reserve")
	sim.Pool("a10g").RefuseWorkers()

	ids := sim.Submit(schedsim.Endpoint("A10G").InPool("a10g"), 1)
	sim.Advance(2 * time.Minute)

	require.Equal(t, []string{""}, sim.PoolsOf(ids))
	sim.ExpectFailed(1)
}

func TestGPUTypeWithoutFallbacksStaysOnItsOwnPools(t *testing.T) {
	sim := schedsim.New(t, chainPools)
	sim.Warm("rtx4090", "l40s", "a10g")
	sim.Pool("t4").RefuseWorkers()

	ids := sim.Submit(schedsim.Endpoint("T4"), 1)
	sim.Advance(2 * time.Minute)

	require.Equal(t, []string{""}, sim.PoolsOf(ids))
	sim.ExpectFailed(1)
}

func TestRequestListingSeveralGPUTypes(t *testing.T) {
	t.Run("uses the fallbacks of the first type that has any", func(t *testing.T) {
		sim := schedsim.New(t, chainPools)
		sim.Warm("l40s")

		ids := sim.Submit(schedsim.Endpoint("H100", "A10G"), 1)
		sim.Advance(2 * time.Second)

		require.Equal(t, []string{"l40s"}, sim.PoolsOf(ids))
	})

	t.Run("treats a pool of a listed type as its own, not as a fallback", func(t *testing.T) {
		sim := schedsim.New(t, chainPools)
		sim.Warm("rtx4090", "a10g")

		// RTX4090 is listed, so the rtx4090 pool competes on priority with
		// the a10g pool instead of waiting behind it.
		ids := sim.Submit(schedsim.Endpoint("RTX4090", "A10G"), 1)
		sim.Advance(2 * time.Second)

		require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
	})
}

const preemptableFallback = `
worker:
  pools:
    a10g:
      gpuType: A10G
    rtx4090:
      gpuType: RTX4090
      priority: 5
      preemptable: true
    l40s:
      gpuType: L40S
      priority: 3
scheduling:
  failover:
    enabled: true
    chains:
      A10G:
        pools: [rtx4090, l40s]
`

func TestOnlyWorkThatAcceptsInterruptionUsesAPreemptableFallback(t *testing.T) {
	sim := schedsim.New(t, preemptableFallback)
	sim.Warm("rtx4090", "l40s")

	function := sim.Submit(schedsim.Function("A10G").For(10*time.Minute), 1)
	sim.Advance(2 * time.Second)
	endpoint := sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 1)
	sim.Advance(2 * time.Second)

	require.Equal(t, []string{"l40s"}, sim.PoolsOf(function))
	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(endpoint))
}

func TestTwoSchedulersPlaceEachRequestOnce(t *testing.T) {
	sim := schedsim.New(t, chainPools)
	sim.AddScheduler()

	ids := sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 12)
	sim.Advance(3 * time.Minute)

	for _, id := range ids {
		require.Len(t, sim.Container(id).Runs, 1, "container %s", id)
	}
	sim.ExpectPlaced("a10g", 12)
	// One worker per request and no more: a replica must not add a worker
	// for a request the other replica is already adding one for.
	require.Equal(t, 12, sim.Pool("a10g").Started())
}
