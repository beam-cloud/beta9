package schedsim_test

import (
	"slices"
	"testing"
	"time"

	schedsim "github.com/beam-cloud/beta9/pkg/scheduler/sim"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// These scenarios cover fallbacks configured on the pool with alsoServes.
// routing_test.go covers the same routes configured through the old
// scheduling.failover.chains, which is converted to alsoServes at startup.

// alsoServesPools is chainPools written the new way.
const alsoServesPools = `
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
      alsoServes: [A10G]
    l40s:
      gpuType: L40S
      priority: 3
      requiresPoolSelector: true
      alsoServes: [A10G]
    t4:
      gpuType: T4
`

// Invariant 1: a request is placed on the first pool in its route that has
// room for it now.
func TestRequestIsPlacedOnTheFirstPoolInItsRouteWithRoom(t *testing.T) {
	sim := schedsim.New(t, alsoServesPools)
	sim.Warm("l40s", "rtx4090", "a10g-reserve", "a10g", "t4")

	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 1)...)
		sim.Advance(2 * time.Second)
	}

	// Pools of the requested GPU type by priority, then the pools that also
	// serve it by priority. T4 is not in the route, so the fifth request adds
	// a worker to the first pool instead of using the idle T4 one.
	require.Equal(t, []string{"a10g", "a10g-reserve", "rtx4090", "l40s"}, sim.PoolsOf(ids[:4]))
	sim.Advance(time.Minute)
	require.Equal(t, []string{"a10g"}, sim.PoolsOf(ids[4:]))
	sim.ExpectPlaced("t4", 0)
}

// A placement reports how far down its route it landed: 0 on a pool of a
// type the request asked for, counting up through the pools that only also
// serve it.
func TestPlacementReportsItsRouteRank(t *testing.T) {
	sim := schedsim.New(t, alsoServesPools)
	sim.Warm("l40s", "rtx4090", "a10g")

	var ids []string
	for i := 0; i < 3; i++ {
		ids = append(ids, sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 1)...)
		sim.Advance(2 * time.Second)
	}
	// A request for any GPU asks for every pool it can use, so landing on
	// the RTX4090 pool is not overflow.
	sim.Warm("rtx4090")
	ids = append(ids, sim.Submit(schedsim.Endpoint("any").For(10*time.Minute), 1)...)
	sim.Advance(2 * time.Second)
	require.Equal(t, []string{"a10g", "rtx4090", "l40s", "rtx4090"}, sim.PoolsOf(ids))

	type placement struct{ pool, requested, placed, rank string }
	placements := map[string]placement{}
	for _, event := range sim.Events(types.EventComputeActionContainerPlaced) {
		require.NotContains(t, event.Attrs, "failover")
		require.NotContains(t, event.Attrs, "chain")
		if !slices.Contains(ids, event.ContainerID) {
			continue // Warming a pool places a request of its own.
		}
		placements[event.ContainerID] = placement{
			pool:      event.PoolName,
			requested: event.Attrs[types.EventComputeAttrRequestedGPU],
			placed:    event.Attrs[types.EventComputeAttrPlacedGPU],
			rank:      event.Attrs[types.EventComputeAttrRouteRank],
		}
	}
	require.Equal(t, map[string]placement{
		ids[0]: {pool: "a10g", requested: "A10G", placed: "A10G", rank: "0"},
		ids[1]: {pool: "rtx4090", requested: "A10G", placed: "RTX4090", rank: "1"},
		ids[2]: {pool: "l40s", requested: "A10G", placed: "L40S", rank: "2"},
		ids[3]: {pool: "rtx4090", requested: "any", placed: "RTX4090", rank: "0"},
	}, placements)
}

func TestGrowthFollowsTheRoute(t *testing.T) {
	sim := schedsim.New(t, alsoServesPools)
	sim.Pool("a10g").RefuseWorkers()
	sim.Pool("a10g-reserve").RefuseWorkers()

	ids := sim.Submit(schedsim.Endpoint("A10G"), 1)
	sim.Advance(2 * time.Minute)

	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
	require.Equal(t, 0, sim.Pool("l40s").Started())
}

func TestPoolThatAlsoServesAGPUTypeStillServesItsOwn(t *testing.T) {
	sim := schedsim.New(t, alsoServesPools)

	ids := sim.Submit(schedsim.Endpoint("RTX4090"), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
}

func TestAlsoServesGoesOneWay(t *testing.T) {
	sim := schedsim.New(t, alsoServesPools)
	sim.Warm("a10g")

	// RTX4090 requests do not run on A10G pools just because the RTX4090
	// pool takes A10G overflow: the request waits for a worker of its own.
	ids := sim.Submit(schedsim.Endpoint("RTX4090"), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
	sim.ExpectPlaced("a10g", 0)
}

func TestGPUTypeWithNoPoolOfItsOwnRunsOnPoolsThatAlsoServeIt(t *testing.T) {
	sim := schedsim.New(t, `
worker:
  pools:
    l40s:
      gpuType: L40S
      alsoServes: [A6000]
`)

	ids := sim.Submit(schedsim.Endpoint("A6000"), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"l40s"}, sim.PoolsOf(ids))
}

func TestFallbackPoolsWithEqualPriorityAreTriedByName(t *testing.T) {
	const pools = `
worker:
  pools:
    a10g:
      gpuType: A10G
    spot-c:
      gpuType: L40S
      alsoServes: [A10G]
    spot-a:
      gpuType: L40S
      alsoServes: [A10G]
    spot-b:
      gpuType: RTX4090
      alsoServes: [A10G]
`
	// The order is the same on every run and every replica. It used to
	// follow map iteration.
	for run := 0; run < 5; run++ {
		sim := schedsim.New(t, pools)
		sim.Warm("spot-c", "spot-b", "spot-a")

		var ids []string
		for i := 0; i < 3; i++ {
			ids = append(ids, sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 1)...)
			sim.Advance(2 * time.Second)
		}
		require.Equal(t, []string{"spot-a", "spot-b", "spot-c"}, sim.PoolsOf(ids))
	}
}

func TestRequestListingSeveralGPUTypesUsesTheFallbacksOfAll(t *testing.T) {
	sim := schedsim.New(t, `
worker:
  pools:
    a10g:
      gpuType: A10G
    t4:
      gpuType: T4
    l40s:
      gpuType: L40S
      priority: -1
      alsoServes: [A10G]
    rtx4090:
      gpuType: RTX4090
      priority: -2
      alsoServes: [T4]
`)
	sim.Warm("rtx4090", "l40s")
	sim.Pool("a10g").RefuseWorkers()
	sim.Pool("t4").RefuseWorkers()

	var ids []string
	for i := 0; i < 2; i++ {
		ids = append(ids, sim.Submit(schedsim.Endpoint("A10G", "T4").For(10*time.Minute), 1)...)
		sim.Advance(2 * time.Second)
	}

	require.Equal(t, []string{"l40s", "rtx4090"}, sim.PoolsOf(ids))
}

func TestOldChainsAndAlsoServesCombine(t *testing.T) {
	sim := schedsim.New(t, `
worker:
  pools:
    a10g:
      gpuType: A10G
    rtx4090:
      gpuType: RTX4090
      priority: -1
    l40s:
      gpuType: L40S
      priority: -2
      alsoServes: [A10G]
scheduling:
  failover:
    enabled: true
    chains:
      A10G:
        pools: [rtx4090]
`)
	sim.Warm("l40s", "rtx4090")
	sim.Pool("a10g").RefuseWorkers()

	var ids []string
	for i := 0; i < 2; i++ {
		ids = append(ids, sim.Submit(schedsim.Endpoint("A10G").For(10*time.Minute), 1)...)
		sim.Advance(2 * time.Second)
	}

	require.Equal(t, []string{"rtx4090", "l40s"}, sim.PoolsOf(ids))
}
