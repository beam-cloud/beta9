package schedsim_test

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	schedsim "github.com/beam-cloud/beta9/pkg/scheduler/sim"
	"github.com/stretchr/testify/require"
)

// A pool with a provisioning block buys its machines from one provider. These
// scenarios hold what such a pool must and must not do.

// spotPool is one pool that buys L40S machines from verda, up to four of
// them. Every machine has one GPU and costs a dollar an hour.
const spotPool = `
worker:
  pools:
    spot-l40s:
      gpuType: L40S
      provisioning:
        provider: verda
        spot: true
        maxNodes: 4
        idleTimeout: 10m
        bootTimeout: 15m
`

// spotPoolWithFloor is the same pool keeping two GPUs free.
const spotPoolWithFloor = spotPool + `
      poolSizing:
        minFreeGPU: "2"
`

// machines counts what exists at the provider, which is what is paid for.
func machines(sim *schedsim.Sim, provider string) int {
	return len(sim.Provider(provider).Machines())
}

func TestSpotPoolBuysAMachineForARequestAndRunsItThere(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	sim.Provider("verda").BootAfter(3 * time.Minute)

	ids := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(time.Minute)

	// The machine is bought at once and is still booting. The request waits
	// for it.
	require.Equal(t, 1, machines(sim, "verda"))
	sim.ExpectWaiting(1)

	sim.Advance(3 * time.Minute)
	require.Equal(t, []string{"spot-l40s"}, sim.PoolsOf(ids))
	require.Equal(t, 1, sim.Provider("verda").Bought())
}

// Invariant 3: a spot pool buys only from its provider and only its GPU type.
func TestSpotPoolBuysOnlyItsGPUTypeFromItsProvider(t *testing.T) {
	sim := schedsim.New(t, spotPool+`
    spot-a10g:
      gpuType: A10G
      provisioning:
        provider: nebius
        spot: true
        maxNodes: 4
`)
	// Both providers sell both GPU types as spot, and each is the cheaper
	// one for the GPU type that the other's pool buys.
	sim.Provider("verda").Stock("A10G", 10).Price("A10G", 50)
	sim.Provider("nebius").Stock("L40S", 10).Price("L40S", 10)

	sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 2)
	sim.Submit(schedsim.Endpoint("A10G").For(time.Hour), 1)
	sim.Advance(5 * time.Minute)

	sim.ExpectPlaced("spot-l40s", 2)
	sim.ExpectPlaced("spot-a10g", 1)
	for _, machine := range sim.Provider("verda").Machines() {
		require.Equal(t, "L40S", machine.GPU)
	}
	for _, machine := range sim.Provider("nebius").Machines() {
		require.Equal(t, "A10G", machine.GPU)
	}
	require.Equal(t, 2, machines(sim, "verda"))
	require.Equal(t, 1, machines(sim, "nebius"))
	// Each paid its own provider's price, not the lower one next door.
	require.Equal(t, int64(2*100+100), sim.HourlyCents())
}

// A pool that asks for spot machines never buys one that is not.
func TestSpotPoolDoesNotBuyOnDemandMachines(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	// verda has no spot L40S. It has the same machine on demand, for less
	// than its spot machines usually cost.
	sim.Provider("verda").Stock("L40S", 0).Sell(providers.Offer{
		ID: "l40s-on-demand", Cloud: "verda", Region: "fin-01", GPU: "L40S", GPUCount: 1,
		CPUMillicores: 8000, MemoryMB: 32 * 1024, HourlyCostMicros: 500_000,
	})

	sim.Submit(schedsim.Endpoint("L40S"), 1)
	sim.Advance(5 * time.Minute)

	require.Zero(t, sim.Provider("verda").Bought())
	sim.ExpectWaiting(1)
}

// Invariant 3: it never holds more than maxNodes machines.
func TestSpotPoolNeverHoldsMoreThanMaxNodes(t *testing.T) {
	sim := schedsim.New(t, spotPoolWithFloor)
	sim.Provider("verda").BootAfter(2 * time.Minute)

	sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 9)
	for range 60 {
		sim.Advance(10 * time.Second)
		require.LessOrEqual(t, machines(sim, "verda"), 4)
	}

	// Four machines run four containers. The rest wait: the pool has no room
	// and may not buy any.
	sim.ExpectPlaced("spot-l40s", 4)
	sim.ExpectWaiting(5)
	require.Equal(t, 4, sim.Provider("verda").Bought())
}

// Invariant 3: it never exceeds the spend cap, which covers every pool.
func TestPoolsThatBuyStayUnderTheSpendCap(t *testing.T) {
	sim := schedsim.New(t, spotPool+`
    spot-a10g:
      gpuType: A10G
      provisioning:
        provider: nebius
        spot: true
        maxNodes: 4
providers:
  maxHourlyCents: 250
`)
	sim.Provider("verda").Price("L40S", 100)
	sim.Provider("nebius").Price("A10G", 100)

	sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 3)
	sim.Submit(schedsim.Endpoint("A10G").For(time.Hour), 3)
	for range 30 {
		sim.Advance(10 * time.Second)
		require.LessOrEqual(t, sim.HourlyCents(), int64(250))
	}

	// $2.50 an hour buys two machines at a dollar each, whichever pool asks.
	require.Equal(t, 2, machines(sim, "verda")+machines(sim, "nebius"))
	sim.ExpectWaiting(4)

	// A machine that leaves makes room under the cap for another.
	sim.Provider("verda").Reclaim(1)
	sim.Advance(5 * time.Minute)
	require.Equal(t, 2, machines(sim, "verda")+machines(sim, "nebius"))
	require.Equal(t, 3, sim.Provider("verda").Bought()+sim.Provider("nebius").Bought())
}

// A machine dearer than maxNodeHourlyCents is not bought.
func TestSpotPoolDoesNotPayMoreThanItsPriceLimit(t *testing.T) {
	sim := schedsim.New(t, `
worker:
  pools:
    spot-l40s:
      gpuType: L40S
      provisioning:
        provider: verda
        spot: true
        maxNodes: 4
        maxNodeHourlyCents: 80
`)
	sim.Provider("verda").Price("L40S", 100)
	sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(3 * time.Minute)
	require.Zero(t, sim.Provider("verda").Bought())

	// The price falls under the limit, and the pool buys.
	sim.Provider("verda").Price("L40S", 70)
	sim.Advance(3 * time.Minute)
	require.Equal(t, 1, sim.Provider("verda").Bought())
	sim.ExpectPlaced("spot-l40s", 1)
}

// Invariant 3: booting machines count as capacity.
func TestBootingMachinesCountTowardsTheFloor(t *testing.T) {
	sim := schedsim.New(t, spotPoolWithFloor)
	// The machines take longer to boot than the ten minutes a worker that
	// is starting is otherwise given.
	sim.Provider("verda").BootAfter(12 * time.Minute)

	// Two machines cover the floor of two GPUs. While they boot the pool is
	// under its floor every second, and buys nothing more.
	sim.Advance(11 * time.Minute)
	require.Equal(t, 2, sim.Provider("verda").Bought())
	require.Equal(t, 0, freeGPUs(sim, "spot-l40s"))
	sim.ExpectWorkers("spot-l40s", 2)

	sim.Advance(3 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, sim.Provider("verda").Bought())
}

func TestRequestsWaitForABootingMachineThatHasRoomForThem(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	sim.Provider("verda").BootAfter(6*time.Minute).Machine(4, 16_000, 64*1024)

	// One machine has four GPUs. Three requests ask for one each, a minute
	// apart, while it boots.
	ids := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(time.Minute)
	ids = append(ids, sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)...)
	sim.Advance(time.Minute)
	ids = append(ids, sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)...)
	sim.Advance(6 * time.Minute)

	require.Equal(t, []string{"spot-l40s", "spot-l40s", "spot-l40s"}, sim.PoolsOf(ids))
	require.Equal(t, 1, sim.Provider("verda").Bought())
}

// Invariant 4: a spot pool with a floor converges to it when stock allows,
// and never releases a machine that holds the floor.
func TestSpotPoolReachesItsFloorFromZeroAndHoldsIt(t *testing.T) {
	sim := schedsim.New(t, spotPoolWithFloor)

	sim.Advance(5 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	held := sim.Provider("verda").Machines()
	require.Len(t, held, 2)

	// The two machines have nothing to run for an hour, far past the idle
	// timeout. They are the floor: neither is released, so neither has to be
	// bought again.
	sim.Advance(time.Hour)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, held, sim.Provider("verda").Machines())
	require.Equal(t, 2, sim.Provider("verda").Bought())
}

func TestSpotPoolKeepsItsFloorFreeUnderLoadAndReturnsToItAfter(t *testing.T) {
	sim := schedsim.New(t, spotPoolWithFloor)
	sim.Advance(5 * time.Minute)

	// Two containers take the two free GPUs. The pool buys two more machines
	// to have two GPUs free again, which is all maxNodes allows.
	ids := sim.Submit(schedsim.Endpoint("L40S").For(20*time.Minute), 2)
	sim.Advance(5 * time.Minute)
	require.Equal(t, []string{"spot-l40s", "spot-l40s"}, sim.PoolsOf(ids))
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 4, machines(sim, "verda"))

	// The load ends. The machines above the floor are released once they
	// have been idle for ten minutes; the floor stays.
	sim.Advance(40 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, machines(sim, "verda"))
	require.Equal(t, 4, sim.Provider("verda").Bought())
}

func TestSpotPoolWithoutAFloorReleasesEveryIdleMachine(t *testing.T) {
	sim := schedsim.New(t, spotPool)

	sim.Submit(schedsim.Endpoint("L40S").For(5*time.Minute), 2)
	sim.Advance(4 * time.Minute)
	require.Equal(t, 2, machines(sim, "verda"))

	// Idle for less than the timeout: kept, in case more work comes.
	sim.Advance(9 * time.Minute)
	require.Equal(t, 2, machines(sim, "verda"))

	sim.Advance(5 * time.Minute)
	require.Zero(t, machines(sim, "verda"))
	sim.ExpectWorkers("spot-l40s", 0)
}

// The floor is best effort: without stock the pool holds what it has and
// keeps trying.
func TestSpotPoolWithoutStockReachesItsFloorOnceThereIsSome(t *testing.T) {
	sim := schedsim.New(t, spotPoolWithFloor)
	sim.Provider("verda").Stock("L40S", 1)

	sim.Advance(10 * time.Minute)
	require.Equal(t, 1, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 1, machines(sim, "verda"))

	sim.Provider("verda").Stock("L40S", 5)
	sim.Advance(10 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, machines(sim, "verda"))
}

// Invariant 2: a pool that cannot grow is skipped without spending an
// attempt, so demand spreads over pools as each runs out.
func TestDemandSpreadsOverSpotPoolsAsEachRunsOutOfStock(t *testing.T) {
	sim := schedsim.New(t, `
worker:
  pools:
    spot-verda-l40s:
      gpuType: L40S
      priority: 20
      provisioning: {provider: verda, spot: true, maxNodes: 8}
    spot-nebius-l40s:
      gpuType: L40S
      priority: 10
      provisioning: {provider: nebius, spot: true, maxNodes: 8}
`)
	sim.Provider("verda").Stock("L40S", 2)
	sim.Provider("nebius").Stock("L40S", 8)

	ids := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 6)
	sim.Advance(6 * time.Minute)

	sim.ExpectPlaced("spot-verda-l40s", 2)
	sim.ExpectPlaced("spot-nebius-l40s", 4)
	sim.ExpectWaiting(0)
	sim.ExpectFailed(0)

	// The four that run at nebius went straight there: none asked verda for
	// a machine it had already sold.
	for _, id := range ids {
		run := sim.Container(id).Runs[0]
		if run.Pool == "spot-nebius-l40s" {
			require.Zero(t, run.Asked["spot-verda-l40s"], "%s asked a pool that could not grow", id)
			require.Equal(t, 1, run.Asked["spot-nebius-l40s"])
		}
	}
}

// A pool whose provider refuses the purchase is left alone for a while, and
// the request moves on.
func TestRequestMovesOnWhenTheProviderRefusesToSell(t *testing.T) {
	sim := schedsim.New(t, `
worker:
  pools:
    spot-verda-l40s:
      gpuType: L40S
      priority: 20
      provisioning: {provider: verda, spot: true, maxNodes: 8}
    spot-nebius-l40s:
      gpuType: L40S
      priority: 10
      provisioning: {provider: nebius, spot: true, maxNodes: 8}
`)
	// verda lists the machine and then has none: the catalogue was stale.
	sim.Provider("verda").RefuseCreates(providers.ErrOutOfStock)

	ids := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 2)
	sim.Advance(2 * time.Minute)

	require.Equal(t, []string{"spot-nebius-l40s", "spot-nebius-l40s"}, sim.PoolsOf(ids))
	require.Zero(t, machines(sim, "verda"))

	// Two minutes on, verda lists the machine as before. It refused so
	// recently that it is not asked again: one refusal is all there was.
	ids = append(ids, sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)...)
	sim.Advance(2 * time.Minute)
	require.Equal(t, "spot-nebius-l40s", sim.Container(ids[2]).Pool())
	asked := 0
	for _, id := range ids {
		asked += sim.Container(id).Runs[0].Asked["spot-verda-l40s"]
	}
	require.Equal(t, 1, asked)
}

// A10G requests overflow into a spot pool of another GPU type that serves
// them, once the A10G pool cannot take them.
func TestRequestsOverflowIntoASpotPoolThatAlsoServesThem(t *testing.T) {
	sim := schedsim.New(t, `
worker:
  pools:
    a10g:
      gpuType: A10G
    spot-l40s:
      gpuType: L40S
      alsoServes: [A10G]
      provisioning: {provider: verda, spot: true, maxNodes: 4}
`)
	sim.Pool("a10g").RefuseWorkers()

	ids := sim.Submit(schedsim.Endpoint("A10G").For(time.Hour), 2)
	sim.Advance(6 * time.Minute)
	require.Equal(t, []string{"spot-l40s", "spot-l40s"}, sim.PoolsOf(ids))
}

// A spot pool takes only work that opted in to interruption.
func TestSpotPoolTakesOnlyWorkThatMayBeInterrupted(t *testing.T) {
	sim := schedsim.New(t, spotPool)

	sim.Submit(schedsim.Function("L40S"), 1)
	sim.Advance(5 * time.Minute)

	require.Zero(t, sim.Provider("verda").Bought())
	sim.ExpectPlaced("spot-l40s", 0)
}

// Invariant 5: every record is polled, so a machine the provider took back
// is found, forgotten and replaced.
func TestReclaimedMachineIsFoundAndReplaced(t *testing.T) {
	sim := schedsim.New(t, spotPoolWithFloor)
	sim.Advance(5 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))

	sim.Provider("verda").Reclaim(1)

	// Well inside the minute that a dead worker's state takes to expire, the
	// machine's worker is gone and its replacement is on the way.
	sim.Advance(20 * time.Second)
	require.Equal(t, 1, freeGPUs(sim, "spot-l40s"))
	sim.ExpectWorkers("spot-l40s", 2)
	require.Equal(t, 2, machines(sim, "verda"))

	sim.Advance(2 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 3, sim.Provider("verda").Bought())
	require.Len(t, sim.MachineRecords(), 2)
}

// Invariant 7, for a machine that was bought: a request that was on its way
// to a machine the provider took back returns to the backlog and runs on the
// machine that replaces it.
func TestRequestQueuedOnAReclaimedMachineReturnsToTheBacklog(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	// One machine has room for two containers.
	sim.Provider("verda").Machine(2, 16_000, 64*1024)
	first := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []string{"spot-l40s"}, sim.PoolsOf(first))

	// The machine takes the next request off its queue and is gone before it
	// starts it.
	workers := sim.Pool("spot-l40s").Workers()
	require.Len(t, workers, 1)
	workers[0].VanishAfterTakingWork()
	second := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(5 * time.Second)
	require.False(t, workers[0].Running())
	require.Empty(t, sim.Container(second[0]).Runs)
	sim.Provider("verda").Reclaim(1)

	sim.Advance(6 * time.Minute)
	sim.ExpectRequeued(1)
	runs := sim.Container(second[0]).Runs
	require.Len(t, runs, 1)
	require.Equal(t, "spot-l40s", runs[0].Pool)
	require.NotEqual(t, workers[0].ID, runs[0].WorkerID)
	require.Equal(t, 2, sim.Provider("verda").Bought())
}

// Reclaim with notice: a machine that is told it is about to be taken back
// no longer counts as room in its pool, which buys its replacement while the
// machine is still there. When the provider takes it, nothing is missing.
func TestMachineGivenNoticeIsReplacedBeforeItIsTaken(t *testing.T) {
	sim := schedsim.New(t, spotPoolWithFloor)
	verda := sim.Provider("verda")
	sim.Advance(5 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, verda.Bought())

	verda.GiveNotice(1)
	sim.Advance(20 * time.Second)
	require.Equal(t, 1, freeGPUs(sim, "spot-l40s"), "the machine that is going is not room")
	require.Equal(t, 3, verda.Bought(), "its replacement is on the way")

	sim.Advance(time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 3, machines(sim, "verda"), "the machine is still there")

	verda.Reclaim(1)
	sim.Advance(time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, machines(sim, "verda"))
	require.Len(t, sim.MachineRecords(), 2)
	require.Equal(t, 3, verda.Bought(), "nothing more had to be bought")
}

// A machine that is about to be taken back is given no new work, and goes on
// running what it has.
func TestMachineGivenNoticeTakesNoNewWork(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	// One machine has room for two containers.
	verda := sim.Provider("verda").Machine(2, 16_000, 64*1024)
	first := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []string{"spot-l40s"}, sim.PoolsOf(first))
	going := sim.Pool("spot-l40s").Workers()[0]

	verda.GiveNotice(1)
	second := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(3 * time.Minute)

	// The second container did not take the room the machine has left. It
	// runs on a machine that was bought for it.
	require.Equal(t, 2, verda.Bought())
	runs := sim.Container(second[0]).Runs
	require.Len(t, runs, 1)
	require.NotEqual(t, going.ID, runs[0].WorkerID)
	require.True(t, going.Running(), "the machine runs on until it is taken")
	require.Len(t, sim.Container(first[0]).Runs, 1)
	require.Equal(t, going.ID, sim.Container(first[0]).Runs[0].WorkerID)
	sim.ExpectFailed(0)

	// The next container takes the room that is left on the machine that
	// stays, as it would have on the first.
	third := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(time.Minute)
	require.Len(t, sim.Container(third[0]).Runs, 1)
	require.Equal(t, runs[0].WorkerID, sim.Container(third[0]).Runs[0].WorkerID)
	require.Equal(t, 2, verda.Bought())
}

// The notice is kept on the machine, not only on its worker: a worker that
// is started again on a machine that is going is given no work either.
func TestMachineGivenNoticeStaysOutOfUseWhenItsWorkerStartsAgain(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	verda := sim.Provider("verda").Machine(2, 16_000, 64*1024)
	sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(2 * time.Minute)
	going := sim.Pool("spot-l40s").Workers()[0]

	verda.GiveNotice(1)
	going.LoseRecord()
	sim.Advance(2 * time.Minute)
	require.True(t, going.Running(), "the worker is up again")

	second := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(3 * time.Minute)
	runs := sim.Container(second[0]).Runs
	require.Len(t, runs, 1)
	require.NotEqual(t, going.ID, runs[0].WorkerID)
	require.Equal(t, 2, verda.Bought())
}

// Invariant 5: tagged instances with no record are terminated.
func TestInstanceNobodyRecordedIsTerminated(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	orphan := sim.Provider("verda").Orphan()
	// Another control plane on the same account, and a machine this one
	// bought for a tenant, are not the pool's to delete.
	foreign := sim.Provider("verda").ForeignMachine()
	tenant := sim.Provider("verda").TenantMachine()

	sim.Advance(2 * time.Minute)

	left := sim.Provider("verda").InstanceIDs()
	require.NotContains(t, left, orphan)
	require.Contains(t, left, foreign)
	require.Contains(t, left, tenant)
}

// Invariant 6: a machine that has not joined by its boot timeout is
// terminated.
func TestMachineThatNeverJoinsIsTerminatedAtItsBootTimeout(t *testing.T) {
	sim := schedsim.New(t, `
worker:
  pools:
    spot-l40s:
      gpuType: L40S
      provisioning: {provider: verda, spot: true, maxNodes: 4, bootTimeout: 5m}
`)
	sim.Provider("verda").NeverJoin()

	sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(4*time.Minute + 30*time.Second)
	first := sim.Provider("verda").InstanceIDs()
	require.Len(t, first, 1)
	// The machine counts as capacity for as long as it may still join, so
	// nothing else is bought for the request.
	sim.ExpectWorkers("spot-l40s", 1)
	require.Equal(t, 1, sim.Provider("verda").Bought())

	// Machines join again from here on, so whatever is bought next works.
	sim.Provider("verda").JoinAgain()
	sim.Advance(time.Minute)
	require.NotContains(t, sim.Provider("verda").InstanceIDs(), first[0])

	// The request did not go down with the machine: another is bought for it.
	sim.Advance(5 * time.Minute)
	sim.ExpectPlaced("spot-l40s", 1)
	require.Equal(t, 2, sim.Provider("verda").Bought())
	require.Equal(t, 1, machines(sim, "verda"))
}

// Invariant 5: the record is written before the provider is asked, so a
// machine whose creation was never confirmed is not lost. It is found by
// its tag, counted, and used.
func TestMachineWhoseCreationWasNeverConfirmedIsFoundAndUsed(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	sim.Provider("verda").BootAfter(2 * time.Minute).LoseNextAnswer()

	ids := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(time.Minute)
	// The purchase failed as far as the scheduler can tell, and the machine
	// exists. It has a record, and a worker that requests can wait for.
	require.Equal(t, 1, machines(sim, "verda"))
	records := sim.MachineRecords()
	require.Len(t, records, 1)
	require.Equal(t, sim.Provider("verda").Machines()[0].ID, records[0].InstanceID)
	sim.ExpectWorkers("spot-l40s", 1)

	sim.Advance(3 * time.Minute)
	require.Equal(t, []string{"spot-l40s"}, sim.PoolsOf(ids))
	require.Equal(t, 1, sim.Provider("verda").Bought(), "the request ran on the machine that was already bought")
}

// Invariant 5, when two things go wrong with one purchase: its answer is lost,
// and the provider then fails to start the machine. The record still leads
// to the instance, which is deleted through it. At no time is there an
// instance without a record.
func TestMachineWhoseCreationWasNeverConfirmedAndThatFailedIsDeleted(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	sim.Provider("verda").BootAfter(5 * time.Second).LoseNextAnswer().FailNextBoot()

	sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	for range 96 {
		sim.Advance(5 * time.Second)
		require.Empty(t, unrecorded(sim, "verda"))
	}

	// The failed machine is gone, and the request runs on the next one,
	// which is bought once the offer that failed is tried again.
	require.Equal(t, 2, sim.Provider("verda").Bought())
	require.Len(t, sim.Provider("verda").InstanceIDs(), 1)
	require.Len(t, sim.MachineRecords(), 1)
	sim.ExpectPlaced("spot-l40s", 1)
}

// unrecorded returns the instances at the provider that no record accounts
// for.
func unrecorded(sim *schedsim.Sim, provider string) []string {
	recorded := map[string]bool{}
	for _, record := range sim.MachineRecords() {
		recorded[record.ID] = true
	}
	var ids []string
	for _, machine := range sim.Provider(provider).Machines() {
		if !recorded[machine.MachineID] {
			ids = append(ids, machine.ID)
		}
	}
	return ids
}

// A machine the provider fails to start is deleted there, not left to cost
// money.
func TestMachineTheProviderFailedToStartIsDeleted(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	sim.Provider("verda").FailNextBoot()

	sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(8 * time.Minute)

	require.Equal(t, 2, sim.Provider("verda").Bought())
	require.Equal(t, 1, machines(sim, "verda"))
	require.Len(t, sim.Provider("verda").InstanceIDs(), 1)
	sim.ExpectPlaced("spot-l40s", 1)
}

// What the provider failed to start is not bought again at once. The next
// machine is the next cheapest the provider sells, so the request does not
// wait while the pool buys the same machine over and over.
func TestPoolBuysSomethingElseAfterAMachineFailsToStart(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	verda := sim.Provider("verda")
	verda.Sell(providers.Offer{
		ID: "l40s-spot", Cloud: "verda", Region: "sim-2", GPU: "L40S", GPUCount: 1,
		CPUMillicores: 8000, MemoryMB: 32 * 1024, HourlyCostMicros: 1_500_000, Spot: true,
	})
	verda.FailNextBoot()

	sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(2 * time.Minute)

	sim.ExpectPlaced("spot-l40s", 1)
	require.Equal(t, 2, verda.Bought())
	bought := verda.Machines()
	require.Len(t, bought, 1)
	require.EqualValues(t, 150, bought[0].HourlyCents, "the machine in the other region")
}

// A request that waits for a machine to boot stops waiting when the machine
// is taken back, and has the pool buy another at once.
func TestRequestWaitingForAMachineThatIsReclaimedWhileBootingGetsAnother(t *testing.T) {
	sim := schedsim.New(t, spotPool)
	verda := sim.Provider("verda").BootAfter(2 * time.Minute)

	ids := sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(30 * time.Second)
	require.Equal(t, 1, verda.Bought())
	verda.Reclaim(1)

	// The next check of the machines finds it gone, and the request asks
	// for another without waiting out the time a machine has to boot.
	sim.Advance(30 * time.Second)
	require.Equal(t, 2, verda.Bought())
	require.Equal(t, 1, machines(sim, "verda"))

	sim.Advance(3 * time.Minute)
	sim.ExpectPlaced("spot-l40s", 1)
	sim.ExpectFailed(0)
	require.Equal(t, 2, verda.Bought())
	require.Len(t, sim.Container(ids[0]).Runs, 1)
}

// A pool that is deleted has its machines released.
func TestMachinesOfADeletedPoolAreReleased(t *testing.T) {
	sim := schedsim.New(t, spotPoolWithFloor)
	sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(5 * time.Minute)
	require.Equal(t, 3, machines(sim, "verda"))

	sim.DeletePool("spot-l40s")

	// Nothing is given back for five minutes: a gateway that is restarting
	// with a new config looks the same as a pool that was deleted.
	sim.Advance(4 * time.Minute)
	require.Equal(t, 3, machines(sim, "verda"))

	// Then the idle machines go. The busy one is given ten minutes for its
	// container to finish, and goes too.
	sim.Advance(2 * time.Minute)
	require.Equal(t, 1, machines(sim, "verda"))
	sim.Advance(10 * time.Minute)
	require.Zero(t, machines(sim, "verda"))
	require.Equal(t, 3, sim.Provider("verda").Bought())
	require.Empty(t, sim.MachineRecords())
	sim.ExpectWorkers("spot-l40s", 0)
}

// Two scheduler replicas, each with its own provisioner, share the records.
func TestTwoSchedulersNeverBuyMoreThanMaxNodesBetweenThem(t *testing.T) {
	sim := schedsim.New(t, spotPoolWithFloor)
	sim.AddScheduler()
	sim.Provider("verda").BootAfter(2 * time.Minute)

	sim.Submit(schedsim.Endpoint("L40S").For(time.Hour), 9)
	for range 60 {
		sim.Advance(10 * time.Second)
		require.LessOrEqual(t, machines(sim, "verda"), 4)
	}
	sim.ExpectPlaced("spot-l40s", 4)
	require.Equal(t, 4, sim.Provider("verda").Bought())
}

func TestTwoSchedulersBuyTheFloorOnce(t *testing.T) {
	sim := schedsim.New(t, spotPoolWithFloor)
	sim.AddScheduler()

	sim.Advance(30 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, sim.Provider("verda").Bought())
}
