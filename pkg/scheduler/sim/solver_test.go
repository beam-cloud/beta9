package schedsim

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// What a pool that buys its machines does with what it knows. Each test says
// what main did in the same scenario before the pool planned its purchases.

const solverPool = `
worker:
  pools:
    l40s:
      gpuType: L40S
      provisioning: {providers: [verda, nebius], spot: true, onDemand: true, maxNodes: 8}
`

// waited returns the longest any of the containers waited to start. One that
// has not started has waited until now.
func waited(sim *Sim, ids []string) time.Duration {
	longest := time.Duration(0)
	for _, id := range ids {
		container := sim.Container(id)
		started := sim.Clock.Now()
		if len(container.Runs) > 0 {
			started = container.Runs[0].StartedAt
		}
		longest = max(longest, started.Sub(container.SubmittedAt))
	}
	return longest
}

// price sets what the provider's L40S costs an hour as spot and on demand,
// and waits for the pools to see it.
func price(provider *Provider, spot, onDemand int64) {
	provider.product("L40S", true).cents, provider.product("L40S", false).cents = spot, onDemand
	provider.apply()
	provider.sim.Advance(20 * time.Second)
}

// Requests that fit one machine share it, however many gateways there are.
// Main bought the same two machines: this is what the plan must not lose.
func TestSolverPacksABurstOntoTheMachinesItNeeds(t *testing.T) {
	for _, replicas := range []int{1, 2} {
		sim := New(t, solverPool)
		for range replicas - 1 {
			sim.AddScheduler()
		}
		sim.Provider("verda").BootAfter(2*time.Minute).Machine(4, 32_000, 128*1024)
		sim.Provider("nebius").Stock("L40S", 0)

		ids := sim.Submit(Endpoint("L40S").For(time.Hour), 8)
		sim.Advance(10 * time.Minute)
		require.Equal(t, 2, sim.Provider("verda").Bought(), "%d gateways", replicas)
		require.LessOrEqual(t, waited(sim, ids), 2*time.Minute+5*time.Second)
	}
}

// A request that waits for its machine goes on saying so, so that one that
// comes later is bought a machine of its own at once.
func TestSolverBuysForARequestThatComesWhileAnotherWaits(t *testing.T) {
	sim := New(t, solverPool)
	sim.Provider("verda").BootAfter(2 * time.Minute)
	sim.Provider("nebius").Stock("L40S", 0)

	ids := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(30 * time.Second)
	ids = append(ids, sim.Submit(Endpoint("L40S").For(time.Hour), 1)...)
	sim.Advance(5 * time.Minute)
	require.Equal(t, 2, sim.Provider("verda").Bought())
	require.LessOrEqual(t, waited(sim, ids), 2*time.Minute+5*time.Second)
}

// The provider lists three offers it no longer has. Main asked for each in
// turn, five seconds apart, and failed the request after the third.
func TestSolverBuysTheNextBestWhenAnOfferIsGone(t *testing.T) {
	sim := New(t, solverPool)
	verda := sim.Provider("verda").BootAfter(time.Minute)
	sim.Provider("nebius").Stock("L40S", 0)
	for i, cents := range []int64{70, 80, 90} {
		verda.Sell(providers.Offer{ID: "l40s-spot", Cloud: "verda", Region: fmt.Sprintf("sim-%d", i+2), GPU: "L40S", GPUCount: 1,
			CPUMillicores: 8000, MemoryMB: 32 * 1024, HourlyCostMicros: cents * 10_000, Spot: true})
	}
	sim.Advance(time.Second)
	verda.extra = nil
	verda.apply()

	ids := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(5 * time.Minute)
	require.Equal(t, 1, verda.Bought())
	require.LessOrEqual(t, waited(sim, ids), time.Minute+5*time.Second)
}

// What a pool buys is worked out for all that waits for it together. Bought
// for one request at a time, each had the cheapest machine that held it:
// three small ones, and then the pool was at maxNodes with requests left
// waiting.
func TestSolverBuysTheMachinesThatCostLeastForAllThatWaits(t *testing.T) {
	sim := New(t, strings.Replace(solverPool, "maxNodes: 8", "maxNodes: 3", 1))
	verda := sim.Provider("verda")
	sim.Provider("nebius").Stock("L40S", 0)
	verda.Sell(providers.Offer{ID: "l40s-4x", Cloud: "verda", Region: "sim-1", GPU: "L40S", GPUCount: 4,
		CPUMillicores: 4 * defaultMachineCPU, MemoryMB: 4 * defaultMachineMemoryMB, HourlyCostMicros: 320 * 10_000})
	sim.Advance(20 * time.Second)
	sold := func() []int { return []int{verda.Bought() - verda.sold("l40s-4x"), verda.sold("l40s-4x")} }

	sim.Submit(Endpoint("L40S").For(3*time.Hour), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []int{1, 0}, sold(), "one request has the machine that holds it, and no more")

	sim.Submit(Endpoint("L40S").For(3*time.Hour), 4)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []int{1, 1}, sold(), "four share a machine that costs less than four small ones")

	// The pool may buy one machine more.
	sim.Submit(Endpoint("L40S").For(3*time.Hour), 3)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []int{1, 2}, sold(), "three have the one machine that holds them all")
	sim.ExpectWaiting(0)
}

// One pool buys from whichever provider costs least. With a pool for each
// provider, main bought all four at the first in priority, for 150 cents an
// hour each: 707 cents in all.
func TestSolverBuysFromTheCheapestProvider(t *testing.T) {
	sim := New(t, solverPool)
	verda, nebius := sim.Provider("verda"), sim.Provider("nebius")
	price(verda, 150, 300)
	price(nebius, 60, 300)

	sim.Submit(Endpoint("L40S").For(time.Hour), 4)
	sim.Advance(100 * time.Minute)
	require.Equal(t, []int{0, 4}, []int{verda.Bought(), nebius.Bought()})
	require.Less(t, sim.SpentCents(), 300.0)

	// A provider that lets the account buy no more costs a second's wait:
	// the next plan buys from the other.
	nebius.RefuseCreates(providers.ErrQuota)
	ids := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(5 * time.Minute)
	require.Equal(t, 1, verda.Bought())
	require.LessOrEqual(t, waited(sim, ids), defaultBootDelay+5*time.Second)
}

// Spot is bought when it is worth its risk for the work that waits. With a
// spot pool ahead of an on-demand one, main ran endpoints on spot machines
// that cost more than on-demand ones.
func TestSolverBuysSpotWhenItIsWorthItsRisk(t *testing.T) {
	sim := New(t, solverPool)
	verda := sim.Provider("verda")
	sim.Provider("nebius").Stock("L40S", 0)
	sandbox := Endpoint("L40S")
	sandbox.kind, sandbox.stubType = "sandbox", types.StubTypeSandbox
	sold := func() []int { return []int{verda.sold("l40s-spot"), verda.sold("l40s-on-demand")} }

	price(verda, 140, 120)
	sim.Submit(Endpoint("L40S").For(3*time.Hour), 2)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []int{0, 2}, sold(), "spot that costs more is not bought")

	// Spot is a sixth cheaper. An endpoint loses little with its machine. A
	// sandbox loses a day's work, and work that may not be interrupted is
	// never put on spot.
	price(verda, 100, 120)
	sim.Submit(Endpoint("L40S").For(3*time.Hour), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []int{1, 2}, sold(), "an endpoint")
	sim.Submit(sandbox.For(3*time.Hour), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []int{1, 3}, sold(), "a sandbox")
	sim.Submit(Function("L40S").For(3*time.Hour), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []int{1, 4}, sold(), "work that may not be interrupted")

	// At half the price, spot is worth it for the sandbox too.
	price(verda, 60, 120)
	sim.Submit(sandbox.For(3*time.Hour), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []int{2, 4}, sold())
	sim.ExpectWaiting(0)
}

// A machine is of the kind it was bought as, also once the scheduler has
// lost the record of its worker. The worker used to come back as one that
// may be taken back, and work that may not be interrupted was bought a
// second machine.
func TestSolverKnowsWhatAMachineWasBoughtAsWhenItsWorkerIsLost(t *testing.T) {
	sim := New(t, solverPool)
	verda := sim.Provider("verda").Machine(2, 16_000, 64*1024)
	sim.Provider("nebius").Stock("L40S", 0)
	price(verda, 100, 120)

	sim.Submit(Function("L40S").For(time.Hour), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, 1, verda.sold("l40s-on-demand"))
	sim.Pool("l40s").Workers()[0].LoseRecord()
	sim.Advance(2 * time.Minute)

	ids := sim.Submit(Function("L40S").For(time.Hour), 1)
	sim.Advance(2 * time.Minute)
	require.Len(t, sim.Container(ids[0]).Runs, 1)
	require.Equal(t, 1, verda.Bought(), "the machine it has holds both")
}

// A machine that takes twice as long as those before it is given back and
// replaced. Main waited out the boot timeout, 20 minutes, by which time the
// request had failed.
func TestSolverReplacesAMachineThatDoesNotComeUp(t *testing.T) {
	sim := New(t, solverPool)
	verda := sim.Provider("verda").BootAfter(time.Minute)
	sim.Provider("nebius").Stock("L40S", 0)
	sim.Submit(Endpoint("L40S").For(time.Minute), 3)
	sim.Advance(20 * time.Minute)

	verda.NeverJoin()
	ids := sim.Submit(Endpoint("L40S").For(10*time.Minute), 1)
	sim.Advance(30 * time.Second)
	verda.JoinAgain()
	sim.Advance(10 * time.Minute)
	sim.ExpectFailed(0)
	require.LessOrEqual(t, waited(sim, ids), 4*time.Minute)
	require.Equal(t, 5, verda.Bought())
}
