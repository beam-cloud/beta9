package schedsim_test

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	schedsim "github.com/beam-cloud/beta9/pkg/scheduler/sim"
	"github.com/stretchr/testify/require"
)

// Seeded random runs. Load, stock, prices, boot times, refusals and reclaims
// change at random for half an hour, and what a pool that buys must never do
// is checked every ten seconds. A failure names its seed, which replays it.
// An hour of simulated time takes about half a minute, so the runs share the
// machine's cores.

const randomPools = `
worker:
  pools:
    spot-l40s:
      gpuType: L40S
      provisioning: {provider: verda, spot: true, maxNodes: 4, idleTimeout: 2m, bootTimeout: 3m}
      poolSizing:
        minFreeGPU: "1"
    spot-a10g:
      gpuType: A10G
      provisioning: {provider: nebius, spot: true, maxNodes: 3, idleTimeout: 2m, bootTimeout: 3m}
providers:
  maxHourlyCents: 500
`

const (
	randomStep = 10 * time.Second
	// tickSlack is how long the provisioner may take to act on something:
	// two of its passes.
	tickSlack = 30 * time.Second
	// launchGrace is how long a purchase the provider never answered is
	// waited for.
	launchGrace = 2 * time.Minute
)

// boughtFrom says what each provider's pool may hold.
var boughtFrom = map[string]struct {
	pool        string
	gpu         string
	maxNodes    int
	bootTimeout time.Duration
}{
	"verda":  {pool: "spot-l40s", gpu: "L40S", maxNodes: 4, bootTimeout: 3 * time.Minute},
	"nebius": {pool: "spot-a10g", gpu: "A10G", maxNodes: 3, bootTimeout: 3 * time.Minute},
}

func TestSpotPoolsKeepTheirInvariantsUnderRandomLoadStockAndReclaims(t *testing.T) {
	// Two runs by default, one of them with two gateways. SCHEDSIM_SEEDS=40
	// runs seeds 1 to 40.
	seeds := 2
	if given := os.Getenv("SCHEDSIM_SEEDS"); given != "" {
		var err error
		seeds, err = strconv.Atoi(given)
		require.NoError(t, err, "SCHEDSIM_SEEDS")
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			t.Parallel() // each run has its own Redis, clock and schedulers
			run := newRandomRun(t, seed)
			for range 180 {
				run.act()
				run.step()
			}

			// Everything calms down: stock, no reclaims, machines that boot.
			for name, bought := range boughtFrom {
				run.sim.Provider(name).Stock(bought.gpu, -1).Price(bought.gpu, 100).
					BootAfter(30 * time.Second).JoinAgain().RefuseCreates(nil)
			}
			// Invariant 4: within half an hour the floor is met by exactly
			// what it takes and nothing else is paid for, and it stays so.
			sim := run.sim
			settled := func() bool {
				return len(sim.Waiting()) == 0 && freeGPUs(sim, "spot-l40s") == 1 &&
					machines(sim, "verda") == 1 && machines(sim, "nebius") == 0
			}
			for i := 0; i < 180 && !settled(); i++ {
				run.step()
			}
			for range 30 {
				run.step()
			}
			sim.ExpectWaiting(0)
			require.Equal(t, 1, freeGPUs(sim, "spot-l40s"))
			require.Equal(t, 1, machines(sim, "verda"))
			require.Zero(t, machines(sim, "nebius"))
			require.Len(t, sim.MachineRecords(), 1)
			sim.ExpectWorkers("spot-l40s", 1)
			sim.ExpectWorkers("spot-a10g", 0)
		})
	}
}

type randomRun struct {
	t   *testing.T
	sim *schedsim.Sim
	rng *rand.Rand

	// orphans are the instances planted without a record, and when.
	orphans map[string]time.Time
	// unbacked is since when a record has had no instance at its provider.
	unbacked map[string]time.Time
	// trail is what was done to the world, for the report of a failure.
	trail []string
}

func newRandomRun(t *testing.T, seed int64) *randomRun {
	run := &randomRun{
		t:        t,
		sim:      schedsim.New(t, randomPools),
		rng:      rand.New(rand.NewSource(seed)),
		orphans:  map[string]time.Time{},
		unbacked: map[string]time.Time{},
	}
	// Every other run has two gateways.
	if seed%2 == 0 {
		run.sim.AddScheduler()
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed %d, the last things done:\n%s", seed, strings.Join(run.trail[max(0, len(run.trail)-25):], "\n"))
		}
	})
	return run
}

func (r *randomRun) did(format string, args ...any) {
	at := r.sim.Clock.Now().Format("15:04:05")
	r.trail = append(r.trail, at+" "+fmt.Sprintf(format, args...))
}

// act changes one thing about the world, or nothing.
func (r *randomRun) act() {
	name := []string{"verda", "nebius"}[r.rng.Intn(2)]
	provider, gpu := r.sim.Provider(name), boughtFrom[name].gpu

	switch roll := r.rng.Intn(100); {
	case roll < 30:
		// About three GPUs' worth of work on average, against the five the
		// spend cap allows: sometimes idle, sometimes more than fits.
		runFor := 30*time.Second + time.Duration(r.rng.Intn(180))*time.Second
		n := 1 + r.rng.Intn(2)
		r.sim.Submit(schedsim.Endpoint(gpu).For(runFor), n)
		r.did("%d %s requests of %s", n, gpu, runFor)
	case roll < 38:
		stock := r.rng.Intn(6)
		provider.Stock(gpu, stock)
		r.did("%s has %d in stock", name, stock)
	case roll < 43:
		provider.Stock(gpu, -1)
		r.did("%s has endless stock", name)
	case roll < 51:
		provider.Reclaim(1)
		r.did("%s reclaims a machine", name)
	case roll < 57:
		boot := []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute, 4 * time.Minute}[r.rng.Intn(4)]
		provider.BootAfter(boot)
		r.did("%s machines boot in %s", name, boot)
	case roll < 62:
		cents := int64(50 + r.rng.Intn(101))
		provider.Price(gpu, cents)
		r.did("%s charges %d cents", name, cents)
	case roll < 67:
		refusal := []error{providers.ErrOutOfStock, providers.ErrQuota, errors.New("the provider is down")}[r.rng.Intn(3)]
		provider.RefuseCreates(refusal)
		r.did("%s refuses creates: %v", name, refusal)
	case roll < 73:
		provider.RefuseCreates(nil)
		r.did("%s accepts creates", name)
	case roll < 76:
		provider.NeverJoin()
		r.did("%s machines never join", name)
	case roll < 81:
		provider.JoinAgain()
		r.did("%s machines join", name)
	case roll < 84:
		provider.LoseNextAnswer()
		r.did("%s loses its next answer", name)
	case roll < 87:
		provider.FailNextBoot()
		r.did("%s fails its next boot", name)
	case roll < 90:
		orphan := provider.Orphan()
		r.orphans[orphan] = r.sim.Clock.Now()
		r.did("%s has %s, which nobody recorded", name, orphan)
	}
}

// step moves time on and checks every invariant that holds at all times.
func (r *randomRun) step() {
	r.sim.Advance(randomStep)
	t, sim, now := r.t, r.sim, r.sim.Clock.Now()

	recorded := map[string]bool{}
	for _, record := range sim.MachineRecords() {
		recorded[record.ID] = true
	}
	atProvider := map[string]bool{} // by the name the machine was bought under

	for name, bought := range boughtFrom {
		held := 0
		for _, machine := range sim.Provider(name).Machines() {
			atProvider[machine.MachineID] = true
			if planted, ok := r.orphans[machine.ID]; ok {
				// Invariant 5: a tagged instance with no record is deleted.
				require.LessOrEqual(t, now.Sub(planted), tickSlack, "%s has no record and still exists", machine.ID)
				continue
			}
			held++

			// Invariant 3: only its GPU type, and only what it asked for.
			require.Equal(t, bought.gpu, machine.GPU, "%s bought %s", bought.pool, machine.ID)
			require.True(t, machine.Spot, "%s bought %s on demand", bought.pool, machine.ID)
			// Invariant 5: whatever exists has a record.
			require.True(t, recorded[machine.MachineID], "%s (%s) exists and has no record", machine.ID, machine.MachineID)
			// Invariant 6: it has joined, or its boot timeout is not over.
			if !machine.Joined {
				require.LessOrEqual(t, now.Sub(machine.BoughtAt), bought.bootTimeout+tickSlack,
					"%s was bought %s ago and has not joined", machine.ID, now.Sub(machine.BoughtAt))
			}
		}
		// Invariant 3: never more than maxNodes.
		require.LessOrEqual(t, held, bought.maxNodes, "%s holds too many machines", bought.pool)
	}
	// Invariant 3: never over the spend cap.
	require.LessOrEqual(t, sim.HourlyCents(), int64(500))

	// Invariant 5: every record is polled, so one whose machine is gone
	// does not stay.
	for _, record := range sim.MachineRecords() {
		if atProvider[record.ID] {
			delete(r.unbacked, record.ID)
			continue
		}
		since, ok := r.unbacked[record.ID]
		if !ok {
			r.unbacked[record.ID] = now
			continue
		}
		allowed := tickSlack
		if record.InstanceID == "" {
			allowed += launchGrace
		}
		require.LessOrEqual(t, now.Sub(since), allowed, "the record of %s has had no machine for %s", record.ID, now.Sub(since))
	}
}
