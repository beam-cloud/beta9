package provisioner

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

// offered is a candidate with this many GPUs at cents an hour, eight cores and
// 32 GB to each GPU. It comes up in three minutes and is never lost.
func offered(id string, gpus uint32, cents int64, change ...func(*candidate)) *candidate {
	c := &candidate{comeUp: 0.05, Offer: providers.Offer{
		Provider: "verda", ID: id, GPU: "L40S", GPUCount: gpus, Spot: true,
		CPUMillicores: 8000 * int64(gpus), MemoryMB: 32_000 * int64(gpus), HourlyCostMicros: cents * microsPerCent,
	}}
	for _, apply := range change {
		apply(c)
	}
	return c
}

func notSpot(c *candidate)                     { c.Spot = false }
func lostEvery(hours float64) func(*candidate) { return func(c *candidate) { c.taken = 1 / hours } }
func upAfter(hours float64) func(*candidate)   { return func(c *candidate) { c.comeUp = hours } }
func inStock(n uint32) func(*candidate)        { return func(c *candidate) { c.Available = n } }

// waiting is n needs of one GPU each, changed as given.
func waiting(n int, change ...func(*scheduler.MachineNeed)) []scheduler.MachineNeed {
	needs := make([]scheduler.MachineNeed, n)
	for i := range needs {
		needs[i] = scheduler.MachineNeed{CPU: 4000, Memory: 16_000, GPUCount: 1, HoursLost: 0.1}
		for _, apply := range change {
			apply(&needs[i])
		}
	}
	return needs
}

func uninterrupted(need *scheduler.MachineNeed) { need.OnDemand = true }
func losing(hours float64) func(*scheduler.MachineNeed) {
	return func(n *scheduler.MachineNeed) { n.HoursLost = hours }
}
func ofGPUs(gpus uint32) func(*scheduler.MachineNeed) {
	return func(n *scheduler.MachineNeed) { n.GPUCount = gpus }
}
func together(needs ...[]scheduler.MachineNeed) []scheduler.MachineNeed {
	var all []scheduler.MachineNeed
	for _, some := range needs {
		all = append(all, some...)
	}
	return all
}

var noLimit = headroom{machines: 64, gpus: math.MaxInt, hourlyMicros: math.MaxInt64}

func TestSolve(t *testing.T) {
	small, big := offered("1x", 1, 100), offered("8x", 8, 640)
	for _, test := range []struct {
		name   string
		needs  []scheduler.MachineNeed
		offers []*candidate
		left   headroom
		want   string
	}{
		{name: "one machine for one request", needs: waiting(1), offers: []*candidate{small, big}, want: "1x"},
		{name: "one machine for many, when it costs less than one each", needs: waiting(8), offers: []*candidate{small, big}, want: "8x"},
		{name: "one each, when that costs less", needs: waiting(8), offers: []*candidate{small, offered("8x", 8, 900)}, want: "1x 1x 1x 1x 1x 1x 1x 1x"},
		{name: "a big one and a small one", needs: waiting(9), offers: []*candidate{small, big}, want: "8x 1x"},
		{name: "room that is paid for is used: seven share the big one", needs: waiting(7), offers: []*candidate{small, big}, want: "8x"},
		{name: "a request for several GPUs has them on one machine", needs: together(waiting(1, ofGPUs(4)), waiting(2)), offers: []*candidate{small, offered("4x", 4, 380), big}, want: "4x 1x 1x"},

		// The limits decide which machines, not only how many.
		{name: "maxNodes: fewer, bigger machines", needs: waiting(16), offers: []*candidate{small, offered("8x", 8, 900)}, left: headroom{machines: 2, gpus: math.MaxInt, hourlyMicros: math.MaxInt64}, want: "8x 8x"},
		{name: "maxGPUs: the cheapest way to fill it", needs: waiting(24), offers: []*candidate{small, offered("4x", 4, 300), offered("8x", 8, 900)}, left: headroom{machines: 64, gpus: 8, hourlyMicros: math.MaxInt64}, want: "4x 4x"},
		{name: "the hourly cap: as many requests as it pays for", needs: waiting(16), offers: []*candidate{small, offered("8x", 8, 900)}, left: headroom{machines: 64, gpus: math.MaxInt, hourlyMicros: 500 * microsPerCent}, want: "1x 1x 1x 1x 1x"},
		{name: "stock: the rest from the next best", needs: waiting(10), offers: []*candidate{small, offered("8x", 8, 640, inStock(1))}, want: "8x 1x 1x"},
		{name: "no machines left to buy", needs: waiting(1), offers: []*candidate{small}, left: headroom{gpus: math.MaxInt, hourlyMicros: math.MaxInt64}, want: ""},

		// Spot and on demand.
		{
			name:   "work that may not be interrupted has its machine first, and the rest takes what room that leaves",
			needs:  together(waiting(3), waiting(1, uninterrupted)),
			offers: []*candidate{offered("spot", 1, 100), offered("on-demand", 2, 300, notSpot)},
			want:   "on-demand spot spot",
		},
		{
			name:   "spot for work that loses little with it",
			needs:  waiting(4),
			offers: []*candidate{offered("spot", 4, 300, lostEvery(5)), offered("on-demand", 4, 400, notSpot)},
			want:   "spot",
		},
		{
			name:   "on demand for work that loses a day",
			needs:  waiting(4, losing(8)),
			offers: []*candidate{offered("spot", 4, 300, lostEvery(5)), offered("on-demand", 4, 400, notSpot)},
			want:   "on-demand",
		},
		{
			name:   "an hour of work that may not be interrupted is worth a machine it may run on: so is the wait for one",
			needs:  waiting(1, uninterrupted),
			offers: []*candidate{offered("spot", 1, 20), offered("slow", 1, 100, notSpot, upAfter(0.06)), offered("fast", 1, 105, notSpot)},
			want:   "fast",
		},
		{
			name:   "a machine of its own for the one request that loses a day",
			needs:  together(waiting(7), waiting(1, losing(8))),
			offers: []*candidate{offered("spot", 8, 400, lostEvery(5)), offered("on-demand", 1, 110, notSpot)},
			want:   "on-demand spot",
		},

		// What is expected of a machine counts for every request that waits
		// for it.
		{
			name:   "a machine that is slow to come up is worth less the more wait for it",
			needs:  waiting(8),
			offers: []*candidate{offered("slow", 8, 600, upAfter(0.06)), offered("fast", 8, 700)},
			want:   "fast",
		},
		{
			name:   "and is bought when only one does",
			needs:  waiting(1),
			offers: []*candidate{offered("slow", 8, 600, upAfter(0.06)), offered("fast", 8, 700)},
			want:   "slow",
		},

		// What cannot be bought for does not stand in the way of the rest.
		{name: "nothing holds it", needs: waiting(1, ofGPUs(16)), offers: []*candidate{small, big}, want: ""},
		{name: "nothing holds one of them", needs: together(waiting(1, ofGPUs(16)), waiting(1)), offers: []*candidate{small, big}, want: "1x"},
		{name: "only spot, for work that may not be interrupted", needs: waiting(1, uninterrupted), offers: []*candidate{small}, want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.left == (headroom{}) {
				test.left = noLimit
			}
			require.Equal(t, test.want, idsOf(solve(test.needs, test.offers, test.left)))
		})
	}
}

func idsOf(machines []*candidate) string {
	ids := ""
	for _, machine := range machines {
		ids += " " + machine.ID
	}
	return ids[min(1, len(ids)):]
}

// Whatever is short and whatever is on offer, solve finds the best there is:
// what looking at every set of machines the limits allow finds.
func TestSolveFindsWhatLookingAtEverySetOfMachinesFinds(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	pick := func(of ...float64) float64 { return of[random.Intn(len(of))] }
	for i := range 3000 {
		offers := make([]*candidate, 1+random.Intn(4))
		for j := range offers {
			gpus := uint32(pick(1, 2, 4, 8))
			offers[j] = offered(fmt.Sprintf("offer-%d", j), gpus, int64(gpus)*int64(60+random.Intn(80)),
				upAfter(pick(0.02, 0.05, 0.3)), inStock(uint32(pick(0, 0, 1, 2))))
			if offers[j].Spot = random.Intn(3) > 0; offers[j].Spot {
				offers[j].taken = pick(0.01, 0.05, 0.3)
			}
		}
		needs := make([]scheduler.MachineNeed, 1+random.Intn(7))
		for j := range needs {
			gpus := uint32(pick(1, 1, 1, 2, 4))
			needs[j] = scheduler.MachineNeed{CPU: 4000 * int64(gpus), Memory: int64(pick(8000, 16_000, 28_000)) * int64(gpus), GPUCount: gpus,
				OnDemand: random.Intn(5) == 0, HoursLost: pick(0.1, 0.5, 8)}
		}
		left := headroom{machines: 1 + random.Intn(4), gpus: math.MaxInt, hourlyMicros: math.MaxInt64}
		if random.Intn(3) == 0 {
			left.gpus = 2 + random.Intn(12)
		}
		if random.Intn(3) == 0 {
			left.hourlyMicros = int64(100+random.Intn(1200)) * microsPerCent
		}

		s := newSolver(needs, offers)
		s.run(left)
		require.Less(t, s.steps, maxSteps, "case %d", i)

		// Every set of machines: none, and each with one more of an offer
		// that is not before its last.
		best := place(s.needs)
		var every func(machines []*candidate, from int, left headroom)
		every = func(machines []*candidate, from int, left headroom) {
			ordered := slices.Clone(machines)
			slices.SortStableFunc(ordered, inOrder)
			if here := place(s.needs, ordered...); here.better(best) {
				best = here
			}
			for j := from; j < len(offers); j++ {
				offer, had := offers[j], 0
				for _, machine := range machines {
					if machine.ID == offer.ID {
						had++
					}
				}
				if left.most(offer) > 0 && (offer.Available == 0 || had < int(offer.Available)) {
					every(append(machines[:len(machines):len(machines)], offer), j, left.after(offer))
				}
			}
		}
		every(nil, 0, left)
		require.Equal(t, best.without, s.best.without, "case %d: %s, and not %s", i, idsOf(s.best.machines), idsOf(best.machines))
		require.InDelta(t, best.cost, s.best.cost, 1e-6*best.cost, "case %d: %s, and not %s", i, idsOf(s.best.machines), idsOf(best.machines))
	}
}

// A pool that is short of more than can be looked through is still answered
// at once, with machines for everything, at close to the least they could
// cost. And one that is short of more than there are steps to try even once
// still buys.
func TestSolveAnswersABurstInItsSteps(t *testing.T) {
	random := rand.New(rand.NewSource(2))
	var offers []*candidate
	for _, gpus := range []uint32{1, 2, 4, 8} {
		for region := range 10 {
			offers = append(offers, offered(fmt.Sprintf("%dx-%d", gpus, region), gpus, int64(gpus)*int64(80+random.Intn(40)),
				upAfter(0.03+0.01*float64(random.Intn(5))), func(c *candidate) { c.Spot = region%2 == 0; c.taken = 0.02 * float64(region%2) }))
		}
	}
	needs := together(waiting(90), waiting(30, ofGPUs(2)), waiting(15, ofGPUs(4), losing(8)), waiting(15, uninterrupted))

	s := newSolver(needs, offers)
	started := time.Now()
	s.run(headroom{machines: 500, gpus: math.MaxInt, hourlyMicros: math.MaxInt64})
	require.Less(t, time.Since(started), time.Second)
	require.GreaterOrEqual(t, s.steps, maxSteps)
	require.Zero(t, s.best.without)
	require.Less(t, s.best.cost, 1.1*(s.lacks(size{})+s.least), "no plan costs less than the room at the lowest price going")

	needs = nil
	for i := range 2 * maxSteps / len(offers) {
		needs = append(needs, waiting(1, func(need *scheduler.MachineNeed) { need.CPU += int64(i) })...)
	}
	require.NotEmpty(t, solve(needs, offers, noLimit))
}
