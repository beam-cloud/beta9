package provisioner

import (
	"cmp"
	"math"
	"slices"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/scheduler"
)

// What a pool buys is worked out here, for everything it is short of at
// once.
//
// The pool says what waits for a machine and fits none that it has or has
// coming (needs). Its providers say what they sell, and the machines bought
// before say what to expect of each offer (candidates). The limits say how
// much more the pool may buy (headroom). solve answers with the machines to
// buy: those that leave the fewest needs without room and, of such, cost
// least (see plan).
//
// It is the question a linear program answers for a fleet of thousands,
// asked here for a pool of a few machines. A linear program buys machines
// in fractions and spreads a request over several, which rounds out over
// thousands and is most of the answer over three. So this deals in whole
// machines and whole requests: it goes through the sets of machines that the
// limits allow, places the needs on each as the pool will, and keeps the
// best. The linear program is still in it, as the bound that says which sets
// cannot beat the best so far and need no look (solver.lacks). It stops
// after a fixed number of steps with the best it has.
//
// Only the first machine of the answer is bought. The pool then says what it
// is short of with that machine, and asks again: so a machine that was sold
// out, or a limit that was reached, changes the rest of the answer.

// maxSteps is how much looking solve does, which is a few milliseconds of
// it: a step is one kind of need tried on one machine. When a pool is short
// of a few machines, that covers every set that could be the best, and the
// answer is the best there is. When it is short of many, it is the best of
// the sets looked at, which are the likeliest (solver.run).
const maxSteps = 200_000

// candidate is an offer a pool may buy, with what the machines bought
// before say to expect of one bought as it.
type candidate struct {
	providers.Offer
	// comeUp is how many hours a machine takes to come up: its boot time,
	// over the odds that it boots at all.
	comeUp float64
	// taken is how often an hour a machine is lost to its provider.
	taken float64
}

func (c candidate) price() float64 { return float64(c.HourlyCostMicros) }

// size is an amount of CPU in millicores, memory in MiB, and GPUs.
type size [3]int64

func sizeOf(need scheduler.MachineNeed) size {
	return size{need.CPU, need.Memory, int64(need.GPUCount)}
}

// roomOf is the room that a machine bought from the offer has.
func roomOf(offer providers.Offer) size {
	return size{offer.CPUMillicores, usableMemoryMB(offer.MemoryMB), int64(offer.GPUCount)}
}

// fits is how many needs of this size the room holds.
func (s size) fits(need size) int {
	n := math.MaxInt
	for i := range s {
		if need[i] > 0 {
			n = min(n, int(s[i]/need[i]))
		}
	}
	return n
}

// with is the size with n more of another.
func (s size) with(n int, more size) size {
	for i := range s {
		s[i] += int64(n) * more[i]
	}
	return s
}

// headroom is how much more a pool may buy before a limit stops it.
type headroom struct {
	machines     int
	gpus         int
	hourlyMicros int64
}

// most is how many machines bought as the offer the limits allow.
func (h headroom) most(offer *candidate) int {
	n := h.machines
	if offer.GPUCount > 0 {
		n = min(n, h.gpus/int(offer.GPUCount))
	}
	if offer.HourlyCostMicros > 0 {
		n = min(n, int(h.hourlyMicros/offer.HourlyCostMicros))
	}
	return n
}

// after is what is left once a machine is bought as the offer.
func (h headroom) after(offer *candidate) headroom {
	return headroom{h.machines - 1, h.gpus - int(offer.GPUCount), h.hourlyMicros - offer.HourlyCostMicros}
}

// weighed is a kind of need: how many there are of it, and what an hour of
// the work of one is worth, which is the price of the cheapest machine on
// offer that holds it.
type weighed struct {
	scheduler.MachineNeed
	count int
	worth float64
}

// on is what one such need costs on a machine bought as the offer, beyond
// the machine's price: waitWorth hours of its work for each hour the machine
// takes to come up, and the hours of its work that are lost each time the
// machine is.
func (w weighed) on(offer *candidate) float64 {
	return w.worth * (waitWorth*offer.comeUp + w.HoursLost*offer.taken)
}

// plan is a set of machines to buy, with the needs placed on them.
type plan struct {
	// machines are in the order to buy them (inOrder).
	machines []*candidate
	// waiting are the needs that the machines have no room for, without is
	// how many those are, and worth what an hour of their work is worth.
	waiting []weighed
	without int
	worth   float64
	// cost is what the plan costs an hour, in micros: the price of the
	// machines, and what each need costs on the machine it is on.
	cost float64
}

// better reports whether the plan leaves fewer needs without room than the
// other or, with as many, costs less.
func (p plan) better(other plan) bool {
	return cmp.Or(cmp.Compare(p.without, other.without), cmp.Compare(p.cost, other.cost)) < 0
}

// inOrder orders the machines of a plan as they are bought and filled: the
// ones least often lost first, those that their provider cannot take back
// ahead of all, and of such the biggest. The needs come hardest first, so
// the work that loses most is on the machines that are lost least.
func inOrder(a, b *candidate) int {
	switch {
	case b.Spot && !a.Spot:
		return -1
	case a.Spot && !b.Spot:
		return 1
	}
	return cmp.Or(cmp.Compare(a.taken, b.taken), cmp.Compare(b.GPUCount, a.GPUCount), cmp.Compare(b.MemoryMB, a.MemoryMB),
		cmp.Compare(b.CPUMillicores, a.CPUMillicores), byPrice(a.Offer, b.Offer))
}

// place puts each need on the first machine that takes it, both in the
// order given. The pool counts the room of its machines the same way (see
// the scheduler's shortfall), so that what is left of a plan once its first
// machine is bought is the plan for what is then short.
func place(needs []weighed, machines ...*candidate) plan {
	p := plan{machines: machines}
	free := make([]size, len(machines))
	for i, machine := range machines {
		free[i] = roomOf(machine.Offer)
		p.cost += machine.price()
	}
	for _, need := range needs {
		for i, machine := range machines {
			if need.count == 0 {
				break
			}
			if machine.Spot && need.OnDemand {
				continue
			}
			n := min(need.count, free[i].fits(sizeOf(need.MachineNeed)))
			free[i] = free[i].with(-n, sizeOf(need.MachineNeed))
			p.cost += float64(n) * need.on(machine)
			need.count -= n
		}
		if need.count > 0 {
			p.waiting, p.without, p.worth = append(p.waiting, need), p.without+need.count, p.worth+float64(need.count)*need.worth
		}
	}
	return p
}

// solve returns the machines to buy for the needs, the first to buy first,
// and none when nothing on offer that the limits allow takes any of them.
func solve(needs []scheduler.MachineNeed, offers []*candidate, left headroom) []*candidate {
	s := newSolver(needs, offers)
	s.run(left)
	return s.best.machines
}

// solver is the search for the best plan for needs.
type solver struct {
	// needs are those that something on offer holds, the hardest first.
	needs  []weighed
	offers []*candidate
	// least is what the needs cost beyond the price of their machines when
	// each is on the offer that suits it best.
	least float64
	// demand is the room all the needs take together, and unit the least
	// that any offer charges for one core, one MiB and one GPU.
	demand size
	unit   [3]float64

	best  plan
	steps int
}

func newSolver(needs []scheduler.MachineNeed, offers []*candidate) *solver {
	s := &solver{offers: offers}
	needs = slices.Clone(needs)
	slices.SortFunc(needs, scheduler.MachineNeed.Compare)
	least := 0.0 // of a need of the last kind
	for _, need := range needs {
		if n := len(s.needs); n > 0 && s.needs[n-1].MachineNeed == need {
			s.needs[n-1].count++
		} else if takers := slices.DeleteFunc(slices.Clone(offers), func(offer *candidate) bool { return !holds(offer.Offer, need) }); len(takers) > 0 {
			cheapest := slices.MinFunc(takers, func(a, b *candidate) int { return cmp.Compare(a.price(), b.price()) })
			kind := weighed{MachineNeed: need, count: 1, worth: cheapest.price()}
			least = kind.on(slices.MinFunc(takers, func(a, b *candidate) int { return cmp.Compare(kind.on(a), kind.on(b)) }))
			s.needs = append(s.needs, kind)
		} else {
			continue // nothing on offer holds it, whatever is bought
		}
		s.least, s.demand = s.least+least, s.demand.with(1, sizeOf(need))
	}
	for i := range s.unit {
		s.unit[i] = math.Inf(1)
		for _, offer := range offers {
			if room := roomOf(offer.Offer)[i]; room > 0 {
				s.unit[i] = min(s.unit[i], offer.price()/float64(room))
			}
		}
	}
	s.best = place(s.needs)
	return s
}

// run looks first at the plan that buys the most promising offer for every
// machine (rank), then at the plans that turn from that for one machine,
// then for two, and so on: until there are none left to look at, or the
// steps are spent.
func (s *solver) run(left headroom) {
	for turns := 0; s.steps < maxSteps && !s.search(nil, s.offers, left, turns); turns++ {
	}
}

// search looks at the plan that buys these machines, and then at those that
// buy one or more of the offers besides, as far as its turns go. It reports
// whether that was all of them. A set of machines is looked at once, in
// whatever order it is put together: the plans that start with an offer have
// every later offer to add, and none of the earlier.
func (s *solver) search(bought, offers []*candidate, left headroom, turns int) (all bool) {
	here := place(s.needs, bought...)
	if s.steps += len(s.needs) * (1 + len(bought) + len(offers)); here.better(s.best) {
		s.best = here
	}
	spent, have := 0.0, size{}
	for _, machine := range bought {
		spent, have = spent+machine.price(), have.with(1, roomOf(machine.Offer))
	}
	offers, all = rank(offers, here, left), true
	for n := range offers {
		// The most promising comes last, so that a turn is taken as early in
		// a plan as it can be: that is where it changes most.
		i := (n + 1) % len(offers)
		// No plan with these machines in it costs less than they do, the
		// room that is then still lacking, and each need where it suits it
		// best. One that cannot beat a plan with room for everything is not
		// looked at.
		floor := spent + offers[i].price() + s.lacks(have.with(1, roomOf(offers[i].Offer))) + s.least
		if s.best.without == 0 && floor >= s.best.cost {
			continue
		}
		// However few the steps, the most promising machine is looked at.
		if (s.steps >= maxSteps && len(bought) > 0) || (i > 0 && turns == 0) {
			all = false
			continue
		}
		at, _ := slices.BinarySearchFunc(bought, offers[i], inOrder)
		all = s.search(slices.Insert(slices.Clone(bought), at, offers[i]), offers[i:], left.after(offers[i]), turns-min(i, 1)) && all
	}
	return all
}

// lacks is the least that the room the needs lack beyond have costs: what it
// would cost were machines sold by the core, the MiB and the GPU, each at
// the lowest price going. It is the linear program without whole machines,
// which no plan with whole ones can undercut.
func (s *solver) lacks(have size) float64 {
	cost := 0.0
	for i, demand := range s.demand {
		if lack := demand - have[i]; lack > 0 {
			cost = max(cost, float64(lack)*s.unit[i])
		}
	}
	return cost
}

// rank returns the offers of which the limits and their stock allow another
// machine after those of the plan, the most promising first: those that
// would leave the fewest needs without room were nothing else bought from
// here on, and of such the one whose next machine costs least for the worth
// of the work it takes.
func rank(offers []*candidate, bought plan, left headroom) []*candidate {
	type ranked struct {
		*candidate
		without int
		each    float64
	}
	var promising []ranked
	for _, offer := range offers {
		most := left.most(offer)
		if stock := int(offer.Available); stock > 0 {
			for _, machine := range bought.machines {
				if machine == offer {
					stock--
				}
			}
			most = min(most, stock)
		}
		if most <= 0 {
			continue
		}
		next := place(bought.waiting, offer)
		r := ranked{candidate: offer, without: bought.without, each: math.Inf(1)}
		if takes := bought.without - next.without; takes > 0 {
			r.without, r.each = max(0, bought.without-takes*min(most, bought.without)), next.cost/(bought.worth-next.worth)
		}
		promising = append(promising, r)
	}
	slices.SortStableFunc(promising, func(a, b ranked) int {
		return cmp.Or(cmp.Compare(a.without, b.without), cmp.Compare(a.each, b.each))
	})
	offers = make([]*candidate, len(promising))
	for i, r := range promising {
		offers[i] = r.candidate
	}
	return offers
}
