package compute

import (
	"errors"
	"sort"

	"github.com/beam-cloud/beta9/pkg/providers"
)

// Purchase is a number of machines of one offer to buy for a pool.
type Purchase struct {
	Offer providers.Offer
	Count uint32
}

// Plan is what to buy to give a pool the machines it asks for.
type Plan struct {
	Purchases []Purchase
	// CostMicros is what the purchases cost together.
	CostMicros int64
}

// Machines is how many machines the plan buys.
func (p Plan) Machines() uint32 {
	var machines uint32
	for _, purchase := range p.Purchases {
		machines += purchase.Count
	}
	return machines
}

// HourlyCostMicros is what the plan's machines cost an hour.
func (p Plan) HourlyCostMicros() int64 {
	var cost int64
	for _, purchase := range p.Purchases {
		cost += purchase.Offer.HourlyCostMicros * int64(purchase.Count)
	}
	return cost
}

var (
	// ErrNoCapacity means the offers a pool can use do not add up to the
	// machines it asks for.
	ErrNoCapacity = errors.New("insufficient compatible capacity")
	// ErrOverBudget means the machines cost more than the pool may spend.
	ErrOverBudget = errors.New("max spend would be exceeded")
)

// Solve chooses the machines to buy for a pool: the cheapest offers
// the pool can use, the more reliable first among equals, and no more of an
// offer than its provider says it has. A machine is paid for by the hour, for
// the pool's whole lifetime.
func Solve(pool Pool, offers []providers.Offer) (Plan, error) {
	if err := pool.Validate(); err != nil {
		return Plan{}, err
	}

	hours := WholeHours(pool.TTL)
	needed := pool.Nodes
	plan := Plan{}
	for _, offer := range usableOffers(offers, pool) {
		if needed == 0 {
			break
		}
		// A provider that does not say how many it has is taken to have
		// as many as are needed.
		count := needed
		if offer.Available > 0 {
			count = min(count, offer.Available)
		}
		plan.Purchases = append(plan.Purchases, Purchase{Offer: offer, Count: count})
		plan.CostMicros += offer.HourlyCostMicros * int64(count) * hours
		needed -= count
	}

	if needed > 0 {
		return Plan{}, ErrNoCapacity
	}
	if pool.MaxSpendMicros > 0 && plan.CostMicros > pool.MaxSpendMicros {
		return Plan{}, ErrOverBudget
	}
	return plan, nil
}

// usableOffers returns the offers the pool can use, cheapest first and the
// more reliable first among equals.
func usableOffers(offers []providers.Offer, pool Pool) []providers.Offer {
	usable := []providers.Offer{}
	for _, offer := range offers {
		if pool.MatchesOffer(offer) {
			usable = append(usable, offer)
		}
	}
	sort.SliceStable(usable, func(i, j int) bool {
		left, right := usable[i], usable[j]
		if left.HourlyCostMicros != right.HourlyCostMicros {
			return left.HourlyCostMicros < right.HourlyCostMicros
		}
		return left.Reliability > right.Reliability
	})
	return usable
}
