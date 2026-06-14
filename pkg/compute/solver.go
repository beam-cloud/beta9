package compute

import (
	"math"
	"slices"
	"sort"
	"time"
)

type Solver struct {
	MaxOffers int
}

func NewSolver() *Solver {
	return &Solver{MaxOffers: 32}
}

func (s *Solver) Solve(in SolveInput) SolvePlan {
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}

	pool := in.Demand.Pool()
	if err := pool.Validate(); err != nil {
		return SolvePlan{Feasible: false, Reason: err.Error()}
	}

	requiredCapacity := requiredDemandCapacity(in.Demand)
	existingCapacity, committedCost, keepActions, deleteActions := usableReservations(in.Reservations, in.Demand, now)
	if existingCapacity >= requiredCapacity {
		return SolvePlan{
			Feasible:            true,
			Actions:             append(keepActions, deleteActions...),
			TotalCapacity:       existingCapacity,
			ExistingCapacity:    existingCapacity,
			CommittedCostMicros: committedCost,
		}
	}

	neededCapacity := requiredCapacity - existingCapacity
	candidates := filterOffers(in.Offers, pool)
	maxOffers := s.MaxOffers
	if in.MaxOffers > 0 {
		maxOffers = in.MaxOffers
	}
	if maxOffers <= 0 {
		maxOffers = 32
	}
	if len(candidates) > maxOffers {
		candidates = candidates[:maxOffers]
	}

	leaseHours := WholeHours(in.Demand.TTL)
	best := solveBounded(candidates, neededCapacity, leaseHours, demandOfferCapacity(in.Demand))
	if !best.ok {
		return SolvePlan{
			Feasible:            false,
			Reason:              "insufficient compatible capacity",
			Actions:             append(keepActions, deleteActions...),
			ExistingCapacity:    existingCapacity,
			CommittedCostMicros: committedCost,
		}
	}

	totalCommitment := committedCost + best.cost
	if in.Demand.MaxSpendMicros > 0 && totalCommitment > in.Demand.MaxSpendMicros {
		return SolvePlan{
			Feasible:            false,
			Reason:              "max spend would be exceeded",
			Actions:             append(keepActions, deleteActions...),
			ExistingCapacity:    existingCapacity,
			CommittedCostMicros: committedCost,
		}
	}

	actions := make([]SolveAction, 0, len(keepActions)+len(best.counts)+len(deleteActions))
	actions = append(actions, keepActions...)
	var newCapacity uint32
	for idx, count := range best.counts {
		if count == 0 {
			continue
		}
		offer := candidates[idx]
		actionCost := offer.HourlyCostMicros * int64(count) * leaseHours
		newCapacity += demandOfferCapacity(in.Demand)(offer) * count
		actions = append(actions, SolveAction{
			Type:       ActionCreate,
			Offer:      offer,
			Count:      count,
			CostMicros: actionCost,
			Reason:     "satisfy reserved pool demand",
		})
	}
	actions = append(actions, deleteActions...)

	return SolvePlan{
		Feasible:              true,
		Actions:               actions,
		TotalCapacity:         existingCapacity + newCapacity,
		ExistingCapacity:      existingCapacity,
		NewCapacity:           newCapacity,
		IncrementalCostMicros: best.cost,
		CommittedCostMicros:   totalCommitment,
	}
}

func usableReservations(reservations []Reservation, demand Demand, now time.Time) (uint32, int64, []SolveAction, []SolveAction) {
	var totalCapacity uint32
	var committedCost int64
	keepActions := make([]SolveAction, 0, len(reservations))
	deleteActions := []SolveAction{}
	leaseEnd := now.Add(demand.TTL)

	for _, reservation := range reservations {
		if !reservation.ActiveAt(now) {
			continue
		}
		if !reservationMatchesDemand(reservation, demand) {
			continue
		}

		capacity := reservationCapacity(reservation, demand)
		if reservation.Source.IsAttached() {
			totalCapacity += capacity
			keepActions = append(keepActions, SolveAction{
				Type:        ActionKeep,
				Reservation: reservation,
				Count:       1,
				Reason:      "attached capacity has no incremental provider cost",
			})
			continue
		}

		totalCapacity += capacity
		cost := existingReservationCommitment(reservation, now, leaseEnd)
		committedCost += cost
		keepActions = append(keepActions, SolveAction{
			Type:        ActionKeep,
			Reservation: reservation,
			Count:       1,
			CostMicros:  cost,
			Reason:      "existing reservation is sunk until billing boundary",
		})
	}

	for _, reservation := range reservations {
		if reservation.Source.IsAttached() || !reservation.ActiveAt(now) {
			continue
		}
		if reservation.BillingRenewalAt.IsZero() || reservation.BillingRenewalAt.After(now) {
			continue
		}
		if reservationMatchesDemand(reservation, demand) {
			continue
		}
		deleteActions = append(deleteActions, SolveAction{
			Type:        ActionDelete,
			Reservation: reservation,
			Count:       1,
			Reason:      "reservation reached renewal boundary and is not needed",
		})
	}

	return totalCapacity, committedCost, keepActions, deleteActions
}

func requiredDemandCapacity(demand Demand) uint32 {
	return demand.Nodes
}

func demandOfferCapacity(demand Demand) func(Offer) uint32 {
	return nodeOfferCapacity
}

func nodeOfferCapacity(offer Offer) uint32 {
	if offer.NodeCount > 0 {
		return offer.NodeCount
	}
	if offer.GPUCount > 0 || offer.CPUMillicores > 0 {
		return 1
	}
	return 0
}

func reservationCapacity(reservation Reservation, demand Demand) uint32 {
	if reservation.NodeCount > 0 {
		return reservation.NodeCount
	}
	if reservation.GPUCount > 0 || reservation.CPUMillicores > 0 {
		return 1
	}
	return 0
}

func existingReservationCommitment(reservation Reservation, now, leaseEnd time.Time) int64 {
	if reservation.Source.IsAttached() {
		return 0
	}
	if reservation.CommittedMicros > 0 {
		return reservation.CommittedMicros
	}
	if reservation.BillingRenewalAt.IsZero() || !reservation.BillingRenewalAt.Before(leaseEnd) {
		return 0
	}

	start := reservation.BillingRenewalAt
	if start.Before(now) {
		start = now
	}
	return reservation.HourlyCostMicros * WholeHours(leaseEnd.Sub(start))
}

func reservationMatchesDemand(reservation Reservation, demand Demand) bool {
	if demand.Selector != "" && reservation.Selector != "" && reservation.Selector != demand.Selector {
		return false
	}
	if demand.PoolName != "" && reservation.PoolName != "" && reservation.PoolName != demand.PoolName {
		return false
	}
	if len(demand.GPUs) > 0 && !slices.Contains(demand.GPUs, reservation.GPU) {
		return false
	}
	if len(demand.Providers) > 0 && !slices.Contains(demand.Providers, reservation.Provider) {
		return false
	}
	return true
}

func filterOffers(offers []Offer, pool Pool) []Offer {
	candidates := []Offer{}
	for _, offer := range offers {
		if pool.MatchesOffer(offer) {
			candidates = append(candidates, offer)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		leftCost := offerSortCost(left, pool)
		rightCost := offerSortCost(right, pool)
		if leftCost == rightCost {
			if left.Reliability == right.Reliability {
				return left.HourlyCostMicros < right.HourlyCostMicros
			}
			return left.Reliability > right.Reliability
		}
		return leftCost < rightCost
	})
	return candidates
}

func offerSortCost(offer Offer, pool Pool) float64 {
	return offer.CostPerNode()
}

type boundedSolution struct {
	ok     bool
	cost   int64
	counts []uint32
}

func solveBounded(offers []Offer, neededCapacity uint32, leaseHours int64, offerCapacity func(Offer) uint32) boundedSolution {
	if neededCapacity == 0 {
		return boundedSolution{ok: true, counts: make([]uint32, len(offers))}
	}
	if leaseHours <= 0 {
		leaseHours = 1
	}

	var maxCapacity uint32
	for _, offer := range offers {
		capacity := offerCapacity(offer)
		if capacity > maxCapacity {
			maxCapacity = capacity
		}
	}
	if maxCapacity == 0 {
		return boundedSolution{}
	}

	limit := int(neededCapacity + maxCapacity)
	const unreachable = int64(math.MaxInt64 / 4)
	costs := make([]int64, limit+1)
	counts := make([][]uint32, limit+1)
	for i := range costs {
		costs[i] = unreachable
		counts[i] = make([]uint32, len(offers))
	}
	costs[0] = 0

	for idx, offer := range offers {
		capacity := offerCapacity(offer)
		if capacity == 0 || offer.Available == 0 {
			continue
		}
		unitCost := offer.HourlyCostMicros * leaseHours
		for used := uint32(0); used < offer.Available; used++ {
			nextCosts := slices.Clone(costs)
			nextCounts := make([][]uint32, len(counts))
			for i := range counts {
				nextCounts[i] = slices.Clone(counts[i])
			}
			for usedCapacity := 0; usedCapacity <= limit; usedCapacity++ {
				if costs[usedCapacity] == unreachable {
					continue
				}
				nextCapacity := usedCapacity + int(capacity)
				if nextCapacity > limit {
					nextCapacity = limit
				}
				nextCost := costs[usedCapacity] + unitCost
				if nextCost < nextCosts[nextCapacity] {
					nextCosts[nextCapacity] = nextCost
					nextCounts[nextCapacity] = slices.Clone(counts[usedCapacity])
					nextCounts[nextCapacity][idx]++
				}
			}
			costs = nextCosts
			counts = nextCounts
		}
	}

	bestCapacity := -1
	bestCost := unreachable
	for capacity := int(neededCapacity); capacity <= limit; capacity++ {
		if costs[capacity] < bestCost {
			bestCost = costs[capacity]
			bestCapacity = capacity
		}
	}
	if bestCapacity == -1 {
		return boundedSolution{}
	}
	return boundedSolution{
		ok:     true,
		cost:   bestCost,
		counts: counts[bestCapacity],
	}
}
