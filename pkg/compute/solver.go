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

	requiredGPUs := in.Demand.TotalGPUs + in.Demand.HeadroomGPUs
	existingGPUs, committedCost, keepActions, deleteActions := usableReservations(in.Reservations, in.Demand, now)
	if existingGPUs >= requiredGPUs {
		return SolvePlan{
			Feasible:            true,
			Actions:             append(keepActions, deleteActions...),
			TotalGPUs:           existingGPUs,
			ExistingGPUs:        existingGPUs,
			CommittedCostMicros: committedCost,
		}
	}

	neededGPUs := requiredGPUs - existingGPUs
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
	best := solveBounded(candidates, neededGPUs, leaseHours)
	if !best.ok {
		return SolvePlan{
			Feasible:            false,
			Reason:              "insufficient compatible capacity",
			Actions:             append(keepActions, deleteActions...),
			ExistingGPUs:        existingGPUs,
			CommittedCostMicros: committedCost,
		}
	}

	totalCommitment := committedCost + best.cost
	if in.Demand.MaxSpendMicros > 0 && totalCommitment > in.Demand.MaxSpendMicros {
		return SolvePlan{
			Feasible:            false,
			Reason:              "max spend would be exceeded",
			Actions:             append(keepActions, deleteActions...),
			ExistingGPUs:        existingGPUs,
			CommittedCostMicros: committedCost,
		}
	}

	actions := make([]SolveAction, 0, len(keepActions)+len(best.counts)+len(deleteActions))
	actions = append(actions, keepActions...)
	var newGPUs uint32
	for idx, count := range best.counts {
		if count == 0 {
			continue
		}
		offer := candidates[idx]
		actionCost := offer.HourlyCostMicros * int64(count) * leaseHours
		newGPUs += offer.GPUCount * count
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
		TotalGPUs:             existingGPUs + newGPUs,
		ExistingGPUs:          existingGPUs,
		NewGPUs:               newGPUs,
		IncrementalCostMicros: best.cost,
		CommittedCostMicros:   totalCommitment,
	}
}

func usableReservations(reservations []Reservation, demand Demand, now time.Time) (uint32, int64, []SolveAction, []SolveAction) {
	var totalGPUs uint32
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

		if reservation.Source.IsAttached() {
			totalGPUs += reservation.GPUCount
			keepActions = append(keepActions, SolveAction{
				Type:        ActionKeep,
				Reservation: reservation,
				Count:       1,
				Reason:      "attached capacity has no incremental provider cost",
			})
			continue
		}

		totalGPUs += reservation.GPUCount
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

	return totalGPUs, committedCost, keepActions, deleteActions
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
		if left.CostPerGPU() == right.CostPerGPU() {
			if left.Reliability == right.Reliability {
				return left.HourlyCostMicros < right.HourlyCostMicros
			}
			return left.Reliability > right.Reliability
		}
		return left.CostPerGPU() < right.CostPerGPU()
	})
	return candidates
}

type boundedSolution struct {
	ok     bool
	cost   int64
	gpus   uint32
	counts []uint32
}

func solveBounded(offers []Offer, neededGPUs uint32, leaseHours int64) boundedSolution {
	if neededGPUs == 0 {
		return boundedSolution{ok: true, counts: make([]uint32, len(offers))}
	}
	if leaseHours <= 0 {
		leaseHours = 1
	}

	var maxGPU uint32
	for _, offer := range offers {
		if offer.GPUCount > maxGPU {
			maxGPU = offer.GPUCount
		}
	}
	if maxGPU == 0 {
		return boundedSolution{}
	}

	limit := int(neededGPUs + maxGPU)
	const unreachable = int64(math.MaxInt64 / 4)
	costs := make([]int64, limit+1)
	counts := make([][]uint32, limit+1)
	gpus := make([]uint32, limit+1)
	for i := range costs {
		costs[i] = unreachable
		counts[i] = make([]uint32, len(offers))
	}
	costs[0] = 0

	for idx, offer := range offers {
		if offer.GPUCount == 0 || offer.Available == 0 {
			continue
		}
		machineCost := offer.HourlyCostMicros * leaseHours
		for used := uint32(0); used < offer.Available; used++ {
			nextCosts := slices.Clone(costs)
			nextCounts := make([][]uint32, len(counts))
			for i := range counts {
				nextCounts[i] = slices.Clone(counts[i])
			}
			nextGPUs := slices.Clone(gpus)
			for gpu := 0; gpu <= limit; gpu++ {
				if costs[gpu] == unreachable {
					continue
				}
				nextGPU := gpu + int(offer.GPUCount)
				if nextGPU > limit {
					nextGPU = limit
				}
				nextCost := costs[gpu] + machineCost
				if nextCost < nextCosts[nextGPU] {
					nextCosts[nextGPU] = nextCost
					nextCounts[nextGPU] = slices.Clone(counts[gpu])
					nextCounts[nextGPU][idx]++
					nextGPUs[nextGPU] = uint32(nextGPU)
				}
			}
			costs = nextCosts
			counts = nextCounts
			gpus = nextGPUs
		}
	}

	bestGPU := -1
	bestCost := unreachable
	for gpu := int(neededGPUs); gpu <= limit; gpu++ {
		if costs[gpu] < bestCost {
			bestCost = costs[gpu]
			bestGPU = gpu
		}
	}
	if bestGPU == -1 {
		return boundedSolution{}
	}
	return boundedSolution{
		ok:     true,
		cost:   bestCost,
		gpus:   uint32(bestGPU),
		counts: counts[bestGPU],
	}
}
