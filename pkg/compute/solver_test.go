package compute

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSolveTenOneGPUWorkers(t *testing.T) {
	plan := NewSolver().Solve(SolveInput{
		Demand: Demand{
			PoolName:       "training",
			GPUs:           []string{"H100"},
			TotalGPUs:      10,
			TTL:            6 * time.Hour,
			MaxSpendMicros: DollarsToMicros(100),
		},
		Offers: []Offer{
			{ID: "8xh100", Provider: "vast", GPU: "H100", GPUCount: 8, HourlyCostMicros: DollarsToMicros(8), Available: 1},
			{ID: "1xh100", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: DollarsToMicros(2), Available: 10},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(10), plan.TotalGPUs)
	require.Equal(t, DollarsToMicros(72), plan.IncrementalCostMicros)
	require.Len(t, plan.Actions, 2)
}

func TestSolveOneEightGPUContainer(t *testing.T) {
	plan := NewSolver().Solve(SolveInput{
		Demand: Demand{
			PoolName:       "single-node",
			GPUs:           []string{"H100"},
			TotalGPUs:      8,
			TTL:            time.Hour,
			MaxSpendMicros: DollarsToMicros(20),
		},
		Offers: []Offer{
			{ID: "1xh100", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: DollarsToMicros(2), Available: 8},
			{ID: "8xh100", Provider: "vast", GPU: "H100", GPUCount: 8, HourlyCostMicros: DollarsToMicros(10), Available: 1},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(8), plan.TotalGPUs)
	require.Equal(t, DollarsToMicros(10), plan.IncrementalCostMicros)
	require.Equal(t, "8xh100", plan.Actions[0].Offer.ID)
}

func TestAttachedCapacityIsFreeAndPreferred(t *testing.T) {
	plan := NewSolver().Solve(SolveInput{
		Demand: Demand{
			PoolName:       "attached",
			GPUs:           []string{"A100-80"},
			TotalGPUs:      4,
			TTL:            time.Hour,
			MaxSpendMicros: DollarsToMicros(1),
		},
		Reservations: []Reservation{
			{ID: "attached-1", PoolName: "attached", GPU: "A100-80", GPUCount: 4, Source: SourceAttached, Status: ReservationActive},
		},
		Offers: []Offer{
			{ID: "paid", Provider: "shadeform", GPU: "A100-80", GPUCount: 4, HourlyCostMicros: DollarsToMicros(20), Available: 1},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(4), plan.ExistingGPUs)
	require.Zero(t, plan.IncrementalCostMicros)
	require.Equal(t, ActionKeep, plan.Actions[0].Type)
}

func TestWholeHourSpendCap(t *testing.T) {
	plan := NewSolver().Solve(SolveInput{
		Demand: Demand{
			PoolName:       "hourly",
			GPUs:           []string{"L40S"},
			TotalGPUs:      1,
			TTL:            61 * time.Minute,
			MaxSpendMicros: DollarsToMicros(5),
		},
		Offers: []Offer{
			{ID: "l40s", Provider: "vast", GPU: "L40S", GPUCount: 1, HourlyCostMicros: DollarsToMicros(3), Available: 1},
		},
	})

	require.False(t, plan.Feasible)
	require.Equal(t, "max spend would be exceeded", plan.Reason)
}

func TestExistingReservationIsSunkUntilRenewal(t *testing.T) {
	now := time.Now()
	plan := NewSolver().Solve(SolveInput{
		Now: now,
		Demand: Demand{
			PoolName:       "renewal",
			GPUs:           []string{"H100"},
			TotalGPUs:      2,
			TTL:            time.Hour,
			MaxSpendMicros: DollarsToMicros(4),
		},
		Reservations: []Reservation{
			{
				ID:               "existing",
				PoolName:         "renewal",
				GPU:              "H100",
				GPUCount:         1,
				Provider:         "vast",
				HourlyCostMicros: DollarsToMicros(100),
				Source:           SourceCLIReservation,
				Status:           ReservationActive,
				BillingRenewalAt: now.Add(2 * time.Hour),
				ExpiresAt:        now.Add(time.Hour),
			},
		},
		Offers: []Offer{
			{ID: "new", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: DollarsToMicros(4), Available: 1},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(1), plan.ExistingGPUs)
	require.Equal(t, DollarsToMicros(4), plan.IncrementalCostMicros)
}
