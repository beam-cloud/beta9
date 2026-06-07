package solver

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/stretchr/testify/require"
)

func TestSolveTenOneGPUWorkers(t *testing.T) {
	plan := New().Solve(compute.SolveInput{
		Demand: compute.Demand{
			PoolName:       "training",
			GPUs:           []string{"H100"},
			TotalGPUs:      10,
			TTL:            6 * time.Hour,
			MaxSpendMicros: compute.DollarsToMicros(100),
		},
		Offers: []compute.Offer{
			{ID: "8xh100", Provider: "vast", GPU: "H100", GPUCount: 8, HourlyCostMicros: compute.DollarsToMicros(8), Available: 1},
			{ID: "1xh100", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: compute.DollarsToMicros(2), Available: 10},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(10), plan.TotalGPUs)
	require.Equal(t, compute.DollarsToMicros(72), plan.IncrementalCostMicros)
	require.Len(t, plan.Actions, 2)
}

func TestSolveOneEightGPUContainer(t *testing.T) {
	plan := New().Solve(compute.SolveInput{
		Demand: compute.Demand{
			PoolName:       "single-node",
			GPUs:           []string{"H100"},
			TotalGPUs:      8,
			TTL:            time.Hour,
			MaxSpendMicros: compute.DollarsToMicros(20),
		},
		Offers: []compute.Offer{
			{ID: "1xh100", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: compute.DollarsToMicros(2), Available: 8},
			{ID: "8xh100", Provider: "vast", GPU: "H100", GPUCount: 8, HourlyCostMicros: compute.DollarsToMicros(10), Available: 1},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(8), plan.TotalGPUs)
	require.Equal(t, compute.DollarsToMicros(10), plan.IncrementalCostMicros)
	require.Equal(t, "8xh100", plan.Actions[0].Offer.ID)
}

func TestAttachedCapacityIsFreeAndPreferred(t *testing.T) {
	plan := New().Solve(compute.SolveInput{
		Demand: compute.Demand{
			PoolName:       "attached",
			GPUs:           []string{"A100-80"},
			TotalGPUs:      4,
			TTL:            time.Hour,
			MaxSpendMicros: compute.DollarsToMicros(1),
		},
		Reservations: []compute.Reservation{
			{ID: "attached-1", PoolName: "attached", GPU: "A100-80", GPUCount: 4, Source: compute.SourceAttached, Status: compute.ReservationActive},
		},
		Offers: []compute.Offer{
			{ID: "paid", Provider: "shadeform", GPU: "A100-80", GPUCount: 4, HourlyCostMicros: compute.DollarsToMicros(20), Available: 1},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(4), plan.ExistingGPUs)
	require.Zero(t, plan.IncrementalCostMicros)
	require.Equal(t, compute.ActionKeep, plan.Actions[0].Type)
}

func TestWholeHourSpendCap(t *testing.T) {
	plan := New().Solve(compute.SolveInput{
		Demand: compute.Demand{
			PoolName:       "hourly",
			GPUs:           []string{"L40S"},
			TotalGPUs:      1,
			TTL:            61 * time.Minute,
			MaxSpendMicros: compute.DollarsToMicros(5),
		},
		Offers: []compute.Offer{
			{ID: "l40s", Provider: "vast", GPU: "L40S", GPUCount: 1, HourlyCostMicros: compute.DollarsToMicros(3), Available: 1},
		},
	})

	require.False(t, plan.Feasible)
	require.Equal(t, "max spend would be exceeded", plan.Reason)
}

func TestExistingReservationIsSunkUntilRenewal(t *testing.T) {
	now := time.Now()
	plan := New().Solve(compute.SolveInput{
		Now: now,
		Demand: compute.Demand{
			PoolName:       "renewal",
			GPUs:           []string{"H100"},
			TotalGPUs:      2,
			TTL:            time.Hour,
			MaxSpendMicros: compute.DollarsToMicros(4),
		},
		Reservations: []compute.Reservation{
			{
				ID:               "existing",
				PoolName:         "renewal",
				GPU:              "H100",
				GPUCount:         1,
				Provider:         "vast",
				HourlyCostMicros: compute.DollarsToMicros(100),
				Source:           compute.SourceCLIReservation,
				Status:           compute.ReservationActive,
				BillingRenewalAt: now.Add(2 * time.Hour),
				ExpiresAt:        now.Add(time.Hour),
			},
		},
		Offers: []compute.Offer{
			{ID: "new", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: compute.DollarsToMicros(4), Available: 1},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(1), plan.ExistingGPUs)
	require.Equal(t, compute.DollarsToMicros(4), plan.IncrementalCostMicros)
}
