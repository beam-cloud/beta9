package solver

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/hybrid"
	"github.com/stretchr/testify/require"
)

func TestSolveTenOneGPUWorkers(t *testing.T) {
	plan := New().Solve(hybrid.SolveInput{
		Demand: hybrid.Demand{
			PoolName:       "training",
			GPUs:           []string{"H100"},
			TotalGPUs:      10,
			TTL:            6 * time.Hour,
			MaxSpendMicros: hybrid.DollarsToMicros(100),
		},
		Offers: []hybrid.Offer{
			{ID: "8xh100", Provider: "vast", GPU: "H100", GPUCount: 8, HourlyCostMicros: hybrid.DollarsToMicros(8), Available: 1},
			{ID: "1xh100", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: hybrid.DollarsToMicros(2), Available: 10},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(10), plan.TotalGPUs)
	require.Equal(t, hybrid.DollarsToMicros(72), plan.IncrementalCostMicros)
	require.Len(t, plan.Actions, 2)
}

func TestSolveOneEightGPUContainer(t *testing.T) {
	plan := New().Solve(hybrid.SolveInput{
		Demand: hybrid.Demand{
			PoolName:       "single-node",
			GPUs:           []string{"H100"},
			TotalGPUs:      8,
			TTL:            time.Hour,
			MaxSpendMicros: hybrid.DollarsToMicros(20),
		},
		Offers: []hybrid.Offer{
			{ID: "1xh100", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: hybrid.DollarsToMicros(2), Available: 8},
			{ID: "8xh100", Provider: "vast", GPU: "H100", GPUCount: 8, HourlyCostMicros: hybrid.DollarsToMicros(10), Available: 1},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(8), plan.TotalGPUs)
	require.Equal(t, hybrid.DollarsToMicros(10), plan.IncrementalCostMicros)
	require.Equal(t, "8xh100", plan.Actions[0].Offer.ID)
}

func TestManualCapacityIsFreeAndPreferred(t *testing.T) {
	plan := New().Solve(hybrid.SolveInput{
		Demand: hybrid.Demand{
			PoolName:       "manual",
			GPUs:           []string{"A100-80"},
			TotalGPUs:      4,
			TTL:            time.Hour,
			MaxSpendMicros: hybrid.DollarsToMicros(1),
		},
		Reservations: []hybrid.Reservation{
			{ID: "manual-1", PoolName: "manual", GPU: "A100-80", GPUCount: 4, Source: hybrid.SourceManual, Status: hybrid.ReservationActive},
		},
		Offers: []hybrid.Offer{
			{ID: "paid", Provider: "shadeform", GPU: "A100-80", GPUCount: 4, HourlyCostMicros: hybrid.DollarsToMicros(20), Available: 1},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(4), plan.ExistingGPUs)
	require.Zero(t, plan.IncrementalCostMicros)
	require.Equal(t, hybrid.ActionKeep, plan.Actions[0].Type)
}

func TestWholeHourSpendCap(t *testing.T) {
	plan := New().Solve(hybrid.SolveInput{
		Demand: hybrid.Demand{
			PoolName:       "hourly",
			GPUs:           []string{"L40S"},
			TotalGPUs:      1,
			TTL:            61 * time.Minute,
			MaxSpendMicros: hybrid.DollarsToMicros(5),
		},
		Offers: []hybrid.Offer{
			{ID: "l40s", Provider: "vast", GPU: "L40S", GPUCount: 1, HourlyCostMicros: hybrid.DollarsToMicros(3), Available: 1},
		},
	})

	require.False(t, plan.Feasible)
	require.Equal(t, "max spend would be exceeded", plan.Reason)
}

func TestExistingReservationIsSunkUntilRenewal(t *testing.T) {
	now := time.Now()
	plan := New().Solve(hybrid.SolveInput{
		Now: now,
		Demand: hybrid.Demand{
			PoolName:       "renewal",
			GPUs:           []string{"H100"},
			TotalGPUs:      2,
			TTL:            time.Hour,
			MaxSpendMicros: hybrid.DollarsToMicros(4),
		},
		Reservations: []hybrid.Reservation{
			{
				ID:               "existing",
				PoolName:         "renewal",
				GPU:              "H100",
				GPUCount:         1,
				Provider:         "vast",
				HourlyCostMicros: hybrid.DollarsToMicros(100),
				Source:           hybrid.SourceCLIReservation,
				Status:           hybrid.ReservationActive,
				BillingRenewalAt: now.Add(2 * time.Hour),
				ExpiresAt:        now.Add(time.Hour),
			},
		},
		Offers: []hybrid.Offer{
			{ID: "new", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: hybrid.DollarsToMicros(4), Available: 1},
		},
	})

	require.True(t, plan.Feasible, plan.Reason)
	require.Equal(t, uint32(1), plan.ExistingGPUs)
	require.Equal(t, hybrid.DollarsToMicros(4), plan.IncrementalCostMicros)
}
