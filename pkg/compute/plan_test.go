package compute

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/stretchr/testify/require"
)

func bought(plan Plan) map[string]uint32 {
	counts := map[string]uint32{}
	for _, purchase := range plan.Purchases {
		counts[purchase.Offer.ID] += purchase.Count
	}
	return counts
}

func TestPlanBuysTheCheapestMachines(t *testing.T) {
	plan, err := PlanPurchases(
		Pool{Name: "training", GPUs: []string{"H100"}, Nodes: 2, TTL: 6 * time.Hour, MaxSpendMicros: providers.DollarsToMicros(100)},
		[]providers.Offer{
			{ID: "8xh100", Provider: "vast", GPU: "H100", GPUCount: 8, HourlyCostMicros: providers.DollarsToMicros(8), Available: 1},
			{ID: "1xh100", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: providers.DollarsToMicros(2), Available: 10},
		},
	)

	require.NoError(t, err)
	require.Equal(t, map[string]uint32{"1xh100": 2}, bought(plan))
	require.Equal(t, uint32(2), plan.Machines())
	require.Equal(t, providers.DollarsToMicros(4), plan.HourlyCostMicros())
	require.Equal(t, providers.DollarsToMicros(24), plan.CostMicros, "two machines for six hours")
}

func TestPlanBuysTheOfferThePoolNames(t *testing.T) {
	plan, err := PlanPurchases(
		Pool{Name: "single-node", GPUs: []string{"H100"}, Nodes: 1, OfferID: "8xh100", TTL: time.Hour, MaxSpendMicros: providers.DollarsToMicros(20)},
		[]providers.Offer{
			{ID: "1xh100", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: providers.DollarsToMicros(2), Available: 8},
			{ID: "8xh100", Provider: "vast", GPU: "H100", GPUCount: 8, HourlyCostMicros: providers.DollarsToMicros(10), Available: 1},
		},
	)

	require.NoError(t, err)
	require.Equal(t, map[string]uint32{"8xh100": 1}, bought(plan))
	require.Equal(t, providers.DollarsToMicros(10), plan.CostMicros)
}

func TestPlanBuysCPUMachinesForAPoolThatNamesNoGPU(t *testing.T) {
	plan, err := PlanPurchases(
		Pool{Name: "cpu-pool", Nodes: 2, TTL: 2 * time.Hour, MaxSpendMicros: providers.DollarsToMicros(10), Providers: []string{"hetzner"}},
		[]providers.Offer{
			{ID: "cpx31", Provider: "hetzner", CPUMillicores: 4000, MemoryMB: 8192, HourlyCostMicros: providers.DollarsToMicros(1.5), Available: 5},
			{ID: "gpu", Provider: "hetzner", GPU: "A10G", GPUCount: 1, HourlyCostMicros: providers.DollarsToMicros(1), Available: 5},
		},
	)

	require.NoError(t, err)
	require.Equal(t, map[string]uint32{"cpx31": 2}, bought(plan))
	require.Equal(t, providers.DollarsToMicros(6), plan.CostMicros)
}

func TestPlanBuysTheMoreReliableOfTwoOffersAtOnePrice(t *testing.T) {
	plan, err := PlanPurchases(
		Pool{Name: "training", GPUs: []string{"H100"}, Nodes: 1, TTL: time.Hour, MaxSpendMicros: providers.DollarsToMicros(10)},
		[]providers.Offer{
			{ID: "shaky", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: providers.DollarsToMicros(2), Reliability: 0.90},
			{ID: "steady", Provider: "vast", GPU: "H100", GPUCount: 1, HourlyCostMicros: providers.DollarsToMicros(2), Reliability: 0.99},
		},
	)

	require.NoError(t, err)
	require.Equal(t, map[string]uint32{"steady": 1}, bought(plan))
}

func TestPlanPaysForAStartedHour(t *testing.T) {
	_, err := PlanPurchases(
		Pool{Name: "hourly", GPUs: []string{"L40S"}, Nodes: 1, TTL: 61 * time.Minute, MaxSpendMicros: providers.DollarsToMicros(5)},
		[]providers.Offer{
			{ID: "l40s", Provider: "vast", GPU: "L40S", GPUCount: 1, HourlyCostMicros: providers.DollarsToMicros(3), Available: 1},
		},
	)

	require.ErrorIs(t, err, ErrOverBudget, "61 minutes at $3 an hour is $6")
}

func TestPlanSpendsUpToWhatThePoolMaySpend(t *testing.T) {
	plan, err := PlanPurchases(
		Pool{Name: "exact", GPUs: []string{"L40S"}, Nodes: 2, TTL: time.Hour, MaxSpendMicros: providers.DollarsToMicros(6)},
		[]providers.Offer{
			{ID: "l40s", Provider: "vast", GPU: "L40S", GPUCount: 1, HourlyCostMicros: providers.DollarsToMicros(3)},
		},
	)

	require.NoError(t, err)
	require.Equal(t, providers.DollarsToMicros(6), plan.CostMicros)
}

func TestPlanBuysAsManyAsNeededOfAnOfferTheProviderDoesNotCount(t *testing.T) {
	plan, err := PlanPurchases(
		Pool{Name: "cpu-pool", Nodes: 3, TTL: time.Hour, MaxSpendMicros: providers.DollarsToMicros(10)},
		[]providers.Offer{
			{ID: "cpx31", Provider: "hetzner", CPUMillicores: 4000, HourlyCostMicros: providers.DollarsToMicros(1)},
		},
	)

	require.NoError(t, err)
	require.Equal(t, map[string]uint32{"cpx31": 3}, bought(plan))
	require.Equal(t, providers.DollarsToMicros(3), plan.CostMicros)
}

func TestPlanBuysNoMoreOfAnOfferThanTheProviderHas(t *testing.T) {
	pool := Pool{Name: "training", GPUs: []string{"H100"}, Nodes: 3, TTL: time.Hour, MaxSpendMicros: providers.DollarsToMicros(100)}
	cheap := providers.Offer{ID: "cheap", Provider: "fake", GPU: "H100", GPUCount: 1, HourlyCostMicros: providers.DollarsToMicros(1), Available: 2}
	dear := providers.Offer{ID: "dear", Provider: "fake", GPU: "H100", GPUCount: 1, HourlyCostMicros: providers.DollarsToMicros(5), Available: 4}

	plan, err := PlanPurchases(pool, []providers.Offer{dear, cheap})
	require.NoError(t, err)
	require.Equal(t, map[string]uint32{"cheap": 2, "dear": 1}, bought(plan))
	require.Equal(t, providers.DollarsToMicros(7), plan.CostMicros)

	_, err = PlanPurchases(pool, []providers.Offer{cheap})
	require.ErrorIs(t, err, ErrNoCapacity)
}

func TestPlanBuysNothingForAPoolThatAsksForNoMachines(t *testing.T) {
	plan, err := PlanPurchases(Pool{Name: "attached"}, []providers.Offer{
		{ID: "cpx31", Provider: "hetzner", CPUMillicores: 4000, HourlyCostMicros: providers.DollarsToMicros(1)},
	})

	require.NoError(t, err)
	require.Empty(t, plan.Purchases)
	require.Zero(t, plan.CostMicros)
}

func TestPlanRefusesAPoolThatIsNotValid(t *testing.T) {
	_, err := PlanPurchases(Pool{Name: "no-lifetime", Nodes: 1, MaxSpendMicros: providers.DollarsToMicros(5)}, nil)
	require.ErrorContains(t, err, "ttl")
}

func TestPoolMatchesOffersOfItsKind(t *testing.T) {
	gpu := providers.Offer{ID: "h100", Provider: "shadeform", Region: "us-east", GPU: "H100", GPUCount: 1, CPUMillicores: 8000, HourlyCostMicros: providers.DollarsToMicros(2)}
	cpu := providers.Offer{ID: "cpx31", Provider: "hetzner", Region: "ash", CPUMillicores: 4000, HourlyCostMicros: providers.DollarsToMicros(1)}

	for name, tc := range map[string]struct {
		pool Pool
		want []string
	}{
		"a GPU pool takes machines with that GPU": {pool: Pool{GPUs: []string{"H100"}, Nodes: 1}, want: []string{"h100"}},
		"another GPU": {pool: Pool{GPUs: []string{"A10G"}, Nodes: 1}, want: []string{}},
		"a pool that names no GPU takes CPU only":  {pool: Pool{Nodes: 1}, want: []string{"cpx31"}},
		"a pool that asks for nothing takes both":  {pool: Pool{}, want: []string{"h100", "cpx31"}},
		"one offer by id":                          {pool: Pool{OfferID: "cpx31"}, want: []string{"cpx31"}},
		"one provider":                             {pool: Pool{Providers: []string{"shadeform"}}, want: []string{"h100"}},
		"one region":                               {pool: Pool{Regions: []string{"ash"}}, want: []string{"cpx31"}},
		"an offer with no stock count still fits":  {pool: Pool{GPUs: []string{"H100"}, Nodes: 4}, want: []string{"h100"}},
		"reliability is not held against no score": {pool: Pool{MinReliability: 0.9}, want: []string{"h100", "cpx31"}},
	} {
		got := []string{}
		for _, offer := range []providers.Offer{gpu, cpu} {
			if tc.pool.MatchesOffer(offer) {
				got = append(got, offer.ID)
			}
		}
		require.Equal(t, tc.want, got, name)
	}

	unreliable := gpu
	unreliable.Reliability = 0.5
	require.False(t, Pool{MinReliability: 0.9}.MatchesOffer(unreliable))
}
