package compute

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/stretchr/testify/require"
)

func TestPoolTakesNoOfferWithoutAPrice(t *testing.T) {
	offer := providers.Offer{ID: "h100", Provider: "shadeform", Region: "us-east", GPU: "H100", GPUCount: 1, CPUMillicores: 8000}
	for _, pool := range []Pool{{}, {GPUs: []string{"H100"}, Nodes: 1}, {OfferID: "h100"}} {
		require.False(t, pool.MatchesOffer(offer), "a machine that cannot be budgeted or billed")
	}

	offer.HourlyCostMicros = providers.DollarsToMicros(2)
	require.True(t, Pool{GPUs: []string{"H100"}, Nodes: 1}.MatchesOffer(offer))
}

func TestPlanNeverBuysAnOfferWithoutAPrice(t *testing.T) {
	pool := Pool{Name: "training", GPUs: []string{"H100"}, Nodes: 1, TTL: time.Hour, MaxSpendMicros: providers.DollarsToMicros(10)}
	unpriced := providers.Offer{ID: "unpriced", Provider: "shadeform", GPU: "H100", GPUCount: 1}
	priced := providers.Offer{ID: "priced", Provider: "shadeform", GPU: "H100", GPUCount: 1, HourlyCostMicros: providers.DollarsToMicros(2)}

	plan, err := PlanPurchases(pool, []providers.Offer{unpriced, priced})
	require.NoError(t, err)
	require.Equal(t, map[string]uint32{"priced": 1}, bought(plan), "no price is not the lowest price")

	_, err = PlanPurchases(pool, []providers.Offer{unpriced})
	require.ErrorIs(t, err, ErrNoCapacity)
}
