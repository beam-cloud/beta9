package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
)

func marketTestRedis(t *testing.T) *common.RedisClient {
	t.Helper()
	server := miniredis.RunT(t)
	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	return redisClient
}

// sellerOfA10G is a provider that sells one A10G for the price.
func sellerOfA10G(name string, micros int64) *providers.Fake {
	provider := providers.NewFake(name, "beam-test")
	provider.SetOffers(providers.Offer{
		ID: "A10G", InstanceType: "A10G", Region: "us-east-1",
		GPU: "A10G", GPUCount: 1, CPUMillicores: 4000, MemoryMB: 16 * 1024,
		HourlyCostMicros: micros, Available: 3,
	})
	return provider
}

func TestTheMarketIsSampledOnlyWhereItIsEnabledAndAProviderIsConfigured(t *testing.T) {
	redisClient := marketTestRedis(t)
	off := false

	disabled := types.AppConfig{}
	disabled.Providers.Market.Enabled = &off
	for name, m := range map[string]providerMarket{
		"it is disabled":            newProviderMarket(disabled, providers.RegistryOf(sellerOfA10G("verda", 1_000_000)), redisClient),
		"no provider is configured": newProviderMarket(types.AppConfig{}, providers.RegistryOf(), redisClient),
	} {
		require.Nil(t, m.sampler, name)
		require.Nil(t, m.reader, name)
		require.Nil(t, m.outcomes, name)
		require.True(t, m.sampledOffers() == nil, "%s: the compute service is given no source of offers, and asks the providers", name)
		m.start(context.Background())
	}
}

// Without S2 the gateways still share what the providers sell now: offers
// are listed from it, and the dashboard is told what is for sale. Nothing of
// the past is kept, and nothing is written of the machines that are bought.
func TestTheMarketIsSharedBetweenGatewaysWithoutAHistory(t *testing.T) {
	for name, config := range map[string]types.AppConfig{
		"S2 is not configured": {},
		"a dry run": func() types.AppConfig {
			config := types.AppConfig{}
			config.Providers.Market.DryRun = true
			config.Database.S2 = types.S2Config{ApiKey: "key", Basin: "basin"}
			return config
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			registry := providers.RegistryOf(sellerOfA10G("verda", 1_000_000))
			m := newProviderMarket(config, registry, marketTestRedis(t))
			require.NotNil(t, m.sampler)
			require.NotNil(t, m.reader)
			require.Nil(t, m.outcomes)
			require.False(t, m.reader.HasHistory())

			at := time.Now().UTC().Truncate(time.Minute)
			m.sampler.Sample(context.Background(), at)

			offers, fresh := m.sampledOffers().Offers(context.Background(), "verda")
			require.True(t, fresh)
			require.Len(t, offers, 1)
			require.Equal(t, "verda", offers[0].Provider)
		})
	}
}

// A tenant is shown what it can reserve, at what it would pay: the
// providers the config sells reservations from, with the margin on top. A
// cluster admin is shown every provider at its own price.
func TestTheMarketShowsATenantWhatItCanReserveAtWhatItWouldPay(t *testing.T) {
	margin := 0.5
	config := types.AppConfig{}
	config.ManagedCompute.BillableMarginPct = &margin
	config.ManagedCompute.Reservations.Providers = []string{"shadeform"}
	registry := providers.RegistryOf(sellerOfA10G("verda", 1_000_000), sellerOfA10G("shadeform", 2_000_000))
	m := newProviderMarket(config, registry, marketTestRedis(t))

	ctx := context.Background()
	m.sampler.Sample(ctx, time.Now().UTC().Truncate(time.Minute))

	tenant, err := m.reader.Now(ctx, false)
	require.NoError(t, err)
	require.Len(t, tenant.Rows, 1, "verda sells only to pools")
	require.Empty(t, tenant.Rows[0].Provider)
	require.NotNil(t, tenant.Rows[0].Price)
	require.Equal(t, int64(3_000_000), *tenant.Rows[0].Price)

	admin, err := m.reader.Now(ctx, true)
	require.NoError(t, err)
	costs := map[string]int64{}
	for _, row := range admin.Rows {
		require.NotNil(t, row.Cost)
		costs[row.Provider] = *row.Cost
	}
	require.Equal(t, map[string]int64{"verda": 1_000_000, "shadeform": 2_000_000}, costs)
}
