package gateway

import (
	"context"
	"slices"

	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/common"
	computesvc "github.com/beam-cloud/beta9/pkg/gateway/services/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/providers/watch"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

// providerMarket is this gateway's part in the record of what the providers
// sell: it samples them when it holds the sampler's lock, writes down what
// becomes of the machines it buys, and answers questions from the record.
//
// All of it is nil on a cluster that does not sample its providers, and each
// part does nothing then.
type providerMarket struct {
	sampler  *watch.Sampler
	reader   *watch.Reader
	outcomes *watch.Recorder
}

// newProviderMarket puts the market together from the config. The market is
// sampled when it is enabled and a provider is configured. Its history is
// kept when S2 is too: without it the gateways still share what the
// providers sell now.
func newProviderMarket(config types.AppConfig, registry *providers.Registry, redisClient *common.RedisClient) providerMarket {
	sampled := config.Providers.Market
	if !sampled.IsEnabled() || len(registry.Names()) == 0 {
		return providerMarket{}
	}

	// A dry run writes nothing, so it is given nowhere to write.
	var history watch.Log
	if !sampled.DryRun {
		kept, err := repository.NewS2MarketLog(config.Database.S2)
		switch {
		case err != nil:
			log.Warn().Err(err).Msg("market: the history of what the providers sell is not kept")
		case kept != nil:
			history = kept
		default:
			log.Info().Msg("market: S2 is not configured, so the history of what the providers sell is not kept")
		}
	}

	// Tenants are shown the providers they can reserve from, at the price
	// they would be billed.
	reserved := computesvc.ReservationProviders(config.ManagedCompute, registry)
	return providerMarket{
		sampler: watch.NewSampler(watch.Options{
			Providers: registry,
			Redis:     redisClient,
			Log:       history,
			Config:    sampled,
		}),
		reader: watch.NewReader(watch.ReaderOptions{
			Providers: registry,
			Redis:     redisClient,
			Log:       history,
			Config:    sampled,
			Sells:     func(provider string) bool { return slices.Contains(reserved, provider) },
			Bill:      func(micros int64) int64 { return computesvc.BillableMicros(config.ManagedCompute, micros) },
		}),
		outcomes: watch.NewRecorder(history, watch.StreamsOf(sampled), nil),
	}
}

// sampledOffers is what the compute service lists offers from, or nil when
// the providers are not sampled: a nil reader is not a source of offers.
func (m providerMarket) sampledOffers() computesvc.SampledOffers {
	if m.reader == nil {
		return nil
	}
	return m.reader
}

func (m providerMarket) start(ctx context.Context) {
	if m.sampler == nil {
		return
	}
	m.outcomes.Start(ctx)
	m.sampler.Start(ctx)
}
