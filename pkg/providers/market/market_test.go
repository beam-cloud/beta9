package market

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noon is when the tests' first sample is taken.
var noon = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

// testMarket is a market with nothing real behind it: providers that sell
// what a test says, a Redis in memory, and a history in memory.
type testMarket struct {
	t        *testing.T
	redis    *common.RedisClient
	log      *MemoryLog
	clock    *common.FakeClock
	registry *providers.Registry
	config   types.ProviderMarketConfig
}

func newTestMarket(t *testing.T, sellers ...providers.Provider) *testMarket {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	return &testMarket{
		t:        t,
		redis:    rdb,
		log:      NewMemoryLog(),
		clock:    common.NewFakeClock(noon),
		registry: providers.RegistryOf(sellers...),
	}
}

// sampler is a gateway's sampler. Every one of a market shares its Redis
// and its history, as the gateways of a cluster do.
func (m *testMarket) sampler(id string, change ...func(*Options)) *Sampler {
	opts := Options{Providers: m.registry, Redis: m.redis, Log: m.log, Config: m.config, Clock: m.clock, ID: id}
	for _, apply := range change {
		apply(&opts)
	}
	return NewSampler(opts)
}

// reader is a new reader, which has no answer kept from before.
func (m *testMarket) reader(change ...func(*ReaderOptions)) *Reader {
	opts := ReaderOptions{Providers: m.registry, Redis: m.redis, Log: m.log, Config: m.config, Clock: m.clock}
	for _, apply := range change {
		apply(&opts)
	}
	return NewReader(opts)
}

// sample takes the sample of a time, and moves the clock to it.
func (m *testMarket) sample(sampler *Sampler, at time.Time) {
	m.t.Helper()
	if at.After(m.clock.Now()) {
		m.clock.Advance(at.Sub(m.clock.Now()))
	}
	sampler.Sample(context.Background(), at)
}

func read[T any](t *testing.T, log *MemoryLog, stream string) []T {
	t.Helper()
	var out []T
	for _, record := range log.All(stream) {
		var value T
		require.NoError(t, json.Unmarshal(record.Body, &value), "a record of %s", stream)
		out = append(out, value)
	}
	return out
}

func (m *testMarket) samples(gpu string) []Sample {
	return read[Sample](m.t, m.log, "series/"+slug(gpu)+"/1m")
}

func (m *testMarket) hours(gpu string) []Hourly {
	return read[Hourly](m.t, m.log, "series/"+slug(gpu)+"/1h")
}

func (m *testMarket) snapshots(provider string) []Snapshot {
	return read[Snapshot](m.t, m.log, "snapshots/"+provider)
}

// silent is a provider that can be made not to answer what it sells.
type silent struct {
	*providers.Fake
	err error
}

func (s *silent) Offers(ctx context.Context, filter providers.OfferFilter) ([]providers.Offer, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.Fake.Offers(ctx, filter)
}

func a10g(id, region string, gpus uint32, micros int64, change ...func(*providers.Offer)) providers.Offer {
	offer := providers.Offer{
		ID: id, Region: region, GPU: "A10G", GPUCount: gpus, HourlyCostMicros: micros,
		CPUMillicores: 8000, MemoryMB: 32768, StorageMB: 100_000,
	}
	for _, apply := range change {
		apply(&offer)
	}
	return offer
}

func spot(offer *providers.Offer) { offer.Spot = true }

func stock(n uint32) func(*providers.Offer) {
	return func(offer *providers.Offer) { offer.Available = n }
}

func cloud(name string) func(*providers.Offer) {
	return func(offer *providers.Offer) { offer.Cloud = name }
}

func rowOf(t *testing.T, sample Sample, provider string, gpus uint32, spot bool) Row {
	t.Helper()
	for _, row := range sample.Rows {
		if row.Provider == provider && row.GPUs == gpus && row.Spot == spot {
			return row
		}
	}
	t.Fatalf("the sample has no row of %s with %d GPUs (spot %t): %+v", provider, gpus, spot, sample.Rows)
	return Row{}
}

func TestOnlyAProviderThatSellsByZoneHasAZoneInWhatItSells(t *testing.T) {
	zone, instanceType := place(providers.Offer{Provider: "aws", ID: "us-east-1a/g5.xlarge", InstanceType: "g5.xlarge"})
	assert.Equal(t, "us-east-1a", zone)
	assert.Equal(t, "g5.xlarge", instanceType)

	zone, instanceType = place(providers.Offer{Provider: "vast", ID: "12345/2"})
	assert.Empty(t, zone, "a Vast host is not in a zone, whatever its ID looks like")
	assert.Equal(t, "12345/2", instanceType)
}
