package market

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
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

func TestEveryProvidersRegionsArePutInTheSamePartsOfTheWorld(t *testing.T) {
	for region, geo := range map[string]string{
		// AWS, GCP and Nebius name a region for where it is.
		"us-east-1": "us-east", "us-east-2": "us-east", "us-west-2": "us-west", "ca-central-1": "ca-east",
		"eu-west-3": "eu-west", "eu-central-1": "eu-central", "eu-north-1": "eu-north", "eu-south-1": "eu-central",
		"ap-southeast-2": "ap-southeast", "ap-northeast-1": "ap-northeast", "ap-south-1": "ap-south",
		"me-central-1": "me-west", "sa-east-1": "other", "af-south-1": "other",
		"us-central1": "us-central", "us-east4": "us-east", "us-south1": "us-central",
		"europe-west4": "eu-west", "europe-west3": "eu-central", "asia-northeast1": "ap-northeast",
		"australia-southeast1": "ap-southeast", "southamerica-east1": "other",
		"eu-north1": "eu-north", "eu-west1": "eu-west",
		// Hetzner names a data centre.
		"ash": "us-east", "hil": "us-west", "fsn1": "eu-central", "nbg1": "eu-central", "hel1": "eu-north", "sin": "ap-southeast",
		// Shadeform names a city and its country, Verda a country, and Vast
		// two letters.
		"newyork-usa-1": "us-east", "dallas-usa-2": "us-central", "frankfurt-deu-1": "eu-central",
		"FIN-01": "eu-north", "ICE-01": "eu-north",
		"US": "us-central", "CA": "ca-east", "DE": "eu-central", "JP": "ap-northeast",
		// A part of the world is itself.
		"us-west": "us-west", "eu-north": "eu-north",
		"": "other", "mars-1": "other",
	} {
		assert.Equal(t, geo, Geo(region), "region %q", region)
	}
}

func TestOnlyAProviderThatSellsByZoneHasAZoneInWhatItSells(t *testing.T) {
	zone, instanceType := place(providers.Offer{Provider: "aws", ID: "us-east-1a/g5.xlarge", InstanceType: "g5.xlarge"})
	assert.Equal(t, "us-east-1a", zone)
	assert.Equal(t, "g5.xlarge", instanceType)

	zone, instanceType = place(providers.Offer{Provider: "vast", ID: "12345/2"})
	assert.Empty(t, zone, "a Vast host is not in a zone, whatever its ID looks like")
	assert.Equal(t, "12345/2", instanceType)
}

func TestASnapshotKeepsEveryFieldOfEveryOffer(t *testing.T) {
	offers := []providers.Offer{{
		ID: "us-east-1a/g5.xlarge", Provider: "aws", Cloud: "aws", InstanceType: "g5.xlarge", Region: "us-east-1",
		GPU: "A10G", GPUCount: 1, CPUMillicores: 4000, MemoryMB: 16384, StorageMB: 250_000,
		HourlyCostMicros: 401_300, Spot: true, Available: 3, Reliability: 0.987,
		DisplayName: "g5.xlarge", Category: "gpu", RegionDisplayName: "N. Virginia", Latitude: 38.9, Longitude: -77.4,
	}}
	body, err := json.Marshal(Snapshot{At: noon, Provider: "aws", Kind: SnapshotFull, Poll: Poll{OK: true, MS: 12, N: 1}, Hash: "abc", Sampler: "gw-1", Offers: offers})
	require.NoError(t, err)

	var back Snapshot
	require.NoError(t, json.Unmarshal(body, &back))
	assert.Equal(t, offers, back.Offers)
	assert.Equal(t, noon, back.At)
	assert.Equal(t, Poll{OK: true, MS: 12, N: 1}, back.Poll)
	assert.Equal(t, "gw-1", back.Sampler)
}

func TestASampleRecordsWhatEveryProviderSells(t *testing.T) {
	shadeform := providers.NewFake("shadeform", "test")
	shadeform.SetOffers(
		a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000, cloud("lambda")),
		a10g("A10G_1x_b", "newyork-usa-1", 1, 1_200_000, cloud("lambda")),
		a10g("A10G_1x_c", "newyork-usa-1", 1, 1_400_000, cloud("lambda")),
		a10g("A10G_4x", "newyork-usa-1", 4, 3_600_000, cloud("lambda")),
		providers.Offer{ID: "cpu-8", Region: "newyork-usa-1", CPUMillicores: 8000, MemoryMB: 16384, HourlyCostMicros: 90_000},
	)
	aws := providers.NewFake("aws", "test")
	aws.SetOffers(
		a10g("us-east-1a/g5.xlarge", "us-east-1", 1, 400_000, spot),
		a10g("us-east-1b/g5.xlarge", "us-east-1", 1, 380_000, spot),
	)
	vast := providers.NewFake("vast", "test")
	vast.SetOffers(
		a10g("501/1", "US", 1, 300_000, stock(1)),
		a10g("502/1", "US", 1, 350_000, stock(1)),
	)
	market := newTestMarket(t, shadeform, aws, vast)

	market.sample(market.sampler("gw-1"), noon)

	samples := market.samples("A10G")
	require.Len(t, samples, 1)
	sample := samples[0]
	assert.Equal(t, noon, sample.At)
	assert.Equal(t, "A10G", sample.GPU)
	require.Len(t, sample.Polls, 3)
	assert.Equal(t, 5, sample.Polls["shadeform"].N, "the poll counts every offer, with or without a GPU")
	assert.True(t, sample.Polls["aws"].OK)

	require.Len(t, sample.Rows, 4, "one row for each provider, region, way of selling and number of GPUs")
	assert.Equal(t, Row{
		Listing: Listing{Provider: "shadeform", Cloud: "lambda", Region: "newyork-usa-1", Geo: "us-east", GPUs: 1},
		Offers:  3, Kind: StockListed, Min: 1_000_000, Median: 1_200_000, Max: 1_400_000, Offer: "A10G_1x",
	}, rowOf(t, sample, "shadeform", 1, false))
	assert.Equal(t, int64(900_000), rowOf(t, sample, "shadeform", 4, false).Min, "a price is for one GPU")

	spotRow := rowOf(t, sample, "aws", 1, true)
	assert.Equal(t, StockUnknown, spotRow.Kind, "AWS gives a price and no word on stock")
	assert.Equal(t, int64(380_000), spotRow.Min)
	assert.Equal(t, "us-east-1b", spotRow.Zone)
	assert.Equal(t, "g5.xlarge", spotRow.Offer)
	assert.Equal(t, "us-east", spotRow.Geo)

	hosts := rowOf(t, sample, "vast", 1, false)
	assert.Equal(t, StockCounted, hosts.Kind)
	assert.Equal(t, uint32(2), hosts.Stock)
	assert.Equal(t, "us-central", hosts.Geo)

	// Everything a provider said is in its snapshot, the machines without a
	// GPU too.
	snapshots := market.snapshots("shadeform")
	require.Len(t, snapshots, 1)
	assert.Equal(t, SnapshotFull, snapshots[0].Kind)
	assert.Len(t, snapshots[0].Offers, 5)
	assert.Equal(t, "gw-1", snapshots[0].Sampler)
	assert.Equal(t, []string{"series/a10g/1m", "snapshots/aws", "snapshots/shadeform", "snapshots/vast"}, market.log.Names())
}

func TestEachStreamKeepsItsRecordsForAsLongAsTheConfigSays(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	market := newTestMarket(t, fake)
	market.config.SnapshotRetention = 48 * time.Hour
	sampler := market.sampler("gw-1")

	for minute := 0; minute <= 60; minute++ {
		market.sample(sampler, noon.Add(time.Duration(minute)*time.Minute))
	}

	assert.Equal(t, 48*time.Hour, market.log.Kept("snapshots/shadeform"))
	assert.Equal(t, 14*24*time.Hour, market.log.Kept("series/a10g/1m"), "the default")
	assert.Zero(t, market.log.Kept("series/a10g/1h"), "what the hours came to is kept for good")
}

func TestAProviderThatDoesNotAnswerIsRecordedAsNotAnswering(t *testing.T) {
	shadeform := providers.NewFake("shadeform", "test")
	shadeform.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	verda := &silent{Fake: providers.NewFake("verda", "test")}
	verda.SetOffers(a10g("1A10.8V", "FIN-01", 1, 800_000))
	market := newTestMarket(t, shadeform, verda)
	sampler := market.sampler("gw-1")
	market.sample(sampler, noon)

	verda.err = &providers.StatusError{Method: "GET", Path: "/v1/instance-types", Status: 503, Body: "upstream unavailable"}
	market.sample(sampler, noon.Add(time.Minute))

	samples := market.samples("A10G")
	require.Len(t, samples, 2)
	assert.Len(t, samples[0].Rows, 2)
	assert.Equal(t, Poll{OK: false, MS: samples[1].Polls["verda"].MS, Err: "503"}, samples[1].Polls["verda"])
	require.Len(t, samples[1].Rows, 1, "a provider that did not answer has no rows: nothing is known of what it has")
	assert.Equal(t, "shadeform", samples[1].Rows[0].Provider)

	snapshots := market.snapshots("verda")
	require.Len(t, snapshots, 2)
	assert.Equal(t, SnapshotError, snapshots[1].Kind)
	assert.Empty(t, snapshots[1].Offers)

	// What it last answered is still what the gateways know of it.
	offers, fresh := market.reader().Offers(context.Background(), "verda")
	assert.True(t, fresh)
	assert.Len(t, offers, 1)
}

func TestWhatSellsOutIsRecordedAsHavingNone(t *testing.T) {
	shadeform := providers.NewFake("shadeform", "test")
	shadeform.SetOffers(
		a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000),
		a10g("A10G_1x", "frankfurt-deu-1", 1, 1_100_000),
	)
	nebius := providers.NewFake("nebius", "test")
	nebius.SetOffers(a10g("gpu-a10/1gpu", "eu-north1", 1, 700_000, spot, stock(4)))
	market := newTestMarket(t, shadeform, nebius)
	sampler := market.sampler("gw-1")
	market.sample(sampler, noon)

	// New York sells out, and Nebius has nothing left at all.
	shadeform.SetOffers(a10g("A10G_1x", "frankfurt-deu-1", 1, 1_100_000))
	nebius.SetOffers()
	market.sample(sampler, noon.Add(time.Minute))

	samples := market.samples("A10G")
	require.Len(t, samples, 2)
	require.Len(t, samples[1].Rows, 3, "what was for sale is still a row, which says there is none")
	var soldOut []string
	for _, row := range samples[1].Rows {
		if !row.InStock() {
			soldOut = append(soldOut, row.Provider+" "+row.Region)
			assert.Zero(t, row.Min)
			assert.Zero(t, row.Stock)
		}
	}
	assert.Equal(t, []string{"nebius eu-north1", "shadeform newyork-usa-1"}, soldOut)
	assert.Equal(t, StockCounted, rowOf(t, samples[1], "nebius", 1, true).Kind, "Nebius counts what it has, and now counts none")
}

func TestOffersThatHaveNotChangedAreNotWrittenAgain(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	market := newTestMarket(t, fake)
	sampler := market.sampler("gw-1")

	market.sample(sampler, noon)
	market.sample(sampler, noon.Add(time.Minute))
	fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_050_000))
	market.sample(sampler, noon.Add(2*time.Minute))

	snapshots := market.snapshots("shadeform")
	require.Len(t, snapshots, 3)
	assert.Equal(t, []string{SnapshotFull, SnapshotSame, SnapshotFull}, []string{snapshots[0].Kind, snapshots[1].Kind, snapshots[2].Kind})
	assert.Equal(t, noon, snapshots[1].Since, "it says where the offers are written")
	assert.Equal(t, snapshots[0].Hash, snapshots[1].Hash)
	assert.NotEqual(t, snapshots[0].Hash, snapshots[2].Hash)
	assert.Empty(t, snapshots[1].Offers)

	// The series has a record for every sample all the same.
	assert.Len(t, market.samples("A10G"), 3)

	// A reader never goes further back than an hour for the offers.
	for minute := 3; minute <= 62; minute++ {
		market.sample(sampler, noon.Add(time.Duration(minute)*time.Minute))
	}
	snapshots = market.snapshots("shadeform")
	assert.Equal(t, SnapshotSame, snapshots[61].Kind)
	assert.Equal(t, SnapshotFull, snapshots[62].Kind, "an hour after they were last written in full")

	// A gateway that takes the sampling over has not seen what was written.
	market.sample(market.sampler("gw-2"), noon.Add(63*time.Minute))
	snapshots = market.snapshots("shadeform")
	assert.Equal(t, SnapshotFull, snapshots[63].Kind)
	assert.Equal(t, "gw-2", snapshots[63].Sampler)
}

func TestASampleIsTakenOnceWhicheverGatewayTakesIt(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	market := newTestMarket(t, fake)

	market.sample(market.sampler("gw-1"), noon)
	market.sample(market.sampler("gw-2"), noon)

	assert.Len(t, market.samples("A10G"), 1)
	snapshots := market.snapshots("shadeform")
	require.Len(t, snapshots, 1)
	assert.Equal(t, "gw-1", snapshots[0].Sampler)
}

func TestADryRunWritesNothingToTheHistory(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	market := newTestMarket(t, fake)
	market.config.DryRun = true

	market.sample(market.sampler("gw-1"), noon)

	assert.Empty(t, market.log.Names())
	offers, fresh := market.reader().Offers(context.Background(), "shadeform")
	assert.True(t, fresh, "what the providers sell now is published all the same")
	assert.Len(t, offers, 1)
}

func TestWithoutAHistoryWhatTheProvidersSellIsStillPublished(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000, cloud("lambda")))
	market := newTestMarket(t, fake)
	noHistory := func(opts *Options) { opts.Log = nil }

	market.sample(market.sampler("gw-1", noHistory), noon)

	reader := market.reader(func(opts *ReaderOptions) { opts.Log = nil })
	offers, fresh := reader.Offers(context.Background(), "shadeform")
	require.True(t, fresh)
	want := a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000, cloud("lambda"))
	want.Provider = "shadeform"
	assert.Equal(t, []providers.Offer{want}, offers, "the offers are as the provider gave them")

	// It stands for what the provider sells for a few intervals, and no
	// longer.
	market.clock.Advance(4 * time.Minute)
	_, fresh = reader.Offers(context.Background(), "shadeform")
	assert.False(t, fresh)

	_, err := reader.Series(context.Background(), SeriesRequest{GPU: "A10G"})
	assert.ErrorIs(t, err, ErrNoHistory)
	now, err := reader.Now(context.Background(), false)
	require.NoError(t, err)
	assert.False(t, now.History)
}

func TestAnHourOfSamplesComesToItsPricesAndHowLongItWasInStock(t *testing.T) {
	polls := map[string]Poll{"nebius": {OK: true, MS: 100}, "aws": {OK: true, MS: 300}}
	listing := Listing{Provider: "nebius", Cloud: "nebius", Region: "eu-north1", Geo: "eu-north", Spot: true, GPUs: 1}
	at := func(minute int, price int64, stock uint32) Sample {
		row := Row{Listing: listing, Kind: StockCounted}
		if stock > 0 {
			row.Offers, row.Stock, row.Min, row.Median, row.Max, row.Offer = 1, stock, price, price, price, "gpu-a10/1gpu"
		}
		return Sample{At: noon.Add(time.Duration(minute) * time.Minute), GPU: "A10G", Polls: polls, Rows: []Row{row}}
	}
	// A dollar for a minute, two dollars for two, sold out for one.
	samples := []Sample{at(0, 1_000_000, 4), at(1, 2_000_000, 2), at(2, 2_000_000, 6), at(3, 0, 0)}
	// Then the provider did not answer, which says nothing of its stock.
	samples = append(samples, Sample{
		At: noon.Add(4 * time.Minute), GPU: "A10G",
		Polls: map[string]Poll{"nebius": {OK: false, MS: 45_000, Err: "timeout"}, "aws": {OK: true, MS: 100}},
	})

	hour := RollUp("A10G", noon, samples)

	assert.Equal(t, noon, hour.At)
	assert.Equal(t, PollHour{Polls: 5, Answered: 4, MS: 100}, hour.Polls["nebius"])
	assert.Equal(t, PollHour{Polls: 5, Answered: 5, MS: 260}, hour.Polls["aws"])
	require.Len(t, hour.Rows, 1)
	assert.Equal(t, HourRow{
		Listing: listing, Kind: StockCounted,
		Open: 1_000_000, Close: 2_000_000, Min: 1_000_000, Max: 2_000_000,
		Mean:    1_666_667, // each price weighed by the minute it stood
		InStock: 0.75, MinStock: 0, MaxStock: 6, Answers: 4, Offer: "gpu-a10/1gpu",
	}, hour.Rows[0])

	// It is written, and read back, as it is.
	body, err := json.Marshal(hour)
	require.NoError(t, err)
	var back Hourly
	require.NoError(t, json.Unmarshal(body, &back))
	assert.Equal(t, hour, back)
}

func TestEveryHourThatEndsIsPutTogetherOnce(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	market := newTestMarket(t, fake)
	sampler := market.sampler("gw-1")

	// The price rises by a cent a minute.
	tick := func(sampler *Sampler, minute int) {
		fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000+int64(minute)*10_000))
		market.sample(sampler, noon.Add(time.Duration(minute)*time.Minute))
	}
	for minute := 0; minute < 60; minute++ {
		tick(sampler, minute)
	}
	assert.Empty(t, market.hours("A10G"), "the hour has not ended")

	tick(sampler, 60)
	hours := market.hours("A10G")
	require.Len(t, hours, 1)
	assert.Equal(t, noon, hours[0].At)
	require.Len(t, hours[0].Rows, 1)
	row := hours[0].Rows[0]
	assert.Equal(t, int64(1_000_000), row.Open)
	assert.Equal(t, int64(1_590_000), row.Close)
	assert.Equal(t, int64(1_295_000), row.Mean)
	assert.Equal(t, 1.0, row.InStock)
	assert.Equal(t, 60, row.Answers)
	assert.Equal(t, 60, hours[0].Polls["shadeform"].Polls)
	assert.Equal(t, 60, hours[0].Polls["shadeform"].Answered)

	// Another gateway takes over in the middle of the next hour. It does
	// not put the hour that was done together again, and does the next.
	successor := market.sampler("gw-2")
	for minute := 61; minute <= 120; minute++ {
		tick(successor, minute)
	}
	hours = market.hours("A10G")
	require.Len(t, hours, 2)
	assert.Equal(t, noon.Add(time.Hour), hours[1].At)
	assert.Equal(t, 60, hours[1].Rows[0].Answers)
}

func TestHoursMissedWhileNoGatewaySampledAreLeftOutAndTheLastIsPutTogether(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	market := newTestMarket(t, fake)
	sampler := market.sampler("gw-1")
	for minute := 0; minute <= 60; minute++ {
		market.sample(sampler, noon.Add(time.Duration(minute)*time.Minute))
	}
	require.Len(t, market.hours("A10G"), 1)

	// Nothing samples from 13:01 to 15:30: the hour of 13:00 has one
	// sample, and the hour of 14:00 none.
	successor := market.sampler("gw-2")
	for minute := 210; minute <= 240; minute++ {
		market.sample(successor, noon.Add(time.Duration(minute)*time.Minute))
	}

	hours := market.hours("A10G")
	var starts []time.Time
	for _, hour := range hours {
		starts = append(starts, hour.At)
	}
	assert.Equal(t, []time.Time{noon, noon.Add(time.Hour), noon.Add(3 * time.Hour)}, starts, "an hour with no samples has no record")
	assert.Equal(t, 1, hours[1].Rows[0].Answers)
	assert.Equal(t, 30, hours[2].Rows[0].Answers)
}

func TestOneGatewaySamplesAndAnotherTakesOverWhenItStops(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	market := newTestMarket(t, fake)
	market.config.Interval = 20 * time.Millisecond
	fast := func(opts *Options) { opts.Clock = nil }

	samplers := func() map[string]int {
		by := map[string]int{}
		for _, snapshot := range market.snapshots("shadeform") {
			by[snapshot.Sampler]++
		}
		return by
	}

	first, stopFirst := context.WithCancel(context.Background())
	leader := market.sampler("gw-1", fast)
	leader.standby = 20 * time.Millisecond
	leader.Start(first)
	require.Eventually(t, func() bool { return samplers()["gw-1"] >= 2 }, 5*time.Second, 5*time.Millisecond)

	second, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	standby := market.sampler("gw-2", fast)
	standby.standby = 20 * time.Millisecond
	standby.Start(second)
	time.Sleep(150 * time.Millisecond)
	assert.Zero(t, samplers()["gw-2"], "a gateway that does not hold the lock does not sample")

	stopFirst()
	require.Eventually(t, func() bool { return samplers()["gw-2"] >= 2 }, 5*time.Second, 5*time.Millisecond)

	// No time was sampled twice, and the gateway that took over wrote the
	// offers in full: it has not seen what the first wrote.
	times := map[time.Time]bool{}
	for _, snapshot := range market.snapshots("shadeform") {
		assert.False(t, times[snapshot.At], "the sample of %s was taken twice", snapshot.At)
		times[snapshot.At] = true
	}
	for _, snapshot := range market.snapshots("shadeform") {
		if snapshot.Sampler == "gw-2" {
			assert.Equal(t, SnapshotFull, snapshot.Kind)
			break
		}
	}
}

func TestAProviderThatHangsDoesNotHoldUpTheSample(t *testing.T) {
	shadeform := providers.NewFake("shadeform", "test")
	shadeform.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	release := make(chan struct{})
	defer close(release)
	hung := &hanging{Fake: providers.NewFake("verda", "test"), release: release}
	market := newTestMarket(t, shadeform, hung)
	sampler := market.sampler("gw-1")
	sampler.askFor, sampler.askGrace = 30*time.Millisecond, 30*time.Millisecond

	market.sample(sampler, noon)
	market.sample(sampler, noon.Add(time.Minute))

	samples := market.samples("A10G")
	require.Len(t, samples, 2)
	assert.Equal(t, "timeout", samples[0].Polls["verda"].Err)
	assert.False(t, samples[1].Polls["verda"].OK)
	assert.True(t, samples[1].Polls["shadeform"].OK)
	assert.Equal(t, 1, hung.asked(), "it is not asked again while it has yet to answer")
}

// hanging is a provider that does not answer what it sells until it is
// released, whatever its caller's deadline.
type hanging struct {
	*providers.Fake
	release chan struct{}
	calls   atomic.Int32
}

func (h *hanging) Offers(ctx context.Context, filter providers.OfferFilter) ([]providers.Offer, error) {
	h.calls.Add(1)
	<-h.release
	return nil, errors.New("released")
}

func (h *hanging) asked() int { return int(h.calls.Load()) }

func TestWhatBecameOfAMachineIsWrittenToTheHistory(t *testing.T) {
	market := newTestMarket(t)
	recorder := NewRecorder(market.log, StreamsOf(market.config), market.clock)
	offer := a10g("us-east-1b/g5.xlarge", "us-east-1", 1, 380_000, spot)
	offer.Provider, offer.InstanceType = "aws", "g5.xlarge"
	bought := OutcomeOf(offer)
	bought.Source = SourceReservation

	recorder.Record(bought.Bought(nil, 1200*time.Millisecond))
	market.clock.Advance(90 * time.Second)
	recorder.Record(bought.CameUp(90 * time.Second))
	market.clock.Advance(time.Hour)
	recorder.Record(bought.Ended(ResultInterrupted, time.Hour+90*time.Second))
	recorder.Flush(context.Background())

	outcomes := read[Outcome](t, market.log, "outcomes")
	require.Len(t, outcomes, 3)
	assert.Equal(t, Outcome{
		At: noon, Event: OutcomeCreate, Result: ResultOK, Source: SourceReservation,
		Provider: "aws", Cloud: "aws", Region: "us-east-1", Geo: "us-east", Offer: "g5.xlarge", Zone: "us-east-1b",
		Spot: true, GPU: "A10G", GPUs: 1, HourlyMicros: 380_000, Took: 1200 * time.Millisecond,
	}, outcomes[0])
	assert.Equal(t, OutcomeActive, outcomes[1].Event)
	assert.Equal(t, 90*time.Second, outcomes[1].Took)
	assert.Equal(t, noon.Add(90*time.Second), outcomes[1].At)
	assert.Equal(t, ResultInterrupted, outcomes[2].Result)
	assert.Zero(t, market.log.Kept("outcomes"), "outcomes are kept for good")
}

func TestARefusedPurchaseIsRecordedByWhyItWasRefused(t *testing.T) {
	bought := OutcomeOf(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	for result, err := range map[string]error{
		ResultOutOfStock: errors.Join(providers.ErrOutOfStock, errors.New("no capacity")),
		ResultQuota:      providers.ErrQuota,
		ResultUnanswered: &providers.StatusError{Status: 502},
		ResultError:      &providers.StatusError{Status: 400, Body: "bad image"},
	} {
		outcome := bought.Bought(err, time.Second)
		assert.Equal(t, result, outcome.Result)
		assert.NotEmpty(t, outcome.Err)
	}
}

func TestWithoutAHistoryNothingIsRecordedAndNothingFails(t *testing.T) {
	recorder := NewRecorder(nil, Streams{}, nil)
	assert.Nil(t, recorder)
	recorder.Start(context.Background())
	recorder.Record(Outcome{Event: OutcomeCreate})
	recorder.Flush(context.Background())
}
