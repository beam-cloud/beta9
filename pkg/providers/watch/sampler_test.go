package watch

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
