package watch

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// The last price of a listing is what it cost when it was last in stock,
// which is not to say that it could be had for that when the hour ended.
// The hour says whether it could.
func TestAnHourSaysWhetherAListingWasStillInStockWhenItEnded(t *testing.T) {
	answered := map[string]Poll{"vast": {OK: true, MS: 100}}
	unanswered := map[string]Poll{"vast": {OK: false, MS: 45_000, Err: "timeout"}}
	listing := Listing{Provider: "vast", Cloud: "vast", Region: "US", Geo: "us", GPUs: 1}
	inStock := Row{Listing: listing, Kind: StockCounted, Offers: 1, Stock: 1, Min: 300_000, Median: 300_000, Max: 300_000, Offer: "501/1"}
	soldOut := Row{Listing: listing, Kind: StockCounted}
	sample := func(minute int, polls map[string]Poll, rows ...Row) Sample {
		return Sample{At: noon.Add(time.Duration(minute) * time.Minute), GPU: "A10G", Polls: polls, Rows: rows}
	}

	tests := []struct {
		name    string
		samples []Sample
		atClose bool
	}{
		{"in stock to the end", []Sample{sample(0, answered, inStock), sample(1, answered, inStock)}, true},
		{"sold out before the end", []Sample{sample(0, answered, inStock), sample(1, answered, soldOut)}, false},
		{"no longer in what its provider answered", []Sample{sample(0, answered, inStock), sample(1, answered)}, false},
		{"back in stock by the end", []Sample{sample(0, answered, soldOut), sample(1, answered, inStock)}, true},
		{
			// Nothing is known of it after its provider last answered.
			"in stock when its provider last answered",
			[]Sample{sample(0, answered, inStock), sample(1, unanswered)}, true,
		},
		{
			"sold out when its provider last answered",
			[]Sample{sample(0, answered, inStock), sample(1, answered, soldOut), sample(2, unanswered)}, false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hour := RollUp("A10G", noon, test.samples)
			require.Len(t, hour.Rows, 1)
			assert.Equal(t, test.atClose, hour.Rows[0].InStockAtClose)
			assert.Equal(t, int64(300_000), hour.Rows[0].Close, "the price it last had")

			body, err := json.Marshal(hour)
			require.NoError(t, err)
			var back Hourly
			require.NoError(t, json.Unmarshal(body, &back))
			assert.Equal(t, hour, back, "it is written, and read back, as it is")
		})
	}
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

// startsOf is when each of the hours began.
func startsOf(hours []Hourly) []time.Time {
	var starts []time.Time
	for _, hour := range hours {
		starts = append(starts, hour.At)
	}
	return starts
}

// rolledUp is the hour of a GPU type that was last put together.
func (m *testMarket) rolledUp(gpu string) time.Time {
	m.t.Helper()
	done, err := NewShared(m.redis).RolledUp(context.Background())
	require.NoError(m.t, err)
	return done[gpu]
}

// A gateway that takes the sampling over after more than a day has more
// hours to put together than it does at once. It starts with the oldest and
// goes on with the next sample, so that no hour that has samples is passed
// over.
func TestHoursLeftForLongerThanADayAreStillPutTogether(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	market := newTestMarket(t, fake)
	sampler := market.sampler("gw-1")
	// The hour of 12:00 is put together at 13:00. The gateway stops at
	// 13:59, before the hour of 13:00 is.
	for minute := 0; minute < 120; minute++ {
		market.sample(sampler, noon.Add(time.Duration(minute)*time.Minute))
	}
	require.Equal(t, []time.Time{noon}, startsOf(market.hours("A10G")))

	// Another takes over 40 hours later.
	successor := market.sampler("gw-2")
	resumed := noon.Add(42 * time.Hour)
	market.sample(successor, resumed)
	hours := market.hours("A10G")
	require.Equal(t, []time.Time{noon, noon.Add(time.Hour)}, startsOf(hours), "the hour that was left is the first to be put together")
	assert.Equal(t, 60, hours[1].Rows[0].Answers)
	assert.Equal(t, noon.Add(rollUpAtMost*time.Hour), market.rolledUp("A10G"), "a day of hours is gone through at once")

	// The rest is gone through with the next sample, and the hours go on
	// from there.
	market.sample(successor, resumed.Add(time.Minute))
	assert.Equal(t, resumed.Add(-time.Hour), market.rolledUp("A10G"))
	for minute := 2; minute <= 60; minute++ {
		market.sample(successor, resumed.Add(time.Duration(minute)*time.Minute))
	}
	assert.Equal(t, []time.Time{noon, noon.Add(time.Hour), resumed}, startsOf(market.hours("A10G")), "an hour with no samples has no record")
}

// A GPU type has no hour put together until its first hour ends. A gateway
// that stops before then leaves that hour to whichever samples next, which
// has to know that the GPU type began there.
func TestTheFirstHourOfAGPUTypeIsPutTogetherHoweverLateTheNextSampleIs(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	a10 := a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000)
	l4 := a10g("L4_1x", "newyork-usa-1", 1, 600_000, func(offer *providers.Offer) { offer.GPU = "L4" })
	fake.SetOffers(a10)
	market := newTestMarket(t, fake)
	sampler := market.sampler("gw-1")
	// The provider starts to sell a second GPU type at 12:30, and the
	// gateway stops at 12:59.
	for minute := 0; minute < 60; minute++ {
		if minute == 30 {
			fake.SetOffers(a10, l4)
		}
		market.sample(sampler, noon.Add(time.Duration(minute)*time.Minute))
	}
	require.Empty(t, market.hours("L4"), "the hour has not ended")

	successor := market.sampler("gw-2")
	market.sample(successor, noon.Add(3*time.Hour+10*time.Minute))
	for _, test := range []struct {
		gpu     string
		answers int
	}{{"A10G", 60}, {"L4", 30}} {
		hours := market.hours(test.gpu)
		require.Equal(t, []time.Time{noon}, startsOf(hours), test.gpu)
		assert.Equal(t, test.answers, hours[0].Rows[0].Answers, test.gpu)
	}
}

// Samples are kept for a time. An hour that is older has none left to be
// put together from, and is not looked for.
func TestHoursWhoseSamplesAreNoLongerKeptAreNotLookedFor(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	market := newTestMarket(t, fake)
	market.config.SeriesRetention = 36 * time.Hour
	sampler := market.sampler("gw-1")
	for minute := 0; minute < 120; minute++ {
		market.sample(sampler, noon.Add(time.Duration(minute)*time.Minute))
	}

	// The history of the test forgets nothing, and still has the samples of
	// 13:00. S2 has dropped them by now.
	successor := market.sampler("gw-2")
	resumed := noon.Add(72 * time.Hour)
	market.sample(successor, resumed)
	assert.Equal(t, resumed.Add(-13*time.Hour), market.rolledUp("A10G"), "a day of hours from the oldest that is kept")
	market.sample(successor, resumed.Add(time.Minute))
	assert.Equal(t, resumed.Add(-time.Hour), market.rolledUp("A10G"))
	assert.Equal(t, []time.Time{noon}, startsOf(market.hours("A10G")))
}
