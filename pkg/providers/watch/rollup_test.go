package watch

import (
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
