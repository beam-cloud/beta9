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

// withMargin is what a tenant pays in these tests: a tenth more than the
// provider charges.
func withMargin(opts *ReaderOptions) {
	opts.Bill = func(micros int64) int64 { return micros * 11 / 10 }
}

func jsonOf(t *testing.T, value any) string {
	t.Helper()
	body, err := json.Marshal(value)
	require.NoError(t, err)
	return string(body)
}

// point is a point of a series as a test expects it. A nil is a null.
func point(at time.Time, price, low, high, stock, inStock any, answered float64) []any {
	return []any{at.UnixMilli(), price, low, high, stock, inStock, answered}
}

func assertPoints(t *testing.T, want [][]any, series Series) {
	t.Helper()
	assert.JSONEq(t, jsonOf(t, want), jsonOf(t, series.Points), "the points of %s", series.Key)
}

func seriesOf(t *testing.T, answer *SeriesAnswer, key string) Series {
	t.Helper()
	for _, series := range answer.Series {
		if series.Key == key {
			return series
		}
	}
	keys := []string{}
	for _, series := range answer.Series {
		keys = append(keys, series.Key)
	}
	t.Fatalf("the answer has no series %q: it has %v", key, keys)
	return Series{}
}

// fourSellers is a market of a GPU sold four ways: on demand by a provider
// that lists what it has, by one that counts it, on spot by one that gives
// no word on its stock, and by one that tenants cannot reserve from.
func fourSellers(t *testing.T) *testMarket {
	t.Helper()
	shadeform := providers.NewFake("shadeform", "test")
	shadeform.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000, cloud("lambda")))
	aws := providers.NewFake("aws", "test")
	aws.SetOffers(a10g("us-east-1b/g5.xlarge", "us-east-1", 1, 380_000, spot))
	vast := providers.NewFake("vast", "test")
	vast.SetOffers(a10g("501/1", "US", 1, 300_000, stock(1)), a10g("502/1", "US", 1, 350_000, stock(1)))
	verda := providers.NewFake("verda", "test")
	verda.SetOffers(a10g("1A10.8V", "FIN-01", 1, 100_000))
	market := newTestMarket(t, shadeform, aws, vast, verda)
	market.sample(market.sampler("gw-1"), noon)
	return market
}

func notVerda(opts *ReaderOptions) {
	opts.Sells = func(provider string) bool { return provider != "verda" }
}

func TestATenantSeesWhatItWouldPayAndNotWhoSellsIt(t *testing.T) {
	market := fourSellers(t)

	now, err := market.reader(withMargin, notVerda).Now(context.Background(), false)
	require.NoError(t, err)

	require.NotNil(t, now.AsOf)
	assert.Equal(t, noon, *now.AsOf)
	assert.Equal(t, 60, now.IntervalSeconds)
	assert.True(t, now.History)
	assert.JSONEq(t, `[{"gpu":"A10G","counts":[1],"geos":["us-central","us-east"],"pricing":["on_demand","spot"]}]`, jsonOf(t, now.GPUs))
	assert.JSONEq(t, `[
		{"gpu":"A10G","gpus":1,"geo":"us-central","pricing":"on_demand","price":330000,"in_stock":true,"stock":2},
		{"gpu":"A10G","gpus":1,"geo":"us-east","pricing":"on_demand","price":1100000,"in_stock":true,"stock":null},
		{"gpu":"A10G","gpus":1,"geo":"us-east","pricing":"spot","price":418000,"in_stock":null,"stock":null}
	]`, jsonOf(t, now.Rows), "what a provider tenants cannot reserve from sells is left out")

	said := jsonOf(t, now)
	for _, name := range []string{"shadeform", "aws", "vast", "verda", "lambda", "g5.xlarge", "us-east-1", "newyork"} {
		assert.NotContains(t, said, name, "a tenant is not told who sells what")
	}
}

func TestAnAdminSeesEveryProvidersListingsAndWhatTheyCost(t *testing.T) {
	market := fourSellers(t)

	now, err := market.reader(withMargin, notVerda).Now(context.Background(), true)
	require.NoError(t, err)

	require.Len(t, now.Rows, 4)
	assert.JSONEq(t, `{
		"gpu":"A10G","gpus":1,"geo":"us-east","pricing":"spot","price":418000,"in_stock":null,"stock":null,
		"provider":"aws","cloud":"aws","region":"us-east-1","offer":"g5.xlarge","zone":"us-east-1b",
		"cost":380000,"offers":1,"stock_kind":"none"
	}`, jsonOf(t, now.Rows[3]))
	assert.Equal(t, "verda", now.Rows[0].Provider, "the cheapest first, whether or not tenants can reserve it")
	assert.Equal(t, int64(100_000), *now.Rows[0].Cost)

	require.Len(t, now.Providers, 4)
	by := map[string]ProviderNow{}
	for _, provider := range now.Providers {
		by[provider.Provider] = provider
	}
	assert.True(t, by["vast"].OK)
	assert.Equal(t, 2, by["vast"].Offers)
	assert.Equal(t, StockCounted, by["vast"].StockKind)
	assert.True(t, by["vast"].Sells)
	assert.False(t, by["verda"].Sells)
	assert.Equal(t, noon, *by["aws"].AskedAt)
	assert.Equal(t, []string{"aws", "shadeform", "vast", "verda"}, now.GPUs[0].Providers)
}

func TestWhatHasSoldOutIsStillShownAsSoldOut(t *testing.T) {
	shadeform := providers.NewFake("shadeform", "test")
	shadeform.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	market := newTestMarket(t, shadeform)
	sampler := market.sampler("gw-1")
	market.sample(sampler, noon)
	shadeform.SetOffers()
	market.sample(sampler, noon.Add(time.Minute))

	now, err := market.reader().Now(context.Background(), false)
	require.NoError(t, err)

	assert.JSONEq(t, `[{"gpu":"A10G","gpus":1,"geo":"us-east","pricing":"on_demand","price":null,"in_stock":false,"stock":null}]`, jsonOf(t, now.Rows))
	assert.Len(t, now.GPUs, 1, "it can still be chosen, to see what it sold for")
}

func TestASeriesIsDrawnFromTheSamples(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	market := newTestMarket(t, fake)
	sampler := market.sampler("gw-1")
	// The price rises by a cent a minute, for ten minutes.
	for minute := 0; minute < 10; minute++ {
		fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000+int64(minute)*10_000))
		market.sample(sampler, noon.Add(time.Duration(minute)*time.Minute))
	}
	market.clock.Advance(time.Minute)

	// A short time has a point for every sample.
	answer, err := market.reader(withMargin).Series(context.Background(), SeriesRequest{GPU: "A10G", Start: noon})
	require.NoError(t, err)
	assert.Equal(t, TierMinute, answer.Tier)
	assert.Equal(t, 60, answer.StepSeconds)
	assert.Equal(t, PointColumns, answer.Cols)
	require.Len(t, answer.Series, 1)
	series := answer.Series[0]
	assert.Equal(t, "on_demand", series.Key)
	assert.Equal(t, "on_demand", series.Pricing)
	require.Len(t, series.Points, 10)
	assert.JSONEq(t, jsonOf(t, point(noon, 1_100_000, 1_100_000, 1_100_000, nil, 1, 1)), jsonOf(t, series.Points[0]))
	assert.JSONEq(t, jsonOf(t, point(noon.Add(9*time.Minute), 1_199_000, 1_199_000, 1_199_000, nil, 1, 1)), jsonOf(t, series.Points[9]))
	assert.Nil(t, answer.Providers, "how the providers answered is for admins")

	// A day has one for every five minutes: where the price ended, and how
	// low and how high it was on the way.
	answer, err = market.reader(withMargin).Series(context.Background(), SeriesRequest{GPU: "A10G"})
	require.NoError(t, err)
	assert.Equal(t, 300, answer.StepSeconds)
	assertPoints(t, [][]any{
		point(noon, 1_144_000, 1_100_000, 1_144_000, nil, 1, 1),
		point(noon.Add(5*time.Minute), 1_199_000, 1_155_000, 1_199_000, nil, 1, 1),
	}, answer.Series[0])
}

func TestAProviderThatDidNotAnswerLeavesAGapAndNotASellOut(t *testing.T) {
	verda := &silent{Fake: providers.NewFake("verda", "test")}
	verda.SetOffers(a10g("1A10.8V", "FIN-01", 1, 800_000))
	market := newTestMarket(t, verda)
	sampler := market.sampler("gw-1")
	market.sample(sampler, noon)

	verda.err = &providers.StatusError{Status: 503}
	market.sample(sampler, noon.Add(time.Minute))

	verda.err = nil
	verda.SetOffers()
	market.sample(sampler, noon.Add(2*time.Minute))

	verda.SetOffers(a10g("1A10.8V", "FIN-01", 1, 850_000))
	market.sample(sampler, noon.Add(3*time.Minute))
	market.clock.Advance(time.Minute)

	answer, err := market.reader().Series(context.Background(), SeriesRequest{GPU: "A10G", Start: noon})
	require.NoError(t, err)

	assertPoints(t, [][]any{
		point(noon, 800_000, 800_000, 800_000, nil, 1, 1),
		// It did not answer: nothing is known, of its price or its stock.
		point(noon.Add(time.Minute), nil, nil, nil, nil, nil, 0),
		// It answered, and had none.
		point(noon.Add(2*time.Minute), nil, nil, nil, nil, 0, 1),
		point(noon.Add(3*time.Minute), 850_000, 850_000, 850_000, nil, 1, 1),
	}, seriesOf(t, answer, "on_demand"))
}

func TestAProviderThatGivesNoWordOnItsStockIsNotSaidToHaveAny(t *testing.T) {
	aws := providers.NewFake("aws", "test")
	aws.SetOffers(a10g("us-east-1b/g5.xlarge", "us-east-1", 1, 380_000, spot))
	nebius := providers.NewFake("nebius", "test")
	nebius.SetOffers(a10g("gpu-a10/1gpu", "eu-north1", 1, 700_000, spot, stock(4)))
	market := newTestMarket(t, aws, nebius)
	market.sample(market.sampler("gw-1"), noon)
	market.clock.Advance(time.Minute)

	answer, err := market.reader().Series(context.Background(), SeriesRequest{GPU: "A10G", Start: noon, Group: []string{ByGeo}})
	require.NoError(t, err)

	assertPoints(t, [][]any{point(noon, 380_000, 380_000, 380_000, nil, nil, 1)}, seriesOf(t, answer, "us-east"))
	assertPoints(t, [][]any{point(noon, 700_000, 700_000, 700_000, 4, 1, 1)}, seriesOf(t, answer, "eu-north"))
	assert.Equal(t, "us-east", answer.Series[0].Key, "the cheapest first")
}

func TestALongTimeIsDrawnFromWhatItsHoursCameTo(t *testing.T) {
	fake := providers.NewFake("shadeform", "test")
	market := newTestMarket(t, fake)
	sampler := market.sampler("gw-1")
	// A dollar in the first hour, and ten cents more in each hour after it,
	// with a dip of a cent in the middle of every hour.
	for minute := 0; minute <= 155; minute++ {
		price := 1_000_000 + int64(minute/60)*100_000
		if minute%60 == 30 {
			price -= 10_000
		}
		fake.SetOffers(a10g("A10G_1x", "newyork-usa-1", 1, price))
		market.sample(sampler, noon.Add(time.Duration(minute)*time.Minute))
	}

	answer, err := market.reader().Series(context.Background(), SeriesRequest{GPU: "A10G", Start: noon.Add(-10 * 24 * time.Hour)})
	require.NoError(t, err)

	assert.Equal(t, TierHour, answer.Tier)
	assert.Equal(t, 3600, answer.StepSeconds)
	assertPoints(t, [][]any{
		point(noon, 1_000_000, 990_000, 1_000_000, nil, 1, 1),
		point(noon.Add(time.Hour), 1_100_000, 1_090_000, 1_100_000, nil, 1, 1),
		// The hour that has not ended, from the samples it has so far.
		point(noon.Add(2*time.Hour), 1_200_000, 1_190_000, 1_200_000, nil, 1, 1),
	}, seriesOf(t, answer, "on_demand"))

	// A longer time still has a point for several hours.
	answer, err = market.reader().Series(context.Background(), SeriesRequest{GPU: "A10G", Start: noon.Add(-60 * 24 * time.Hour)})
	require.NoError(t, err)
	assert.Equal(t, 3*3600, answer.StepSeconds)
	assertPoints(t, [][]any{point(noon, 1_200_000, 990_000, 1_200_000, nil, 1, 1)}, seriesOf(t, answer, "on_demand"))
}

func TestSeriesAreToldApartByWhatTheQuestionGroupsBy(t *testing.T) {
	market := fourSellers(t)
	market.clock.Advance(time.Minute)
	ask := func(q SeriesRequest) []string {
		q.GPU, q.Start = "A10G", noon
		answer, err := market.reader(notVerda).Series(context.Background(), q)
		require.NoError(t, err)
		keys := []string{}
		for _, series := range answer.Series {
			keys = append(keys, series.Key)
		}
		return keys
	}

	assert.Equal(t, []string{"on_demand", "spot"}, ask(SeriesRequest{}), "by how the machine is sold, unless the question says")
	assert.Equal(t, []string{"us-central|on_demand", "us-east|spot", "us-east|on_demand"}, ask(SeriesRequest{Group: []string{ByGeo, ByPricing}}))
	assert.Equal(t, []string{"all"}, ask(SeriesRequest{Group: []string{}}))
	assert.Equal(t, []string{"spot"}, ask(SeriesRequest{Pricing: PricingSpot}))
	assert.Equal(t, []string{"on_demand"}, ask(SeriesRequest{Geos: []string{"us-central"}}))
	assert.Empty(t, ask(SeriesRequest{Counts: []uint32{8}}))
	assert.Empty(t, ask(SeriesRequest{Geos: []string{"eu-north"}}), "what tenants cannot reserve is not in a tenant's chart")
}

func TestOnlyAnAdminMayAskWhoSells(t *testing.T) {
	market := fourSellers(t)
	market.clock.Advance(time.Minute)
	reader := market.reader(withMargin, notVerda)

	for _, q := range []SeriesRequest{
		{Group: []string{ByProvider}},
		{Group: []string{ByCloud}},
		{Group: []string{ByRegion}},
		{Providers: []string{"aws"}},
		{Clouds: []string{"lambda"}},
		{Regions: []string{"us-east-1"}},
		{Raw: true},
	} {
		q.GPU, q.Start = "A10G", noon
		_, err := reader.Series(context.Background(), q)
		assert.ErrorIs(t, err, ErrAdminOnly, "%+v", q)
	}

	answer, err := reader.Series(context.Background(), SeriesRequest{GPU: "A10G", Start: noon, Admin: true, Group: []string{ByProvider}, Raw: true})
	require.NoError(t, err)
	assert.True(t, answer.Raw)
	require.Len(t, answer.Series, 4, "an admin sees the providers that tenants cannot reserve from too")
	assert.Equal(t, "verda", answer.Series[0].Provider)
	assertPoints(t, [][]any{point(noon, 100_000, 100_000, 100_000, nil, 1, 1)}, answer.Series[0])
	assert.Equal(t, 1, answer.Providers["vast"].Answered)

	// What tenants pay, unless the admin asks for what it costs.
	answer, err = reader.Series(context.Background(), SeriesRequest{GPU: "A10G", Start: noon, Admin: true, Providers: []string{"vast"}})
	require.NoError(t, err)
	assertPoints(t, [][]any{point(noon, 330_000, 330_000, 330_000, 2, 1, 1)}, seriesOf(t, answer, "on_demand"))
}

func TestAQuestionThatCannotBeAnsweredSaysWhatIsWrongWithIt(t *testing.T) {
	market := fourSellers(t)
	reader := market.reader()

	for what, q := range map[string]SeriesRequest{
		"gpu is required": {},
		`pricing must be on_demand, spot or any, not "cheap"`:   {GPU: "A10G", Pricing: "cheap"},
		`group must be of pricing, geo and gpus, not "weather"`: {GPU: "A10G", Group: []string{"weather"}},
		"end must be after start":                               {GPU: "A10G", Start: noon, End: noon.Add(-time.Hour)},
		"the time asked about must be two years or less":        {GPU: "A10G", Start: noon.Add(-3 * 366 * 24 * time.Hour)},
	} {
		_, err := reader.Series(context.Background(), q)
		var wrong *RequestError
		require.ErrorAs(t, err, &wrong, what)
		assert.Equal(t, what, wrong.What)
	}
}

func TestAnAnswerIsGivenAgainForAShortWhile(t *testing.T) {
	market := fourSellers(t)
	market.clock.Advance(time.Minute)
	reader := market.reader()
	ask := func() *SeriesAnswer {
		answer, err := reader.Series(context.Background(), SeriesRequest{GPU: "A10G", Start: noon})
		require.NoError(t, err)
		return answer
	}

	first := ask()
	market.clock.Advance(10 * time.Second)
	assert.Same(t, first, ask())

	market.clock.Advance(time.Minute)
	assert.NotSame(t, first, ask())
}
