package market

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	// minuteSpan is the longest time a chart is drawn from the samples as
	// they were taken. A longer one is drawn from what the hours came to.
	minuteSpan = 48 * time.Hour
	// longestSpan is the longest time a chart can be asked for.
	longestSpan = 2 * 366 * 24 * time.Hour
	// The most points a series is drawn with, from samples and from hours.
	minutePoints = 400
	hourPoints   = 750
	// seriesAtMost is the most series an answer holds: the cheapest.
	seriesAtMost = 60

	// How much of the history is read at once, and how many reads at a time.
	minuteChunk     = 6 * time.Hour
	hourChunk       = 30 * 24 * time.Hour
	readConcurrency = 6
)

var (
	minuteSteps = []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, 30 * time.Minute}
	hourSteps   = []time.Duration{time.Hour, 3 * time.Hour, 6 * time.Hour, 12 * time.Hour, 24 * time.Hour}
)

// What a chart's series can be told apart by.
const (
	ByPricing  = "pricing"
	ByGeo      = "geo"
	ByGPUs     = "gpus"
	ByProvider = "provider"
	ByCloud    = "cloud"
	ByRegion   = "region"
)

// SeriesRequest asks what a GPU type sold for over a time.
type SeriesRequest struct {
	GPU string
	// What to leave out. Empty means nothing.
	Counts  []uint32
	Geos    []string
	Pricing string
	// Group is what the series are told apart by. Nil means by how the
	// machine is sold.
	Group []string
	// Start and End are the time asked about. Zero means the last day.
	Start, End time.Time

	// Admin says that a cluster admin asks. The rest is for admins only.
	Admin     bool
	Providers []string
	Clouds    []string
	Regions   []string
	// Raw asks for what the providers charge, and not what a tenant pays.
	Raw bool
}

// SeriesAnswer is a chart of what a GPU type sold for.
type SeriesAnswer struct {
	GPU   string    `json:"gpu"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	// Tier is what the chart was drawn from: the samples, or the hours.
	Tier        Tier `json:"tier"`
	StepSeconds int  `json:"step_seconds"`
	// Raw says that prices are the providers', and not what a tenant pays.
	Raw bool `json:"raw,omitempty"`
	// Cols names what each point holds.
	Cols   []string `json:"cols"`
	Series []Series `json:"series"`
	// Truncated says that there were more series than an answer holds.
	Truncated bool `json:"truncated,omitempty"`
	// Providers is how every provider answered over the time. Admins only.
	Providers map[string]PollHour `json:"providers,omitempty"`
}

// PointColumns is what each point of a series holds, in order:
//
//   - ts: when the step began, in milliseconds.
//   - price: the cheapest price at the end of the step, for one GPU for an
//     hour, in millionths of a dollar. Null when none was for sale.
//   - low, high: the lowest and the highest that the cheapest price was
//     during the step.
//   - stock: the most machines that could be bought during the step. Null
//     where no provider counts them.
//   - in_stock: the share of the step for which a machine could be had.
//     Null where the providers give a price and no word on stock.
//   - answered: the share of the times they were asked that the providers
//     answered. A step with none is a gap in the record, not a sell-out.
var PointColumns = []string{"ts", "price", "low", "high", "stock", "in_stock", "answered"}

// Series is one line of a chart, and what it is of.
type Series struct {
	Key      string  `json:"key"`
	Pricing  string  `json:"pricing,omitempty"`
	Geo      string  `json:"geo,omitempty"`
	GPUs     uint32  `json:"gpus,omitempty"`
	Provider string  `json:"provider,omitempty"`
	Cloud    string  `json:"cloud,omitempty"`
	Region   string  `json:"region,omitempty"`
	Points   [][]any `json:"points"`

	last *int64
}

func (q *SeriesRequest) normalize(now time.Time) error {
	q.GPU = strings.TrimSpace(q.GPU)
	if q.GPU == "" {
		return badRequest("gpu is required")
	}
	switch q.Pricing {
	case "", PricingAny:
		q.Pricing = PricingAny
	case PricingOnDemand, PricingSpot:
	default:
		return badRequest("pricing must be on_demand, spot or any, not %q", q.Pricing)
	}
	if q.Group == nil {
		q.Group = []string{ByPricing}
	}
	for _, by := range q.Group {
		switch by {
		case ByPricing, ByGeo, ByGPUs:
		case ByProvider, ByCloud, ByRegion:
			if !q.Admin {
				return ErrAdminOnly
			}
		default:
			return badRequest("group must be of pricing, geo and gpus, not %q", by)
		}
	}
	if !q.Admin && (len(q.Providers) > 0 || len(q.Clouds) > 0 || len(q.Regions) > 0 || q.Raw) {
		return ErrAdminOnly
	}
	if q.End.IsZero() {
		q.End = now
	}
	if q.Start.IsZero() {
		q.Start = q.End.Add(-24 * time.Hour)
	}
	q.Start, q.End = q.Start.UTC(), q.End.UTC()
	if q.End.After(now) {
		q.End = now.UTC()
	}
	switch span := q.End.Sub(q.Start); {
	case span <= 0:
		return badRequest("end must be after start")
	case span > longestSpan:
		return badRequest("the time asked about must be two years or less")
	}
	return nil
}

func (q SeriesRequest) cacheKey(step time.Duration) string {
	return fmt.Sprintf("series:%s|%v|%v|%s|%v|%d|%d|%t|%v|%v|%v|%t",
		q.GPU, q.Counts, q.Geos, q.Pricing, q.Group,
		q.Start.Truncate(step).Unix(), q.End.Truncate(max(step, time.Minute)).Unix(),
		q.Admin, q.Providers, q.Clouds, q.Regions, q.Raw)
}

// wants reports whether a listing is one the question asks about.
func (q SeriesRequest) wants(listing Listing) bool {
	in := func(list []string, value string) bool { return len(list) == 0 || slices.Contains(list, value) }
	switch {
	case len(q.Counts) > 0 && !slices.Contains(q.Counts, listing.GPUs),
		q.Pricing != PricingAny && q.Pricing != pricingOf(listing.Spot),
		!in(q.Geos, listing.Geo), !in(q.Providers, listing.Provider),
		!in(q.Clouds, listing.Cloud), !in(q.Regions, listing.Region):
		return false
	}
	return true
}

// seriesOf is the series a listing belongs to: what the series are told
// apart by, and nothing else of the listing.
func (q SeriesRequest) seriesOf(listing Listing) Series {
	var series Series
	parts := make([]string, 0, len(q.Group))
	for _, by := range q.Group {
		switch by {
		case ByPricing:
			series.Pricing = pricingOf(listing.Spot)
			parts = append(parts, series.Pricing)
		case ByGeo:
			series.Geo = listing.Geo
			parts = append(parts, series.Geo)
		case ByGPUs:
			series.GPUs = listing.GPUs
			parts = append(parts, strconv.FormatUint(uint64(series.GPUs), 10))
		case ByProvider:
			series.Provider = listing.Provider
			parts = append(parts, series.Provider)
		case ByCloud:
			series.Cloud = listing.Cloud
			parts = append(parts, series.Cloud)
		case ByRegion:
			series.Region = listing.Region
			parts = append(parts, series.Region)
		}
	}
	series.Key = cmp.Or(strings.Join(parts, "|"), "all")
	return series
}

// tierOf chooses what a chart of the time asked about is drawn from, and how
// far apart its points are: the samples for a time that is short and recent
// enough to have them, and the hours beyond that.
func (r *Reader) tierOf(q SeriesRequest, now time.Time) (Tier, time.Duration) {
	span := q.End.Sub(q.Start)
	kept := r.streams.SamplesFor
	if span <= minuteSpan && (kept == 0 || q.Start.After(now.Add(-kept).Add(time.Hour))) {
		for _, step := range minuteSteps {
			if step >= r.interval && span <= minutePoints*step {
				return TierMinute, step
			}
		}
		return TierMinute, max(r.interval, minuteSteps[len(minuteSteps)-1])
	}
	for _, step := range hourSteps {
		if span <= hourPoints*step {
			return TierHour, step
		}
	}
	return TierHour, hourSteps[len(hourSteps)-1]
}

// Series answers what a GPU type sold for over a time.
func (r *Reader) Series(ctx context.Context, q SeriesRequest) (*SeriesAnswer, error) {
	if r.log == nil {
		return nil, ErrNoHistory
	}
	now := r.clock.Now()
	if err := q.normalize(now); err != nil {
		return nil, err
	}
	tier, step := r.tierOf(q, now)
	return cache(r, q.cacheKey(step), seriesCachedFor, func() (*SeriesAnswer, error) {
		return r.series(ctx, q, tier, step, now)
	})
}

// seen is what a series comes to at one time of the record: a sample, or an
// hour of them.
type seen struct {
	at               time.Time
	price, low, high int64
	stock            int64
	counted          bool
	// inStock is the share of the time for which a machine could be had,
	// and known whether the record says.
	inStock float64
	known   bool
	// asked is how often the providers of the series were asked, and
	// answered how often they answered.
	asked, answered int
}

// line is a series as it is read from the record, before its points are
// drawn.
type line struct {
	series    Series
	providers map[string]bool
	seen      []seen
}

func (r *Reader) series(ctx context.Context, q SeriesRequest, tier Tier, step time.Duration, now time.Time) (*SeriesAnswer, error) {
	out := &SeriesAnswer{
		GPU: q.GPU, Start: q.Start, End: q.End, Tier: tier, StepSeconds: int(step.Seconds()),
		Raw: q.Raw, Cols: PointColumns, Series: []Series{},
	}
	wants := func(listing Listing) bool {
		return q.wants(listing) && (q.Admin || r.sells(listing.Provider))
	}

	var lines map[string]*line
	var polls map[string]PollHour
	var err error
	// The first step holds the start asked for.
	from := q.Start.Truncate(step)
	switch tier {
	case TierMinute:
		var samples []Sample
		if samples, err = r.samples(ctx, q.GPU, from, q.End); err == nil {
			lines, polls = linesOfSamples(q, wants, samples)
		}
	default:
		var hours []Hourly
		if hours, err = r.hours(ctx, q.GPU, from, q.End, now); err == nil {
			lines, polls = linesOfHours(q, wants, hours)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("read the market's history of %s: %w", q.GPU, err)
	}
	if q.Admin {
		out.Providers = polls
	}

	price := r.bill
	if q.Raw {
		price = func(micros int64) int64 { return micros }
	}
	for _, line := range lines {
		series := line.series
		series.Points, series.last = pointsOf(line.seen, step, price)
		if len(series.Points) > 0 {
			out.Series = append(out.Series, series)
		}
	}
	// The cheapest first: they are what an answer keeps when there are too
	// many, and what a chart puts first.
	slices.SortFunc(out.Series, func(a, b Series) int {
		return cmp.Or(comparePrices(a.last, b.last), cmp.Compare(a.Key, b.Key))
	})
	if len(out.Series) > seriesAtMost {
		out.Series, out.Truncated = out.Series[:seriesAtMost], true
	}
	return out, nil
}

// samples reads the samples of a time, several parts of it at once.
func (r *Reader) samples(ctx context.Context, gpu string, from, to time.Time) ([]Sample, error) {
	stream := r.streams.Series(gpu, TierMinute)
	return readParts(ctx, from, to, minuteChunk, func(ctx context.Context, from, to time.Time) ([]Sample, error) {
		return readSamples(ctx, r.log, stream, gpu, from, to)
	})
}

// hours reads what the hours of a time came to. The hour that has not ended
// and the one that may not have been put together yet are worked out from
// their samples.
func (r *Reader) hours(ctx context.Context, gpu string, from, to, now time.Time) ([]Hourly, error) {
	stream := r.streams.Series(gpu, TierHour)
	from = from.Truncate(time.Hour)
	hours, err := readParts(ctx, from, to, hourChunk, func(ctx context.Context, from, to time.Time) ([]Hourly, error) {
		return readHours(ctx, r.log, stream, gpu, from, to)
	})
	if err != nil {
		return nil, err
	}
	have := map[int64]bool{}
	for _, hour := range hours {
		have[hour.At.Unix()] = true
	}
	current := now.UTC().Truncate(time.Hour)
	for _, hour := range []time.Time{current.Add(-time.Hour), current} {
		if have[hour.Unix()] || hour.Before(from) || !hour.Before(to) {
			continue
		}
		samples, err := readSamples(ctx, r.log, r.streams.Series(gpu, TierMinute), gpu, hour, hour.Add(time.Hour))
		if err != nil {
			return nil, err
		}
		if len(samples) > 0 {
			hours = append(hours, RollUp(gpu, hour, samples))
		}
	}
	return hours, nil
}

// readParts reads a time in parts of at most a length, several at once, and
// returns what they hold in order.
func readParts[T any](ctx context.Context, from, to time.Time, part time.Duration, read func(ctx context.Context, from, to time.Time) ([]T, error)) ([]T, error) {
	var starts []time.Time
	for at := from; at.Before(to); at = at.Add(part) {
		starts = append(starts, at)
	}
	parts := make([][]T, len(starts))
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(readConcurrency)
	for i, start := range starts {
		group.Go(func() (err error) {
			end := start.Add(part)
			if end.After(to) {
				end = to
			}
			parts[i], err = read(ctx, start, end)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return slices.Concat(parts...), nil
}

// linesOfSamples reads the series a question asks for out of samples, with
// one sighting of each series for each sample.
func linesOfSamples(q SeriesRequest, wants func(Listing) bool, samples []Sample) (map[string]*line, map[string]PollHour) {
	lines := map[string]*line{}
	quotes := map[string][]*quote{}
	for i, sample := range samples {
		for _, row := range sample.Rows {
			if !wants(row.Listing) {
				continue
			}
			series := q.seriesOf(row.Listing)
			at, ok := lines[series.Key]
			if !ok {
				at = &line{series: series, providers: map[string]bool{}}
				lines[series.Key] = at
				quotes[series.Key] = make([]*quote, len(samples))
			}
			at.providers[row.Provider] = true
			if quotes[series.Key][i] == nil {
				quotes[series.Key][i] = &quote{}
			}
			quotes[series.Key][i].add(row)
		}
	}

	for key, line := range lines {
		for i, sample := range samples {
			at := seen{at: sample.At}
			for provider := range line.providers {
				if poll, asked := sample.Polls[provider]; asked {
					at.asked++
					if poll.OK {
						at.answered++
					}
				}
			}
			// A sample in which none of them answered says nothing.
			if quoted := quotes[key][i]; quoted != nil && at.answered > 0 {
				at.price, at.low, at.high = quoted.price, quoted.price, quoted.price
				at.stock, at.counted = quoted.stock, quoted.counted
				if has := quoted.inStock(); has != nil {
					at.known = true
					if *has {
						at.inStock = 1
					}
				}
			}
			line.seen = append(line.seen, at)
		}
	}

	tally := map[string]PollHour{}
	took := map[string]int64{}
	for _, sample := range samples {
		for provider, poll := range sample.Polls {
			sum := tally[provider]
			sum.Polls++
			if poll.OK {
				sum.Answered++
				took[provider] += poll.MS
			}
			tally[provider] = sum
		}
	}
	for provider, sum := range tally {
		if sum.Answered > 0 {
			sum.MS = took[provider] / int64(sum.Answered)
			tally[provider] = sum
		}
	}
	return lines, tally
}

// linesOfHours reads the series a question asks for out of what hours came
// to, with one sighting of each series for each hour.
//
// An hour keeps each listing's prices, and not how the listings stood to
// each other from one sample to the next. What several of them came to
// together is worked out from that as well as it can be: the lowest they
// reached is exact, and the highest the cheapest of them reached is taken
// from the listings that were in stock throughout.
func linesOfHours(q SeriesRequest, wants func(Listing) bool, hours []Hourly) (map[string]*line, map[string]PollHour) {
	type sums struct {
		seen
		steadyHigh, anyHigh int64
		says, unlisted      bool
	}
	lines := map[string]*line{}
	at := map[string][]*sums{}
	for i, hour := range hours {
		for _, row := range hour.Rows {
			if !wants(row.Listing) {
				continue
			}
			series := q.seriesOf(row.Listing)
			if _, ok := lines[series.Key]; !ok {
				lines[series.Key] = &line{series: series, providers: map[string]bool{}}
				at[series.Key] = make([]*sums, len(hours))
			}
			lines[series.Key].providers[row.Provider] = true
			sum := at[series.Key][i]
			if sum == nil {
				sum = &sums{seen: seen{at: hour.At}, unlisted: true}
				at[series.Key][i] = sum
			}
			if row.Kind == StockCounted {
				sum.counted = true
				sum.stock += int64(row.MaxStock)
			}
			if row.Kind != StockUnknown {
				sum.says = true
				sum.inStock = max(sum.inStock, row.InStock)
			}
			if row.InStock <= 0 || row.Close <= 0 {
				continue
			}
			sum.unlisted = false
			sum.price = lowest(sum.price, row.Close)
			sum.low = lowest(sum.low, row.Min)
			sum.anyHigh = max(sum.anyHigh, row.Max)
			if row.InStock >= 0.999 {
				sum.steadyHigh = lowest(sum.steadyHigh, row.Max)
			}
		}
	}

	tally := map[string]PollHour{}
	took := map[string]int64{}
	for _, hour := range hours {
		for provider, poll := range hour.Polls {
			sum := tally[provider]
			sum.Polls += poll.Polls
			sum.Answered += poll.Answered
			took[provider] += poll.MS * int64(poll.Answered)
			tally[provider] = sum
		}
	}
	for provider, sum := range tally {
		if sum.Answered > 0 {
			sum.MS = took[provider] / int64(sum.Answered)
			tally[provider] = sum
		}
	}

	for key, line := range lines {
		for i, hour := range hours {
			sighting := seen{at: hour.At}
			for provider := range line.providers {
				sighting.asked += hour.Polls[provider].Polls
				sighting.answered += hour.Polls[provider].Answered
			}
			if sum := at[key][i]; sum != nil && sighting.answered > 0 {
				asked, answered := sighting.asked, sighting.answered
				sighting = sum.seen
				sighting.asked, sighting.answered = asked, answered
				sighting.high = cmp.Or(sum.steadyHigh, sum.anyHigh)
				// Providers that say nothing of their stock are known to
				// have none only when they list no price at all.
				sighting.known = sum.says || sum.unlisted
			}
			line.seen = append(line.seen, sighting)
		}
	}
	return lines, tally
}

// lowest is the lower of two prices, of which zero is none.
func lowest(a, b int64) int64 {
	switch {
	case a <= 0:
		return b
	case b <= 0:
		return a
	}
	return min(a, b)
}

// pointsOf draws the points of a series: one for each step in which the
// record has anything of it. It also returns the last price of the series.
func pointsOf(sightings []seen, step time.Duration, price func(int64) int64) (points [][]any, last *int64) {
	for start := 0; start < len(sightings); {
		begins := sightings[start].at.Truncate(step)
		end := start
		for end < len(sightings) && sightings[end].at.Truncate(step).Equal(begins) {
			end++
		}
		point, closing := pointOf(begins, sightings[start:end], price)
		if point != nil {
			points = append(points, point)
			if closing != nil {
				last = closing
			}
		}
		start = end
	}
	return points, last
}

func pointOf(begins time.Time, sightings []seen, price func(int64) int64) (point []any, closing *int64) {
	var sum seen
	var inStock float64
	said := 0
	for _, at := range sightings {
		sum.asked += at.asked
		sum.answered += at.answered
		if at.price > 0 {
			sum.price = at.price
		} else if at.answered > 0 {
			sum.price = 0 // it sold out since
		}
		sum.low = lowest(sum.low, at.low)
		sum.high = max(sum.high, at.high)
		if at.counted {
			sum.counted = true
			sum.stock = max(sum.stock, at.stock)
		}
		if at.known {
			inStock += at.inStock
			said++
		}
	}
	if sum.asked == 0 {
		return nil, nil
	}

	closing = priced(price(sum.price))
	point = []any{begins.UnixMilli(), closing, priced(price(sum.low)), priced(price(sum.high)), nil, nil, share(float64(sum.answered) / float64(sum.asked))}
	if sum.counted && sum.answered > 0 {
		point[4] = sum.stock
	}
	if said > 0 {
		point[5] = share(inStock / float64(said))
	}
	return point, closing
}

// share rounds a share to thousandths, which is as fine as it is known.
func share(value float64) float64 {
	return math.Round(value*1000) / 1000
}
