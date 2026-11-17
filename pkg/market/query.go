package market

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	"golang.org/x/sync/errgroup"
)

const (
	// freshFor is how many sampling intervals what a provider last answered
	// stands for what it sells now.
	freshFor = 3

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

	// How long an answer is given again without being worked out anew. The
	// record changes once an interval.
	seriesCachedFor = 30 * time.Second
	nowCachedFor    = 10 * time.Second
	cachedAtMost    = 256

	// How much of the history is read at once, and how many reads at a time.
	minuteChunk     = 6 * time.Hour
	hourChunk       = 30 * 24 * time.Hour
	readConcurrency = 6
)

var (
	minuteSteps = []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, 30 * time.Minute}
	hourSteps   = []time.Duration{time.Hour, 3 * time.Hour, 6 * time.Hour, 12 * time.Hour, 24 * time.Hour}
)

// ErrNoHistory is the answer to a question about the past where no history
// is kept.
var ErrNoHistory = errors.New("the market's history is not kept here: S2 is not configured")

// ErrAdminOnly is the answer to a question that names providers from someone
// who may not know them.
var ErrAdminOnly = errors.New("which provider sells what is for cluster admins only")

// RequestError is what is wrong with a question.
type RequestError struct{ What string }

func (e *RequestError) Error() string { return e.What }

func badRequest(format string, args ...any) error {
	return &RequestError{What: fmt.Sprintf(format, args...)}
}

// What a chart's series can be told apart by.
const (
	ByPricing  = "pricing"
	ByGeo      = "geo"
	ByGPUs     = "gpus"
	ByProvider = "provider"
	ByCloud    = "cloud"
	ByRegion   = "region"
)

// How machines are sold, as a question and an answer name it.
const (
	PricingOnDemand = "on_demand"
	PricingSpot     = "spot"
	PricingAny      = "any"
)

func pricingOf(spot bool) string {
	if spot {
		return PricingSpot
	}
	return PricingOnDemand
}

type ReaderOptions struct {
	Providers *providers.Registry
	Redis     *common.RedisClient
	// Log holds the history. Nil means none is kept.
	Log    Log
	Config types.ProviderMarketConfig
	// Sells reports whether tenants can reserve machines of a provider. Only
	// those are shown to them. Nil means every provider.
	Sells func(provider string) bool
	// Bill is what a tenant pays for what costs the provider's price. Nil
	// means the same.
	Bill func(micros int64) int64
	// Clock is the reader's source of time. Nil means the wall clock.
	Clock common.Clock
}

// Reader answers questions from the market's record: what is for sale now,
// and what it sold for over time.
//
// It answers two kinds of asker. A tenant sees what it would pay, by GPU,
// part of the world and how the machine is sold, and never who sells it. A
// cluster admin also sees every provider's own listings and prices.
type Reader struct {
	providers *providers.Registry
	shared    *Shared
	log       Log
	streams   Streams
	interval  time.Duration
	sells     func(string) bool
	bill      func(int64) int64
	clock     common.Clock

	mu     sync.Mutex
	cached map[string]cachedAnswer
}

type cachedAnswer struct {
	answer any
	until  time.Time
}

func NewReader(opts ReaderOptions) *Reader {
	sells, bill := opts.Sells, opts.Bill
	if sells == nil {
		sells = func(string) bool { return true }
	}
	if bill == nil {
		bill = func(micros int64) int64 { return micros }
	}
	return &Reader{
		providers: opts.Providers,
		shared:    NewShared(opts.Redis),
		log:       opts.Log,
		streams:   StreamsOf(opts.Config),
		interval:  opts.Config.IntervalOrDefault(),
		sells:     sells,
		bill:      bill,
		clock:     common.ClockOrReal(opts.Clock),
		cached:    map[string]cachedAnswer{},
	}
}

// HasHistory reports whether questions about the past can be answered.
func (r *Reader) HasHistory() bool { return r.log != nil }

// Offers returns what a provider sold when the sampler last asked it, and
// whether that is recent enough to stand for what it sells now.
func (r *Reader) Offers(ctx context.Context, provider string) ([]providers.Offer, bool) {
	latest, ok, err := r.shared.Latest(ctx, provider)
	if err != nil || !ok || r.clock.Now().Sub(latest.At) > freshFor*r.interval {
		return nil, false
	}
	return latest.Offers, true
}

// cache returns the answer kept under key, or works it out and keeps it.
func cache[T any](r *Reader, key string, keep time.Duration, work func() (T, error)) (T, error) {
	now := r.clock.Now()
	r.mu.Lock()
	kept, ok := r.cached[key]
	r.mu.Unlock()
	if ok && now.Before(kept.until) {
		if answer, ok := kept.answer.(T); ok {
			return answer, nil
		}
	}
	answer, err := work()
	if err != nil {
		return answer, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.cached) >= cachedAtMost {
		maps.DeleteFunc(r.cached, func(_ string, kept cachedAnswer) bool { return !now.Before(kept.until) })
		if len(r.cached) >= cachedAtMost {
			clear(r.cached)
		}
	}
	r.cached[key] = cachedAnswer{answer: answer, until: now.Add(keep)}
	return answer, nil
}

// Now is what is for sale at present.
type Now struct {
	// AsOf is when the providers were last asked. It is nil when none has
	// answered lately.
	AsOf            *time.Time `json:"as_of"`
	IntervalSeconds int        `json:"interval_seconds"`
	// History says whether Series can be asked.
	History bool `json:"history"`
	// GPUs are the GPU types on record, and what each can be had as.
	GPUs []GPUChoices `json:"gpus"`
	Rows []NowRow     `json:"rows"`
	// Providers is how every provider last answered. Admins only.
	Providers []ProviderNow `json:"providers,omitempty"`
}

// GPUChoices is what a GPU type has been for sale as within seenFor.
type GPUChoices struct {
	GPU     string   `json:"gpu"`
	Counts  []uint32 `json:"counts"`
	Geos    []string `json:"geos"`
	Pricing []string `json:"pricing"`
	// Admins only.
	Providers []string `json:"providers,omitempty"`
}

// NowRow is one kind of machine as it is for sale now: to a tenant, the
// cheapest anyone sells it for; to an admin, one provider's listing.
type NowRow struct {
	GPU     string `json:"gpu"`
	GPUs    uint32 `json:"gpus"`
	Geo     string `json:"geo"`
	Pricing string `json:"pricing"`
	// Price is what a tenant pays for one GPU for an hour, in millionths of
	// a dollar. It is nil when none is for sale.
	Price *int64 `json:"price"`
	// InStock is nil when the provider gives a price and no word on stock.
	InStock *bool `json:"in_stock"`
	// Stock is how many machines can be bought, where they are counted.
	Stock *int64 `json:"stock"`

	// Admins only: who sells it, as what, and what it costs.
	Provider  string    `json:"provider,omitempty"`
	Cloud     string    `json:"cloud,omitempty"`
	Region    string    `json:"region,omitempty"`
	Offer     string    `json:"offer,omitempty"`
	Zone      string    `json:"zone,omitempty"`
	Cost      *int64    `json:"cost,omitempty"`
	Offers    int       `json:"offers,omitempty"`
	StockKind StockKind `json:"stock_kind,omitempty"`
}

// ProviderNow is how a provider last answered.
type ProviderNow struct {
	Provider string `json:"provider"`
	// Sells says whether tenants can reserve its machines.
	Sells     bool       `json:"sells"`
	OK        bool       `json:"ok"`
	MS        int64      `json:"ms"`
	Offers    int        `json:"offers"`
	Error     string     `json:"error,omitempty"`
	AskedAt   *time.Time `json:"asked_at"`
	StockKind StockKind  `json:"stock_kind"`
}

// Now answers what is for sale at present, from what the sampler last
// published.
func (r *Reader) Now(ctx context.Context, admin bool) (*Now, error) {
	return cache(r, "now:"+strconv.FormatBool(admin), nowCachedFor, func() (*Now, error) {
		return r.now(ctx, admin)
	})
}

func (r *Reader) now(ctx context.Context, admin bool) (*Now, error) {
	now := r.clock.Now()
	polls, err := r.shared.Polls(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("read how the providers answered: %w", err)
	}
	seen, err := r.shared.Seen(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("read what has been for sale: %w", err)
	}

	out := &Now{IntervalSeconds: int(r.interval.Seconds()), History: r.HasHistory(), GPUs: []GPUChoices{}, Rows: []NowRow{}}
	rows := map[string][]Row{} // by GPU
	listed := map[string]bool{}
	answered := map[string]StockKind{}
	for _, provider := range r.providers.Names() {
		health := ProviderNow{Provider: provider, Sells: r.sells(provider), StockKind: stockKindByName(provider)}
		if poll, ok := polls[provider]; ok {
			at := time.UnixMilli(poll.TS).UTC()
			health.OK, health.MS, health.Offers, health.Error, health.AskedAt = poll.OK, poll.MS, poll.N, poll.Err, &at
		}
		latest, ok, err := r.shared.Latest(ctx, provider)
		if err != nil {
			return nil, fmt.Errorf("read what %s sells: %w", provider, err)
		}
		if ok && now.Sub(latest.At) <= freshFor*r.interval {
			kind := stockKindOf(provider, latest.Offers)
			answered[provider], health.StockKind = kind, kind
			if out.AsOf == nil || latest.At.After(*out.AsOf) {
				out.AsOf = &latest.At
			}
			for gpu, offers := range byGPU(latest.Offers) {
				for _, row := range rowsOf(offers, kind) {
					rows[gpu] = append(rows[gpu], row)
					listed[seenMember(gpu, row.Listing)] = true
				}
			}
		}
		if admin {
			out.Providers = append(out.Providers, health)
		}
	}

	// What has been for sale and is not now is sold out, where its provider
	// answered: the row says so.
	had := map[string]*choices{}
	for _, was := range seen {
		if !admin && !r.sells(was.Provider) {
			continue
		}
		if _, known := r.providers.Get(was.Provider); !known {
			continue
		}
		choice, ok := had[was.GPU]
		if !ok {
			choice = newChoices()
			had[was.GPU] = choice
		}
		choice.add(was.Listing)
		kind, ok := answered[was.Provider]
		if !ok || listed[seenMember(was.GPU, was.Listing)] {
			continue
		}
		if kind == StockListed {
			kind = stockKindByName(was.Provider)
		}
		rows[was.GPU] = append(rows[was.GPU], Row{Listing: was.Listing, Kind: kind})
	}

	for _, gpu := range slices.Sorted(maps.Keys(rows)) {
		if _, ok := had[gpu]; !ok {
			had[gpu] = newChoices()
		}
		if admin {
			for _, row := range rows[gpu] {
				had[gpu].add(row.Listing)
				out.Rows = append(out.Rows, r.adminRow(gpu, row))
			}
			continue
		}
		groups := map[Listing]*quote{}
		for _, row := range rows[gpu] {
			if !r.sells(row.Provider) {
				continue
			}
			had[gpu].add(row.Listing)
			group := Listing{Geo: row.Geo, Spot: row.Spot, GPUs: row.GPUs}
			if groups[group] == nil {
				groups[group] = &quote{}
			}
			groups[group].add(row)
		}
		for _, group := range slices.SortedFunc(maps.Keys(groups), compareListings) {
			q := groups[group]
			out.Rows = append(out.Rows, NowRow{
				GPU: gpu, GPUs: group.GPUs, Geo: group.Geo, Pricing: pricingOf(group.Spot),
				Price: priced(r.bill(q.price)), InStock: q.inStock(), Stock: q.stockCount(),
			})
		}
	}
	for _, gpu := range slices.Sorted(maps.Keys(had)) {
		if choice := had[gpu].of(gpu, admin); len(choice.Counts) > 0 {
			out.GPUs = append(out.GPUs, choice)
		}
	}
	slices.SortStableFunc(out.Rows, func(a, b NowRow) int {
		return cmp.Or(cmp.Compare(a.GPU, b.GPU), cmp.Compare(a.GPUs, b.GPUs), cmp.Compare(a.Pricing, b.Pricing), comparePrices(a.Price, b.Price))
	})
	return out, nil
}

func (r *Reader) adminRow(gpu string, row Row) NowRow {
	q := &quote{}
	q.add(row)
	return NowRow{
		GPU: gpu, GPUs: row.GPUs, Geo: row.Geo, Pricing: pricingOf(row.Spot),
		Price: priced(r.bill(q.price)), InStock: q.inStock(), Stock: q.stockCount(),
		Provider: row.Provider, Cloud: row.Cloud, Region: row.Region, Offer: row.Offer, Zone: row.Zone,
		Cost: priced(q.price), Offers: row.Offers, StockKind: row.Kind,
	}
}

// priced is a price as an answer gives it: nil when there is none.
func priced(micros int64) *int64 {
	if micros <= 0 {
		return nil
	}
	return &micros
}

// comparePrices puts what has no price last.
func comparePrices(a, b *int64) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return 1
	case b == nil:
		return -1
	}
	return cmp.Compare(*a, *b)
}

// choices gathers what a GPU type can be had as.
type choices struct {
	counts    map[uint32]bool
	geos      map[string]bool
	pricing   map[string]bool
	providers map[string]bool
}

func newChoices() *choices {
	return &choices{counts: map[uint32]bool{}, geos: map[string]bool{}, pricing: map[string]bool{}, providers: map[string]bool{}}
}

func (c *choices) add(listing Listing) {
	c.counts[listing.GPUs] = true
	c.geos[listing.Geo] = true
	c.pricing[pricingOf(listing.Spot)] = true
	c.providers[listing.Provider] = true
}

func (c *choices) of(gpu string, admin bool) GPUChoices {
	out := GPUChoices{
		GPU:     gpu,
		Counts:  slices.Sorted(maps.Keys(c.counts)),
		Geos:    slices.Sorted(maps.Keys(c.geos)),
		Pricing: slices.Sorted(maps.Keys(c.pricing)),
	}
	if admin {
		out.Providers = slices.Sorted(maps.Keys(c.providers))
	}
	return out
}

// quote is what several listings come to at one time: the cheapest of them,
// and what is known of their stock.
type quote struct {
	// price is the cheapest listed, for one GPU for an hour. Zero is none.
	price int64
	// stock is how many machines can be bought, of the listings that count
	// them. counted says whether any does.
	stock   int64
	counted bool
	// listed says whether any has an offer, and known whether any that has
	// one is of a provider that lists only what it has in stock.
	listed bool
	known  bool
}

func (q *quote) add(row Row) {
	if row.Kind == StockCounted {
		q.counted = true
		q.stock += int64(row.Stock)
	}
	if !row.InStock() {
		return
	}
	q.listed = true
	if row.Kind != StockUnknown {
		q.known = true
	}
	if q.price == 0 || row.Min < q.price {
		q.price = row.Min
	}
}

// inStock is whether a machine can be had: nil when the only prices are of
// providers that do not say.
func (q *quote) inStock() *bool {
	switch {
	case !q.listed:
		return new(bool)
	case q.known:
		yes := true
		return &yes
	}
	return nil
}

func (q *quote) stockCount() *int64 {
	if !q.counted {
		return nil
	}
	return &q.stock
}

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
