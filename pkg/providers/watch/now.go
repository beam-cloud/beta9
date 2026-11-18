package watch

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"
)

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
