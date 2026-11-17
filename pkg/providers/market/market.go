// Package market keeps a record of what the providers sell: the price and
// the stock of every kind of machine, at every provider, sampled at an
// interval.
//
// One gateway samples at a time, the one that holds the sampler's lock. What
// the providers last answered is kept in Redis, where every gateway reads it.
// The history is kept in S2, in streams made for two kinds of question:
//
//   - snapshots/<provider> holds every field of every offer, so that nothing
//     a provider said is lost.
//   - series/<gpu>/1m holds, for one GPU type, one row for each place and way
//     it is sold. A chart of a GPU is drawn from this stream alone.
//   - series/<gpu>/1h holds what an hour of those samples comes to, and is
//     kept for good.
//   - outcomes holds what became of the machines that were bought: the only
//     word on stock there is for a provider that does not say what it has.
//
// Prices are the provider's, in millionths of a dollar. What a tenant pays is
// worked out where the record is read.
//
// The record is written by a Sampler (sampler.go, rollup.go) and a Recorder
// of outcomes (outcomes.go), in the formats of records.go. It is read by a
// Reader (reader.go), which answers what is for sale now (now.go) and what a
// GPU sold for over time (series.go). Log (log.go) is the history, which
// pkg/repository keeps in S2, and Shared (shared.go) is the state in Redis.
package market

import (
	"cmp"
	"slices"
	"strings"
	"unicode"

	"github.com/beam-cloud/beta9/pkg/providers"
)

// StockKind is what a provider says about how much it has of an offer.
// Providers mean different things by listing one, and a reader that takes
// them all for the same thing reads stock where none was reported.
type StockKind string

const (
	// StockCounted: the provider counts the machines that can be bought.
	// Nebius counts what the account may start, and Vast lists every host.
	StockCounted StockKind = "count"
	// StockListed: the provider lists an offer while it has some, and does
	// not say how many.
	StockListed StockKind = "listed"
	// StockUnknown: the provider gives a price and no word on stock. Whether
	// a machine can be had there is known only from buying one.
	StockUnknown StockKind = "none"
)

// stockKindOf is what the provider's offers say about its stock.
func stockKindOf(provider string, offers []providers.Offer) StockKind {
	switch provider {
	case "aws", "gcp":
		return stockKindByName(provider)
	}
	if slices.ContainsFunc(offers, func(offer providers.Offer) bool { return offer.Available > 0 }) {
		return StockCounted
	}
	return stockKindByName(provider)
}

// stockKindByName is what a provider says about its stock when it has none
// to show, and so nothing to tell by.
func stockKindByName(provider string) StockKind {
	switch provider {
	case "aws", "gcp":
		return StockUnknown
	case "nebius", "vast":
		return StockCounted
	}
	return StockListed
}

// Listing is what one row of a GPU's series is about: machines with a number
// of that GPU, sold one way, by one cloud of one provider in one region.
type Listing struct {
	Provider string
	// Cloud owns the hardware. It differs from Provider when the provider
	// resells other clouds.
	Cloud  string
	Region string
	// Geo is the part of the world the region is in, which is the same for
	// every provider: see Geo.
	Geo  string
	Spot bool
	GPUs uint32
}

func compareListings(a, b Listing) int {
	return cmp.Or(
		cmp.Compare(a.Provider, b.Provider),
		cmp.Compare(a.Cloud, b.Cloud),
		cmp.Compare(a.Region, b.Region),
		cmp.Compare(a.GPUs, b.GPUs),
		cmp.Compare(btoi(a.Spot), btoi(b.Spot)),
	)
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Row is a listing at the time of one sample.
type Row struct {
	Listing
	// Offers is how many offers the provider listed. Zero means it answered
	// and listed none: it is out of stock, or no longer sells there.
	Offers int
	// Stock is how many machines could be bought, for a provider that
	// counts them.
	Stock uint32
	Kind  StockKind
	// The prices of the offers, cheapest, median and dearest, for one GPU
	// for an hour.
	Min, Median, Max int64
	// Offer is the instance type of the cheapest offer, and Zone the zone it
	// is in where the provider sells by zone.
	Offer string
	Zone  string
}

// InStock reports whether the provider listed anything.
func (r Row) InStock() bool { return r.Offers > 0 }

// place splits what an offer's ID names into the zone and the instance
// type. AWS and GCP sell by zone, and name an offer "zone/type". Everyone
// else has no zone, and the ID is what is sold.
func place(offer providers.Offer) (zone, instanceType string) {
	instanceType = cmp.Or(offer.InstanceType, offer.ID)
	switch offer.Provider {
	case "aws", "gcp":
		if before, after, ok := strings.Cut(offer.ID, "/"); ok {
			return before, cmp.Or(offer.InstanceType, after)
		}
	}
	return "", instanceType
}

// perGPU is what one GPU of a machine costs for an hour.
func perGPU(offer providers.Offer) int64 {
	if offer.GPUCount == 0 {
		return offer.HourlyCostMicros
	}
	return (offer.HourlyCostMicros + int64(offer.GPUCount)/2) / int64(offer.GPUCount)
}

// rowsOf puts a provider's offers of one GPU together by listing.
func rowsOf(offers []providers.Offer, kind StockKind) []Row {
	byListing := map[Listing][]providers.Offer{}
	for _, offer := range offers {
		listing := listingOf(offer)
		byListing[listing] = append(byListing[listing], offer)
	}
	rows := make([]Row, 0, len(byListing))
	for listing, listed := range byListing {
		slices.SortFunc(listed, func(a, b providers.Offer) int {
			return cmp.Or(cmp.Compare(perGPU(a), perGPU(b)), cmp.Compare(a.ID, b.ID))
		})
		row := Row{
			Listing: listing,
			Offers:  len(listed),
			Kind:    kind,
			Min:     perGPU(listed[0]),
			Median:  perGPU(listed[len(listed)/2]),
			Max:     perGPU(listed[len(listed)-1]),
		}
		row.Zone, row.Offer = place(listed[0])
		if kind == StockCounted {
			for _, offer := range listed {
				row.Stock += offer.Available
			}
		}
		rows = append(rows, row)
	}
	slices.SortFunc(rows, func(a, b Row) int { return compareListings(a.Listing, b.Listing) })
	return rows
}

func listingOf(offer providers.Offer) Listing {
	return Listing{
		Provider: offer.Provider,
		Cloud:    cmp.Or(offer.Cloud, offer.Provider),
		Region:   offer.Region,
		Geo:      Geo(offer.Region),
		Spot:     offer.Spot,
		GPUs:     offer.GPUCount,
	}
}

// slug writes a name as part of a stream's name: lowercase letters and
// digits, with a dash for anything else.
func slug(name string) string {
	var b strings.Builder
	dash := false
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		if unicode.IsLetter(r) && r < unicode.MaxASCII || unicode.IsDigit(r) && r < unicode.MaxASCII {
			if dash && b.Len() > 0 {
				b.WriteByte('-')
			}
			dash = false
			b.WriteRune(r)
			continue
		}
		dash = true
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}

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
