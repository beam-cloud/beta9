// Package providers buys machines from clouds and GPU marketplaces.
//
// A Provider reports what the provider knows: what it sells, and the state of
// what was bought. Pools, selectors and billing belong to its callers.
package providers

import (
	"context"
	"math"
	"slices"
	"time"
)

type Provider interface {
	// Name is the key the provider is configured under, such as "shadeform".
	Name() string

	// Offers lists what matches the filter and can be bought now.
	Offers(ctx context.Context, filter OfferFilter) ([]Offer, error)

	// Create buys one machine. It fails with ErrOutOfStock when the provider
	// has none left and ErrQuota when the account may not buy more.
	Create(ctx context.Context, req CreateRequest) (Instance, error)

	// Get fails with ErrNotFound once the provider no longer has the instance.
	Get(ctx context.Context, id string) (Instance, error)

	// List returns the instances this control plane created that the
	// provider still has.
	List(ctx context.Context) ([]Instance, error)

	// Extend moves the time at which the provider deletes the instance by
	// itself to until. Providers without such a timer do nothing.
	Extend(ctx context.Context, id string, until time.Time) error

	// Delete succeeds when the instance is gone, including when it already
	// was.
	Delete(ctx context.Context, id string) error
}

// OfferFilter narrows Offers. The zero filter matches everything.
type OfferFilter struct {
	GPUs    []string // any of these GPU types
	Regions []string // any of these regions
}

func (f OfferFilter) Matches(offer Offer) bool {
	if len(f.GPUs) > 0 && !slices.Contains(f.GPUs, offer.GPU) {
		return false
	}
	return len(f.Regions) == 0 || slices.Contains(f.Regions, offer.Region)
}

// Offer is one kind of machine in one region.
type Offer struct {
	// ID is the provider's name for what is sold. With Cloud and Region it
	// tells Create what to buy.
	ID       string
	Provider string
	// Cloud owns the hardware. It differs from Provider when the provider
	// resells other clouds.
	Cloud        string
	InstanceType string
	Region       string

	GPU           string // Beam's name for the GPU type; empty without a GPU
	GPUCount      uint32
	CPUMillicores int64
	MemoryMB      int64
	StorageMB     int64

	HourlyCostMicros int64
	// Spot marks a machine the provider may take back at any time, sold for
	// less. Create buys the offer as it is listed: spot when Spot is set.
	Spot bool

	// Available is how many machines can be bought now, when the provider
	// counts them. Offers returns only what is in stock, so zero means the
	// provider gave no count, not that there are none.
	Available   uint32
	Reliability float64

	DisplayName       string
	Category          string
	RegionDisplayName string
	Latitude          float64
	Longitude         float64
}

type CreateRequest struct {
	Offer Offer

	// Name is a hostname: lowercase letters, digits and dashes, at most 63
	// characters. A provider with a tighter limit shortens it.
	Name string

	// StartupScript is shell that runs as root when the machine first
	// boots. A provider may run it again on a later boot, as GCP does.
	StartupScript string

	// Tags are stored with the instance and returned by Get and List.
	Tags map[string]string

	// Until is the time at which a provider that can delete an instance by
	// itself is told to delete this one. It is passed on as it is: a caller
	// that wants a margin adds it. Zero means no end.
	Until time.Time

	// MaxHourlyCostMicros is the most the buyer pays per hour for a spot
	// machine, where the provider can be held to it. AWS and Nebius take a
	// price limit and are given this one. GCP, Verda and Shadeform take
	// none: a spot machine there pays the provider's spot price whatever it
	// rises to, and this is not looked at. Nor is it on Vast, where the
	// price bid is the price paid: Vast is bid the host's price of the
	// moment, and rents nothing that has come to cost more than the offer
	// said. Zero means no limit.
	MaxHourlyCostMicros int64
}

// Instance is what a provider says about a machine that was bought.
type Instance struct {
	ID     string
	Status Status
	// StatusDetail is the provider's own wording, such as "Running startup
	// script".
	StatusDetail string

	PublicIP string
	SSHHost  string
	SSHPort  uint32

	Tags map[string]string
}

type Status string

const (
	StatusPending Status = "pending" // bought, not running yet
	StatusActive  Status = "active"  // running
	StatusFailed  Status = "failed"  // the provider gave up on it; it still exists
	StatusDeleted Status = "deleted" // being deleted, or gone

	// StatusInterrupted: the provider took a spot machine back. What is left
	// of it, a stopped machine or its disk, may exist and cost money until
	// it is deleted.
	StatusInterrupted Status = "interrupted"

	// StatusUnknown is a status the client has no mapping for. It says
	// nothing about the instance, least of all that it is running.
	StatusUnknown Status = "unknown"
)

// DollarsToMicros converts a price to the millionths of a dollar that offers
// are priced in.
func DollarsToMicros(amount float64) int64 {
	return int64(math.Round(amount * 1_000_000))
}
