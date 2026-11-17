package compute

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
)

var poolNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

func ValidatePoolName(name string) error {
	if name != "" && !poolNamePattern.MatchString(name) {
		return fmt.Errorf("invalid pool name %q", name)
	}
	return nil
}

// Pricing says which of the two ways a provider sells a machine a pool buys.
type Pricing string

const (
	// PricingOnDemand buys machines that are the buyer's until they are
	// given back. It is what a pool that names no pricing buys.
	PricingOnDemand Pricing = ""
	// PricingSpot buys machines the provider may take back at any time,
	// sold for less.
	PricingSpot Pricing = "spot"
	// PricingAny buys whichever of the two costs less.
	PricingAny Pricing = "any"
)

// ParsePricing reads a pool's pricing as the API writes it. Empty and
// "on_demand" are both on-demand.
func ParsePricing(value string) (Pricing, error) {
	switch strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), "-", "_") {
	case "", "on_demand":
		return PricingOnDemand, nil
	case string(PricingSpot):
		return PricingSpot, nil
	case string(PricingAny):
		return PricingAny, nil
	}
	return PricingOnDemand, fmt.Errorf("pricing must be on_demand, spot or any, not %q", value)
}

// Buys reports whether a pool with this pricing buys a machine that is sold
// as spot, or one that is not.
func (p Pricing) Buys(spot bool) bool {
	switch p {
	case PricingAny:
		return true
	case PricingSpot:
		return spot
	}
	return !spot
}

// Pool is what a tenant asked for when reserving machines.
type Pool struct {
	Name           string
	Selector       string
	GPUs           []string
	Nodes          uint32
	OfferID        string
	TTL            time.Duration
	MaxSpendMicros int64
	Providers      []string
	Regions        []string
	MinReliability float64
	// Pricing is on-demand unless the tenant asked for spot machines, which
	// their provider can take back.
	Pricing Pricing
}

func (p Pool) Validate() error {
	if err := ValidatePoolName(p.Name); err != nil {
		return err
	}
	// What only a reservation has a use for.
	reserves := p.TTL > 0 || p.MaxSpendMicros > 0 || len(p.Providers) > 0 || len(p.Regions) > 0 || p.MinReliability > 0 || p.Pricing != PricingOnDemand
	switch {
	case p.Nodes == 0 && reserves:
		return errors.New("pool reservations require nodes > 0")
	case p.Nodes > 0 && p.TTL <= 0:
		return errors.New("pool reservations require ttl")
	case p.Nodes > 0 && p.MaxSpendMicros <= 0:
		return errors.New("pool reservations require max_spend")
	case p.MinReliability < 0 || p.MinReliability > 1:
		return errors.New("min_reliability must be between 0 and 1")
	}
	return nil
}

// MatchesOffer reports whether a machine of the offer could join the pool.
func (p Pool) MatchesOffer(offer providers.Offer) bool {
	wanted := providers.OfferFilter{GPUs: p.GPUs, Regions: p.Regions}
	switch {
	// What has no price can be neither budgeted nor billed, and must not
	// pass for the cheapest machine there is.
	case offer.HourlyCostMicros <= 0,
		p.OfferID != "" && p.OfferID != offer.ID,
		// An instance type that is sold both ways is two offers with one ID.
		!p.Pricing.Buys(offer.Spot),
		!wanted.Matches(offer),
		len(p.Providers) > 0 && !slices.Contains(p.Providers, offer.Provider),
		p.MinReliability > 0 && offer.Reliability > 0 && offer.Reliability < p.MinReliability:
		return false
	case len(p.GPUs) > 0:
		return offer.GPUCount > 0
	case p.Nodes > 0:
		// A pool that names no GPU and asks for machines wants CPU machines.
		return offer.GPUCount == 0 && offer.CPUMillicores > 0
	}
	return offer.GPUCount > 0 || offer.CPUMillicores > 0
}

// ParseTTL reads a pool lifetime such as "24h". Empty means none.
func ParseTTL(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	return time.ParseDuration(value)
}

// CapacitySource says where a pool's or a reservation's machines come from.
type CapacitySource string

const (
	SourceCLIReservation CapacitySource = "cli_reservation"
	SourceAttached       CapacitySource = "attached"
	// SourceManual is the old name of SourceAttached, still found in stored
	// records.
	SourceManual CapacitySource = "manual"
	SourceAWS    CapacitySource = "aws"
	// sourceAutosolver is found only in stored records, on the reservations
	// of a reconciler that is gone. Nothing looks after their machines.
	sourceAutosolver CapacitySource = "autosolver"
)

func (s CapacitySource) Canonical() CapacitySource {
	if s == SourceManual {
		return SourceAttached
	}
	return s
}

// IsAttached reports whether the machines are the tenant's own, so that Beam
// neither buys nor bills them.
func (s CapacitySource) IsAttached() bool {
	return s == SourceAttached || s == SourceManual || s == SourceAWS
}

type ReservationStatus string

const (
	ReservationPending     ReservationStatus = "pending"
	ReservationActive      ReservationStatus = "active"
	ReservationTerminating ReservationStatus = "terminating"
	ReservationDeleted     ReservationStatus = "deleted"
	ReservationFailed      ReservationStatus = "failed"
	// ReservationInterrupted is how a spot machine's reservation ends when
	// its provider takes the machine back. That is what a spot machine is
	// sold as, so it is no failure.
	ReservationInterrupted ReservationStatus = "interrupted"
)

// Reservation is one machine Beam bought for a tenant's pool: what was bought,
// and on what terms. It is stored as JSON under these field names.
type Reservation struct {
	ID               string
	PoolName         string
	Name             string
	Selector         string
	Provider         string
	Cloud            string
	Region           string
	OfferID          string
	InstanceType     string
	InstanceID       string
	MachineID        string
	GPU              string
	GPUCount         uint32
	NodeCount        uint32
	CPUMillicores    int64
	MemoryMB         int64
	StorageMB        int64
	HourlyCostMicros int64
	CommittedMicros  int64
	// Spot marks a machine its provider may take back at any time.
	Spot                  bool
	Source                CapacitySource
	Status                ReservationStatus
	CreatedAt             time.Time
	ExpiresAt             time.Time
	BillingRenewalAt      time.Time
	BillingCursorAt       time.Time
	LastStatusCheckAt     time.Time
	LastReconcileAt       time.Time
	LastStatusMessage     string
	LastError             string
	TerminatingReason     string
	RegistrationTokenHash string
	PublicIP              string
	SSHHost               string
	SSHPort               uint32
	// ExtendPending is set from the moment a reservation is renewed until
	// its provider has been told how long the machine is now wanted. A
	// provider that could not be told is told again on the next pass.
	ExtendPending bool
}

// Closed reports whether the reservation has ended: its machine was deleted,
// failed, or was taken back by its provider.
func (r Reservation) Closed() bool {
	return r.Status == ReservationDeleted || r.Dismissable()
}

// Dismissable reports whether the reservation ended in a way its tenant is
// shown. Such a reservation stays in the pool's record until the tenant
// dismisses it.
func (r Reservation) Dismissable() bool {
	return r.Status == ReservationFailed || r.Status == ReservationInterrupted
}

// Open reports whether the machine is still wanted: the reservation has not
// ended and is not being given back.
func (r Reservation) Open() bool {
	return !r.Closed() && r.Status != ReservationTerminating
}

// ActiveAt reports whether the reservation is open and has not run out at the
// given time.
func (r Reservation) ActiveAt(now time.Time) bool {
	return r.Open() && (r.ExpiresAt.IsZero() || r.ExpiresAt.After(now))
}

// Managed reports whether Beam bought the machine for the tenant, and so
// bills, renews and deletes it. It did for every reservation that is made
// now; the answer is no only for a stored record whose Source says the
// machine is the tenant's own, or that nothing looks after it.
func (r Reservation) Managed() bool {
	return !r.Source.IsAttached() && r.Source != sourceAutosolver
}

// WholeHours rounds a duration up to the hours a provider bills for it.
func WholeHours(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(math.Ceil(float64(d) / float64(time.Hour)))
}
