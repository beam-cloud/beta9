package compute

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
)

const PoolStatusActive = "active"

var poolNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

func ValidatePoolName(name string) error {
	if name != "" && !poolNamePattern.MatchString(name) {
		return fmt.Errorf("invalid pool name %q", name)
	}
	return nil
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
}

func (p Pool) Validate() error {
	if err := ValidatePoolName(p.Name); err != nil {
		return err
	}
	if p.Nodes == 0 && (p.TTL > 0 || p.MaxSpendMicros > 0 || len(p.Providers) > 0 || len(p.Regions) > 0 || p.MinReliability > 0) {
		return errors.New("pool reservations require nodes > 0")
	}
	if p.Nodes > 0 {
		if p.TTL <= 0 {
			return errors.New("pool reservations require ttl")
		}
		if p.MaxSpendMicros <= 0 {
			return errors.New("pool reservations require max_spend")
		}
	}
	if p.MinReliability < 0 || p.MinReliability > 1 {
		return errors.New("min_reliability must be between 0 and 1")
	}
	return nil
}

// MatchesOffer reports whether a machine of the offer could join the pool.
func (p Pool) MatchesOffer(offer providers.Offer) bool {
	// What has no price can be neither budgeted nor billed, and must not
	// pass for the cheapest machine there is.
	if offer.HourlyCostMicros <= 0 {
		return false
	}
	if p.OfferID != "" && p.OfferID != offer.ID {
		return false
	}
	if !(providers.OfferFilter{GPUs: p.GPUs, Regions: p.Regions}).Matches(offer) {
		return false
	}
	if len(p.Providers) > 0 && !slices.Contains(p.Providers, offer.Provider) {
		return false
	}
	if p.MinReliability > 0 && offer.Reliability > 0 && offer.Reliability < p.MinReliability {
		return false
	}
	if len(p.GPUs) > 0 {
		return offer.GPUCount > 0
	}
	if p.Nodes > 0 {
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
