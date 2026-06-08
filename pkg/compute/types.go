package compute

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	SourceAutosolver     CapacitySource = "autosolver"
	SourceCLIReservation CapacitySource = "cli_reservation"
	SourceAttached       CapacitySource = "attached"
	SourceManual         CapacitySource = "manual"

	ReservationPending     ReservationStatus = "pending"
	ReservationActive      ReservationStatus = "active"
	ReservationTerminating ReservationStatus = "terminating"
	ReservationDeleted     ReservationStatus = "deleted"
	ReservationFailed      ReservationStatus = "failed"

	ActionCreate SolveActionType = "create"
	ActionKeep   SolveActionType = "keep"
	ActionDelete SolveActionType = "delete"
)

var poolNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

type CapacitySource string
type ReservationStatus string
type SolveActionType string

func (s CapacitySource) Canonical() CapacitySource {
	if s == SourceManual {
		return SourceAttached
	}
	return s
}

func (s CapacitySource) IsAttached() bool {
	return s == SourceAttached || s == SourceManual
}

type Vendor interface {
	Name() string
	ListOffers(ctx context.Context, req OfferRequest) ([]Offer, error)
	CreateReservation(ctx context.Context, req ReservationRequest) (*Reservation, error)
	GetReservation(ctx context.Context, id string) (*Reservation, error)
	DeleteReservation(ctx context.Context, id string) error
}

type OfferRequest struct {
	GPUs            []string
	TotalGPUs       uint32
	OfferID         string
	Providers       []string
	Regions         []string
	MinReliability  float64
	MaxHourlyMicros int64
}

type ReservationRequest struct {
	PoolName          string
	Selector          string
	Offer             Offer
	Count             uint32
	TTL               time.Duration
	MaxSpendMicros    int64
	Source            CapacitySource
	RegistrationToken string
	BootstrapCommand  string
}

type Offer struct {
	ID               string
	Provider         string // aggregator/vendor (e.g. "shadeform"), used for pool filtering
	Cloud            string // underlying cloud the vendor sources from (e.g. "imwt")
	InstanceType     string
	Region           string
	GPU              string
	GPUCount         uint32
	CPUMillicores    int64
	MemoryMB         int64
	StorageMB        int64
	HourlyCostMicros int64
	Reliability      float64
	Available        uint32
	Labels           map[string]string
	Raw              []byte
}

func (o Offer) CostPerGPU() float64 {
	if o.GPUCount == 0 {
		return math.Inf(1)
	}
	return float64(o.HourlyCostMicros) / float64(o.GPUCount)
}

type Reservation struct {
	ID                 string
	PoolName           string
	Selector           string
	Provider           string
	OfferID            string
	InstanceType       string
	InstanceID         string
	MachineID          string
	GPU                string
	GPUCount           uint32
	CPUMillicores      int64
	MemoryMB           int64
	HourlyCostMicros   int64
	CommittedMicros    int64
	Source             CapacitySource
	Status             ReservationStatus
	CreatedAt          time.Time
	ExpiresAt          time.Time
	BillingRenewalAt   time.Time
	BillingCursorAt    time.Time
	LastStatusCheckAt  time.Time
	LastBillingCheckAt time.Time
	LastReconcileAt    time.Time
	LastStatusMessage  string
	LastError          string
	TerminatingReason  string
}

func (r Reservation) ActiveAt(now time.Time) bool {
	if r.Status == ReservationDeleted || r.Status == ReservationFailed || r.Status == ReservationTerminating {
		return false
	}
	return r.ExpiresAt.IsZero() || r.ExpiresAt.After(now)
}

func (r Reservation) Managed() bool {
	return !r.Source.IsAttached() && r.Source != SourceAutosolver
}

func (r Reservation) Offer() Offer {
	return Offer{
		ID:               r.OfferID,
		Provider:         r.Provider,
		InstanceType:     r.InstanceType,
		GPU:              r.GPU,
		GPUCount:         r.GPUCount,
		CPUMillicores:    r.CPUMillicores,
		MemoryMB:         r.MemoryMB,
		HourlyCostMicros: r.HourlyCostMicros,
		Available:        1,
	}
}

type Pool struct {
	Name           string
	Selector       string
	GPUs           []string
	TotalGPUs      uint32
	OfferID        string
	TTL            time.Duration
	MaxSpendMicros int64
	Providers      []string
	Regions        []string
	MinReliability float64
	Source         CapacitySource
}

func (p Pool) ReservationRequired() bool {
	return p.TotalGPUs > 0 || p.TTL > 0 || p.MaxSpendMicros > 0 || len(p.Providers) > 0 || len(p.Regions) > 0 || p.MinReliability > 0
}

func ValidatePoolName(name string) error {
	if name != "" && !poolNamePattern.MatchString(name) {
		return fmt.Errorf("invalid pool name %q", name)
	}
	return nil
}

func (p Pool) Validate() error {
	if err := ValidatePoolName(p.Name); err != nil {
		return err
	}
	if p.TotalGPUs == 0 && p.ReservationRequired() {
		return errors.New("pool reservations require gpus > 0")
	}
	if p.TotalGPUs > 0 {
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

func (p Pool) MatchesOffer(offer Offer) bool {
	if p.OfferID != "" && p.OfferID != offer.ID {
		return false
	}
	if len(p.GPUs) > 0 && !slices.Contains(p.GPUs, offer.GPU) {
		return false
	}
	if len(p.Providers) > 0 && !slices.Contains(p.Providers, offer.Provider) {
		return false
	}
	if len(p.Regions) > 0 && !slices.Contains(p.Regions, offer.Region) {
		return false
	}
	if p.MinReliability > 0 && offer.Reliability > 0 && offer.Reliability < p.MinReliability {
		return false
	}
	return offer.GPUCount > 0 && offer.Available > 0
}

func (p Pool) NormalizedSelector(workspaceID, stubID string) string {
	if p.Selector != "" {
		return p.Selector
	}
	if p.Name != "" {
		return p.Name
	}

	parts := []string{"private"}
	if workspaceID != "" {
		parts = append(parts, workspaceID)
	}
	if stubID != "" {
		parts = append(parts, stubID)
	}
	return strings.Join(parts, "-")
}

type Demand struct {
	PoolName       string
	Selector       string
	GPUs           []string
	TotalGPUs      uint32
	OfferID        string
	CPUMillicores  int64
	MemoryMB       int64
	TTL            time.Duration
	MaxSpendMicros int64
	Providers      []string
	Regions        []string
	MinReliability float64
	HeadroomGPUs   uint32
}

func (d Demand) Pool() Pool {
	return Pool{
		Name:           d.PoolName,
		Selector:       d.Selector,
		GPUs:           d.GPUs,
		TotalGPUs:      d.TotalGPUs + d.HeadroomGPUs,
		OfferID:        d.OfferID,
		TTL:            d.TTL,
		MaxSpendMicros: d.MaxSpendMicros,
		Providers:      d.Providers,
		Regions:        d.Regions,
		MinReliability: d.MinReliability,
	}
}

type SolveInput struct {
	Demand       Demand
	Offers       []Offer
	Reservations []Reservation
	Now          time.Time
	MaxOffers    int
}

type SolvePlan struct {
	Feasible              bool
	Reason                string
	Actions               []SolveAction
	TotalGPUs             uint32
	ExistingGPUs          uint32
	NewGPUs               uint32
	IncrementalCostMicros int64
	CommittedCostMicros   int64
}

type SolveAction struct {
	Type        SolveActionType
	Offer       Offer
	Reservation Reservation
	Count       uint32
	CostMicros  int64
	Reason      string
}

type LedgerEntry struct {
	PoolName      string
	WorkspaceID   string
	ReservationID string
	Source        CapacitySource
	AmountMicros  int64
	StartedAt     time.Time
	EndedAt       time.Time
}

func ParseTTL(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	return time.ParseDuration(value)
}

func DollarsToMicros(value float64) int64 {
	return int64(math.Round(value * 1_000_000))
}

func MicrosToDollars(value int64) float64 {
	return float64(value) / 1_000_000
}

func WholeHours(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	hours := int64(math.Ceil(float64(d) / float64(time.Hour)))
	if hours < 1 {
		return 1
	}
	return hours
}
