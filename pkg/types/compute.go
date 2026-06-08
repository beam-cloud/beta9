package types

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
	ComputeSourceAutosolver     ComputeCapacitySource = "autosolver"
	ComputeSourceCLIReservation ComputeCapacitySource = "cli_reservation"
	ComputeSourceAttached       ComputeCapacitySource = "attached"
	ComputeSourceManual         ComputeCapacitySource = "manual"

	ComputeReservationPending     ComputeReservationStatus = "pending"
	ComputeReservationActive      ComputeReservationStatus = "active"
	ComputeReservationTerminating ComputeReservationStatus = "terminating"
	ComputeReservationDeleted     ComputeReservationStatus = "deleted"
	ComputeReservationFailed      ComputeReservationStatus = "failed"

	ComputeActionCreate ComputeSolveActionType = "create"
	ComputeActionKeep   ComputeSolveActionType = "keep"
	ComputeActionDelete ComputeSolveActionType = "delete"
)

var computePoolNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

type ComputeCapacitySource string
type ComputeReservationStatus string
type ComputeSolveActionType string

func (s ComputeCapacitySource) Canonical() ComputeCapacitySource {
	if s == ComputeSourceManual {
		return ComputeSourceAttached
	}
	return s
}

func (s ComputeCapacitySource) IsAttached() bool {
	return s == ComputeSourceAttached || s == ComputeSourceManual
}

type ComputeVendor interface {
	Name() string
	ListOffers(ctx context.Context, req ComputeOfferRequest) ([]ComputeOffer, error)
	CreateReservation(ctx context.Context, req ComputeReservationRequest) (*ComputeReservation, error)
	GetReservation(ctx context.Context, id string) (*ComputeReservation, error)
	DeleteReservation(ctx context.Context, id string) error
}

type ComputeOfferRequest struct {
	GPUs            []string
	TotalGPUs       uint32
	OfferID         string
	Providers       []string
	Regions         []string
	MinReliability  float64
	MaxHourlyMicros int64
}

type ComputeReservationRequest struct {
	PoolName          string
	Selector          string
	Offer             ComputeOffer
	Count             uint32
	TTL               time.Duration
	MaxSpendMicros    int64
	Source            ComputeCapacitySource
	RegistrationToken string
	BootstrapCommand  string
}

type ComputeOffer struct {
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

func (o ComputeOffer) CostPerGPU() float64 {
	if o.GPUCount == 0 {
		return math.Inf(1)
	}
	return float64(o.HourlyCostMicros) / float64(o.GPUCount)
}

type ComputeReservation struct {
	ID                    string
	PoolName              string
	Selector              string
	Provider              string
	OfferID               string
	InstanceType          string
	InstanceID            string
	MachineID             string
	GPU                   string
	GPUCount              uint32
	CPUMillicores         int64
	MemoryMB              int64
	HourlyCostMicros      int64
	CommittedMicros       int64
	Source                ComputeCapacitySource
	Status                ComputeReservationStatus
	CreatedAt             time.Time
	ExpiresAt             time.Time
	BillingRenewalAt      time.Time
	BillingCursorAt       time.Time
	LastStatusCheckAt     time.Time
	LastBillingCheckAt    time.Time
	LastReconcileAt       time.Time
	LastStatusMessage     string
	LastError             string
	TerminatingReason     string
	RegistrationTokenHash string
}

func (r ComputeReservation) ActiveAt(now time.Time) bool {
	if r.Status == ComputeReservationDeleted || r.Status == ComputeReservationFailed || r.Status == ComputeReservationTerminating {
		return false
	}
	return r.ExpiresAt.IsZero() || r.ExpiresAt.After(now)
}

func (r ComputeReservation) Managed() bool {
	return !r.Source.IsAttached() && r.Source != ComputeSourceAutosolver
}

func (r ComputeReservation) Offer() ComputeOffer {
	return ComputeOffer{
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

type ComputePool struct {
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
	Source         ComputeCapacitySource
}

func (p ComputePool) ReservationRequired() bool {
	return p.TotalGPUs > 0 || p.TTL > 0 || p.MaxSpendMicros > 0 || len(p.Providers) > 0 || len(p.Regions) > 0 || p.MinReliability > 0
}

func ValidateComputePoolName(name string) error {
	if name != "" && !computePoolNamePattern.MatchString(name) {
		return fmt.Errorf("invalid pool name %q", name)
	}
	return nil
}

func (p ComputePool) Validate() error {
	if err := ValidateComputePoolName(p.Name); err != nil {
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

func (p ComputePool) MatchesOffer(offer ComputeOffer) bool {
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

func (p ComputePool) NormalizedSelector(workspaceID, stubID string) string {
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

type ComputeDemand struct {
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

func (d ComputeDemand) Pool() ComputePool {
	return ComputePool{
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

type ComputeSolveInput struct {
	Demand       ComputeDemand
	Offers       []ComputeOffer
	Reservations []ComputeReservation
	Now          time.Time
	MaxOffers    int
}

type ComputeSolvePlan struct {
	Feasible              bool
	Reason                string
	Actions               []ComputeSolveAction
	TotalGPUs             uint32
	ExistingGPUs          uint32
	NewGPUs               uint32
	IncrementalCostMicros int64
	CommittedCostMicros   int64
}

type ComputeSolveAction struct {
	Type        ComputeSolveActionType
	Offer       ComputeOffer
	Reservation ComputeReservation
	Count       uint32
	CostMicros  int64
	Reason      string
}

type ComputeLedgerEntry struct {
	PoolName      string
	WorkspaceID   string
	ReservationID string
	Source        ComputeCapacitySource
	AmountMicros  int64
	StartedAt     time.Time
	EndedAt       time.Time
}

func ParseComputeTTL(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	return time.ParseDuration(value)
}

func ComputeDollarsToMicros(value float64) int64 {
	return int64(math.Round(value * 1_000_000))
}

func ComputeMicrosToDollars(value int64) float64 {
	return float64(value) / 1_000_000
}

func ComputeWholeHours(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	hours := int64(math.Ceil(float64(d) / float64(time.Hour)))
	if hours < 1 {
		return 1
	}
	return hours
}
