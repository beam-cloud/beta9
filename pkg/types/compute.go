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
	ComputeSourceAutosolver        ComputeCapacitySource = "autosolver"
	ComputeSourceCLIReservation    ComputeCapacitySource = "cli_reservation"
	ComputeSourceAttached          ComputeCapacitySource = "attached"
	ComputeSourceManual            ComputeCapacitySource = "manual"
	ComputeSourceAWSCloudFormation ComputeCapacitySource = "aws_cloudformation"

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
	return s == ComputeSourceAttached || s == ComputeSourceManual || s == ComputeSourceAWSCloudFormation
}

type ComputeVendor interface {
	Name() string
	ListOffers(ctx context.Context, req ComputeOfferRequest) ([]ComputeOffer, error)
	CreateReservation(ctx context.Context, req ComputeReservationRequest) (*ComputeReservation, error)
	GetReservation(ctx context.Context, id string) (*ComputeReservation, error)
	// ExtendReservation pushes a new expiry to any provider-side auto-delete
	// threshold so the provider cannot kill an instance our control plane
	// still considers alive. Vendors without provider-side thresholds no-op.
	ExtendReservation(ctx context.Context, id string, expiresAt time.Time) error
	DeleteReservation(ctx context.Context, id string) error
}

type ComputeOfferRequest struct {
	GPUs            []string
	Nodes           uint32
	OfferID         string
	Providers       []string
	Regions         []string
	MinReliability  float64
	MaxHourlyMicros int64
}

type ComputeReservationRequest struct {
	PoolName          string
	MachineID         string
	Name              string
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
	ID                string
	Provider          string // aggregator/vendor (e.g. "shadeform"), used for pool filtering
	Cloud             string // underlying cloud the vendor sources from (e.g. "imwt")
	InstanceType      string
	Region            string
	GPU               string
	GPUCount          uint32
	NodeCount         uint32
	CPUMillicores     int64
	MemoryMB          int64
	StorageMB         int64
	HourlyCostMicros  int64
	Reliability       float64
	Available         uint32
	DisplayName       string
	Category          string
	RegionDisplayName string
	Latitude          float64
	Longitude         float64
	Labels            map[string]string
	Raw               []byte
}

func (o ComputeOffer) CostPerNode() float64 {
	nodeCount := o.NodeCount
	if nodeCount == 0 && (o.GPUCount > 0 || o.CPUMillicores > 0) {
		nodeCount = 1
	}
	if nodeCount == 0 {
		return math.Inf(1)
	}
	return float64(o.HourlyCostMicros) / float64(nodeCount)
}

type ComputeReservation struct {
	ID                    string
	PoolName              string
	Name                  string
	Selector              string
	Provider              string
	Cloud                 string
	Region                string
	OfferID               string
	InstanceType          string
	InstanceID            string
	MachineID             string
	GPU                   string
	GPUCount              uint32
	NodeCount             uint32
	CPUMillicores         int64
	MemoryMB              int64
	StorageMB             int64
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
		NodeCount:        r.NodeCount,
		CPUMillicores:    r.CPUMillicores,
		MemoryMB:         r.MemoryMB,
		StorageMB:        r.StorageMB,
		HourlyCostMicros: r.HourlyCostMicros,
		Available:        1,
	}
}

type ComputePool struct {
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
	Source         ComputeCapacitySource
}

func (p ComputePool) RequiresReservation() bool {
	return p.Nodes > 0 || p.TTL > 0 || p.MaxSpendMicros > 0 || len(p.Providers) > 0 || len(p.Regions) > 0 || p.MinReliability > 0
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
	if p.Nodes == 0 && p.RequiresReservation() {
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
	if len(p.GPUs) > 0 {
		return offer.GPUCount > 0 && offer.Available > 0
	}
	if p.Nodes > 0 {
		return offer.Available > 0 && offer.GPUCount == 0 && (offer.NodeCount > 0 || offer.CPUMillicores > 0)
	}
	return offer.Available > 0 && (offer.GPUCount > 0 || offer.NodeCount > 0 || offer.CPUMillicores > 0)
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
	Nodes          uint32
	OfferID        string
	CPUMillicores  int64
	MemoryMB       int64
	TTL            time.Duration
	MaxSpendMicros int64
	Providers      []string
	Regions        []string
	MinReliability float64
}

func (d ComputeDemand) Pool() ComputePool {
	return ComputePool{
		Name:           d.PoolName,
		Selector:       d.Selector,
		GPUs:           d.GPUs,
		Nodes:          d.Nodes,
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
	TotalCapacity         uint32
	ExistingCapacity      uint32
	NewCapacity           uint32
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
