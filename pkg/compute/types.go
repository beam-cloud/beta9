package compute

import (
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	SourceAutosolver        = types.ComputeSourceAutosolver
	SourceCLIReservation    = types.ComputeSourceCLIReservation
	SourceAttached          = types.ComputeSourceAttached
	SourceManual            = types.ComputeSourceManual
	SourceAWSCloudFormation = types.ComputeSourceAWSCloudFormation

	ReservationPending     = types.ComputeReservationPending
	ReservationActive      = types.ComputeReservationActive
	ReservationTerminating = types.ComputeReservationTerminating
	ReservationDeleted     = types.ComputeReservationDeleted
	ReservationFailed      = types.ComputeReservationFailed

	ActionCreate = types.ComputeActionCreate
	ActionKeep   = types.ComputeActionKeep
	ActionDelete = types.ComputeActionDelete
)

type CapacitySource = types.ComputeCapacitySource
type ReservationStatus = types.ComputeReservationStatus
type SolveActionType = types.ComputeSolveActionType
type Vendor = types.ComputeVendor
type OfferRequest = types.ComputeOfferRequest
type ReservationRequest = types.ComputeReservationRequest
type Offer = types.ComputeOffer
type Reservation = types.ComputeReservation
type Pool = types.ComputePool
type Demand = types.ComputeDemand
type SolveInput = types.ComputeSolveInput
type SolvePlan = types.ComputeSolvePlan
type SolveAction = types.ComputeSolveAction

func ValidatePoolName(name string) error {
	return types.ValidateComputePoolName(name)
}

func ParseTTL(value string) (time.Duration, error) {
	return types.ParseComputeTTL(value)
}

func DollarsToMicros(value float64) int64 {
	return types.ComputeDollarsToMicros(value)
}

func MicrosToDollars(value int64) float64 {
	return types.ComputeMicrosToDollars(value)
}

func WholeHours(d time.Duration) int64 {
	return types.ComputeWholeHours(d)
}

func NormalizeGPU(value string) string {
	return string(types.NormalizeGPUType(value))
}
