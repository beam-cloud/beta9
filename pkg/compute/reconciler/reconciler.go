package reconciler

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/compute/solver"
)

type Store interface {
	ListReservations(ctx context.Context, poolName string) ([]compute.Reservation, error)
	SaveReservation(ctx context.Context, reservation *compute.Reservation) error
	MarkReservationDeleted(ctx context.Context, reservationID string) error
}

type Reconciler struct {
	Vendors map[string]compute.Vendor
	Store   Store
	Solver  *solver.Solver
}

func New(vendors []compute.Vendor, store Store) *Reconciler {
	vendorMap := map[string]compute.Vendor{}
	for _, vendor := range vendors {
		vendorMap[vendor.Name()] = vendor
	}
	return &Reconciler{
		Vendors: vendorMap,
		Store:   store,
		Solver:  solver.New(),
	}
}

func (r *Reconciler) Plan(ctx context.Context, demand compute.Demand) (compute.SolvePlan, error) {
	offers, err := r.collectOffers(ctx, demand)
	if err != nil {
		return compute.SolvePlan{}, err
	}

	var reservations []compute.Reservation
	if r.Store != nil {
		reservations, err = r.Store.ListReservations(ctx, demand.PoolName)
		if err != nil {
			return compute.SolvePlan{}, err
		}
	}

	return r.Solver.Solve(compute.SolveInput{
		Demand:       demand,
		Offers:       offers,
		Reservations: reservations,
	}), nil
}

func (r *Reconciler) Apply(ctx context.Context, demand compute.Demand, plan compute.SolvePlan, bootstrapCommand string) error {
	if !plan.Feasible {
		return fmt.Errorf("cannot apply infeasible plan: %s", plan.Reason)
	}
	for _, action := range plan.Actions {
		switch action.Type {
		case compute.ActionCreate:
			vendor, ok := r.Vendors[action.Offer.Provider]
			if !ok {
				return fmt.Errorf("vendor %q is not configured", action.Offer.Provider)
			}
			for i := uint32(0); i < action.Count; i++ {
				reservation, err := vendor.CreateReservation(ctx, compute.ReservationRequest{
					PoolName:         demand.PoolName,
					Selector:         demand.Selector,
					Offer:            action.Offer,
					Count:            1,
					TTL:              demand.TTL,
					MaxSpendMicros:   demand.MaxSpendMicros,
					Source:           compute.SourceAutosolver,
					BootstrapCommand: bootstrapCommand,
				})
				if err != nil {
					return err
				}
				if r.Store != nil {
					if err := r.Store.SaveReservation(ctx, reservation); err != nil {
						return err
					}
				}
			}
		case compute.ActionDelete:
			vendor, ok := r.Vendors[action.Reservation.Provider]
			if !ok {
				return fmt.Errorf("vendor %q is not configured", action.Reservation.Provider)
			}
			if err := vendor.DeleteReservation(ctx, action.Reservation.InstanceID); err != nil {
				return err
			}
			if r.Store != nil {
				if err := r.Store.MarkReservationDeleted(ctx, action.Reservation.ID); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *Reconciler) collectOffers(ctx context.Context, demand compute.Demand) ([]compute.Offer, error) {
	request := compute.OfferRequest{
		GPUs:           demand.GPUs,
		TotalGPUs:      demand.TotalGPUs + demand.HeadroomGPUs,
		Providers:      demand.Providers,
		Regions:        demand.Regions,
		MinReliability: demand.MinReliability,
	}

	offers := []compute.Offer{}
	for _, vendor := range r.Vendors {
		if len(demand.Providers) > 0 {
			found := false
			for _, provider := range demand.Providers {
				if provider == vendor.Name() {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		vendorOffers, err := vendor.ListOffers(ctx, request)
		if err != nil {
			return nil, err
		}
		offers = append(offers, vendorOffers...)
	}
	return offers, nil
}
