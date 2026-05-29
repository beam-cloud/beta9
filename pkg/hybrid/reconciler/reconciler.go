package reconciler

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/hybrid"
	"github.com/beam-cloud/beta9/pkg/hybrid/solver"
)

type Store interface {
	ListReservations(ctx context.Context, poolName string) ([]hybrid.Reservation, error)
	SaveReservation(ctx context.Context, reservation *hybrid.Reservation) error
	MarkReservationDeleted(ctx context.Context, reservationID string) error
}

type Reconciler struct {
	Vendors map[string]hybrid.Vendor
	Store   Store
	Solver  *solver.Solver
}

func New(vendors []hybrid.Vendor, store Store) *Reconciler {
	vendorMap := map[string]hybrid.Vendor{}
	for _, vendor := range vendors {
		vendorMap[vendor.Name()] = vendor
	}
	return &Reconciler{
		Vendors: vendorMap,
		Store:   store,
		Solver:  solver.New(),
	}
}

func (r *Reconciler) Plan(ctx context.Context, demand hybrid.Demand) (hybrid.SolvePlan, error) {
	offers, err := r.collectOffers(ctx, demand)
	if err != nil {
		return hybrid.SolvePlan{}, err
	}

	var reservations []hybrid.Reservation
	if r.Store != nil {
		reservations, err = r.Store.ListReservations(ctx, demand.PoolName)
		if err != nil {
			return hybrid.SolvePlan{}, err
		}
	}

	return r.Solver.Solve(hybrid.SolveInput{
		Demand:       demand,
		Offers:       offers,
		Reservations: reservations,
	}), nil
}

func (r *Reconciler) Apply(ctx context.Context, demand hybrid.Demand, plan hybrid.SolvePlan, bootstrapCommand string) error {
	if !plan.Feasible {
		return fmt.Errorf("cannot apply infeasible plan: %s", plan.Reason)
	}
	for _, action := range plan.Actions {
		switch action.Type {
		case hybrid.ActionCreate:
			vendor, ok := r.Vendors[action.Offer.Provider]
			if !ok {
				return fmt.Errorf("vendor %q is not configured", action.Offer.Provider)
			}
			for i := uint32(0); i < action.Count; i++ {
				reservation, err := vendor.CreateReservation(ctx, hybrid.ReservationRequest{
					PoolName:         demand.PoolName,
					Selector:         demand.Selector,
					Offer:            action.Offer,
					Count:            1,
					TTL:              demand.TTL,
					MaxSpendMicros:   demand.MaxSpendMicros,
					Source:           hybrid.SourceAutosolver,
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
		case hybrid.ActionDelete:
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

func (r *Reconciler) collectOffers(ctx context.Context, demand hybrid.Demand) ([]hybrid.Offer, error) {
	request := hybrid.OfferRequest{
		GPUs:           demand.GPUs,
		TotalGPUs:      demand.TotalGPUs + demand.HeadroomGPUs,
		Providers:      demand.Providers,
		Regions:        demand.Regions,
		MinReliability: demand.MinReliability,
	}

	offers := []hybrid.Offer{}
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
