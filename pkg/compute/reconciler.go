package compute

import (
	"context"
	"fmt"
	"sort"
)

type ReservationStore interface {
	ListReservations(ctx context.Context, poolName string) ([]Reservation, error)
	SaveReservation(ctx context.Context, reservation *Reservation) error
	MarkReservationDeleted(ctx context.Context, reservationID string) error
}

type Reconciler struct {
	Vendors map[string]Vendor
	Store   ReservationStore
	Solver  *Solver
}

func NewReconciler(vendors []Vendor, store ReservationStore) *Reconciler {
	vendorMap := map[string]Vendor{}
	for _, vendor := range vendors {
		vendorMap[vendor.Name()] = vendor
	}
	return &Reconciler{
		Vendors: vendorMap,
		Store:   store,
		Solver:  NewSolver(),
	}
}

func (r *Reconciler) Plan(ctx context.Context, demand Demand) (SolvePlan, error) {
	offers, err := r.collectOffers(ctx, demand)
	if err != nil {
		return SolvePlan{}, err
	}

	var reservations []Reservation
	if r.Store != nil {
		reservations, err = r.Store.ListReservations(ctx, demand.PoolName)
		if err != nil {
			return SolvePlan{}, err
		}
	}

	return r.Solver.Solve(SolveInput{
		Demand:       demand,
		Offers:       offers,
		Reservations: reservations,
	}), nil
}

func (r *Reconciler) Apply(ctx context.Context, demand Demand, plan SolvePlan, bootstrapCommand string) error {
	if !plan.Feasible {
		return fmt.Errorf("cannot apply infeasible plan: %s", plan.Reason)
	}
	for _, action := range plan.Actions {
		switch action.Type {
		case ActionCreate:
			vendor, err := r.vendor(action.Offer.Provider)
			if err != nil {
				return err
			}
			for i := uint32(0); i < action.Count; i++ {
				reservation, err := vendor.CreateReservation(ctx, ReservationRequest{
					PoolName:         demand.PoolName,
					Selector:         demand.Selector,
					Offer:            action.Offer,
					Count:            1,
					TTL:              demand.TTL,
					MaxSpendMicros:   demand.MaxSpendMicros,
					Source:           SourceAutosolver,
					BootstrapCommand: bootstrapCommand,
				})
				if err != nil {
					return err
				}
				if r.Store != nil {
					if err := r.Store.SaveReservation(ctx, reservation); err != nil {
						return r.cleanupUnsavedReservation(ctx, vendor, reservation, err)
					}
				}
			}
		case ActionDelete:
			vendor, err := r.vendor(action.Reservation.Provider)
			if err != nil {
				return err
			}
			if err := vendor.DeleteReservation(ctx, reservationInstanceID(action.Reservation)); err != nil {
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

func (r *Reconciler) vendor(name string) (Vendor, error) {
	vendor, ok := r.Vendors[name]
	if !ok {
		return nil, fmt.Errorf("vendor %q is not configured", name)
	}
	return vendor, nil
}

func (r *Reconciler) cleanupUnsavedReservation(ctx context.Context, vendor Vendor, reservation *Reservation, saveErr error) error {
	if reservation == nil {
		return saveErr
	}
	if deleteErr := vendor.DeleteReservation(ctx, reservationInstanceID(*reservation)); deleteErr != nil {
		return fmt.Errorf("save reservation: %w; cleanup reservation %q failed: %v", saveErr, reservationInstanceID(*reservation), deleteErr)
	}
	return saveErr
}

func reservationInstanceID(reservation Reservation) string {
	if reservation.InstanceID != "" {
		return reservation.InstanceID
	}
	return reservation.ID
}

func (r *Reconciler) collectOffers(ctx context.Context, demand Demand) ([]Offer, error) {
	request := OfferRequest{
		GPUs:           demand.GPUs,
		TotalGPUs:      demand.TotalGPUs + demand.HeadroomGPUs,
		OfferID:        demand.OfferID,
		Providers:      demand.Providers,
		Regions:        demand.Regions,
		MinReliability: demand.MinReliability,
	}

	offers := []Offer{}
	for _, vendor := range r.orderedVendors(demand.Providers) {
		vendorOffers, err := vendor.ListOffers(ctx, request)
		if err != nil {
			return nil, err
		}
		offers = append(offers, vendorOffers...)
	}
	return offers, nil
}

func (r *Reconciler) orderedVendors(providers []string) []Vendor {
	if len(providers) > 0 {
		out := make([]Vendor, 0, len(providers))
		seen := map[string]struct{}{}
		for _, provider := range providers {
			if _, ok := seen[provider]; ok {
				continue
			}
			seen[provider] = struct{}{}
			if vendor, ok := r.Vendors[provider]; ok {
				out = append(out, vendor)
			}
		}
		return out
	}

	names := make([]string, 0, len(r.Vendors))
	for name := range r.Vendors {
		names = append(names, name)
	}
	sort.Strings(names)

	out := make([]Vendor, 0, len(names))
	for _, name := range names {
		out = append(out, r.Vendors[name])
	}
	return out
}
