package compute

import (
	"context"
	"errors"
	"testing"
)

type fakeVendor struct {
	name              string
	offers            []Offer
	created           *Reservation
	deletedInstanceID string
}

func (v *fakeVendor) Name() string {
	return v.name
}

func (v *fakeVendor) ListOffers(ctx context.Context, req OfferRequest) ([]Offer, error) {
	return v.offers, nil
}

func (v *fakeVendor) CreateReservation(ctx context.Context, req ReservationRequest) (*Reservation, error) {
	v.created = &Reservation{
		ID:         "reservation-one",
		Provider:   v.name,
		InstanceID: "instance-one",
	}
	return v.created, nil
}

func (v *fakeVendor) GetReservation(ctx context.Context, id string) (*Reservation, error) {
	return nil, nil
}

func (v *fakeVendor) DeleteReservation(ctx context.Context, id string) error {
	v.deletedInstanceID = id
	return nil
}

type failingStore struct{}

func (failingStore) ListReservations(ctx context.Context, poolName string) ([]Reservation, error) {
	return nil, nil
}

func (failingStore) SaveReservation(ctx context.Context, reservation *Reservation) error {
	return errors.New("save failed")
}

func (failingStore) MarkReservationDeleted(ctx context.Context, reservationID string) error {
	return nil
}

func TestApplyDeletesReservationWhenStoreSaveFails(t *testing.T) {
	vendor := &fakeVendor{name: "vast"}
	r := NewReconciler([]Vendor{vendor}, failingStore{})

	err := r.Apply(context.Background(), Demand{PoolName: "pool"}, SolvePlan{
		Feasible: true,
		Actions: []SolveAction{
			{
				Type:  ActionCreate,
				Count: 1,
				Offer: Offer{Provider: "vast"},
			},
		},
	}, "")
	if err == nil {
		t.Fatal("expected apply to fail")
	}
	if vendor.deletedInstanceID != "instance-one" {
		t.Fatalf("deleted instance id = %q, want instance-one", vendor.deletedInstanceID)
	}
}

func TestCollectOffersOrdersVendorsDeterministically(t *testing.T) {
	r := NewReconciler([]Vendor{
		&fakeVendor{name: "z", offers: []Offer{{Provider: "z"}}},
		&fakeVendor{name: "a", offers: []Offer{{Provider: "a"}}},
	}, nil)

	offers, err := r.collectOffers(context.Background(), Demand{})
	if err != nil {
		t.Fatal(err)
	}
	if len(offers) != 2 || offers[0].Provider != "a" || offers[1].Provider != "z" {
		t.Fatalf("offers = %#v, want providers a,z", offers)
	}
}
