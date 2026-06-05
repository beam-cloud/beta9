package reconciler

import (
	"context"
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/compute"
)

type fakeVendor struct {
	name              string
	offers            []compute.Offer
	created           *compute.Reservation
	deletedInstanceID string
}

func (v *fakeVendor) Name() string {
	return v.name
}

func (v *fakeVendor) ListOffers(ctx context.Context, req compute.OfferRequest) ([]compute.Offer, error) {
	return v.offers, nil
}

func (v *fakeVendor) CreateReservation(ctx context.Context, req compute.ReservationRequest) (*compute.Reservation, error) {
	v.created = &compute.Reservation{
		ID:         "reservation-one",
		Provider:   v.name,
		InstanceID: "instance-one",
	}
	return v.created, nil
}

func (v *fakeVendor) GetReservation(ctx context.Context, id string) (*compute.Reservation, error) {
	return nil, nil
}

func (v *fakeVendor) DeleteReservation(ctx context.Context, id string) error {
	v.deletedInstanceID = id
	return nil
}

type failingStore struct{}

func (failingStore) ListReservations(ctx context.Context, poolName string) ([]compute.Reservation, error) {
	return nil, nil
}

func (failingStore) SaveReservation(ctx context.Context, reservation *compute.Reservation) error {
	return errors.New("save failed")
}

func (failingStore) MarkReservationDeleted(ctx context.Context, reservationID string) error {
	return nil
}

func TestApplyDeletesReservationWhenStoreSaveFails(t *testing.T) {
	vendor := &fakeVendor{name: "vast"}
	r := New([]compute.Vendor{vendor}, failingStore{})

	err := r.Apply(context.Background(), compute.Demand{PoolName: "pool"}, compute.SolvePlan{
		Feasible: true,
		Actions: []compute.SolveAction{
			{
				Type:  compute.ActionCreate,
				Count: 1,
				Offer: compute.Offer{Provider: "vast"},
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
	r := New([]compute.Vendor{
		&fakeVendor{name: "z", offers: []compute.Offer{{Provider: "z"}}},
		&fakeVendor{name: "a", offers: []compute.Offer{{Provider: "a"}}},
	}, nil)

	offers, err := r.collectOffers(context.Background(), compute.Demand{})
	if err != nil {
		t.Fatal(err)
	}
	if len(offers) != 2 || offers[0].Provider != "a" || offers[1].Provider != "z" {
		t.Fatalf("offers = %#v, want providers a,z", offers)
	}
}
