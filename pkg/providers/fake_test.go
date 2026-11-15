package providers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFakeSellsOutOfAnOfferThatIsCounted(t *testing.T) {
	ctx := context.Background()
	fake := NewFake("fake", testOwner)
	counted := Offer{ID: "a10g-1x", Cloud: "fake", Region: "us-east", GPU: "A10G", GPUCount: 1, Available: 2}
	uncounted := Offer{ID: "cpu-4", Cloud: "fake", Region: "us-east"}
	fake.SetOffers(counted, uncounted)

	for range 2 {
		_, err := fake.Create(ctx, CreateRequest{Offer: counted, Name: "a"})
		require.NoError(t, err)
	}
	_, err := fake.Create(ctx, CreateRequest{Offer: counted, Name: "a"})
	require.ErrorIs(t, err, ErrOutOfStock)

	offers, err := fake.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.Len(t, offers, 1, "a sold-out offer is no longer listed")
	require.Equal(t, "cpu-4", offers[0].ID)

	for range 5 {
		_, err := fake.Create(ctx, CreateRequest{Offer: uncounted, Name: "a"})
		require.NoError(t, err)
	}
	require.Len(t, fake.Creates(), 7)
}

func TestFakeDoesNotSellWhatItDoesNotOffer(t *testing.T) {
	fake := NewFake("fake", testOwner)
	fake.SetOffers(Offer{ID: "a10g-1x", Cloud: "fake", Region: "us-east"})

	_, err := fake.Create(context.Background(), CreateRequest{Offer: Offer{ID: "a10g-1x", Cloud: "fake", Region: "us-west"}, Name: "a"})
	require.ErrorIs(t, err, ErrOutOfStock)
}

func TestFakeRecordsWhatWasAskedOfIt(t *testing.T) {
	ctx := context.Background()
	fake := NewFake("fake", testOwner)
	offer := Offer{ID: "a10g-1x", Cloud: "fake", Region: "us-east"}
	fake.SetOffers(offer)
	until := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)

	instance, err := fake.Create(ctx, CreateRequest{Offer: offer, Name: "a", Until: until})
	require.NoError(t, err)
	require.Equal(t, until, fake.Until(instance.ID))

	require.NoError(t, fake.Extend(ctx, instance.ID, until.Add(time.Hour)))
	require.Equal(t, until.Add(time.Hour), fake.Until(instance.ID))
	require.EqualError(t, fake.Extend(ctx, "gone", until), "fake has no instance gone")

	fake.SetStatus(instance.ID, StatusActive, "ready")
	found, err := fake.Get(ctx, instance.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: instance.ID, Status: StatusActive, StatusDetail: "ready"}, found)

	require.NoError(t, fake.Delete(ctx, instance.ID))
	require.Equal(t, []string{instance.ID}, fake.Deletes())
}

func TestFakeKeepsAnInstanceWhoseDeleteFails(t *testing.T) {
	ctx := context.Background()
	fake := NewFake("fake", testOwner)
	id := fake.Plant(testOwner, StatusActive, nil)

	fake.FailDeletes(errors.New("provider is down"))
	require.EqualError(t, fake.Delete(ctx, id), "provider is down")
	_, err := fake.Get(ctx, id)
	require.NoError(t, err)

	fake.FailDeletes(nil)
	require.NoError(t, fake.Delete(ctx, id))
	_, err = fake.Get(ctx, id)
	require.ErrorIs(t, err, ErrNotFound)
	require.Equal(t, []string{id, id}, fake.Deletes())
}

func TestFakeRefusesCreatesAfterTheOnesItLetsThrough(t *testing.T) {
	ctx := context.Background()
	fake := NewFake("fake", testOwner)
	offer := Offer{ID: "a10g-1x", Cloud: "fake", Region: "us-east"}
	fake.SetOffers(offer)
	fake.RefuseCreatesAfter(2, ErrQuota)

	for range 2 {
		_, err := fake.Create(ctx, CreateRequest{Offer: offer, Name: "a"})
		require.NoError(t, err)
	}
	for range 2 {
		_, err := fake.Create(ctx, CreateRequest{Offer: offer, Name: "a"})
		require.ErrorIs(t, err, ErrQuota)
	}

	fake.RefuseCreates(nil)
	_, err := fake.Create(ctx, CreateRequest{Offer: offer, Name: "a"})
	require.NoError(t, err)
}
