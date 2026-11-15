package providers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Every provider client, and the Fake that stands in for them, must behave
// the same way through the Provider interface. Each check below runs against
// all of them.
//
// A real client talks to a stand-in for its provider's API, in the client's
// test file. What a provider publishes without an account is recorded from
// it: Shadeform's, Verda's and Vast's catalogues, Nebius's and Google's spot
// prices. The other answers are built from the response examples and
// schemas in each provider's published API description, not recorded from a
// live account.
// AWS is the exception: its client calls the AWS SDK, and the stand-in
// answers those calls by the rules of EC2's API reference.

const (
	testOwner    = "beam-test-1a2b3c"
	foreignOwner = "beam-other-9z8y7x"
)

// A site is a provider client together with the provider's side of the
// conversation.
type site struct {
	provider Provider

	// statuses is every status the provider documents, and what each means.
	statuses map[string]Status
	// forgetsAtOnce marks a provider with no status for an instance that
	// is being deleted: it is gone when the delete returns.
	forgetsAtOnce bool
	// report makes the provider describe an instance with its own status word.
	report func(id, status string)
	// refuse makes the provider turn down creates for lack of stock
	// (ErrOutOfStock) or of quota (ErrQuota), until it is called with nil.
	refuse func(reason error)
	// lose removes an instance without the client having asked.
	lose func(id string)
	// bought returns the name and boot script the provider last received.
	bought func() (name, script string)
	// plantForeign adds an instance that another control plane bought on the
	// same account, and returns its ID.
	plantForeign func() string

	// The next two are set for a provider that sells spot machines.
	//
	// boughtSpot reports whether the provider was last asked for a spot
	// machine.
	boughtSpot func() bool
	// takeBack makes the provider take a spot machine back, as it does when
	// it wants the capacity for something else.
	takeBack func(id string)
}

// conformanceSites opens a stand-in for each provider there is a client for.
var conformanceSites = map[string]func(t *testing.T) site{
	"shadeform": newShadeformSite,
	"hetzner":   newHetznerSite,
	"verda":     newVerdaSite,
	"nebius":    newNebiusSite,
	"aws":       newAWSSite,
	"gcp":       newGCPSite,
	"vast":      newVastSite,
	"fake":      newFakeSite,
}

func TestProvidersConform(t *testing.T) {
	checks := map[string]func(t *testing.T, s site){
		"offers can be bought and are complete":           checkOffers,
		"offers are narrowed by the filter":               checkOfferFilter,
		"create starts a pending instance":                checkCreate,
		"create reports out of stock and quota by name":   checkCreateRefusals,
		"create refuses tags a provider could not store":  checkCreateRefusesBadTags,
		"every status has one meaning":                    checkStatuses,
		"an unknown status is never active":               checkUnknownStatus,
		"a missing instance is not found":                 checkNotFound,
		"delete succeeds, and again when already gone":    checkDelete,
		"list returns what this control plane bought":     checkList,
		"extend keeps an instance":                        checkExtend,
		"a spot offer is bought as spot, no other is":     checkSpotCreate,
		"a machine the provider took back is interrupted": checkInterrupted,
	}
	for name, open := range conformanceSites {
		for check, run := range checks {
			t.Run(name+"/"+check, func(t *testing.T) { run(t, open(t)) })
		}
	}
}

func checkOffers(t *testing.T, s site) {
	offers, err := s.provider.Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.NotEmpty(t, offers)

	type key struct {
		id, cloud, region string
		spot              bool
	}
	seen := map[key]bool{}
	for _, offer := range offers {
		require.NotEmpty(t, offer.ID, "%+v", offer)
		require.Equal(t, s.provider.Name(), offer.Provider, "%+v", offer)
		require.NotEmpty(t, offer.Cloud, "%+v", offer)
		require.NotEmpty(t, offer.InstanceType, "%+v", offer)
		require.NotEmpty(t, offer.Region, "%+v", offer)
		require.Positive(t, offer.CPUMillicores, "%+v", offer)
		require.Positive(t, offer.MemoryMB, "%+v", offer)
		require.Positive(t, offer.HourlyCostMicros, "%+v", offer)
		require.Equal(t, offer.GPU != "", offer.GPUCount > 0, "a GPU type and a GPU count come together: %+v", offer)

		k := key{offer.ID, offer.Cloud, offer.Region, offer.Spot}
		require.False(t, seen[k], "offer listed twice: %+v", offer)
		seen[k] = true
	}
}

func checkOfferFilter(t *testing.T, s site) {
	ctx := context.Background()
	all, err := s.provider.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.NotEmpty(t, all)
	wanted := all[len(all)-1]

	inRegion, err := s.provider.Offers(ctx, OfferFilter{Regions: []string{wanted.Region}})
	require.NoError(t, err)
	require.Contains(t, inRegion, wanted)
	for _, offer := range inRegion {
		require.Equal(t, wanted.Region, offer.Region)
	}

	nowhere, err := s.provider.Offers(ctx, OfferFilter{Regions: []string{"no-such-region"}})
	require.NoError(t, err)
	require.Empty(t, nowhere)

	noSuchGPU, err := s.provider.Offers(ctx, OfferFilter{GPUs: []string{"NO-SUCH-GPU"}})
	require.NoError(t, err)
	require.Empty(t, noSuchGPU)

	if wanted.GPU != "" {
		withGPU, err := s.provider.Offers(ctx, OfferFilter{GPUs: []string{wanted.GPU}})
		require.NoError(t, err)
		require.Contains(t, withGPU, wanted)
		for _, offer := range withGPU {
			require.Equal(t, wanted.GPU, offer.GPU)
		}
	}
}

// buy creates an instance of the provider's first offer.
func buy(t *testing.T, s site, name string, tags map[string]string) Instance {
	t.Helper()
	ctx := context.Background()
	offers, err := s.provider.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.NotEmpty(t, offers)
	instance, err := s.provider.Create(ctx, CreateRequest{
		Offer:         offers[0],
		Name:          name,
		StartupScript: "curl -fsSL https://gateway.example/join | sh",
		Tags:          tags,
		Until:         time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	return instance
}

func checkCreate(t *testing.T, s site) {
	tags := map[string]string{"beam-machine": "a1b2c3d4"}
	instance := buy(t, s, "beam-a10g-a1b2c3d4", tags)

	require.NotEmpty(t, instance.ID)
	require.Equal(t, StatusPending, instance.Status)
	require.Equal(t, tags, instance.Tags)

	name, script := s.bought()
	require.Equal(t, "beam-a10g-a1b2c3d4", name)
	require.Contains(t, script, "curl -fsSL https://gateway.example/join | sh")

	found, err := s.provider.Get(context.Background(), instance.ID)
	require.NoError(t, err)
	require.Equal(t, instance.ID, found.ID)
	require.Equal(t, StatusPending, found.Status)
	require.Equal(t, tags, found.Tags)
}

func checkCreateRefusals(t *testing.T, s site) {
	ctx := context.Background()
	offers, err := s.provider.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	req := CreateRequest{Offer: offers[0], Name: "beam-a10g-a1b2c3d4"}

	for _, reason := range []error{ErrOutOfStock, ErrQuota} {
		s.refuse(reason)
		instance, err := s.provider.Create(ctx, req)
		require.ErrorIs(t, err, reason)
		for _, other := range []error{ErrOutOfStock, ErrQuota, ErrNotFound} {
			if other != reason {
				require.False(t, errors.Is(err, other), "%v is not also %v", err, other)
			}
		}
		require.Empty(t, instance.ID)
	}

	bought, err := s.provider.List(ctx)
	require.NoError(t, err)
	require.Empty(t, bought, "a refused create buys nothing")

	s.refuse(nil)
	_, err = s.provider.Create(ctx, req)
	require.NoError(t, err)
}

func checkCreateRefusesBadTags(t *testing.T, s site) {
	ctx := context.Background()
	offers, err := s.provider.Offers(ctx, OfferFilter{})
	require.NoError(t, err)

	for _, tags := range []map[string]string{
		{"Beam_Pool": "a10g"},
		{"beam-pool": "has spaces"},
		{OwnerTag: "someone-else"},
	} {
		_, err := s.provider.Create(ctx, CreateRequest{Offer: offers[0], Name: "beam-a10g-a1b2c3d4", Tags: tags})
		require.Error(t, err, "%v", tags)
	}

	bought, err := s.provider.List(ctx)
	require.NoError(t, err)
	require.Empty(t, bought)
}

func checkStatuses(t *testing.T, s site) {
	instance := buy(t, s, "beam-a10g-a1b2c3d4", nil)
	require.NotEmpty(t, s.statuses)

	meanings := map[Status]bool{}
	for reported, want := range s.statuses {
		s.report(instance.ID, reported)
		got, err := s.provider.Get(context.Background(), instance.ID)
		require.NoError(t, err)
		require.Equal(t, want, got.Status, "provider status %q", reported)
		meanings[want] = true
	}
	for _, status := range []Status{StatusPending, StatusActive, StatusDeleted} {
		if status == StatusDeleted && s.forgetsAtOnce {
			continue
		}
		require.True(t, meanings[status], "no provider status means %q", status)
	}
}

func checkUnknownStatus(t *testing.T, s site) {
	instance := buy(t, s, "beam-a10g-a1b2c3d4", nil)
	s.report(instance.ID, "a-status-nobody-documented")

	got, err := s.provider.Get(context.Background(), instance.ID)
	require.NoError(t, err)
	require.Equal(t, StatusUnknown, got.Status)
}

func checkNotFound(t *testing.T, s site) {
	ctx := context.Background()
	_, err := s.provider.Get(ctx, "424242")
	require.ErrorIs(t, err, ErrNotFound)

	instance := buy(t, s, "beam-a10g-a1b2c3d4", nil)
	s.lose(instance.ID)
	_, err = s.provider.Get(ctx, instance.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func checkDelete(t *testing.T, s site) {
	ctx := context.Background()
	instance := buy(t, s, "beam-a10g-a1b2c3d4", nil)

	require.NoError(t, s.provider.Delete(ctx, instance.ID))
	if found, err := s.provider.Get(ctx, instance.ID); err == nil {
		require.Equal(t, StatusDeleted, found.Status)
	} else {
		require.ErrorIs(t, err, ErrNotFound)
	}

	s.lose(instance.ID)
	require.NoError(t, s.provider.Delete(ctx, instance.ID), "deleting what is already gone succeeds")
	require.NoError(t, s.provider.Delete(ctx, "424242"), "deleting what never existed succeeds")
}

func checkList(t *testing.T, s site) {
	ctx := context.Background()
	empty, err := s.provider.List(ctx)
	require.NoError(t, err)
	require.Empty(t, empty)

	first := buy(t, s, "beam-a10g-a1b2c3d4", map[string]string{"beam-machine": "a1b2c3d4"})
	second := buy(t, s, "beam-a10g-e5f6a7b8", map[string]string{"beam-machine": "e5f6a7b8"})
	foreign := s.plantForeign()

	listed, err := s.provider.List(ctx)
	require.NoError(t, err)
	tagsByID := map[string]map[string]string{}
	for _, instance := range listed {
		tagsByID[instance.ID] = instance.Tags
	}
	require.Equal(t, map[string]map[string]string{
		first.ID:  {"beam-machine": "a1b2c3d4"},
		second.ID: {"beam-machine": "e5f6a7b8"},
	}, tagsByID)
	require.NotContains(t, tagsByID, foreign)

	_, err = s.provider.Get(ctx, foreign)
	require.NoError(t, err, "the other control plane's instance does exist")
}

func checkExtend(t *testing.T, s site) {
	ctx := context.Background()
	instance := buy(t, s, "beam-a10g-a1b2c3d4", nil)
	require.NoError(t, s.provider.Extend(ctx, instance.ID, time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)))

	found, err := s.provider.Get(ctx, instance.ID)
	require.NoError(t, err)
	require.Equal(t, StatusPending, found.Status)
}

// offerWhere returns the provider's first offer that is, or is not, spot.
func offerWhere(t *testing.T, s site, spot bool) (Offer, bool) {
	t.Helper()
	offers, err := s.provider.Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	for _, offer := range offers {
		if offer.Spot == spot {
			return offer, true
		}
	}
	return Offer{}, false
}

func checkSpotCreate(t *testing.T, s site) {
	ctx := context.Background()
	spot, sellsSpot := offerWhere(t, s, true)
	if s.boughtSpot == nil {
		require.False(t, sellsSpot, "the provider lists spot offers, and its site cannot tell how they are bought")
		return
	}
	require.True(t, sellsSpot, "the provider is said to sell spot and lists none")

	_, err := s.provider.Create(ctx, CreateRequest{Offer: spot, Name: "beam-spot-a1b2c3d4"})
	require.NoError(t, err)
	require.True(t, s.boughtSpot(), "a spot offer was bought on demand")

	// A machine that was to cost the on-demand price must not be one the
	// provider can take back, nor the other way round.
	if onDemand, ok := offerWhere(t, s, false); ok {
		_, err := s.provider.Create(ctx, CreateRequest{Offer: onDemand, Name: "beam-od-e5f6a7b8"})
		require.NoError(t, err)
		require.False(t, s.boughtSpot(), "an on-demand offer was bought as spot")
	}
}

func checkInterrupted(t *testing.T, s site) {
	if s.takeBack == nil {
		return
	}
	ctx := context.Background()
	spot, ok := offerWhere(t, s, true)
	require.True(t, ok)
	instance, err := s.provider.Create(ctx, CreateRequest{Offer: spot, Name: "beam-spot-a1b2c3d4"})
	require.NoError(t, err)

	s.takeBack(instance.ID)
	found, err := s.provider.Get(ctx, instance.ID)
	require.NoError(t, err)
	require.Equal(t, StatusInterrupted, found.Status)

	// What is left of it can be deleted like any instance.
	require.NoError(t, s.provider.Delete(ctx, instance.ID))
	if found, err := s.provider.Get(ctx, instance.ID); err == nil {
		require.Contains(t, []Status{StatusDeleted, StatusInterrupted}, found.Status)
	} else {
		require.ErrorIs(t, err, ErrNotFound)
	}
}

func newFakeSite(t *testing.T) site {
	fake := NewFake("fake", testOwner)
	fake.SetOffers(
		Offer{ID: "a10g-1x", Cloud: "fake", InstanceType: "a10g-1x", Region: "us-east", GPU: "A10G", GPUCount: 1, CPUMillicores: 8000, MemoryMB: 32 * 1024, HourlyCostMicros: 600_000},
		Offer{ID: "cpu-4", Cloud: "fake", InstanceType: "cpu-4", Region: "us-west", CPUMillicores: 4000, MemoryMB: 16 * 1024, HourlyCostMicros: 50_000},
		Offer{ID: "a10g-1x", Cloud: "fake", InstanceType: "a10g-1x", Region: "us-east", GPU: "A10G", GPUCount: 1, CPUMillicores: 8000, MemoryMB: 32 * 1024, HourlyCostMicros: 250_000, Spot: true},
	)
	known := map[string]Status{}
	for _, status := range []Status{StatusPending, StatusActive, StatusFailed, StatusInterrupted, StatusDeleted} {
		known[string(status)] = status
	}
	return site{
		provider: fake,
		statuses: known,
		report: func(id, reported string) {
			status, ok := known[reported]
			if !ok {
				status = StatusUnknown
			}
			fake.SetStatus(id, status, "")
		},
		refuse: fake.RefuseCreates,
		lose:   fake.Forget,
		bought: func() (string, string) {
			creates := fake.Creates()
			last := creates[len(creates)-1]
			return last.Name, last.StartupScript
		},
		plantForeign: func() string { return fake.Plant(foreignOwner, StatusActive, nil) },
		boughtSpot: func() bool {
			creates := fake.Creates()
			return creates[len(creates)-1].Offer.Spot
		},
		takeBack: func(id string) { fake.SetStatus(id, StatusInterrupted, "reclaimed") },
	}
}
