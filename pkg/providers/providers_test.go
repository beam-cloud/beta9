package providers

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every provider client, and the Fake that stands in for them, must behave
// the same way through the Provider interface. Each check below runs against
// all of them.
//
// A real client talks to a stand-in for its provider's API, which is with
// the client's own tests below. What a provider publishes without an account
// is recorded from it: Shadeform's, Verda's and Vast's catalogues, Nebius's
// and Google's spot prices. The other answers are built from the response
// examples and schemas in each provider's published API description, not
// recorded from a live account.
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

// sample returns one of the answers in sampleAnswers, which are the ones
// described above: what a provider's API said when it was asked, or the
// example in its API description. It may run on a stand-in's goroutine,
// where a test may be failed but not stopped.
func sample(t *testing.T, name string) []byte {
	answers, err := samples()
	assert.NoError(t, err)
	assert.Contains(t, answers, name)
	return answers[name]
}

var samples = sync.OnceValues(func() (map[string]json.RawMessage, error) {
	var answers map[string]json.RawMessage
	return answers, json.Unmarshal([]byte(sampleAnswers), &answers)
})

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

// What every client keeps to, whichever provider it speaks to. Each rule is
// tried on the clients where breaking it would cost something: a record
// dropped while its machine runs, a machine that is not this control
// plane's deleted, a pass that never ends.

// forbid makes a stand-in turn down every call from here on, as a provider
// does whose key has lost its rights. The call that gives out tokens is let
// through: the provider is there, and says no.
func forbid(server *httptest.Server) func() {
	return func() {
		provider := server.Config.Handler
		server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "token") {
				provider.ServeHTTP(w, r)
				return
			}
			http.Error(w, "this key may not do that", http.StatusForbidden)
		})
	}
}

// A provider that turns a call down has said nothing about the machine. Read
// as "gone", its answer would drop the record of a machine that goes on
// running; a list it refused, read as empty, would do so for every machine;
// and a delete it refused has deleted nothing. Each client passes the
// refusal on.
func TestACallThatIsRefusedIsNotReadAsAMachineThatIsGone(t *testing.T) {
	for name, open := range map[string]func(t *testing.T) (client Provider, offer Offer, refuse func()){
		"hetzner": func(t *testing.T) (Provider, Offer, func()) {
			api := newHetznerAPI(t)
			return api.client(testOwner), Offer{ID: "cpx31", Region: "ash"}, forbid(api.server)
		},
		"shadeform": func(t *testing.T) (Provider, Offer, func()) {
			api := newShadeformAPI(t)
			return api.client(testOwner), Offer{ID: "H100", Cloud: "scaleway", Region: "paris-france-1"}, forbid(api.server)
		},
		"vast": func(t *testing.T) (Provider, Offer, func()) {
			api := newVastAPI(t)
			return api.client(testOwner), vast5090, forbid(api.server)
		},
		"verda": func(t *testing.T) (Provider, Offer, func()) {
			api := newVerdaAPI(t)
			return api.client(testOwner), Offer{ID: "1L40S.20V", Region: "FIN-01"}, forbid(api.server)
		},
		"nebius": func(t *testing.T) (Provider, Offer, func()) {
			api := newNebiusAPI(t)
			return api.client(testOwner), nebiusH200, forbid(api.server)
		},
		"gcp": func(t *testing.T) (Provider, Offer, func()) {
			api := newGCPAPI(t)
			return api.client(testOwner), gcpL4InA, forbid(api.server)
		},
		"aws": func(t *testing.T) (Provider, Offer, func()) {
			cloud := newAWSCloud(t)
			return cloud.client(testOwner), awsA10GInA, func() {
				refusal := awsError("UnauthorizedOperation", "You are not authorized to perform this operation.")
				for _, ec2 := range cloud.regions {
					ec2.describeRefusal, ec2.terminateRefusal = refusal, refusal
				}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, offer, refuse := open(t)
			ctx := context.Background()
			created, err := client.Create(ctx, CreateRequest{Offer: offer, Name: "beam-machine-a1b2c3d4"})
			require.NoError(t, err)

			refuse()
			_, err = client.Get(ctx, created.ID)
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrNotFound)
			_, err = client.List(ctx)
			require.Error(t, err, "a list that was refused is not an empty list")
			require.Error(t, client.Delete(ctx, created.ID), "a delete that was refused has deleted nothing")
		})
	}
}

// A login that fails is not the provider's answer to the call it was for.
// Its status says nothing about the call: a token endpoint that answers 404
// has not said that a machine is gone, and one that is not well has had its
// own three tries.
func TestAFailedLoginIsNotTakenForTheCallsOwnAnswer(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		logins int
	}{
		"the token endpoint is not there":   {http.StatusNotFound, 1},
		"the credentials are turned down":   {http.StatusUnauthorized, 1},
		"the token endpoint is not well":    {http.StatusServiceUnavailable, maxAttempts},
		"the token endpoint is overwhelmed": {http.StatusTooManyRequests, maxAttempts},
	} {
		t.Run(name, func(t *testing.T) {
			logins, calls := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/token" {
					logins++
					http.Error(w, "no token for you", tc.status)
					return
				}
				calls++
			}))
			defer server.Close()
			noSleep := func(context.Context, time.Duration) error { return nil }
			login := newAPI(server.URL, withoutCredentials())
			token := newAccessToken(time.Now, func(ctx context.Context) (string, time.Duration, error) {
				return grant(ctx, login, call{path: "/token"})
			})
			api := newAPI(server.URL, token.bearer())
			login.sleep, api.sleep = noSleep, noSleep

			err := api.do(context.Background(), call{method: http.MethodGet, path: "/instances/1"})
			require.ErrorContains(t, err, "GET /instances/1: get an access token: POST /token: status ")
			require.ErrorContains(t, err, "no token for you")
			require.Zero(t, statusOf(err), "the call itself was not answered")
			require.False(t, errors.Is(err, errNoAnswer))
			require.Equal(t, tc.logins, logins, "the login's own attempts, and no more")
			require.Zero(t, calls, "nothing is sent without credentials")
		})
	}
}

// The worst reading of a failed login: a machine that cannot be asked about
// taken for one that is gone, and its record dropped while it runs.
func TestNebiusDoesNotTakeAFailedLoginForAMachineThatIsGone(t *testing.T) {
	api := newNebiusAPI(t)
	ctx := context.Background()
	created, err := api.client(testOwner).Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4"})
	require.NoError(t, err)

	moved := httptest.NewServer(http.NotFoundHandler())
	defer moved.Close()
	client := api.client(testOwner, func(config *types.NebiusProviderConfig) { config.TokenURL = moved.URL + "/oauth2/token/exchange" })

	_, err = client.Get(ctx, created.ID)
	require.ErrorContains(t, err, "get an access token")
	require.NotErrorIs(t, err, ErrNotFound)
	require.ErrorContains(t, client.Delete(ctx, created.ID), "get an access token")
	require.Empty(t, api.deletes)
}

// What List returns may be deleted as an orphan. Which instances are this
// control plane's is therefore not left to the provider's filter alone: each
// one that comes back is looked at.

func TestHetznerListReturnsOnlyItsOwnEvenIfHetznerReturnsMore(t *testing.T) {
	api := newHetznerAPI(t)
	ctx := context.Background()
	offer := Offer{ID: "cpx31", Region: "ash"}
	mine, err := api.client(testOwner).Create(ctx, CreateRequest{Offer: offer, Name: "beam-cpu-a1b2c3d4"})
	require.NoError(t, err)
	_, err = api.client(foreignOwner).Create(ctx, CreateRequest{Offer: offer, Name: "someone-elses"})
	require.NoError(t, err)

	api.mu.Lock()
	api.listsEverything = true
	api.mu.Unlock()
	listed, err := api.client(testOwner).List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, mine.ID, listed[0].ID)
}

func TestAWSListReturnsOnlyItsOwnEvenIfEC2ReturnsMore(t *testing.T) {
	cloud := newAWSCloud(t)
	ctx := context.Background()
	mine, err := cloud.client(testOwner).Create(ctx, CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4"})
	require.NoError(t, err)
	_, err = cloud.client(foreignOwner).Create(ctx, CreateRequest{Offer: awsA10GInA, Name: "someone-elses"})
	require.NoError(t, err)

	cloud.regions["us-east-1"].listsEverything = true
	listed, err := cloud.client(testOwner).List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, mine.ID, listed[0].ID)
}

// Nebius has no filter to leave it to: a project's instances all come back.
func TestNebiusListReturnsOnlyItsOwnOfAProjectsInstances(t *testing.T) {
	api := newNebiusAPI(t)
	ctx := context.Background()
	mine, err := api.client(testOwner).Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4"})
	require.NoError(t, err)
	_, err = api.client(foreignOwner).Create(ctx, CreateRequest{Offer: nebiusH200, Name: "someone-elses"})
	require.NoError(t, err)
	// An instance that nothing here bought has no owner at all.
	api.mu.Lock()
	api.instances = append(api.instances, map[string]any{
		"metadata": map[string]any{"id": "computeinstance-e00byhand", "parentId": nebiusNorth, "name": "by-hand"},
		"spec":     map[string]any{},
		"status":   map[string]any{"state": "RUNNING"},
	})
	api.mu.Unlock()

	listed, err := api.client(testOwner).List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, mine.ID, listed[0].ID)
}

// What a machine is bought with is not needed to read it or to delete it. A
// machine must not go on running because the image it booted was withdrawn,
// or because its region's network was changed.
func TestAWSReadsAndDeletesAMachineWhateverItsRegionLacksForBuying(t *testing.T) {
	for name, lack := range map[string]func(*awsEC2){
		"its image":           func(ec2 *awsEC2) { ec2.images = nil },
		"its default subnets": func(ec2 *awsEC2) { ec2.subnets = nil },
	} {
		t.Run(name, func(t *testing.T) {
			cloud := newAWSCloud(t)
			ctx := context.Background()
			tags := map[string]string{"beam-machine": "a1b2c3d4"}
			created, err := cloud.client(testOwner).Create(ctx, CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4", Tags: tags})
			require.NoError(t, err)

			// A gateway that starts now has read nothing of the region.
			lack(cloud.regions["us-east-1"])
			client := cloud.client(testOwner)
			_, err = client.Create(ctx, CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-e5f6a7b8"})
			require.Error(t, err, "nothing can be bought there")

			listed, err := client.List(ctx)
			require.NoError(t, err)
			require.Equal(t, []Instance{{ID: created.ID, Status: StatusPending, Tags: tags}}, listed)
			found, err := client.Get(ctx, created.ID)
			require.NoError(t, err)
			require.Equal(t, listed[0], found)

			require.NoError(t, client.Delete(ctx, created.ID))
			found, err = client.Get(ctx, created.ID)
			require.NoError(t, err)
			require.Equal(t, StatusDeleted, found.Status)
		})
	}
}

// Nebius lists a project's instances by the project alone. The subnet
// machines are put in is asked for when one is bought.
func TestNebiusListsItsMachinesWhateverItsProjectsLackForBuying(t *testing.T) {
	api := newNebiusAPI(t)
	ctx := context.Background()
	tags := map[string]string{"beam-machine": "a1b2c3d4"}
	created, err := api.client(testOwner).Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4", Tags: tags})
	require.NoError(t, err)

	// Someone adds a subnet to the other project, and a gateway starts.
	api.subnets[nebiusCentral] = append(api.subnets[nebiusCentral], "vpcsubnet-u00second")
	client := api.client(testOwner)
	_, err = client.Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-e5f6a7b8"})
	require.ErrorContains(t, err, "has 2 subnets", "nothing can be bought until the subnet is named")

	subnetsAsked := api.sent("GET", "/vpc/v1/subnets")
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []Instance{{ID: created.ID, Status: StatusPending, Tags: tags}}, listed)
	require.Equal(t, subnetsAsked, api.sent("GET", "/vpc/v1/subnets"), "listing asks about no subnet")

	require.NoError(t, client.Delete(ctx, created.ID))
	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusDeleted, found.Status)
}

// A project that is named twice cannot be bought in, and what was bought in
// it is listed once.
func TestNebiusListsAProjectOnceHoweverOftenItIsNamed(t *testing.T) {
	api := newNebiusAPI(t)
	ctx := context.Background()
	created, err := api.client(testOwner).Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4"})
	require.NoError(t, err)

	thrice := api.client(testOwner, func(config *types.NebiusProviderConfig) {
		config.Projects = []types.NebiusProjectConfig{{ID: nebiusNorth}, {ID: nebiusCentral}, {ID: nebiusNorth}}
	})
	listed, err := thrice.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []Instance{{ID: created.ID, Status: StatusPending}}, listed)
}

// An instance's ID is one part of the address a call goes to. Whatever it
// holds, it names an instance: it must not turn the call into one about
// another instance, or into another call.

func TestHetznerAnIDReachesNothingButItsServer(t *testing.T) {
	api := newHetznerAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: Offer{ID: "cpx31", Region: "ash"}, Name: "beam-cpu-a1b2c3d4"})
	require.NoError(t, err)

	for _, id := range []string{created.ID + "?", created.ID + "#", "0/../" + created.ID} {
		_, err := client.Get(ctx, id)
		require.ErrorIs(t, err, ErrNotFound, id)
		require.NoError(t, client.Delete(ctx, id), id)
	}
	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err, "none of those deleted it")
	require.Equal(t, StatusPending, found.Status)
}

func TestShadeformAnIDReachesNothingButItsInstance(t *testing.T) {
	api := newShadeformAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: Offer{ID: "H100", Cloud: "scaleway", Region: "paris-france-1"}, Name: "beam-h100-a1b2c3d4"})
	require.NoError(t, err)

	until := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{created.ID + "/info?", created.ID + "/delete#", "0/../" + created.ID} {
		_, err := client.Get(ctx, id)
		require.ErrorIs(t, err, ErrNotFound, id)
		require.Error(t, client.Extend(ctx, id, until), id)
		require.NoError(t, client.Delete(ctx, id), id)
	}
	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusPending, found.Status, "none of those deleted it")
	require.Empty(t, api.updates, "nor moved its timer")
}

// endless makes a stand-in answer one of its lists with page after page, and
// returns the count of them. It ends the list a little after a client should
// have given up, so that a client that reads on fails its test and does not
// hang it. page returns the nth page, and last a page that ends the list.
func endless(server *httptest.Server, path string, page func(n int) any, last any) *int {
	asked := new(int)
	next := server.Config.Handler
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != path {
			next.ServeHTTP(w, r)
			return
		}
		*asked++
		if *asked > maxPages+3 {
			writeJSON(w, http.StatusOK, last)
			return
		}
		writeJSON(w, http.StatusOK, page(*asked))
	})
	return asked
}

// A provider that never ends a list would keep a client reading for ever,
// and with it the pass that checks every machine. A list that long is
// given up on.
func TestAListThatDoesNotEndIsGivenUpOn(t *testing.T) {
	ctx := context.Background()
	token := func(n int) string { return fmt.Sprintf("page-%d", n+1) }
	full := func(size int, item func(i int) any) []any {
		items := make([]any, size)
		for i := range items {
			items[i] = item(i)
		}
		return items
	}

	for name, list := range map[string]func(t *testing.T) (asked *int, err error){
		"nebius": func(t *testing.T) (*int, error) {
			api := newNebiusAPI(t)
			asked := endless(api.server, "/compute/v1/instances",
				func(n int) any { return map[string]any{"items": []any{}, "nextPageToken": token(n)} },
				map[string]any{"items": []any{}})
			_, err := api.client(testOwner).List(ctx)
			return asked, err
		},
		"gcp": func(t *testing.T) (*int, error) {
			api := newGCPAPI(t)
			asked := endless(api.server, "/compute/v1/projects/"+gcpTestProject+"/aggregated/instances",
				func(n int) any { return map[string]any{"items": map[string]any{}, "nextPageToken": token(n)} },
				map[string]any{"items": map[string]any{}})
			_, err := api.client(testOwner).List(ctx)
			return asked, err
		},
		"vast": func(t *testing.T) (*int, error) {
			api := newVastAPI(t)
			asked := endless(api.server, "/api/v1/instances/",
				func(n int) any { return map[string]any{"instances": []any{}, "next_token": token(n)} },
				map[string]any{"instances": []any{}, "next_token": nil})
			_, err := api.client(testOwner).List(ctx)
			return asked, err
		},
		"hetzner": func(t *testing.T) (*int, error) {
			api := newHetznerAPI(t)
			pagination := func(next any) map[string]any {
				return map[string]any{"servers": []any{}, "meta": map[string]any{"pagination": map[string]any{"next_page": next}}}
			}
			asked := endless(api.server, "/servers", func(n int) any { return pagination(n + 1) }, pagination(nil))
			_, err := api.client(testOwner).List(ctx)
			return asked, err
		},
		// The next two end a list with a page that is not full.
		"shadeform": func(t *testing.T) (*int, error) {
			api := newShadeformAPI(t)
			someoneElses := full(shadeformPageSize, func(i int) any { return map[string]any{"id": fmt.Sprintf("instance-%d", i), "status": "active"} })
			asked := endless(api.server, "/instances",
				func(int) any { return map[string]any{"instances": someoneElses} },
				map[string]any{"instances": []any{}})
			_, err := api.client(testOwner).List(ctx)
			return asked, err
		},
		"verda": func(t *testing.T) (*int, error) {
			api := newVerdaAPI(t)
			someoneElses := full(verdaPageSize, func(i int) any { return map[string]any{"id": fmt.Sprintf("script-%d", i), "name": "someone else's"} })
			asked := endless(api.server, "/scripts", func(int) any { return someoneElses }, []any{})
			return asked, api.client(testOwner).deleteUnusedScripts(ctx, nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			asked, err := list(t)
			require.ErrorContains(t, err, "more than 1000 pages")
			require.Equal(t, maxPages, *asked)
		})
	}
}

// The AWS SDK reads a list page by page, by the token each page ends with.
// A list that keeps ending with the same token has no next page.
func TestAWSStopsReadingAListThatRepeatsItsToken(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	ctx := context.Background()
	_, err := client.Create(ctx, CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4"})
	require.NoError(t, err)

	for _, ec2 := range cloud.regions {
		ec2.repeatsToken, ec2.calls = true, map[string]int{}
	}
	// A gateway that starts now reads every list there is.
	client = cloud.client(testOwner)
	offers, err := client.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.Len(t, offers, 10)
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, listed)

	// Each list was read twice in each of the two regions: once for its
	// first page, and once to find that the next ends as the first did.
	for _, call := range []string{"DescribeSubnets", "DescribeInstanceTypes", "DescribeSpotPriceHistory", "DescribeInstances"} {
		require.Equal(t, 4, cloud.called(call), call)
	}
}

// Hetzner says in each page whether another follows. A page that does not
// say is not taken for the last.
func TestHetznerDoesNotTakeAPageItCannotReadForTheLast(t *testing.T) {
	api := newHetznerAPI(t)
	next := api.server.Config.Handler
	api.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/servers" {
			next.ServeHTTP(w, r)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"servers": []any{}, "meta": "soon"})
	})

	_, err := api.client(testOwner).List(context.Background())
	require.ErrorContains(t, err, "GET /servers")
	require.ErrorContains(t, err, "decode meta")
}

// A region that cannot be read is left out of the offers. The others are
// still bought in.
func TestAWSOffersTheRegionsItCanRead(t *testing.T) {
	for name, tc := range map[string]struct {
		lack func(*awsEC2)
		want string
	}{
		"a region without subnets": {
			lack: func(ec2 *awsEC2) { ec2.subnets = nil },
			want: "AWS region us-west-2: it has no default subnets",
		},
		"a region whose prices EC2 does not tell": {
			lack: func(ec2 *awsEC2) { ec2.priceRefusal = awsError("RequestLimitExceeded", "Request limit exceeded.") },
			want: "AWS Spot prices in us-west-2: api error RequestLimitExceeded",
		},
	} {
		t.Run(name, func(t *testing.T) {
			cloud := newAWSCloud(t)
			west := cloud.regions["us-west-2"]
			subnets := west.subnets
			tc.lack(west)
			client := cloud.client(testOwner)
			ctx := context.Background()

			offers, err := client.Offers(ctx, OfferFilter{})
			require.NoError(t, err)
			require.Len(t, offers, 9)
			for _, offer := range offers {
				require.Equal(t, "us-east-1", offer.Region)
			}

			// Asked for that region alone, nothing answered.
			_, err = client.Offers(ctx, OfferFilter{Regions: []string{"us-west-2"}})
			require.ErrorContains(t, err, tc.want)

			// It is asked again the next time, and not remembered.
			west.subnets, west.priceRefusal = subnets, nil
			offers, err = client.Offers(ctx, OfferFilter{})
			require.NoError(t, err)
			require.Len(t, offers, 10)
		})
	}
}

func TestAWSSaysWhatEachRegionLacksWhenNoneCanBeRead(t *testing.T) {
	cloud := newAWSCloud(t)
	cloud.regions["us-east-1"].images = nil
	cloud.regions["us-west-2"].subnets = nil

	_, err := cloud.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.ErrorContains(t, err, "AWS region us-east-1: it has no image")
	require.ErrorContains(t, err, "AWS region us-west-2: it has no default subnets")
}

func TestGCPOffersTheRegionsItCanRead(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*gcpAPI, *types.GCPProviderConfig)
		want   string
	}{
		"a region Google does not have": {
			change: func(_ *gcpAPI, config *types.GCPProviderConfig) {
				config.Regions = append(config.Regions, types.GCPProviderRegionConfig{Name: "mars-north1"})
			},
			want: "GCP region mars-north1: GET /projects/beam-test/regions/mars-north1: status 404",
		},
		"a region whose prices Google does not tell": {
			change: func(api *gcpAPI, config *types.GCPProviderConfig) {
				config.Regions = append(config.Regions, types.GCPProviderRegionConfig{Name: "mars-north1"})
				api.machineTypes["mars-north1-a"] = []map[string]any{gcpMachineTypeOf("g2-standard-32", 32, 128, "nvidia-l4", 1)}
				api.priceRefusals = map[string]gcpReply{"g2-standard-32": {http.StatusServiceUnavailable, "backendError", "Backend Error"}}
			},
			want: "GCP region mars-north1: the Spot price of g2-standard-32",
		},
	} {
		t.Run(name, func(t *testing.T) {
			api := newGCPAPI(t)
			client := api.client(testOwner, func(config *types.GCPProviderConfig) { tc.change(api, config) })
			ctx := context.Background()

			offers, err := client.Offers(ctx, OfferFilter{})
			require.NoError(t, err)
			for _, offer := range offers {
				require.NotEqual(t, "mars-north1", offer.Region)
			}
			require.NotEmpty(t, offers)

			// Asked for that region alone, nothing answered.
			_, err = client.Offers(ctx, OfferFilter{Regions: []string{"mars-north1"}})
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestGCPSaysWhatEachRegionLacksWhenNoneCanBeRead(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner, func(config *types.GCPProviderConfig) {
		config.Regions = []types.GCPProviderRegionConfig{{Name: "mars-north1"}, {Name: "asia-east1"}}
	})

	_, err := client.Offers(context.Background(), OfferFilter{})
	require.ErrorContains(t, err, "GCP region mars-north1: GET /projects/beam-test/regions/mars-north1: status 404")
	require.ErrorContains(t, err, "GCP region asia-east1: none of its 1 GPU machine types has a Spot price in dollars")
}

// The spend caps count on every machine having a price, and the cheapest
// offer is the one that is bought: an offer that costs nothing would be
// bought first and counted for nothing.

func TestShadeformDoesNotOfferWhatItCannotPrice(t *testing.T) {
	api := newShadeformAPI(t)
	api.catalogue = `{"instance_types": [{
		"cloud": "scaleway", "shade_instance_type": "L4", "deployment_type": "vm", "hourly_price": 0,
		"configuration": {"gpu_type": "L4", "num_gpus": 1, "vcpus": 8, "memory_in_gb": 48, "storage_in_gb": 500},
		"availability": [
			{"region": "paris-france-5", "available": true, "rental_type": "on_demand"},
			{"region": "paris-france-5", "available": true, "rental_type": "spot", "hourly_price": ""},
			{"region": "paris-france-1", "available": true, "rental_type": "spot", "hourly_price": "0"},
			{"region": "warsaw-poland-1", "available": true, "rental_type": "spot", "hourly_price": "0.30"}
		]
	}]}`

	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Equal(t, []Offer{{
		ID: "L4", Provider: "shadeform", Cloud: "scaleway", InstanceType: "L4", Region: "warsaw-poland-1",
		GPU: "L4", GPUCount: 1, CPUMillicores: 8000, MemoryMB: 48 * 1024, StorageMB: 500 * 1024, HourlyCostMicros: 300_000, Spot: true,
	}}, offers)
}

func TestShadeformOffersWhatItCanReadOfACatalogue(t *testing.T) {
	api := newShadeformAPI(t)
	// Shadeform describes sizes in whole numbers. One type here does not.
	api.catalogue = `{"instance_types": [{
		"cloud": "scaleway", "shade_instance_type": "L4", "deployment_type": "vm", "hourly_price": 95,
		"configuration": {"gpu_type": "L4", "num_gpus": 1, "vcpus": 8, "memory_in_gb": 47.5, "storage_in_gb": 500},
		"availability": [{"region": "paris-france-5", "available": true, "rental_type": "on_demand"}]
	}, {
		"cloud": "scaleway", "shade_instance_type": "H100", "deployment_type": "vm", "hourly_price": 330,
		"configuration": {"gpu_type": "H100", "num_gpus": 1, "vcpus": 24, "memory_in_gb": 240, "storage_in_gb": 1000},
		"availability": [{"region": "paris-france-1", "available": true, "rental_type": "on_demand"}]
	}]}`

	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Equal(t, []Offer{{
		ID: "H100", Provider: "shadeform", Cloud: "scaleway", InstanceType: "H100", Region: "paris-france-1",
		GPU: "H100", GPUCount: 1, CPUMillicores: 24000, MemoryMB: 240 * 1024, StorageMB: 1000 * 1024, HourlyCostMicros: 3_300_000,
	}}, offers, "the type that cannot be read is left out, and hides nothing")

	// A catalogue of which nothing can be read is not one that sells nothing.
	api.catalogue = `{"instance_types": [{"shade_instance_type": "L4", "configuration": {"num_gpus": "one"}}]}`
	_, err = api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.ErrorContains(t, err, "Shadeform's instance types could not be read: L4: ")
}

func TestVastDoesNotOfferWhatItCannotPrice(t *testing.T) {
	api := newVastAPI(t)
	// A host that asks nothing for its machine or for its disk.
	api.offer(90001, 1, map[string]any{"min_bid": 0.0, "storage_cost": 0.0})

	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Len(t, offers, 11)
	for _, offer := range offers {
		require.NotEqual(t, "90001/1", offer.ID)
		require.Positive(t, offer.HourlyCostMicros)
	}
}

func TestGPUsAreNamedAsBeamNamesThem(t *testing.T) {
	for catalog, want := range map[string]types.GpuType{
		"NVIDIA L40S":      types.GPU_L40S,
		"GeForce RTX 4090": types.GPU_RTX4090,
		// What EC2 calls the GPU of its g7e instances.
		"RTX PRO Server 6000": types.GPU_RTX_PRO6000,
		// Not the B200 and B300 their names contain.
		"GB200":            "GB200",
		"GB300 SXM6 288GB": "GB300 SXM6 288GB",
	} {
		require.Equal(t, want, gpuName(catalog), catalog)
	}
}

func TestOwnedTagsAddTheOwnerToTheCallersTags(t *testing.T) {
	callers := map[string]string{"beam-machine": "a1b2c3d4"}
	owned, err := ownedTags("beam-prod-1a2b3c", callers)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"beam-machine": "a1b2c3d4", OwnerTag: "beam-prod-1a2b3c"}, owned)
	require.Equal(t, map[string]string{"beam-machine": "a1b2c3d4"}, callers, "the caller's map is left alone")

	require.Equal(t, callers, callerTags(owned))
	require.Nil(t, callerTags(map[string]string{OwnerTag: "beam-prod-1a2b3c"}))
}

func TestOwnedTagsRefuseWhatSomeProviderCouldNotStore(t *testing.T) {
	for name, tags := range map[string]map[string]string{
		"key with underscore":   {"beam_pool": "a"},
		"key begins on a digit": {"1st-pool": "a"},
		"value with a dot":      {"pool": "a.b"},
		"empty value":           {"pool": ""},
		"value too long":        {"pool": strings.Repeat("a", 64)},
		"the owner tag itself":  {OwnerTag: "beam"},
	} {
		_, err := ownedTags("beam", tags)
		require.Error(t, err, name)
	}

	// Only a key has to begin on a letter, which Google asks of a label's.
	_, err := ownedTags("beam", map[string]string{"k": "v", strings.Repeat("b", 63): "0", "k8s": "1st"})
	require.NoError(t, err)

	// The owner is not looked at here: the registry checks it once, before
	// it makes a client.
	require.Error(t, checkOwner("Beam"))
	require.NoError(t, checkOwner("0"+strings.Repeat("a", 62)))
}

// Shadeform and Vast keep words with an instance, and no pairs.
func TestTagsAreWrittenAsWordsInTheOrderOfTheirKeysAndReadBack(t *testing.T) {
	tags := map[string]string{"beam-pool-machine": "machine-a1b2c3d4", "beam-pool": "rtx5090", OwnerTag: testOwner}
	words := tagWords(tags)
	require.Equal(t, []string{"beam-owner=" + testOwner, "beam-pool=rtx5090", "beam-pool-machine=machine-a1b2c3d4"}, words)
	require.Equal(t, tags, wordTags(words))

	// What somebody else wrote next to them is no tag.
	require.Equal(t, tags, wordTags(append([]string{"my-training-job", ""}, words...)))
	require.Empty(t, tagWords(nil))
	require.Empty(t, wordTags(nil))
}

func TestRegistryHoldsTheProvidersThatHaveCredentials(t *testing.T) {
	require.Empty(t, NewRegistry(types.ProviderConfig{}, testOwner).Names())

	registry := NewRegistry(types.ProviderConfig{
		Shadeform: types.ShadeformProviderConfig{ApiKey: "key"},
		Hetzner:   types.HetznerProviderConfig{ApiToken: "token"},
		Verda:     types.VerdaProviderConfig{ClientID: "id", ClientSecret: "secret"},
		Nebius:    types.NebiusProviderConfig{ServiceAccountID: "account", PublicKeyID: "key", PrivateKey: testServiceKeyPEM(t)},
		// AWS may have its credentials from where the gateway runs. It
		// is in use once it has a region to buy in.
		AWS:  types.AWSProviderConfig{Regions: []types.AWSProviderRegionConfig{{Name: "us-east-1"}}},
		GCP:  types.GCPProviderConfig{ServiceAccountKey: gcpTestKeyJSON(t, "https://oauth2.googleapis.com/token")},
		Vast: types.VastProviderConfig{ApiKey: "key"},
	}, testOwner)
	require.Equal(t, []string{"aws", "gcp", "hetzner", "nebius", "shadeform", "vast", "verda"}, registry.Names())

	shadeform, ok := registry.Get("shadeform")
	require.True(t, ok)
	require.Equal(t, "shadeform", shadeform.Name())

	_, ok = registry.Get("lambda")
	require.False(t, ok)
}

func TestRegistryThatWasNeverBuiltHasNoProviders(t *testing.T) {
	var registry *Registry
	_, ok := registry.Get("shadeform")
	require.False(t, ok)
	require.Empty(t, registry.Names())
}

func TestRegistryOfHoldsWhatItIsGiven(t *testing.T) {
	registry := RegistryOf(NewFake("b", testOwner), NewFake("a", testOwner))
	require.Equal(t, []string{"a", "b"}, registry.Names())
	provider, ok := registry.Get("a")
	require.True(t, ok)
	require.Equal(t, "a", provider.Name())
}

func TestConfiguredNamesIsNilSafeSortedAndCallerOwned(t *testing.T) {
	var missing *Registry
	require.Nil(t, missing.ConfiguredNames())
	require.Empty(t, RegistryOf().ConfiguredNames())
	registry := RegistryOf(NewFake("z", testOwner), NewFake("a", testOwner), NewFake("m", testOwner))
	names := registry.ConfiguredNames()
	require.Equal(t, []string{"a", "m", "z"}, names)
	names[0] = "changed"
	require.Equal(t, []string{"a", "m", "z"}, registry.ConfiguredNames())
}

func TestConfiguredNamesIncludesDisabledWithoutEnablingDiscovery(t *testing.T) {
	registry := NewRegistry(types.ProviderConfig{
		Disabled:  []string{"VAST"},
		Vast:      types.VastProviderConfig{ApiKey: "synthetic-key"},
		Shadeform: types.ShadeformProviderConfig{ApiKey: "synthetic-key"},
		Hetzner:   types.HetznerProviderConfig{ApiToken: "synthetic-key"},
	}, testOwner)
	require.Equal(t, []string{hetznerName, shadeformName, vastName}, registry.ConfiguredNames())
	require.Equal(t, []string{hetznerName, shadeformName}, registry.Names())
	require.False(t, registry.IsEnabled(vastName))
	_, configured := registry.Get(vastName)
	require.True(t, configured)
}

// The names under providers.disabled are written as an operator writes them:
// in any case, with spaces around them, and more than once.
func TestDisabledProvidersKeepCredentialsWithoutSellingMachines(t *testing.T) {
	var requests atomic.Int32
	vastAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer vastAPI.Close()
	registry := NewRegistry(types.ProviderConfig{
		Disabled:  []string{" AWS ", "gcp", "aws", "VAST"},
		AWS:       types.AWSProviderConfig{Regions: []types.AWSProviderRegionConfig{{Name: "us-east-1"}}},
		GCP:       types.GCPProviderConfig{ServiceAccountKey: gcpTestKeyJSON(t, "https://oauth2.googleapis.com/token")},
		Shadeform: types.ShadeformProviderConfig{ApiKey: "synthetic-key"},
		Vast:      types.VastProviderConfig{ApiKey: "synthetic-key", BaseURL: vastAPI.URL},
	}, testOwner)
	require.Equal(t, []string{shadeformName}, registry.Names())
	for _, name := range []string{awsName, gcpName, vastName} {
		client, ok := registry.Get(name)
		require.True(t, ok, "credentials remain available for existing-machine cleanup")
		require.False(t, registry.IsEnabled(name))
		offers, err := client.Offers(context.Background(), OfferFilter{})
		require.NoError(t, err)
		require.Empty(t, offers, "disabling must not contact a provider or expose offers")
		_, err = client.Create(context.Background(), CreateRequest{})
		require.ErrorIs(t, err, ErrDisabled)
		require.False(t, Unanswered(err), "a disabled create buys nothing")
	}
	require.Zero(t, requests.Load(), "disabled Vast discovery and purchases must consume no API quota")
}

func TestDisabledProviderStillManagesAndDeletesExistingMachines(t *testing.T) {
	fake := NewFake(awsName, testOwner)
	id := fake.Plant(testOwner, StatusActive, map[string]string{"beam-machine": "existing"})
	registry := RegistryOf(disabledProvider{Provider: fake})
	client, ok := registry.Get(awsName)
	require.True(t, ok)
	ctx := context.Background()
	got, err := client.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, StatusActive, got.Status)
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.NoError(t, client.Extend(ctx, id, time.Now().Add(time.Hour)))
	require.NoError(t, client.Delete(ctx, id))
	_, err = client.Get(ctx, id)
	require.ErrorIs(t, err, ErrNotFound)
}

// loggedError is a line of the log at the error level.
type loggedError struct {
	Provider string `json:"provider"`
	Error    string `json:"error"`
	Message  string `json:"message"`
}

// loggedErrors collects what is logged while the test runs, and returns the
// errors among it.
func loggedErrors(t *testing.T) func() []loggedError {
	t.Helper()
	var written bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(&written)
	t.Cleanup(func() { log.Logger = previous })
	return func() []loggedError {
		errs := []loggedError{}
		for lines := bufio.NewScanner(bytes.NewReader(written.Bytes())); lines.Scan(); {
			var line struct {
				Level string `json:"level"`
				loggedError
			}
			require.NoError(t, json.Unmarshal(lines.Bytes(), &line), lines.Text())
			if line.Level == "error" {
				errs = append(errs, line.loggedError)
			}
		}
		return errs
	}
}

// The owner goes on every machine as the value of a tag. One that some
// provider could not store is turned down once, where the clients are made,
// and none is made: a gateway starts all the same, and buys nothing.
func TestRegistryHasNoProvidersForAnOwnerThatCannotBeStored(t *testing.T) {
	config := types.ProviderConfig{
		Shadeform: types.ShadeformProviderConfig{ApiKey: "key"},
		Hetzner:   types.HetznerProviderConfig{ApiToken: "token"},
	}
	for name, owner := range map[string]string{
		"no owner":       "",
		"in capitals":    "Beam",
		"too long":       strings.Repeat("a", 64),
		"ends on a dash": "beam-",
	} {
		t.Run(name, func(t *testing.T) {
			errs := loggedErrors(t)
			require.Empty(t, NewRegistry(config, owner).Names())
			require.Len(t, errs(), 1)
			require.Contains(t, errs()[0].Error, "provider owner")
		})
	}

	errs := loggedErrors(t)
	require.Equal(t, []string{"hetzner", "shadeform"}, NewRegistry(config, strings.Repeat("a", 63)).Names())
	require.Empty(t, errs())
}

// A name under providers.disabled that is misspelt would leave the provider
// it was meant for buying. The log says which name is no provider's, and the
// rest of the list holds.
func TestRegistrySaysWhenADisabledNameIsNoProviders(t *testing.T) {
	const misspelt = "shadeforn"
	config := types.ProviderConfig{
		Shadeform: types.ShadeformProviderConfig{ApiKey: "key"},
		Hetzner:   types.HetznerProviderConfig{ApiToken: "token"},
	}

	errs := loggedErrors(t)
	config.Disabled = []string{misspelt, hetznerName}
	require.Equal(t, []string{shadeformName}, NewRegistry(config, testOwner).Names())
	require.Len(t, errs(), 1)
	require.Equal(t, misspelt, errs()[0].Provider)
	require.Contains(t, errs()[0].Message, "providers.disabled names no provider")

	// A provider without credentials sells nothing as it is, and an entry
	// that was left empty names nothing. Neither is a mistake.
	errs = loggedErrors(t)
	config.Disabled = []string{vastName, "", "  "}
	require.Equal(t, []string{hetznerName, shadeformName}, NewRegistry(config, testOwner).Names())
	require.Empty(t, errs())
}

// Every provider a registry can hold can be disabled: a provider that is
// added without its name here would be reported as misspelt.
func TestEveryProviderOfARegistryCanBeDisabled(t *testing.T) {
	config := types.ProviderConfig{
		Shadeform: types.ShadeformProviderConfig{ApiKey: "key"},
		Hetzner:   types.HetznerProviderConfig{ApiToken: "token"},
		Verda:     types.VerdaProviderConfig{ClientID: "id", ClientSecret: "secret"},
		Nebius:    types.NebiusProviderConfig{ServiceAccountID: "account", PublicKeyID: "key", PrivateKey: testServiceKeyPEM(t)},
		AWS:       types.AWSProviderConfig{Regions: []types.AWSProviderRegionConfig{{Name: "us-east-1"}}},
		GCP:       types.GCPProviderConfig{ServiceAccountKey: gcpTestKeyJSON(t, "https://oauth2.googleapis.com/token")},
		Vast:      types.VastProviderConfig{ApiKey: "key"},
	}
	all := NewRegistry(config, testOwner).Names()
	require.ElementsMatch(t, providerNames, all)

	errs := loggedErrors(t)
	config.Disabled = all
	registry := NewRegistry(config, testOwner)
	require.Empty(t, registry.Names())
	require.Equal(t, all, registry.ConfiguredNames())
	require.Empty(t, errs())
}

// A provider that has half of what it signs in with, or a key that cannot
// be read, could not make one call. It is left out, and the log says once
// which provider and what is wrong with it. The others are used.
func TestRegistryLeavesOutAProviderWhoseCredentialsAreIncompleteOrCannotBeRead(t *testing.T) {
	gcpKey := gcpTestKeyJSON(t, "https://oauth2.googleapis.com/token")
	nebius := types.NebiusProviderConfig{ServiceAccountID: nebiusTestAccount, PublicKeyID: nebiusTestKeyID, PrivateKey: testServiceKeyPEM(t)}
	without := func(change func(*types.NebiusProviderConfig)) types.NebiusProviderConfig {
		changed := nebius
		change(&changed)
		return changed
	}
	region := []types.AWSProviderRegionConfig{{Name: "us-east-1"}}

	for name, tc := range map[string]struct {
		config   types.ProviderConfig
		provider string
		want     string
	}{
		"verda without its secret": {
			config:   types.ProviderConfig{Verda: types.VerdaProviderConfig{ClientID: verdaTestClientID}},
			provider: "verda", want: "providers.verda.clientSecret is not set",
		},
		"verda without its id": {
			config:   types.ProviderConfig{Verda: types.VerdaProviderConfig{ClientSecret: verdaTestSecret}},
			provider: "verda", want: "providers.verda.clientId is not set",
		},
		"nebius without its account": {
			config:   types.ProviderConfig{Nebius: without(func(c *types.NebiusProviderConfig) { c.ServiceAccountID = "" })},
			provider: "nebius", want: "providers.nebius.serviceAccountId is not set",
		},
		"nebius without the ID of its key": {
			config:   types.ProviderConfig{Nebius: without(func(c *types.NebiusProviderConfig) { c.PublicKeyID = "" })},
			provider: "nebius", want: "providers.nebius.publicKeyId is not set",
		},
		"nebius without its key": {
			config:   types.ProviderConfig{Nebius: without(func(c *types.NebiusProviderConfig) { c.PrivateKey = "" })},
			provider: "nebius", want: "providers.nebius.privateKey is not set",
		},
		"nebius with a key that is not PEM": {
			config:   types.ProviderConfig{Nebius: without(func(c *types.NebiusProviderConfig) { c.PrivateKey = "c2VjcmV0" })},
			provider: "nebius", want: "providers.nebius.privateKey: the private key is not in PEM form",
		},
		"nebius projects and nothing to sign in with": {
			config:   types.ProviderConfig{Nebius: types.NebiusProviderConfig{Projects: []types.NebiusProjectConfig{{ID: nebiusNorth}}}},
			provider: "nebius", want: "providers.nebius.serviceAccountId is not set",
		},
		"aws keys and no region": {
			config:   types.ProviderConfig{AWS: types.AWSProviderConfig{AccessKeyID: "AKIABEAMTEST", SecretAccessKey: "secret"}},
			provider: "aws", want: "providers.aws.regions names no region",
		},
		"aws with half a key": {
			config:   types.ProviderConfig{AWS: types.AWSProviderConfig{AccessKeyID: "AKIABEAMTEST", Regions: region}},
			provider: "aws", want: "providers.aws.secretAccessKey is not set",
		},
		"gcp regions and no key": {
			config:   types.ProviderConfig{GCP: types.GCPProviderConfig{Regions: []types.GCPProviderRegionConfig{{Name: "us-central1"}}}},
			provider: "gcp", want: "providers.gcp.serviceAccountKey is not set",
		},
		"gcp project and no key": {
			config:   types.ProviderConfig{GCP: types.GCPProviderConfig{Project: gcpTestProject}},
			provider: "gcp", want: "providers.gcp.serviceAccountKey is not set",
		},
		"gcp key that is not JSON": {
			config:   types.ProviderConfig{GCP: types.GCPProviderConfig{ServiceAccountKey: "-----BEGIN PRIVATE KEY-----"}},
			provider: "gcp", want: "providers.gcp.serviceAccountKey is not a service account's JSON key",
		},
		"gcp key file without a key": {
			config:   types.ProviderConfig{GCP: types.GCPProviderConfig{ServiceAccountKey: `{"client_email": "` + gcpTestEmail + `", "project_id": "beam-test"}`}},
			provider: "gcp", want: "providers.gcp.serviceAccountKey: the private key is not in PEM form",
		},
		"gcp key that names no project": {
			config:   types.ProviderConfig{GCP: types.GCPProviderConfig{ServiceAccountKey: strings.Replace(gcpKey, `"project_id":"beam-test"`, `"project_id":""`, 1)}},
			provider: "gcp", want: "providers.gcp.project is not set, and the service account's key names no project",
		},
	} {
		t.Run(name, func(t *testing.T) {
			errs := loggedErrors(t)
			tc.config.Shadeform = types.ShadeformProviderConfig{ApiKey: "key"}
			require.Equal(t, []string{"shadeform"}, NewRegistry(tc.config, testOwner).Names())
			require.Len(t, errs(), 1)
			require.Equal(t, tc.provider, errs()[0].Provider)
			require.Contains(t, errs()[0].Error, tc.want)
		})
	}

	// A provider that nothing was set for is not in use, which is no
	// mistake. The default config has Hetzner's settings and no token.
	errs := loggedErrors(t)
	unused := types.ProviderConfig{
		Hetzner: types.HetznerProviderConfig{BaseURL: "https://api.hetzner.cloud/v1", Image: "ubuntu-24.04", DefaultRegions: []string{"ash", "hil"}},
		Vast:    types.VastProviderConfig{BaseURL: "https://console.vast.ai", DiskGB: 300},
		AWS:     types.AWSProviderConfig{ImageName: "an image", DiskGB: 300, KeyName: "ops"},
		GCP:     types.GCPProviderConfig{Image: "an image", DiskGB: 300},
		Nebius:  types.NebiusProviderConfig{ImageFamily: "a family", DiskGB: 300},
		Verda:   types.VerdaProviderConfig{Images: []string{"an image"}, DiskGB: 300},
	}
	require.Empty(t, NewRegistry(unused, testOwner).Names())
	require.Empty(t, errs())

	// What is complete is used, and nothing is said of it.
	complete := types.ProviderConfig{
		Verda:  types.VerdaProviderConfig{ClientID: verdaTestClientID, ClientSecret: verdaTestSecret},
		Nebius: nebius,
		AWS:    types.AWSProviderConfig{AccessKeyID: "AKIABEAMTEST", SecretAccessKey: "secret", Regions: region},
		GCP:    types.GCPProviderConfig{ServiceAccountKey: gcpKey},
	}
	require.Equal(t, []string{"aws", "gcp", "nebius", "verda"}, NewRegistry(complete, testOwner).Names())
	require.Empty(t, errs())
}

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

func TestFakeLosesAnAnswerAsAProviderDoes(t *testing.T) {
	ctx := context.Background()
	fake := NewFake("fake", testOwner)
	offer := Offer{ID: "a10g-1x", Cloud: "fake", Region: "us-east"}
	fake.SetOffers(offer)
	fake.LoseAnswers(1)

	_, err := fake.Create(ctx, CreateRequest{Offer: offer, Name: "a", Tags: map[string]string{"machine": "m-1"}})
	require.ErrorContains(t, err, "the connection was reset before the answer arrived")
	require.True(t, Unanswered(err), "the buyer cannot tell what became of the purchase")

	// The machine was made all the same, and is known by its tags.
	made, err := fake.List(ctx)
	require.NoError(t, err)
	require.Len(t, made, 1)
	require.Equal(t, "m-1", made[0].Tags["machine"])

	_, err = fake.Create(ctx, CreateRequest{Offer: offer, Name: "b"})
	require.NoError(t, err, "the next answer arrives")
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

// flakyAPI answers each request with the next of its replies, and the last
// one from then on.
type flakyAPI struct {
	server *httptest.Server
	api    *api

	mu      sync.Mutex
	replies []reply
	seen    int
	waits   []time.Duration
}

type reply struct {
	status     int
	body       string
	retryAfter string
	// hangUp closes the connection without answering.
	hangUp bool
}

func newFlakyAPI(t *testing.T, replies ...reply) *flakyAPI {
	flaky := &flakyAPI{replies: replies}
	flaky.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flaky.mu.Lock()
		next := flaky.replies[min(flaky.seen, len(flaky.replies)-1)]
		flaky.seen++
		flaky.mu.Unlock()

		if next.hangUp {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		if next.retryAfter != "" {
			w.Header().Set("Retry-After", next.retryAfter)
		}
		w.WriteHeader(next.status)
		w.Write([]byte(next.body))
	}))
	t.Cleanup(flaky.server.Close)
	flaky.api = newAPI(flaky.server.URL+"/", withHeader("X-Key", "secret"))
	flaky.api.sleep = func(ctx context.Context, d time.Duration) error {
		flaky.waits = append(flaky.waits, d)
		return ctx.Err()
	}
	return flaky
}

func (f *flakyAPI) requests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen
}

var (
	ok          = reply{status: http.StatusOK, body: `{"id":"abc"}`}
	unavailable = reply{status: http.StatusServiceUnavailable, body: "try later"}
	hangUp      = reply{hangUp: true}
)

type answer struct {
	ID string `json:"id"`
}

func TestAPISendsJSONWithCredentialsAndDecodesTheAnswer(t *testing.T) {
	var got *http.Request
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Write([]byte(`{"id":"abc"}`))
	}))
	defer server.Close()
	api := newAPI(server.URL, withHeader("X-Key", "secret"))

	var out answer
	require.NoError(t, api.do(context.Background(), call{method: http.MethodPost, path: "/things?x=1", body: map[string]int{"n": 2}, out: &out}))

	require.Equal(t, "abc", out.ID)
	require.Equal(t, "/things?x=1", got.URL.RequestURI())
	require.Equal(t, "secret", got.Header.Get("X-Key"))
	require.Equal(t, "application/json", got.Header.Get("Content-Type"))
	require.Equal(t, "application/json", got.Header.Get("Accept"))
	require.JSONEq(t, `{"n":2}`, gotBody)
}

// A token endpoint takes a form, not JSON.
func TestAPISendsAFormWhenGivenOne(t *testing.T) {
	var contentType, gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Write([]byte(`{"id":"abc"}`))
	}))
	defer server.Close()
	api := newAPI(server.URL, withoutCredentials())

	var out answer
	form := url.Values{"grant_type": {"urn:example:grant"}, "assertion": {"a.b c"}}
	require.NoError(t, api.do(context.Background(), call{method: http.MethodPost, form: form, out: &out}))

	require.Equal(t, "abc", out.ID)
	require.Equal(t, "application/x-www-form-urlencoded", contentType)
	require.Equal(t, "assertion=a.b+c&grant_type=urn%3Aexample%3Agrant", gotBody)
}

func TestAPIReadsAgainWhenTheProviderFailsOrDoesNotAnswer(t *testing.T) {
	for name, first := range map[string]reply{"failed": unavailable, "no answer": hangUp} {
		t.Run(name, func(t *testing.T) {
			flaky := newFlakyAPI(t, first, first, ok)

			var out answer
			require.NoError(t, flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things", out: &out}))
			require.Equal(t, "abc", out.ID)
			require.Equal(t, 3, flaky.requests())
			require.Equal(t, []time.Duration{500 * time.Millisecond, time.Second}, flaky.waits)
		})
	}
}

func TestAPIGivesUpAfterThreeAttempts(t *testing.T) {
	flaky := newFlakyAPI(t, unavailable)

	err := flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things"})
	require.Equal(t, http.StatusServiceUnavailable, statusOf(err))
	require.Equal(t, 3, flaky.requests())
}

func TestAPIDoesNotRepeatAPurchaseThatMayHaveGoneThrough(t *testing.T) {
	for name, first := range map[string]reply{"failed": unavailable, "no answer": hangUp} {
		t.Run(name, func(t *testing.T) {
			flaky := newFlakyAPI(t, first, ok)

			err := flaky.api.do(context.Background(), call{method: http.MethodPost, path: "/things", body: struct{}{}})
			require.Error(t, err)
			require.True(t, Unanswered(err), "the buyer is told that it may have")
			require.True(t, Unanswered(fmt.Errorf("buy a machine: %w", err)))
			require.Equal(t, 1, flaky.requests())
			require.Empty(t, flaky.waits)
		})
	}
}

func TestAPIRepeatsAWriteThatIsSafeToRepeat(t *testing.T) {
	flaky := newFlakyAPI(t, hangUp, unavailable, ok)

	require.NoError(t, flaky.api.do(context.Background(), call{method: http.MethodDelete, path: "/things/1", repeatable: true}))
	require.Equal(t, 3, flaky.requests())
}

func TestAPIFetchesCredentialsAgainOnceWhenTheyAreTurnedDown(t *testing.T) {
	unauthorized := reply{status: http.StatusUnauthorized, body: "token expired"}
	purchase := call{method: http.MethodPost, path: "/things", body: struct{}{}}

	flaky := newFlakyAPI(t, unauthorized, ok)
	renewed := 0
	flaky.api.authorize.renew = func() { renewed++ }
	require.NoError(t, flaky.api.do(context.Background(), purchase))
	require.Equal(t, 2, flaky.requests())
	require.Equal(t, 1, renewed)
	require.Empty(t, flaky.waits)

	// New credentials that are turned down too are the wrong credentials.
	flaky = newFlakyAPI(t, unauthorized)
	renewed = 0
	flaky.api.authorize.renew = func() { renewed++ }
	err := flaky.api.do(context.Background(), purchase)
	require.Equal(t, http.StatusUnauthorized, statusOf(err))
	require.Equal(t, 2, flaky.requests())
	require.Equal(t, 1, renewed)
}

func TestAPIDoesNotRepeatACallTheProviderRefused(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusNotFound, http.StatusConflict} {
		flaky := newFlakyAPI(t, reply{status: status, body: "no"}, ok)

		err := flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things"})
		require.Equal(t, status, statusOf(err))
		require.False(t, Unanswered(err), "a refusal is the provider's answer")
		require.Equal(t, 1, flaky.requests())
	}
}

func TestAPIWaitsAsLongAsARateLimitAsks(t *testing.T) {
	flaky := newFlakyAPI(t, reply{status: http.StatusTooManyRequests, retryAfter: "3"}, reply{status: http.StatusTooManyRequests}, ok)

	// A rate-limited call was turned away before it did anything, so even a
	// purchase is sent again.
	require.NoError(t, flaky.api.do(context.Background(), call{method: http.MethodPost, path: "/things", body: struct{}{}}))
	require.Equal(t, 3, flaky.requests())
	require.Equal(t, []time.Duration{3 * time.Second, time.Second}, flaky.waits)
}

func TestAPIDoesNotWaitOutALongRateLimit(t *testing.T) {
	flaky := newFlakyAPI(t, reply{status: http.StatusTooManyRequests, retryAfter: "60"}, ok)

	err := flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things"})
	require.Equal(t, http.StatusTooManyRequests, statusOf(err))
	require.False(t, Unanswered(err), "a call that was turned away did nothing")
	require.Equal(t, 1, flaky.requests())
	require.Empty(t, flaky.waits)
}

func TestAPIStopsWhenTheCallerGivesUp(t *testing.T) {
	flaky := newFlakyAPI(t, unavailable)
	ctx, cancel := context.WithCancel(context.Background())
	flaky.api.sleep = func(ctx context.Context, d time.Duration) error {
		cancel()
		return ctx.Err()
	}

	err := flaky.api.do(ctx, call{method: http.MethodGet, path: "/things"})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, flaky.requests())
}

func TestAPIGivesAProviderThirtySecondsToAnswer(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer server.Close()
	defer close(release)

	var deadline time.Duration
	api := newAPI(server.URL, withHeader("X-Key", "secret"))
	api.client = &http.Client{Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
		until, has := req.Context().Deadline()
		require.True(t, has, "a call to a provider always has a deadline")
		deadline = time.Until(until)
		return nil, errors.New("not sent")
	})}

	require.Error(t, api.do(context.Background(), call{method: http.MethodPost, path: "/things"}))
	require.InDelta(t, requestTimeout.Seconds(), deadline.Seconds(), 1)
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// A refusal is kept whole, so that a client can read why. What is shown of
// it, in logs and to users, is its start.
func TestStatusErrorShowsTheStartOfALongAnswer(t *testing.T) {
	flaky := newFlakyAPI(t, reply{status: http.StatusBadRequest, body: strings.Repeat("x", 5000) + "y"})

	err := flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things"})
	var status *StatusError
	require.ErrorAs(t, err, &status)
	require.Len(t, status.Body, 5001)
	require.Equal(t, shownErrorBytes, strings.Count(err.Error(), "x"))
	require.NotContains(t, err.Error(), "y")
	require.Equal(t, http.MethodGet, status.Method)
	require.Equal(t, "/things", status.Path)
}

func TestAPIReportsAnAnswerItCannotDecode(t *testing.T) {
	flaky := newFlakyAPI(t, reply{status: http.StatusOK, body: "<html>"})

	var out answer
	err := flaky.api.do(context.Background(), call{method: http.MethodGet, path: "/things", out: &out})
	require.ErrorContains(t, err, "GET /things: decode response")
	require.Equal(t, 1, flaky.requests(), "an answer that arrived is not asked for again")
}

// What is shown of a refusal goes into logs and into messages to users,
// which take text only. A cut can fall inside a character.
func TestStatusErrorShowsValidTextWhereTheCutSplitsACharacter(t *testing.T) {
	refusal := &StatusError{Method: http.MethodGet, Path: "/things", Status: http.StatusBadRequest, Body: "x" + strings.Repeat("é", shownErrorBytes)}
	shown := refusal.Error()
	require.True(t, utf8.ValidString(shown), shown)
	// One byte of x, then two for each é: the cut falls inside the 512th.
	require.Equal(t, shownErrorBytes/2-1, strings.Count(shown, "é"))

	// Nor is what a provider sent always text.
	garbled := &StatusError{Method: http.MethodGet, Path: "/things", Status: http.StatusBadGateway, Body: "bad \xff\xfe gateway"}
	require.Equal(t, "GET /things: status 502: bad  gateway", garbled.Error())
}

func TestAccessTokenIsFetchedOnceAndAgainBeforeItExpires(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	fetched := 0
	token := newAccessToken(func() time.Time { return now }, func(context.Context) (string, time.Duration, error) {
		fetched++
		return fmt.Sprintf("token-%d", fetched), time.Hour, nil
	})
	header := func() string {
		req, err := http.NewRequest(http.MethodGet, "https://provider.example/", nil)
		require.NoError(t, err)
		require.NoError(t, token.bearer().add(context.Background(), req))
		return req.Header.Get("Authorization")
	}

	require.Equal(t, "Bearer token-1", header())
	now = now.Add(time.Hour - renewTokenBefore - time.Second)
	require.Equal(t, "Bearer token-1", header())
	require.Equal(t, 1, fetched)

	// From here a call might arrive after the token expired.
	now = now.Add(time.Second)
	require.Equal(t, "Bearer token-2", header())
	require.Equal(t, 2, fetched)
}

func TestAccessTokenThatCannotBeFetchedFailsTheCallAndIsTriedAgain(t *testing.T) {
	refused := errors.New("invalid client secret")
	fail := true
	token := newAccessToken(time.Now, func(context.Context) (string, time.Duration, error) {
		if fail {
			return "", 0, refused
		}
		return "token", time.Hour, nil
	})

	_, err := token.get(context.Background())
	require.ErrorIs(t, err, refused)

	fail = false
	got, err := token.get(context.Background())
	require.NoError(t, err)
	require.Equal(t, "token", got)
}

func TestAnEmptyAccessTokenIsNotUsed(t *testing.T) {
	token := newAccessToken(time.Now, func(context.Context) (string, time.Duration, error) { return "", time.Hour, nil })
	_, err := token.get(context.Background())
	require.Error(t, err)
}

// A service account's key comes as PKCS#8 or, from older tools, as PKCS#1.
// One that was put in an environment variable or copied out of a JSON file
// has its line breaks spelled out.
func TestAServiceAccountKeyIsReadInEveryFormItIsHandedOutIn(t *testing.T) {
	key := testServiceKey()
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	newer := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}))
	older := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))

	for name, text := range map[string]string{
		"PKCS#8":                  newer,
		"PKCS#1":                  older,
		"line breaks spelled out": strings.ReplaceAll(newer, "\n", `\n`),
	} {
		read, err := rsaKey(text)
		require.NoError(t, err, name)
		require.True(t, key.Equal(read), name)
	}

	elliptical, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(elliptical)
	require.NoError(t, err)
	for name, text := range map[string]string{
		"not PEM":        "c2VjcmV0",
		"not a key":      string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("secret")})),
		"not an RSA key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
	} {
		_, err := rsaKey(text)
		require.Error(t, err, name)
	}
}

func TestASignedRequestCarriesItsClaimsAndCanBeVerified(t *testing.T) {
	key := testServiceKey()
	signed, err := signedJWT(key, "key-1", map[string]any{"iss": "account", "exp": 1794623400})
	require.NoError(t, err)

	parts := strings.Split(signed, ".")
	require.Len(t, parts, 3)
	decode := func(part string) map[string]any {
		data, err := base64.RawURLEncoding.DecodeString(part)
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(data, &decoded))
		return decoded
	}
	require.Equal(t, map[string]any{"alg": "RS256", "typ": "JWT", "kid": "key-1"}, decode(parts[0]))
	require.Equal(t, map[string]any{"iss": "account", "exp": float64(1794623400)}, decode(parts[1]))

	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	require.NoError(t, rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], signature))

	// A key that has no ID is not given one.
	unnamed, err := signedJWT(key, "", map[string]any{})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"alg": "RS256", "typ": "JWT"}, decode(strings.Split(unnamed, ".")[0]))
}

func TestAPriceInDollarsIsReadFromANumberOrAString(t *testing.T) {
	for text, micros := range map[string]int64{`"44"`: 44_000_000, `"1.50"`: 1_500_000, `0.375`: 375_000, `"0.01920"`: 19_200, `""`: 0, `null`: 0} {
		price := dollars(1)
		require.NoError(t, json.Unmarshal([]byte(text), &price), text)
		require.Equal(t, micros, int64(price), text)
	}
	var price dollars
	require.Error(t, json.Unmarshal([]byte(`"a lot"`), &price))
}

func TestACountIsReadFromANumberOrAString(t *testing.T) {
	for text, want := range map[string]int64{`8`: 8, `"8"`: 8, `"1600"`: 1600, `0`: 0, `""`: 0, `null`: 0} {
		n := count(1)
		require.NoError(t, json.Unmarshal([]byte(text), &n), text)
		require.Equal(t, want, int64(n), text)
	}
	var n count
	require.Error(t, json.Unmarshal([]byte(`"many"`), &n))
	require.Error(t, json.Unmarshal([]byte(`1.5`), &n))
}

// apiStandIn is what the stand-ins for the providers' HTTP APIs share: the
// server, the time there, and a record of what the client asked of it. A
// stand-in answers each request with mu held.
type apiStandIn struct {
	t      *testing.T
	server *httptest.Server

	mu  sync.Mutex
	now time.Time
	// requests are the method and path of each request, in order.
	requests []string
	// creates are the bodies of the creates that were asked for, in order.
	creates []map[string]any
}

func (a *apiStandIn) clock() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.now
}

func (a *apiStandIn) after(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.now = a.now.Add(d)
}

// sent counts the requests of the method to paths that begin with path.
func (a *apiStandIn) sent(method, path string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	sent := 0
	for _, request := range a.requests {
		if strings.HasPrefix(request, method+" "+path) {
			sent++
		}
	}
	return sent
}

// asked returns the requests received since the last call.
func (a *apiStandIn) asked() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	requests := a.requests
	a.requests = nil
	return requests
}

func (a *apiStandIn) lastCreate() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NotEmpty(a.t, a.creates)
	return a.creates[len(a.creates)-1]
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// awsCloud stands in for EC2. The client talks to it through the AWS SDK's
// own request and response types, so what is checked here is what the client
// asks for and what it makes of the answers, not how the SDK puts them on
// the wire. The stand-in keeps the rules of EC2's API reference for the
// calls this client makes: filters, idempotency tokens, error codes, and
// which states an instance goes through. The instance types are a few of
// AWS's published ones. The Spot prices are made up, because EC2 tells them
// to its customers only.
type awsCloud struct {
	t   *testing.T
	mu  sync.Mutex
	now time.Time
	// regions holds each region's EC2.
	regions map[string]*awsEC2
}

type awsEC2 struct {
	cloud  *awsCloud
	region string

	types     []ec2types.InstanceTypeInfo
	prices    []ec2types.SpotPrice
	subnets   []ec2types.Subnet
	images    []ec2types.Image
	instances []ec2types.Instance
	runs      []*ec2.RunInstancesInput
	// byToken remembers which instance an idempotency token ran.
	byToken map[string]string
	refusal error
	// terminateRefusal, when set, is the answer to every terminate.
	terminateRefusal error
	// describeRefusal, when set, is the answer to every question about
	// instances.
	describeRefusal error
	// priceRefusal, when set, is the answer to every question about prices.
	priceRefusal error
	// listsEverything makes the instance list ignore the filters asked for.
	listsEverything bool
	nextID          int
	calls           map[string]int
	// pageOf, when set, is the most a list returns at a time.
	pageOf int
	// repeatsToken makes every page of a list end with the same token, as
	// a list that never ends does. repeated counts those pages.
	repeatsToken bool
	repeated     int
}

const (
	awsTestImage    = "ami-0beam0000000newer"
	awsTestOldImage = "ami-0beam0000000older"
)

func awsTypeInfo(name string, vcpus int32, memoryGiB int64, gpu string, gpus, gpuMiB int32) ec2types.InstanceTypeInfo {
	return ec2types.InstanceTypeInfo{
		InstanceType:          ec2types.InstanceType(name),
		VCpuInfo:              &ec2types.VCpuInfo{DefaultVCpus: aws.Int32(vcpus)},
		MemoryInfo:            &ec2types.MemoryInfo{SizeInMiB: aws.Int64(memoryGiB * 1024)},
		ProcessorInfo:         &ec2types.ProcessorInfo{SupportedArchitectures: []ec2types.ArchitectureType{ec2types.ArchitectureTypeX8664}},
		SupportedUsageClasses: []ec2types.UsageClassType{ec2types.UsageClassTypeOnDemand, ec2types.UsageClassTypeSpot},
		GpuInfo: &ec2types.GpuInfo{Gpus: []ec2types.GpuDeviceInfo{{
			Name:         aws.String(gpu),
			Manufacturer: aws.String("NVIDIA"),
			Count:        aws.Int32(gpus),
			MemoryInfo:   &ec2types.GpuDeviceMemoryInfo{SizeInMiB: aws.Int32(gpuMiB)},
		}}},
	}
}

func newAWSCloud(t *testing.T) *awsCloud {
	cloud := &awsCloud{t: t, now: time.Date(2026, 11, 14, 3, 0, 0, 0, time.UTC), regions: map[string]*awsEC2{}}

	// What every region sells. The last five are not for this client: a
	// share of a GPU, a GPU that is not NVIDIA's, an Arm machine, a type
	// that is not sold as Spot and so has no Spot price, and a machine
	// without a GPU.
	share := awsTypeInfo("g6f.large", 2, 8, "L4", 1, 3072)
	share.GpuInfo.Gpus[0].GpuPartitionSize = aws.Float64(0.125)
	radeon := awsTypeInfo("g4ad.xlarge", 4, 16, "Radeon Pro V520", 1, 8192)
	radeon.GpuInfo.Gpus[0].Manufacturer = aws.String("AMD")
	arm := awsTypeInfo("g5g.xlarge", 4, 8, "T4g", 1, 16384)
	arm.ProcessorInfo.SupportedArchitectures = []ec2types.ArchitectureType{ec2types.ArchitectureTypeArm64}
	onDemandOnly := awsTypeInfo("p2.xlarge", 4, 61, "K80", 1, 12288)
	onDemandOnly.SupportedUsageClasses = []ec2types.UsageClassType{ec2types.UsageClassTypeOnDemand}
	plain := awsTypeInfo("m5.large", 2, 8, "", 0, 0)
	plain.GpuInfo = nil
	sold := []ec2types.InstanceTypeInfo{
		awsTypeInfo("g4dn.xlarge", 4, 16, "T4", 1, 16384),
		awsTypeInfo("g5.xlarge", 4, 16, "A10G", 1, 24576),
		awsTypeInfo("g6e.xlarge", 4, 32, "L40S", 1, 46068),
		awsTypeInfo("p4d.24xlarge", 96, 1152, "A100", 8, 40960),
		awsTypeInfo("p4de.24xlarge", 96, 1152, "A100", 8, 81920),
		awsTypeInfo("p5.48xlarge", 192, 2048, "H100", 8, 81920),
		awsTypeInfo("p3.2xlarge", 8, 61, "V100", 1, 16384),
		awsTypeInfo("p3dn.24xlarge", 96, 768, "V100", 8, 32768),
		awsTypeInfo("g7e.2xlarge", 8, 64, "RTX PRO Server 6000", 1, 98304),
		share, radeon, arm, onDemandOnly, plain,
	}

	image := func(id, name, created string, architecture ec2types.ArchitectureValues) ec2types.Image {
		return ec2types.Image{
			ImageId: aws.String(id), Name: aws.String(name), CreationDate: aws.String(created), Architecture: architecture,
			State: ec2types.ImageStateAvailable, OwnerId: aws.String("898082745236"), ImageOwnerAlias: aws.String("amazon"), RootDeviceName: aws.String("/dev/sda1"),
		}
	}
	for region, zones := range map[string][]string{"us-east-1": {"a", "b", "c"}, "us-west-2": {"a"}} {
		ec2 := &awsEC2{cloud: cloud, region: region, types: sold, byToken: map[string]string{}, calls: map[string]int{}}
		for _, zone := range zones {
			// us-east-1c has a subnet, and no default one.
			ec2.subnets = append(ec2.subnets, ec2types.Subnet{
				SubnetId:         aws.String("subnet-" + region + zone),
				AvailabilityZone: aws.String(region + zone),
				VpcId:            aws.String("vpc-" + region),
				DefaultForAz:     aws.Bool(region+zone != "us-east-1c"),
			})
		}
		ec2.images = []ec2types.Image{
			image(awsTestOldImage, "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 22.04) 20260901", "2026-09-01T10:00:00.000Z", ec2types.ArchitectureValuesX8664),
			image(awsTestImage, "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 22.04) 20261101", "2026-11-01T10:00:00.000Z", ec2types.ArchitectureValuesX8664),
			image("ami-0beam000000000arm", "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 22.04) 20261105", "2026-11-05T10:00:00.000Z", ec2types.ArchitectureValuesArm64),
			image("ami-0beam00000ubuntu24", "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 24.04) 20261110", "2026-11-10T10:00:00.000Z", ec2types.ArchitectureValuesX8664),
		}
		// Anyone may publish an image under Amazon's name for one.
		lookalike := image("ami-0beam0000someones", "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 22.04) 20261112", "2026-11-12T10:00:00.000Z", ec2types.ArchitectureValuesX8664)
		lookalike.OwnerId, lookalike.ImageOwnerAlias = aws.String("123456789012"), nil
		ec2.images = append(ec2.images, lookalike)
		cloud.regions[region] = ec2
	}

	price := func(region, zone, instanceType, dollars string, age time.Duration) {
		cloud.regions[region].prices = append(cloud.regions[region].prices, ec2types.SpotPrice{
			AvailabilityZone: aws.String(region + zone), InstanceType: ec2types.InstanceType(instanceType),
			ProductDescription: ec2types.RIProductDescription("Linux/UNIX"), SpotPrice: aws.String(dollars), Timestamp: aws.Time(cloud.now.Add(-age)),
		})
	}
	price("us-east-1", "a", "g5.xlarge", "0.402100", 3*time.Hour)
	// The price a zone had before is not its price.
	price("us-east-1", "b", "g5.xlarge", "0.450000", 9*time.Hour)
	price("us-east-1", "b", "g5.xlarge", "0.389900", 2*time.Hour)
	// A zone the account has no subnet to use in.
	price("us-east-1", "c", "g5.xlarge", "0.350000", time.Hour)
	price("us-east-1", "b", "g4dn.xlarge", "0.157800", 5*time.Hour)
	price("us-east-1", "a", "g6e.xlarge", "0.930400", 4*time.Hour)
	price("us-east-1", "a", "p4d.24xlarge", "9.661700", time.Hour)
	price("us-east-1", "b", "p4de.24xlarge", "12.404000", time.Hour)
	price("us-east-1", "a", "p3.2xlarge", "0.918000", time.Hour)
	price("us-east-1", "a", "p3dn.24xlarge", "9.364800", time.Hour)
	price("us-east-1", "b", "g7e.2xlarge", "1.336000", time.Hour)
	// The types this client does not buy have Spot prices too.
	price("us-east-1", "a", "g6f.large", "0.080000", time.Hour)
	price("us-east-1", "a", "g4ad.xlarge", "0.120000", time.Hour)
	price("us-east-1", "a", "g5g.xlarge", "0.140000", time.Hour)
	price("us-east-1", "a", "m5.large", "0.035000", time.Hour)
	price("us-west-2", "a", "g5.xlarge", "0.512300", time.Hour)
	// The same machine with Windows on it costs more, whenever its price
	// was set.
	windows := cloud.regions["us-west-2"].prices[0]
	windows.ProductDescription, windows.SpotPrice, windows.Timestamp = "Windows", aws.String("0.696300"), aws.Time(cloud.now.Add(-time.Minute))
	cloud.regions["us-west-2"].prices = append(cloud.regions["us-west-2"].prices, windows)
	return cloud
}

// client returns a client of the stand-in's EC2, configured as a deployment
// is unless the test changes it.
func (cloud *awsCloud) client(owner string, configure ...func(*types.AWSProviderConfig)) *AWS {
	config := types.AWSProviderConfig{Regions: []types.AWSProviderRegionConfig{{Name: "us-east-1"}, {Name: "us-west-2"}}}
	for _, change := range configure {
		change(&config)
	}
	client, err := NewAWS(config, owner)
	require.NoError(cloud.t, err)
	// The stand-in's clock, and its EC2 in place of the SDK's.
	client.now, client.ec2 = cloud.clock, cloud.ec2
	return client
}

// ec2 returns the stand-in's EC2 of a region.
func (cloud *awsCloud) ec2(region string) (ec2API, error) {
	ec2, ok := cloud.regions[region]
	if !ok {
		return nil, fmt.Errorf("no such region: %s", region)
	}
	return ec2, nil
}

func (cloud *awsCloud) clock() time.Time {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	return cloud.now
}

func (cloud *awsCloud) after(d time.Duration) {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	cloud.now = cloud.now.Add(d)
}

// called counts the calls of one kind to every region.
func (cloud *awsCloud) called(call string) int {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	total := 0
	for _, ec2 := range cloud.regions {
		total += ec2.calls[call]
	}
	return total
}

// lastRun returns the last create any region received.
func (cloud *awsCloud) lastRun(region string) *ec2.RunInstancesInput {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	runs := cloud.regions[region].runs
	require.NotEmpty(cloud.t, runs)
	return runs[len(runs)-1]
}

// find returns the instance with an ID of this client's, "region/i-...".
func (cloud *awsCloud) find(id string) (*awsEC2, int) {
	region, instance, _ := strings.Cut(id, "/")
	ec2 := cloud.regions[region]
	if ec2 == nil {
		return nil, -1
	}
	return ec2, slices.IndexFunc(ec2.instances, func(i ec2types.Instance) bool { return aws.ToString(i.InstanceId) == instance })
}

// state makes EC2 report an instance in the state, for the reason if any.
func (cloud *awsCloud) state(id, state string, reason *ec2types.StateReason) {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	ec2, at := cloud.find(id)
	require.GreaterOrEqual(cloud.t, at, 0, "no instance %s", id)
	ec2.instances[at].State = &ec2types.InstanceState{Name: ec2types.InstanceStateName(state)}
	ec2.instances[at].StateReason = reason
}

func (cloud *awsCloud) lose(id string) {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	if ec2, at := cloud.find(id); at >= 0 {
		ec2.instances = slices.Delete(ec2.instances, at, at+1)
	}
}

func (cloud *awsCloud) refuse(with error) {
	cloud.mu.Lock()
	defer cloud.mu.Unlock()
	for _, ec2 := range cloud.regions {
		ec2.refusal = with
	}
}

func awsError(code, message string) error {
	return &smithy.GenericAPIError{Code: code, Message: message}
}

// awsFilters reports whether a resource passes EC2 filters: for each filter,
// one of its values has to fit one of the resource's values under that
// name, where * stands for anything.
func awsFilters(filters []ec2types.Filter, values func(name string) []string) bool {
	for _, filter := range filters {
		fits := slices.ContainsFunc(values(aws.ToString(filter.Name)), func(value string) bool {
			return slices.ContainsFunc(filter.Values, func(pattern string) bool {
				ok, _ := path.Match(pattern, value)
				return ok
			})
		})
		if !fits {
			return false
		}
	}
	return true
}

// awsPage cuts a list into EC2's pages.
func awsPage[T any](ec2 *awsEC2, items []T, token *string) ([]T, *string) {
	if ec2.repeatsToken {
		// A client that reads on would never stop, and hang its test.
		if ec2.repeated++; ec2.repeated > 100 {
			ec2.cloud.t.Errorf("the client read %d pages of lists that repeat their token", ec2.repeated)
			return items, nil
		}
		return items, aws.String("the-same-again")
	}
	start, _ := strconv.Atoi(aws.ToString(token))
	end := len(items)
	if ec2.pageOf > 0 {
		end = min(start+ec2.pageOf, len(items))
	}
	if end < len(items) {
		return items[start:end], aws.String(strconv.Itoa(end))
	}
	return items[min(start, end):end], nil
}

func (e *awsEC2) begin(call string) func() {
	e.cloud.mu.Lock()
	e.calls[call]++
	return e.cloud.mu.Unlock
}

func (e *awsEC2) DescribeInstanceTypes(ctx context.Context, in *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	defer e.begin("DescribeInstanceTypes")()
	var matching []ec2types.InstanceTypeInfo
	for _, info := range e.types {
		fits := awsFilters(in.Filters, func(name string) []string {
			switch name {
			case "processor-info.supported-architecture":
				architectures := []string{}
				for _, architecture := range info.ProcessorInfo.SupportedArchitectures {
					architectures = append(architectures, string(architecture))
				}
				return architectures
			}
			e.cloud.t.Errorf("the stand-in does not know the instance type filter %q", name)
			return nil
		})
		if fits {
			matching = append(matching, info)
		}
	}
	page, next := awsPage(e, matching, in.NextToken)
	return &ec2.DescribeInstanceTypesOutput{InstanceTypes: page, NextToken: next}, nil
}

// DescribeSpotPriceHistory answers as EC2 does when given a start time: the
// price in force then for each place, and the changes since.
func (e *awsEC2) DescribeSpotPriceHistory(ctx context.Context, in *ec2.DescribeSpotPriceHistoryInput, _ ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error) {
	defer e.begin("DescribeSpotPriceHistory")()
	if e.priceRefusal != nil {
		return nil, e.priceRefusal
	}
	if in.StartTime == nil {
		return nil, awsError("InvalidParameterValue", "the stand-in answers only from a start time")
	}
	type place struct {
		zone, product string
		instanceType  ec2types.InstanceType
	}
	inForce := map[place]ec2types.SpotPrice{}
	var matching []ec2types.SpotPrice
	for _, price := range e.prices {
		if len(in.InstanceTypes) > 0 && !slices.Contains(in.InstanceTypes, price.InstanceType) {
			continue
		}
		if len(in.ProductDescriptions) > 0 && !slices.Contains(in.ProductDescriptions, string(price.ProductDescription)) {
			continue
		}
		if price.Timestamp.After(*in.StartTime) {
			matching = append(matching, price)
			continue
		}
		at := place{aws.ToString(price.AvailabilityZone), string(price.ProductDescription), price.InstanceType}
		if known, ok := inForce[at]; !ok || price.Timestamp.After(*known.Timestamp) {
			inForce[at] = price
		}
	}
	for _, price := range inForce {
		matching = append(matching, price)
	}
	slices.SortFunc(matching, func(a, b ec2types.SpotPrice) int {
		return strings.Compare(aws.ToString(a.AvailabilityZone)+string(a.InstanceType), aws.ToString(b.AvailabilityZone)+string(b.InstanceType))
	})
	page, next := awsPage(e, matching, in.NextToken)
	// EC2 ends this list with an empty token, where its others end with none.
	if next == nil {
		next = aws.String("")
	}
	return &ec2.DescribeSpotPriceHistoryOutput{SpotPriceHistory: page, NextToken: next}, nil
}

func (e *awsEC2) DescribeSubnets(ctx context.Context, in *ec2.DescribeSubnetsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	defer e.begin("DescribeSubnets")()
	for _, id := range in.SubnetIds {
		if !slices.ContainsFunc(e.subnets, func(s ec2types.Subnet) bool { return aws.ToString(s.SubnetId) == id }) {
			return nil, awsError("InvalidSubnetID.NotFound", "The subnet ID '"+id+"' does not exist")
		}
	}
	var matching []ec2types.Subnet
	for _, subnet := range e.subnets {
		named := len(in.SubnetIds) == 0 || slices.Contains(in.SubnetIds, aws.ToString(subnet.SubnetId))
		fits := awsFilters(in.Filters, func(name string) []string {
			if name == "default-for-az" {
				return []string{strconv.FormatBool(aws.ToBool(subnet.DefaultForAz))}
			}
			e.cloud.t.Errorf("the stand-in does not know the subnet filter %q", name)
			return nil
		})
		if named && fits {
			matching = append(matching, subnet)
		}
	}
	page, next := awsPage(e, matching, in.NextToken)
	return &ec2.DescribeSubnetsOutput{Subnets: page, NextToken: next}, nil
}

func (e *awsEC2) DescribeImages(ctx context.Context, in *ec2.DescribeImagesInput, _ ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error) {
	defer e.begin("DescribeImages")()
	for _, id := range in.ImageIds {
		if !slices.ContainsFunc(e.images, func(i ec2types.Image) bool { return aws.ToString(i.ImageId) == id }) {
			return nil, awsError("InvalidAMIID.NotFound", "The image id '["+id+"]' does not exist")
		}
	}
	var matching []ec2types.Image
	for _, image := range e.images {
		named := len(in.ImageIds) == 0 || slices.Contains(in.ImageIds, aws.ToString(image.ImageId))
		owned := len(in.Owners) == 0 || slices.Contains(in.Owners, aws.ToString(image.ImageOwnerAlias)) || slices.Contains(in.Owners, aws.ToString(image.OwnerId))
		fits := awsFilters(in.Filters, func(name string) []string {
			switch name {
			case "name":
				return []string{aws.ToString(image.Name)}
			case "state":
				return []string{string(image.State)}
			case "architecture":
				return []string{string(image.Architecture)}
			}
			e.cloud.t.Errorf("the stand-in does not know the image filter %q", name)
			return nil
		})
		if named && owned && fits {
			matching = append(matching, image)
		}
	}
	return &ec2.DescribeImagesOutput{Images: matching}, nil
}

func (e *awsEC2) RunInstances(ctx context.Context, in *ec2.RunInstancesInput, _ ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error) {
	defer e.begin("RunInstances")()
	if e.refusal != nil {
		return nil, e.refusal
	}
	if why := e.wrong(in); why != nil {
		return nil, why
	}
	// "If you repeat the request with the same client token, the same
	// response is returned."
	if id, ran := e.byToken[aws.ToString(in.ClientToken)]; ran && in.ClientToken != nil {
		at := slices.IndexFunc(e.instances, func(i ec2types.Instance) bool { return aws.ToString(i.InstanceId) == id })
		return &ec2.RunInstancesOutput{Instances: []ec2types.Instance{e.instances[at]}}, nil
	}
	e.runs = append(e.runs, in)

	e.nextID++
	instance := ec2types.Instance{
		InstanceId:   aws.String(fmt.Sprintf("i-0%s%014d", e.region[len(e.region)-1:], e.nextID)),
		InstanceType: in.InstanceType,
		ImageId:      in.ImageId,
		SubnetId:     in.NetworkInterfaces[0].SubnetId,
		State:        &ec2types.InstanceState{Name: ec2types.InstanceStateNamePending},
	}
	if in.InstanceMarketOptions != nil && in.InstanceMarketOptions.MarketType == ec2types.MarketTypeSpot {
		instance.InstanceLifecycle = ec2types.InstanceLifecycleTypeSpot
	}
	for _, specification := range in.TagSpecifications {
		if specification.ResourceType == ec2types.ResourceTypeInstance {
			instance.Tags = slices.Clone(specification.Tags)
		}
	}
	e.instances = append(e.instances, instance)
	if in.ClientToken != nil {
		e.byToken[*in.ClientToken] = *instance.InstanceId
	}
	return &ec2.RunInstancesOutput{Instances: []ec2types.Instance{instance}}, nil
}

// wrong says what about a create EC2 would turn down.
func (e *awsEC2) wrong(in *ec2.RunInstancesInput) error {
	invalid := func(why string) error { return awsError("InvalidParameterCombination", why) }
	switch {
	case aws.ToInt32(in.MinCount) != 1 || aws.ToInt32(in.MaxCount) != 1:
		return invalid("the stand-in runs one instance at a time")
	case !slices.ContainsFunc(e.images, func(i ec2types.Image) bool { return aws.ToString(i.ImageId) == aws.ToString(in.ImageId) }):
		return awsError("InvalidAMIID.NotFound", "The image id '["+aws.ToString(in.ImageId)+"]' does not exist")
	case !slices.ContainsFunc(e.types, func(t ec2types.InstanceTypeInfo) bool { return t.InstanceType == in.InstanceType }):
		return awsError("InvalidParameterValue", "Invalid value '"+string(in.InstanceType)+"' for InstanceType")
	case len(in.NetworkInterfaces) != 1 || in.SubnetId != nil || len(in.SecurityGroupIds) > 0:
		return invalid("Network interfaces and an instance-level subnet ID or security groups may not be specified on the same request")
	case len(aws.ToString(in.ClientToken)) > 64:
		return awsError("InvalidParameterValue", "the client token is longer than 64 characters")
	}
	subnet := aws.ToString(in.NetworkInterfaces[0].SubnetId)
	if !slices.ContainsFunc(e.subnets, func(s ec2types.Subnet) bool { return aws.ToString(s.SubnetId) == subnet }) {
		return awsError("InvalidSubnetID.NotFound", "The subnet ID '"+subnet+"' does not exist")
	}
	if in.UserData != nil {
		if _, err := base64.StdEncoding.DecodeString(*in.UserData); err != nil {
			return awsError("InvalidUserData.Malformed", "Invalid BASE64 encoding of user data")
		}
	}
	return nil
}

func (e *awsEC2) DescribeInstances(ctx context.Context, in *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	defer e.begin("DescribeInstances")()
	if e.describeRefusal != nil {
		return nil, e.describeRefusal
	}
	for _, id := range in.InstanceIds {
		if !strings.HasPrefix(id, "i-") {
			return nil, awsError("InvalidInstanceID.Malformed", "Invalid id: \""+id+"\"")
		}
		if !slices.ContainsFunc(e.instances, func(i ec2types.Instance) bool { return aws.ToString(i.InstanceId) == id }) {
			return nil, awsError("InvalidInstanceID.NotFound", "The instance ID '"+id+"' does not exist")
		}
	}
	var matching []ec2types.Reservation
	for _, instance := range e.instances {
		named := len(in.InstanceIds) == 0 || slices.Contains(in.InstanceIds, aws.ToString(instance.InstanceId))
		fits := awsFilters(in.Filters, func(name string) []string {
			if key, ok := strings.CutPrefix(name, "tag:"); ok {
				values := []string{}
				for _, tag := range instance.Tags {
					if aws.ToString(tag.Key) == key {
						values = append(values, aws.ToString(tag.Value))
					}
				}
				return values
			}
			e.cloud.t.Errorf("the stand-in does not know the instance filter %q", name)
			return nil
		})
		if named && (fits || e.listsEverything) {
			matching = append(matching, ec2types.Reservation{Instances: []ec2types.Instance{instance}})
		}
	}
	page, next := awsPage(e, matching, in.NextToken)
	return &ec2.DescribeInstancesOutput{Reservations: page, NextToken: next}, nil
}

func (e *awsEC2) TerminateInstances(ctx context.Context, in *ec2.TerminateInstancesInput, _ ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error) {
	defer e.begin("TerminateInstances")()
	if e.terminateRefusal != nil {
		return nil, e.terminateRefusal
	}
	for _, id := range in.InstanceIds {
		at := slices.IndexFunc(e.instances, func(i ec2types.Instance) bool { return aws.ToString(i.InstanceId) == id })
		if at < 0 {
			return nil, awsError("InvalidInstanceID.NotFound", "The instance ID '"+id+"' does not exist")
		}
		// Terminating what is terminated already succeeds and changes
		// nothing.
		switch e.instances[at].State.Name {
		case ec2types.InstanceStateNameShuttingDown, ec2types.InstanceStateNameTerminated:
		default:
			e.instances[at].State = &ec2types.InstanceState{Name: ec2types.InstanceStateNameShuttingDown}
			e.instances[at].StateReason = &ec2types.StateReason{Code: aws.String("Client.UserInitiatedShutdown"), Message: aws.String("Client.UserInitiatedShutdown: User initiated shutdown")}
		}
	}
	return &ec2.TerminateInstancesOutput{}, nil
}

// How EC2 turns a create down, by its API reference.
var awsRefusals = map[error]error{
	ErrOutOfStock: awsError("InsufficientInstanceCapacity", "There is no Spot capacity available that matches your request."),
	ErrQuota:      awsError("MaxSpotInstanceCountExceeded", "Max spot instance count exceeded"),
}

// The reason EC2 gives for an instance it took back.
var awsTakenBack = &ec2types.StateReason{
	Code:    aws.String("Server.SpotInstanceTermination"),
	Message: aws.String("Server.SpotInstanceTermination: Spot instance termination"),
}

func newAWSSite(t *testing.T) site {
	cloud := newAWSCloud(t)
	return site{
		provider: cloud.client(testOwner),
		statuses: map[string]Status{
			"pending":       StatusPending,
			"running":       StatusActive,
			"shutting-down": StatusDeleted,
			"terminated":    StatusDeleted,
			"stopping":      StatusFailed,
			"stopped":       StatusFailed,
		},
		report: func(id, status string) { cloud.state(id, status, nil) },
		refuse: func(reason error) { cloud.refuse(awsRefusals[reason]) },
		lose:   cloud.lose,
		bought: func() (string, string) {
			run := cloud.lastRun("us-east-1")
			script, err := base64.StdEncoding.DecodeString(aws.ToString(run.UserData))
			require.NoError(t, err)
			return awsTagsOf(run)[awsNameTag], string(script)
		},
		plantForeign: func() string {
			offers, err := cloud.client(foreignOwner).Offers(context.Background(), OfferFilter{})
			require.NoError(t, err)
			instance, err := cloud.client(foreignOwner).Create(context.Background(), CreateRequest{Offer: offers[0], Name: "someone-elses"})
			require.NoError(t, err)
			return instance.ID
		},
		boughtSpot: func() bool {
			market := cloud.lastRun("us-east-1").InstanceMarketOptions
			return market != nil && market.MarketType == ec2types.MarketTypeSpot
		},
		takeBack: func(id string) { cloud.state(id, "terminated", awsTakenBack) },
	}
}

// awsTagsOf returns the tags a create put on its instance.
func awsTagsOf(run *ec2.RunInstancesInput) map[string]string {
	tags := map[string]string{}
	for _, specification := range run.TagSpecifications {
		if specification.ResourceType == ec2types.ResourceTypeInstance {
			for _, tag := range specification.Tags {
				tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
			}
		}
	}
	return tags
}

// What a machine's 200 GB disk costs an hour, at $0.08 a gigabyte a month.
const awsTestDisk = 21_917

func awsOffer(zone, instanceType, gpu string, gpus uint32, vcpus, memoryGiB, micros int64) Offer {
	return Offer{
		ID: zone + "/" + instanceType, Provider: "aws", Cloud: "aws", InstanceType: instanceType, Region: zone[:len(zone)-1],
		GPU: gpu, GPUCount: gpus, CPUMillicores: vcpus * 1000, MemoryMB: memoryGiB * 1024, StorageMB: 200 * 1024,
		HourlyCostMicros: micros + awsTestDisk, DiskHourlyMicros: awsTestDisk, Spot: true,
	}
}

var (
	awsA10GInA  = awsOffer("us-east-1a", "g5.xlarge", "A10G", 1, 4, 16, 402_100)
	awsA10GInB  = awsOffer("us-east-1b", "g5.xlarge", "A10G", 1, 4, 16, 389_900)
	awsA10GWest = awsOffer("us-west-2a", "g5.xlarge", "A10G", 1, 4, 16, 512_300)
)

func TestAWSOffersEachGPUTypeInEachZoneThatHasASpotPrice(t *testing.T) {
	cloud := newAWSCloud(t)
	offers, err := cloud.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)

	// EC2 calls both A100s "A100" and both V100s "V100"; their memory tells
	// them apart. The H100 type has no Spot price anywhere, and us-east-1c
	// no subnet to use. Of the two zones that sell the A10G, the cheaper
	// comes first.
	require.Equal(t, []Offer{
		awsOffer("us-east-1a", "p4d.24xlarge", "A100-40", 8, 96, 1152, 9_661_700),
		awsOffer("us-east-1b", "p4de.24xlarge", "A100-80", 8, 96, 1152, 12_404_000),
		awsA10GInB,
		awsA10GInA,
		awsOffer("us-east-1a", "g6e.xlarge", "L40S", 1, 4, 32, 930_400),
		awsOffer("us-east-1b", "g7e.2xlarge", "RTXPro6000", 1, 8, 64, 1_336_000),
		awsOffer("us-east-1b", "g4dn.xlarge", "T4", 1, 4, 16, 157_800),
		awsOffer("us-east-1a", "p3.2xlarge", "V100", 1, 8, 61, 918_000),
		awsOffer("us-east-1a", "p3dn.24xlarge", "V100-32", 8, 96, 768, 9_364_800),
		awsA10GWest,
	}, offers)
}

// A price that changes as it is asked for comes back next to the one it
// replaced.
func TestAWSTakesTheLatestOfTwoPricesForAZone(t *testing.T) {
	cloud := newAWSCloud(t)
	changed := cloud.regions["us-west-2"].prices[0]
	changed.SpotPrice, changed.Timestamp = aws.String("0.530000"), aws.Time(cloud.now.Add(time.Second))
	cloud.regions["us-west-2"].prices = append([]ec2types.SpotPrice{changed}, cloud.regions["us-west-2"].prices...)

	offers, err := cloud.client(testOwner).Offers(context.Background(), OfferFilter{Regions: []string{"us-west-2"}})
	require.NoError(t, err)
	require.Equal(t, []Offer{awsOffer("us-west-2a", "g5.xlarge", "A10G", 1, 4, 16, 530_000)}, offers)
	require.Zero(t, cloud.regions["us-east-1"].calls["DescribeSpotPriceHistory"], "a region that was not asked for is not read")
}

func TestAWSReadsEveryPageOfAList(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	ctx := context.Background()
	for _, ec2 := range cloud.regions {
		ec2.pageOf = 1
	}

	offers, err := client.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.Len(t, offers, 10)

	for _, name := range []string{"beam-a10g-a1b2c3d4", "beam-a10g-e5f6a7b8", "beam-a10g-c9d0e1f2"} {
		_, err := client.Create(ctx, CreateRequest{Offer: awsA10GInA, Name: name})
		require.NoError(t, err)
	}
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 3)
}

func TestAWSReadsARegionRarelyAndItsPricesEveryTime(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	for range 3 {
		_, err := client.Offers(context.Background(), OfferFilter{})
		require.NoError(t, err)
	}
	for _, call := range []string{"DescribeInstanceTypes", "DescribeSubnets", "DescribeImages"} {
		require.Equal(t, 2, cloud.called(call), "%s, once for each region", call)
	}
	require.Equal(t, 6, cloud.called("DescribeSpotPriceHistory"))

	cloud.after(awsRegionFor)
	_, err := client.Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Equal(t, 4, cloud.called("DescribeInstanceTypes"))
}

func TestAWSCreateSendsWhatEC2Needs(t *testing.T) {
	cloud := newAWSCloud(t)
	created, err := cloud.client(testOwner).Create(context.Background(), CreateRequest{
		Offer:         awsA10GInB,
		Name:          "beam-a10g-a1b2c3d4",
		StartupScript: "curl -fsSL https://gateway.example/join | sh",
		Tags:          map[string]string{"beam-machine": "a1b2c3d4"},
	})
	require.NoError(t, err)
	require.Equal(t, Instance{ID: "us-east-1/i-0100000000000001", Status: StatusPending, Tags: map[string]string{"beam-machine": "a1b2c3d4"}}, created)

	tags := []ec2types.Tag{
		{Key: aws.String("Name"), Value: aws.String("beam-a10g-a1b2c3d4")},
		{Key: aws.String("beam-machine"), Value: aws.String("a1b2c3d4")},
		{Key: aws.String(OwnerTag), Value: aws.String(testOwner)},
	}
	require.Equal(t, &ec2.RunInstancesInput{
		// The newest image of the name, for the machine's kind of CPU.
		ImageId:      aws.String(awsTestImage),
		InstanceType: "g5.xlarge",
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		ClientToken:  aws.String("beam-a10g-a1b2c3d4"),
		InstanceMarketOptions: &ec2types.InstanceMarketOptionsRequest{
			MarketType: ec2types.MarketTypeSpot,
			// No price limit was asked for.
			SpotOptions: &ec2types.SpotMarketOptions{
				SpotInstanceType:             ec2types.SpotInstanceTypeOneTime,
				InstanceInterruptionBehavior: ec2types.InstanceInterruptionBehaviorTerminate,
			},
		},
		NetworkInterfaces: []ec2types.InstanceNetworkInterfaceSpecification{{
			DeviceIndex: aws.Int32(0),
			// The default subnet of the offer's zone.
			SubnetId:                 aws.String("subnet-us-east-1b"),
			AssociatePublicIpAddress: aws.Bool(true),
		}},
		BlockDeviceMappings: []ec2types.BlockDeviceMapping{{
			DeviceName: aws.String("/dev/sda1"),
			Ebs:        &ec2types.EbsBlockDevice{VolumeSize: aws.Int32(200), VolumeType: ec2types.VolumeTypeGp3, DeleteOnTermination: aws.Bool(true)},
		}},
		InstanceInitiatedShutdownBehavior: ec2types.ShutdownBehaviorTerminate,
		MetadataOptions:                   &ec2types.InstanceMetadataOptionsRequest{HttpTokens: ec2types.HttpTokensStateRequired},
		TagSpecifications: []ec2types.TagSpecification{
			{ResourceType: ec2types.ResourceTypeInstance, Tags: tags},
			{ResourceType: ec2types.ResourceTypeVolume, Tags: tags},
		},
		UserData: aws.String(base64.StdEncoding.EncodeToString([]byte("#!/usr/bin/env bash\nset -euo pipefail\ncurl -fsSL https://gateway.example/join | sh\n"))),
	}, cloud.lastRun("us-east-1"))
}

func TestAWSPassesThePriceLimitLessTheDisk(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)

	// $0.50 an hour for the machine leaves $0.478083 for the instance.
	_, err := client.Create(context.Background(), CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4", MaxHourlyCostMicros: 500_000})
	require.NoError(t, err)
	require.Equal(t, "0.4780", aws.ToString(cloud.lastRun("us-east-1").InstanceMarketOptions.SpotOptions.MaxPrice))

	_, err = client.Create(context.Background(), CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-e5f6a7b8", MaxHourlyCostMicros: 12_345_678})
	require.NoError(t, err)
	require.Equal(t, "12.3237", aws.ToString(cloud.lastRun("us-east-1").InstanceMarketOptions.SpotOptions.MaxPrice))

	_, err = client.Create(context.Background(), CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-c9d0e1f2", MaxHourlyCostMicros: awsTestDisk})
	require.ErrorContains(t, err, "leaves nothing for an AWS instance")
	require.Len(t, cloud.regions["us-east-1"].runs, 2)
}

// The SDK sends a call again when it gets no answer. A create carries a
// token that EC2 runs one instance for, however often it arrives.
func TestAWSACreateSentTwiceBuysOneMachine(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	req := CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4"}

	first, err := client.Create(context.Background(), req)
	require.NoError(t, err)
	again, err := client.Create(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, first, again)
	require.Len(t, cloud.regions["us-east-1"].instances, 1)
	// The token is the machine's name, which is its alone.
	require.Equal(t, "beam-a10g-a1b2c3d4", aws.ToString(cloud.lastRun("us-east-1").ClientToken))
}

func TestAWSNamesWhyACreateWasTurnedDown(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	req := CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4"}
	for code, want := range map[string]error{
		"InsufficientInstanceCapacity": ErrOutOfStock,
		"SpotMaxPriceTooLow":           ErrOutOfStock,
		"MaxSpotInstanceCountExceeded": ErrQuota,
		"VcpuLimitExceeded":            ErrQuota,
		"InstanceLimitExceeded":        ErrQuota,
	} {
		cloud.refuse(awsError(code, "refused"))
		_, err := client.Create(context.Background(), req)
		require.ErrorIs(t, err, want, code)
	}

	// The configuration is wrong, which waiting does not put right.
	cloud.refuse(awsError("Unsupported", "The requested configuration is currently not supported."))
	_, err := client.Create(context.Background(), req)
	require.ErrorContains(t, err, "not supported")
	require.NotErrorIs(t, err, ErrOutOfStock)
	require.NotErrorIs(t, err, ErrQuota)
}

// EC2 has no API for on-demand prices, so this client has no offer for an
// on-demand instance, and does not buy one for the price of a Spot one.
func TestAWSBuysNothingButSpotInstances(t *testing.T) {
	cloud := newAWSCloud(t)
	onDemand := awsA10GInA
	onDemand.Spot = false
	_, err := cloud.client(testOwner).Create(context.Background(), CreateRequest{Offer: onDemand, Name: "beam-a10g-a1b2c3d4"})
	require.ErrorContains(t, err, "Spot")

	for name, offer := range map[string]Offer{
		"a zone without a subnet":  awsOffer("us-east-1c", "g5.xlarge", "A10G", 1, 4, 16, 350_000),
		"a region not configured":  awsOffer("eu-west-1a", "g5.xlarge", "A10G", 1, 4, 16, 350_000),
		"a zone of another region": {ID: "us-east-1a/g5.xlarge", Region: "eu-west-1", Spot: true},
		"no zone":                  {ID: "g5.xlarge", Region: "us-east-1", Spot: true},
	} {
		_, err := cloud.client(testOwner).Create(context.Background(), CreateRequest{Offer: offer, Name: "beam-a10g-a1b2c3d4"})
		require.Error(t, err, name)
	}
	require.Zero(t, cloud.called("RunInstances"))
}

func TestAWSUsesTheSubnetsGroupsImageAndKeyItIsGiven(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner, func(config *types.AWSProviderConfig) {
		config.Regions[0] = types.AWSProviderRegionConfig{
			Name:             "us-east-1",
			SubnetIDs:        []string{"subnet-us-east-1c"},
			SecurityGroupIDs: []string{"sg-0beamworkers"},
			ImageID:          awsTestOldImage,
		}
		config.KeyName = "ops"
		config.DiskGB = 500
		config.ImageName = "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 24.04) *"
	})

	offers, err := client.Offers(context.Background(), OfferFilter{Regions: []string{"us-east-1"}})
	require.NoError(t, err)
	// The one zone that has a named subnet, with the price of the disk.
	inC := awsOffer("us-east-1c", "g5.xlarge", "A10G", 1, 4, 16, 350_000)
	inC.StorageMB, inC.HourlyCostMicros, inC.DiskHourlyMicros = 500*1024, 350_000+54_794, 54_794
	require.Equal(t, []Offer{inC}, offers)

	_, err = client.Create(context.Background(), CreateRequest{Offer: inC, Name: "beam-a10g-a1b2c3d4"})
	require.NoError(t, err)
	run := cloud.lastRun("us-east-1")
	require.Equal(t, awsTestOldImage, aws.ToString(run.ImageId))
	require.Equal(t, "ops", aws.ToString(run.KeyName))
	require.Equal(t, "subnet-us-east-1c", aws.ToString(run.NetworkInterfaces[0].SubnetId))
	require.Equal(t, []string{"sg-0beamworkers"}, run.NetworkInterfaces[0].Groups)
	require.Equal(t, int32(500), aws.ToInt32(run.BlockDeviceMappings[0].Ebs.VolumeSize))
	require.Nil(t, run.UserData, "no script was asked for")

	// The other region keeps its defaults, and boots the newest image of
	// the name that was given.
	_, err = client.Create(context.Background(), CreateRequest{Offer: awsA10GWest, Name: "beam-a10g-e5f6a7b8"})
	require.NoError(t, err)
	require.Empty(t, cloud.lastRun("us-west-2").NetworkInterfaces[0].Groups)
	require.Equal(t, "ami-0beam00000ubuntu24", aws.ToString(cloud.lastRun("us-west-2").ImageId))
}

// A region that cannot be bought in is left out of the offers. Asked for it
// alone, the client says what it lacks.
func TestAWSSaysWhatARegionLacks(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*awsCloud, *types.AWSProviderConfig)
		region string
		want   string
		// unreachable marks a region whose machines cannot be read either.
		unreachable bool
	}{
		"no default subnets": {
			change: func(cloud *awsCloud, _ *types.AWSProviderConfig) { cloud.regions["us-west-2"].subnets = nil },
			region: "us-west-2",
			want:   "AWS region us-west-2: it has no default subnets: name the subnets to use as subnetIds under providers.aws.regions",
		},
		"a subnet that is not there": {
			change: func(_ *awsCloud, config *types.AWSProviderConfig) {
				config.Regions[1].SubnetIDs = []string{"subnet-nosuch"}
			},
			region: "us-west-2",
			want:   "AWS region us-west-2: its subnets:",
		},
		"no image of the name": {
			change: func(_ *awsCloud, config *types.AWSProviderConfig) { config.ImageName = "No Such Image *" },
			region: "us-east-1",
			want:   `AWS region us-east-1: it has no image "No Such Image *"`,
		},
		"an image that is not there": {
			change: func(_ *awsCloud, config *types.AWSProviderConfig) { config.Regions[0].ImageID = "ami-nosuch" },
			region: "us-east-1",
			want:   "AWS region us-east-1: its image:",
		},
		"a region EC2 does not have": {
			change: func(_ *awsCloud, config *types.AWSProviderConfig) {
				config.Regions = append(config.Regions, types.AWSProviderRegionConfig{Name: "mars-north-1"})
			},
			region:      "mars-north-1",
			want:        "AWS region mars-north-1: no such region",
			unreachable: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cloud := newAWSCloud(t)
			client := cloud.client(testOwner, func(config *types.AWSProviderConfig) { tc.change(cloud, config) })
			_, err := client.Offers(context.Background(), OfferFilter{Regions: []string{tc.region}})
			require.ErrorContains(t, err, tc.want)
			_, err = client.List(context.Background())
			if tc.unreachable {
				require.ErrorContains(t, err, tc.want, "what cannot be listed must not read as nothing bought")
			} else {
				require.NoError(t, err, "what was bought there can still be listed")
			}
		})
	}
}

// AWS says why it ended an instance. The reasons for a Spot Instance it
// took back start with Server.Spot.
func TestAWSReadsASpotInstanceItTookBackAsInterrupted(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4"})
	require.NoError(t, err)
	read := func(state string, reason *ec2types.StateReason) Instance {
		t.Helper()
		cloud.state(created.ID, state, reason)
		found, err := client.Get(ctx, created.ID)
		require.NoError(t, err)
		return found
	}

	// AWS is taking it back: two minutes after the notice it shuts down.
	require.Equal(t, Instance{ID: created.ID, Status: StatusInterrupted, StatusDetail: "shutting-down: Server.SpotInstanceTermination: Spot instance termination"},
		read("shutting-down", awsTakenBack))
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)

	require.Equal(t, StatusInterrupted, read("terminated", awsTakenBack).Status)
	stoppedByAWS := &ec2types.StateReason{Code: aws.String("Server.SpotInstanceShutdown"), Message: aws.String("Server.SpotInstanceShutdown: Spot instance shutdown")}
	require.Equal(t, StatusInterrupted, read("stopped", stoppedByAWS).Status)

	// Ended for another reason, it was not taken back.
	byUser := &ec2types.StateReason{Code: aws.String("Client.UserInitiatedShutdown"), Message: aws.String("Client.UserInitiatedShutdown: User initiated shutdown")}
	require.Equal(t, Instance{ID: created.ID, Status: StatusDeleted}, read("terminated", byUser))
	failed := &ec2types.StateReason{Code: aws.String("Server.InternalError"), Message: aws.String("Server.InternalError: Internal error on launch")}
	require.Equal(t, Instance{ID: created.ID, Status: StatusFailed, StatusDetail: "stopped: Server.InternalError: Internal error on launch"}, read("stopped", failed))
	require.Equal(t, Instance{ID: created.ID, Status: StatusUnknown, StatusDetail: "rebooting"}, read("rebooting", nil))
}

// EC2 shows a terminated instance for about an hour. Nothing is left of it
// to delete or to pay for.
func TestAWSListLeavesOutWhatIsTerminated(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	ctx := context.Background()
	east, err := client.Create(ctx, CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4", Tags: map[string]string{"beam-machine": "a1b2c3d4"}})
	require.NoError(t, err)
	west, err := client.Create(ctx, CreateRequest{Offer: awsA10GWest, Name: "beam-a10g-e5f6a7b8", Tags: map[string]string{"beam-machine": "e5f6a7b8"}})
	require.NoError(t, err)

	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []Instance{
		{ID: east.ID, Status: StatusPending, Tags: map[string]string{"beam-machine": "a1b2c3d4"}},
		{ID: west.ID, Status: StatusPending, Tags: map[string]string{"beam-machine": "e5f6a7b8"}},
	}, listed)

	cloud.state(east.ID, "terminated", awsTakenBack)
	listed, err = client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, west.ID, listed[0].ID)
}

func TestAWSReportsWhereAMachineCanBeReached(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	created, err := client.Create(context.Background(), CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4"})
	require.NoError(t, err)

	found, err := client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Empty(t, found.PublicIP, "a machine that is not up has no address")

	cloud.state(created.ID, "running", nil)
	cloud.regions["us-east-1"].instances[0].PublicIpAddress = aws.String("54.210.10.20")
	found, err = client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusActive, PublicIP: "54.210.10.20", SSHHost: "54.210.10.20", SSHPort: 22}, found)
}

// An instance's ID here names its region, because EC2's is the region's
// own. One that is not a region and an instance is nobody's.
func TestAWSFindsAnInstanceByTheRegionInItsID(t *testing.T) {
	cloud := newAWSCloud(t)
	client := cloud.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: awsA10GWest, Name: "beam-a10g-a1b2c3d4"})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(created.ID, "us-west-2/i-"), created.ID)

	_, instance, _ := strings.Cut(created.ID, "/")
	for _, id := range []string{"us-east-1/" + instance, instance, "us-west-2/", "/" + instance, "us-west-2/not-an-id"} {
		_, err := client.Get(ctx, id)
		require.ErrorIs(t, err, ErrNotFound, id)
		require.NoError(t, client.Delete(ctx, id), id)
	}
	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusPending, found.Status, "none of those deleted it")

	// A region that was taken out of the config still has the machines
	// bought in it. They are not gone, and saying so would drop the record
	// of a machine that goes on running.
	eastOnly := cloud.client(testOwner, func(config *types.AWSProviderConfig) {
		config.Regions = []types.AWSProviderRegionConfig{{Name: "us-east-1"}}
	})
	_, err = eastOnly.Get(ctx, created.ID)
	require.ErrorContains(t, err, `providers.aws.regions does not name "us-west-2"`)
	require.NotErrorIs(t, err, ErrNotFound)
	err = eastOnly.Delete(ctx, created.ID)
	require.ErrorContains(t, err, `providers.aws.regions does not name "us-west-2"`)
	found, err = client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusPending, found.Status, "it was not deleted")

	// A delete that EC2 refuses for another reason is an error.
	cloud.regions["us-west-2"].terminateRefusal = awsError("UnauthorizedOperation", "You are not authorized to perform this operation.")
	require.ErrorContains(t, client.Delete(ctx, created.ID), "not authorized")
	cloud.regions["us-west-2"].terminateRefusal = nil
	require.NoError(t, client.Delete(ctx, created.ID))
	found, err = client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusDeleted, found.Status)
}

// Every other test of this client talks to a stand-in for EC2. This one
// looks at the client the AWS SDK is asked for: it is the region's, and it
// signs with the IAM user's key that the config holds.
func TestAWSTalksToARegionsEC2WithTheKeyItIsGiven(t *testing.T) {
	client, err := NewAWS(types.AWSProviderConfig{
		AccessKeyID:     "AKIABEAMTEST",
		SecretAccessKey: "secret",
		Regions:         []types.AWSProviderRegionConfig{{Name: "us-east-1"}, {Name: "us-west-2"}},
	}, testOwner)
	require.NoError(t, err)

	sdk, err := client.connect(t.Context(), "us-west-2")
	require.NoError(t, err)
	options := sdk.(*ec2.Client).Options()
	require.Equal(t, "us-west-2", options.Region)
	key, err := options.Credentials.Retrieve(t.Context())
	require.NoError(t, err)
	require.Equal(t, "AKIABEAMTEST", key.AccessKeyID)
	require.Equal(t, "secret", key.SecretAccessKey)
}

const (
	gcpTestProject = "beam-test"
	gcpTestEmail   = "buyer@beam-test.iam.gserviceaccount.com"
	gcpTestKeyID   = "0f1e2d3c4b5a69788796a5b4c3d2e1f001234567"
)

// gcpTestKeyJSON is the key file Google hands out for the test service
// account.
func gcpTestKeyJSON(t *testing.T, tokenURL string) string {
	t.Helper()
	key, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     gcpTestProject,
		"private_key_id": gcpTestKeyID,
		"private_key":    testServiceKeyPEM(t),
		"client_email":   gcpTestEmail,
		"token_uri":      tokenURL,
	})
	require.NoError(t, err)
	return string(key)
}

// gcpAPI stands in for Compute Engine. It answers with the shapes of
// Google's published API description
// (https://compute.googleapis.com/$discovery/rest?version=v1) and keeps the
// rules its documentation gives for a Spot VM with GPUs: which disks a
// machine series boots from, what a Spot VM's scheduling may say, how a
// request ID makes a create safe to repeat. The Spot prices of us-central1
// are the ones on Google's price list on 2026-11-13. The rest of what an
// account sees is made up, because Google shows it to its customers only:
// which zones have which machine types, and the codes its refusals come
// with, which are the ones its troubleshooting pages name.
type gcpAPI struct {
	apiStandIn

	nextID int
	// tokens counts the access tokens given out.
	tokens int
	// slept is every wait the client asked for.
	slept []time.Duration

	// machineTypes are each zone's.
	machineTypes map[string][]map[string]any
	// prices are the price histories of each region's machine types.
	prices     map[string]map[string][]map[string]any
	subnets    []string
	instances  []map[string]any
	operations map[string]*gcpTestOperation
	// byRequest remembers which operation a request ID started.
	byRequest map[string]string

	// pageOf, when set, is the most a list returns at a time.
	pageOf int
	// refusal, when set, is the answer to every create.
	refusal *gcpReply
	// failure, when set, is how every create ends after it was accepted.
	failure *gcpReply
	// answersAfter is how often an operation is asked about before it is
	// done. Negative means never.
	answersAfter int
	// losesOperations makes every question about an operation fail.
	losesOperations bool
	// deleteRefusal, when set, is the answer to every delete.
	deleteRefusal *gcpReply
	// priceRefusals are the answers to the question of a machine type's
	// price history, by machine type.
	priceRefusals map[string]gcpReply
}

// gcpReply is an error of Compute Engine's: an HTTP status, with the reason
// of a call that was turned down or the code of an operation that failed.
type gcpReply struct {
	status  int
	reason  string
	message string
}

type gcpTestOperation struct {
	body    map[string]any
	failure *gcpReply
	// asked counts the questions about it.
	asked int
}

func gcpMachineTypeOf(name string, vcpus, memoryGB int, accelerator string, count int) map[string]any {
	machineType := map[string]any{
		"kind": "compute#machineType", "name": name, "guestCpus": vcpus, "memoryMb": memoryGB * 1024, "architecture": "X86_64",
	}
	if accelerator != "" {
		machineType["accelerators"] = []any{map[string]any{"guestAcceleratorType": accelerator, "guestAcceleratorCount": count}}
	}
	return machineType
}

func gcpMoney(dollars float64, from string) map[string]any {
	units := int64(dollars)
	return map[string]any{
		"interval":  map[string]any{"startTime": from},
		"listPrice": map[string]any{"currencyCode": "USD", "units": strconv.FormatInt(units, 10), "nanos": int64((dollars-float64(units))*1e9 + 0.5)},
	}
}

func newGCPAPI(t *testing.T) *gcpAPI {
	api := &gcpAPI{
		apiStandIn: apiStandIn{t: t, now: time.Date(2026, 11, 14, 4, 0, 0, 0, time.UTC)},
		operations: map[string]*gcpTestOperation{},
		byRequest:  map[string]string{},
		subnets:    []string{"regions/us-central1/subnetworks/beam-workers", "projects/beam-network/regions/europe-west4/subnetworks/shared"},
	}

	l4 := gcpMachineTypeOf("g2-standard-4", 4, 16, "nvidia-l4", 1)
	l4x2 := gcpMachineTypeOf("g2-standard-24", 24, 96, "nvidia-l4", 2)
	l4x4 := gcpMachineTypeOf("g2-standard-48", 48, 192, "nvidia-l4", 4)
	a100 := gcpMachineTypeOf("a2-highgpu-1g", 12, 85, "nvidia-tesla-a100", 1)
	a100x80 := gcpMachineTypeOf("a2-ultragpu-1g", 12, 170, "nvidia-a100-80gb", 1)
	h100 := gcpMachineTypeOf("a3-highgpu-1g", 26, 234, "nvidia-h100-80gb", 1)
	h200 := gcpMachineTypeOf("a3-ultragpu-8g", 224, 2952, "nvidia-h200-141gb", 8)
	b200 := gcpMachineTypeOf("a4-highgpu-8g", 224, 3968, "nvidia-b200", 8)
	rtx := gcpMachineTypeOf("g4-standard-48", 48, 180, "nvidia-rtx-pro-6000", 1)
	// Not sold as a Spot VM: Compute Engine has no price history to give.
	mega := gcpMachineTypeOf("a3-megagpu-8g", 208, 1872, "nvidia-h100-mega-80gb", 8)
	// Not for this client: no GPU, an Arm machine, an accelerator that is
	// not NVIDIA's, and a machine type Google has retired.
	plain := gcpMachineTypeOf("n1-standard-4", 4, 15, "", 0)
	arm := gcpMachineTypeOf("a4x-highgpu-1g", 35, 240, "nvidia-gb200", 1)
	arm["architecture"] = "ARM64"
	tpu := gcpMachineTypeOf("ct5lp-hightpu-1t", 24, 48, "ct5lp", 1)
	retired := gcpMachineTypeOf("a2-highgpu-2g", 24, 170, "nvidia-tesla-a100", 2)
	retired["deprecated"] = map[string]any{"state": "DEPRECATED"}
	current := gcpMachineTypeOf("a3-highgpu-2g", 52, 468, "nvidia-h100-80gb", 2)
	current["deprecated"] = map[string]any{"state": "ACTIVE"}
	api.machineTypes = map[string][]map[string]any{
		"us-central1-a":  {plain, l4, l4x2, l4x4, a100, arm, tpu},
		"us-central1-b":  {plain, l4, h100, h200, current, mega},
		"us-central1-c":  {a100x80, rtx, retired},
		"europe-west4-a": {plain, l4, b200},
		"europe-west4-b": {plain},
		// A region this client is not told to buy in.
		"asia-east1-a": {plain, l4},
	}

	api.prices = map[string]map[string][]map[string]any{
		"us-central1": {
			// The history comes in no order, and the last price is the
			// one in force.
			"g2-standard-4":  {gcpMoney(0.3961, "2026-03-02T08:00:00Z"), gcpMoney(0.423, "2026-09-18T07:00:00Z"), gcpMoney(0.4102, "2026-06-11T07:00:00Z")},
			"a2-highgpu-1g":  {gcpMoney(2.188, "2026-10-01T07:00:00Z")},
			"a2-ultragpu-1g": {gcpMoney(3.036, "2026-10-01T07:00:00Z")},
			"a3-highgpu-1g":  {gcpMoney(6.588, "2026-10-01T07:00:00Z")},
			"a3-highgpu-2g":  {gcpMoney(13.176, "2026-10-01T07:00:00Z")},
			"a3-ultragpu-8g": {gcpMoney(50.871, "2026-10-01T07:00:00Z")},
			"g4-standard-48": {gcpMoney(1.718, "2026-10-01T07:00:00Z")},
			// A price of nothing is not one to buy at.
			"g2-standard-24": {gcpMoney(0, "2026-10-01T07:00:00Z")},
		},
		"europe-west4": {
			"g2-standard-4": {gcpMoney(0.467, "2026-10-01T07:00:00Z")},
			"a4-highgpu-8g": {gcpMoney(46.2, "2026-10-01T07:00:00Z")},
		},
	}
	// Priced in another currency.
	api.prices["us-central1"]["g2-standard-48"] = []map[string]any{{
		"interval":  map[string]any{"startTime": "2026-10-01T07:00:00Z"},
		"listPrice": map[string]any{"currencyCode": "EUR", "units": "1", "nanos": 500000000},
	}}

	const project = "/compute/v1/projects/{project}"
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+project+"/regions/{region}", api.region)
	mux.HandleFunc("GET "+project+"/zones/{zone}/machineTypes", api.listMachineTypes)
	mux.HandleFunc("POST "+project+"/regions/{region}/advice/capacityHistory", api.priceHistory)
	mux.HandleFunc("POST "+project+"/zones/{zone}/instances", api.insert)
	mux.HandleFunc("GET "+project+"/zones/{zone}/operations/{operation}", api.operation)
	mux.HandleFunc("GET "+project+"/zones/{zone}/instances/{instance}", api.get)
	mux.HandleFunc("DELETE "+project+"/zones/{zone}/instances/{instance}", api.delete)
	mux.HandleFunc("GET "+project+"/aggregated/instances", api.list)
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.mu.Lock()
		defer api.mu.Unlock()
		api.requests = append(api.requests, r.Method+" "+r.URL.RequestURI())
		if r.Method == http.MethodPost && r.URL.Path == "/token" {
			api.token(w, r)
			return
		}
		var token int
		_, err := fmt.Sscanf(r.Header.Get("Authorization"), "Bearer gcp-token-%d", &token)
		if err != nil || token < 1 || token > api.tokens {
			api.fail(w, gcpReply{http.StatusUnauthorized, "authError", "Invalid Credentials"})
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/compute/v1/projects/"+gcpTestProject+"/") {
			api.fail(w, gcpReply{http.StatusNotFound, "notFound", "The resource '" + r.URL.Path + "' was not found"})
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(api.server.Close)
	return api
}

// client returns a client of the stand-in's Compute Engine, configured as a
// deployment is unless the test changes it.
func (a *gcpAPI) client(owner string, configure ...func(*types.GCPProviderConfig)) *GCP {
	config := types.GCPProviderConfig{
		ServiceAccountKey: gcpTestKeyJSON(a.t, a.server.URL+"/token"),
		Regions:           []types.GCPProviderRegionConfig{{Name: "us-central1"}, {Name: "europe-west4"}},
		BaseURL:           a.server.URL + "/compute/v1",
	}
	for _, change := range configure {
		change(&config)
	}
	client, err := NewGCP(config, owner)
	require.NoError(a.t, err)
	// The stand-in's clock, and waits that are written down and not kept.
	client.now = a.clock
	client.api.sleep = func(_ context.Context, d time.Duration) error {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.slept = append(a.slept, d)
		return nil
	}
	client.login.sleep = client.api.sleep
	return client
}

func (a *gcpAPI) fail(w http.ResponseWriter, reply gcpReply) {
	writeJSON(w, reply.status, map[string]any{"error": map[string]any{
		"code":    reply.status,
		"message": reply.message,
		"errors":  []any{map[string]any{"message": reply.message, "domain": "global", "reason": reply.reason}},
	}})
}

func (a *gcpAPI) address(parts ...string) string {
	return "https://www.googleapis.com/compute/v1/projects/" + gcpTestProject + "/" + strings.Join(parts, "/")
}

// token trades a request signed by the service account for an access token,
// and turns down one that is not as Google's documentation asks for it.
func (a *gcpAPI) token(w http.ResponseWriter, r *http.Request) {
	refuse := func(why string) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_grant", "error_description": why})
	}
	if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" || r.ParseForm() != nil {
		refuse("the request is not a form")
		return
	}
	if r.PostForm.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
		refuse("unsupported grant type")
		return
	}
	parts := strings.Split(r.PostForm.Get("assertion"), ".")
	if len(parts) != 3 {
		refuse("the assertion is not a JWT")
		return
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err != nil || rsa.VerifyPKCS1v15(&testServiceKey().PublicKey, crypto.SHA256, digest[:], signature) != nil {
		refuse("Invalid JWT Signature.")
		return
	}
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	var claims struct {
		Issuer   string `json:"iss"`
		Scope    string `json:"scope"`
		Audience string `json:"aud"`
		IssuedAt int64  `json:"iat"`
		Expires  int64  `json:"exp"`
	}
	for part, into := range map[string]any{parts[0]: &header, parts[1]: &claims} {
		data, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil || json.Unmarshal(data, into) != nil {
			refuse("the assertion cannot be read")
			return
		}
	}
	age := a.now.Sub(time.Unix(claims.IssuedAt, 0))
	lasts := time.Unix(claims.Expires, 0).Sub(time.Unix(claims.IssuedAt, 0))
	switch {
	case header.Algorithm != "RS256" || header.KeyID != gcpTestKeyID:
		refuse("the assertion names another key")
	case claims.Issuer != gcpTestEmail:
		refuse("Invalid JWT: the issuer is not a service account of this key")
	case claims.Scope != "https://www.googleapis.com/auth/compute":
		refuse("Invalid OAuth scope or ID token audience provided.")
	case claims.Audience != a.server.URL+"/token":
		refuse("Invalid JWT: the audience is not the token endpoint")
	case age < -time.Minute || age > time.Minute || lasts <= 0 || lasts > time.Hour:
		refuse("Invalid JWT: Token must be a short-lived token (60 minutes) and in a reasonable timeframe.")
	default:
		a.tokens++
		writeJSON(w, http.StatusOK, map[string]any{"access_token": fmt.Sprintf("gcp-token-%d", a.tokens), "expires_in": 3599, "token_type": "Bearer"})
	}
}

// zones returns the zones of a region, in order.
func (a *gcpAPI) zones(region string) []string {
	zones := []string{}
	for zone := range a.machineTypes {
		if strings.HasPrefix(zone, region+"-") {
			zones = append(zones, zone)
		}
	}
	slices.Sort(zones)
	return zones
}

func (a *gcpAPI) region(w http.ResponseWriter, r *http.Request) {
	region := r.PathValue("region")
	zones := a.zones(region)
	if len(zones) == 0 {
		a.fail(w, gcpReply{http.StatusNotFound, "notFound", "The resource 'projects/" + gcpTestProject + "/regions/" + region + "' was not found"})
		return
	}
	addresses := []string{}
	for _, zone := range zones {
		addresses = append(addresses, a.address("zones", zone))
	}
	writeJSON(w, http.StatusOK, map[string]any{"kind": "compute#region", "name": region, "status": "UP", "zones": addresses})
}

// page cuts a list into Compute Engine's pages.
func (a *gcpAPI) page(r *http.Request, total int) (start, end int, next string, ok bool) {
	size, err := strconv.Atoi(r.URL.Query().Get("maxResults"))
	if err != nil || size < 1 || size > 500 {
		return 0, 0, "", false
	}
	if a.pageOf > 0 {
		size = min(size, a.pageOf)
	}
	start, _ = strconv.Atoi(r.URL.Query().Get("pageToken"))
	start = min(start, total)
	end = min(start+size, total)
	if end < total {
		next = strconv.Itoa(end)
	}
	return start, end, next, true
}

func (a *gcpAPI) listMachineTypes(w http.ResponseWriter, r *http.Request) {
	machineTypes, known := a.machineTypes[r.PathValue("zone")]
	if !known {
		a.fail(w, gcpReply{http.StatusBadRequest, "invalid", "Invalid value for field 'zone': '" + r.PathValue("zone") + "'. Unknown zone."})
		return
	}
	start, end, next, ok := a.page(r, len(machineTypes))
	if !ok {
		a.fail(w, gcpReply{http.StatusBadRequest, "invalid", "Invalid value for field 'maxResults'"})
		return
	}
	body := map[string]any{"kind": "compute#machineTypeList", "items": machineTypes[start:end]}
	if next != "" {
		body["nextPageToken"] = next
	}
	writeJSON(w, http.StatusOK, body)
}

func (a *gcpAPI) priceHistory(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Types              []string `json:"types"`
		InstanceProperties struct {
			MachineType string `json:"machineType"`
			Scheduling  struct {
				ProvisioningModel string `json:"provisioningModel"`
			} `json:"scheduling"`
		} `json:"instanceProperties"`
	}
	assert.NoError(a.t, json.NewDecoder(r.Body).Decode(&body))
	region, machineType := r.PathValue("region"), body.InstanceProperties.MachineType
	if !slices.Equal(body.Types, []string{"PRICE"}) || body.InstanceProperties.Scheduling.ProvisioningModel != "SPOT" {
		a.fail(w, gcpReply{http.StatusBadRequest, "invalid", "the stand-in tells the price history of Spot VMs only"})
		return
	}
	if refusal, refused := a.priceRefusals[machineType]; refused {
		a.fail(w, refusal)
		return
	}
	history, known := a.prices[region][machineType]
	if !known {
		a.fail(w, gcpReply{http.StatusBadRequest, "invalid", "Invalid value for field 'instanceProperties.machineType': '" + machineType + "'. Capacity history is not supported for the machine type."})
		return
	}
	answer := map[string]any{"location": a.address("regions", region), "machineType": machineType}
	if len(history) > 0 {
		answer["priceHistory"] = history
	}
	writeJSON(w, http.StatusOK, answer)
}

var (
	gcpTestName  = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
	gcpTestKey   = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	gcpTestValue = regexp.MustCompile(`^[a-z0-9_-]{0,63}$`)
)

func (a *gcpAPI) insert(w http.ResponseWriter, r *http.Request) {
	zone := r.PathValue("zone")
	var body map[string]any
	assert.NoError(a.t, json.NewDecoder(r.Body).Decode(&body))

	// "The request ID must be a valid UUID with the exception that zero
	// UUID is not supported." A request that was received before is not
	// run again.
	requestID := r.URL.Query().Get("requestId")
	if requestID != "" {
		if parsed, err := uuid.Parse(requestID); err != nil || parsed == uuid.Nil {
			a.fail(w, gcpReply{http.StatusBadRequest, "invalid", "Invalid value for field 'requestId'"})
			return
		}
		if name, received := a.byRequest[zone+"/"+requestID]; received {
			writeJSON(w, http.StatusOK, a.operations[name].body)
			return
		}
	}
	if a.refusal != nil {
		a.fail(w, *a.refusal)
		return
	}
	if why := a.wrong(zone, body); why != nil {
		a.fail(w, *why)
		return
	}
	a.creates = append(a.creates, body)

	a.nextID++
	name := fmt.Sprintf("operation-17630934%05d-insert", a.nextID)
	operation := &gcpTestOperation{failure: a.failure, body: map[string]any{
		"kind": "compute#operation", "name": name, "operationType": "insert", "status": "RUNNING", "progress": 0,
		"zone": a.address("zones", zone), "targetLink": a.address("zones", zone, "instances", fmt.Sprint(body["name"])),
	}}
	a.operations[name] = operation
	if requestID != "" {
		a.byRequest[zone+"/"+requestID] = name
	}
	if operation.failure == nil {
		instance := map[string]any{
			"kind": "compute#instance", "id": strconv.Itoa(4000000000000000000 + a.nextID), "name": body["name"], "zone": a.address("zones", zone),
			"status": "PROVISIONING", "machineType": a.address(fmt.Sprint(body["machineType"])),
			"labels": body["labels"], "scheduling": body["scheduling"], "metadata": body["metadata"],
			"networkInterfaces": []any{map[string]any{"name": "nic0", "networkIP": "10.128.0.2", "accessConfigs": []any{map[string]any{"name": "External NAT", "type": "ONE_TO_ONE_NAT"}}}},
		}
		a.instances = append(a.instances, instance)
	}
	writeJSON(w, http.StatusOK, operation.body)
}

// wrong says what about a create Compute Engine would turn down.
func (a *gcpAPI) wrong(zone string, body map[string]any) *gcpReply {
	invalid := func(why string) *gcpReply { return &gcpReply{http.StatusBadRequest, "invalid", why} }
	name, _ := body["name"].(string)
	if !gcpTestName.MatchString(name) {
		return invalid("Invalid value for field 'resource.name': '" + name + "'. Must be a match of regex '[a-z]([-a-z0-9]{0,61}[a-z0-9])?'")
	}
	if a.find(zone, name) >= 0 {
		return &gcpReply{http.StatusConflict, "alreadyExists", "The resource 'projects/" + gcpTestProject + "/zones/" + zone + "/instances/" + name + "' already exists"}
	}
	labels, _ := body["labels"].(map[string]any)
	for key, value := range labels {
		if !gcpTestKey.MatchString(key) || !gcpTestValue.MatchString(fmt.Sprint(value)) {
			return invalid("Invalid value for field 'resource.labels': '" + key + "'")
		}
	}

	machineType, isOfZone := strings.CutPrefix(fmt.Sprint(body["machineType"]), "zones/"+zone+"/machineTypes/")
	sold := slices.ContainsFunc(a.machineTypes[zone], func(known map[string]any) bool { return known["name"] == machineType })
	if !isOfZone || !sold {
		return invalid("Invalid value for field 'resource.machineType': '" + fmt.Sprint(body["machineType"]) + "'. Machine type with name '" + machineType + "' does not exist in zone '" + zone + "'.")
	}

	disks, _ := body["disks"].([]any)
	if len(disks) != 1 {
		return invalid("the stand-in boots a machine from one disk")
	}
	disk := disks[0].(map[string]any)
	params, _ := disk["initializeParams"].(map[string]any)
	kind, isOfZone := strings.CutPrefix(fmt.Sprint(params["diskType"]), "zones/"+zone+"/diskTypes/")
	// What each series boots from, by Google's page on its GPU machines.
	persistent, hyperdisk := kind == "pd-balanced" || kind == "pd-ssd", kind == "hyperdisk-balanced"
	series := machineType[:strings.LastIndex(machineType, "-")]
	var boots bool
	switch {
	case strings.HasPrefix(series, "a2-") || strings.HasPrefix(series, "g2-"):
		boots = persistent
	case series == "a3-ultragpu" || strings.HasPrefix(series, "a4-") || strings.HasPrefix(series, "g4-"):
		boots = hyperdisk
	default:
		boots = persistent || hyperdisk
	}
	switch {
	case disk["boot"] != true || !strings.HasPrefix(fmt.Sprint(params["sourceImage"]), "projects/"):
		return invalid("the machine's disk is not a boot disk made from an image")
	case !isOfZone || !boots:
		return invalid("[" + kind + "] features are not compatible for creating instance.")
	}
	if _, err := strconv.ParseInt(fmt.Sprint(params["diskSizeGb"]), 10, 64); err != nil {
		return invalid("Invalid value for field 'resource.disks[0].initializeParams.diskSizeGb'")
	}

	networks, _ := body["networkInterfaces"].([]any)
	if len(networks) != 1 {
		return invalid("the stand-in gives a machine one network interface")
	}
	network := networks[0].(map[string]any)
	subnet, hasSubnet := network["subnetwork"].(string)
	inSubnet := hasSubnet && slices.Contains(a.subnets, subnet) && strings.Contains(subnet, "regions/"+zone[:strings.LastIndex(zone, "-")]+"/")
	if !inSubnet && network["network"] != "global/networks/default" {
		return invalid("Invalid value for field 'resource.networkInterfaces[0]': the network or subnetwork does not exist in the machine's region")
	}

	scheduling, _ := body["scheduling"].(map[string]any)
	spot := scheduling["provisioningModel"] == "SPOT"
	switch {
	case scheduling["onHostMaintenance"] != "TERMINATE":
		return invalid("Instances with guest accelerators do not support live migration.")
	case spot && scheduling["automaticRestart"] != false:
		return invalid("Scheduling must have preemptible be false when AutomaticRestart is true.")
	case !spot && (scheduling["preemptionNoticeDuration"] != nil || scheduling["instanceTerminationAction"] != nil):
		return invalid("Invalid value for field 'resource.scheduling': these are for Spot VMs only")
	}
	return nil
}

// find returns the place of an instance among the stand-in's.
func (a *gcpAPI) find(zone, name string) int {
	return slices.IndexFunc(a.instances, func(instance map[string]any) bool {
		return instance["name"] == name && instance["zone"] == a.address("zones", zone)
	})
}

func (a *gcpAPI) operation(w http.ResponseWriter, r *http.Request) {
	operation, known := a.operations[r.PathValue("operation")]
	switch {
	case a.losesOperations:
		a.fail(w, gcpReply{http.StatusServiceUnavailable, "backendError", "Backend Error"})
		return
	case !known:
		a.fail(w, gcpReply{http.StatusNotFound, "notFound", "The resource '" + r.PathValue("operation") + "' was not found"})
		return
	}
	operation.asked++
	if a.answersAfter >= 0 && operation.asked > a.answersAfter {
		operation.body["status"], operation.body["progress"] = "DONE", 100
		if failure := operation.failure; failure != nil {
			operation.body["httpErrorStatusCode"] = failure.status
			operation.body["error"] = map[string]any{"errors": []any{map[string]any{"code": failure.reason, "message": failure.message}}}
		}
	}
	writeJSON(w, http.StatusOK, operation.body)
}

func (a *gcpAPI) get(w http.ResponseWriter, r *http.Request) {
	i := a.find(r.PathValue("zone"), r.PathValue("instance"))
	if i < 0 {
		a.fail(w, gcpReply{http.StatusNotFound, "notFound", "The resource 'projects/" + gcpTestProject + "/zones/" + r.PathValue("zone") + "/instances/" + r.PathValue("instance") + "' was not found"})
		return
	}
	writeJSON(w, http.StatusOK, a.instances[i])
}

func (a *gcpAPI) delete(w http.ResponseWriter, r *http.Request) {
	zone, name := r.PathValue("zone"), r.PathValue("instance")
	i := a.find(zone, name)
	switch {
	case a.deleteRefusal != nil:
		a.fail(w, *a.deleteRefusal)
		return
	case i < 0:
		a.fail(w, gcpReply{http.StatusNotFound, "notFound", "The resource 'projects/" + gcpTestProject + "/zones/" + zone + "/instances/" + name + "' was not found"})
		return
	}
	// Whether Compute Engine takes a delete of what it is deleting already
	// is not written down. It does here; deleteRefusal makes it refuse.
	a.instances[i]["status"] = "STOPPING"
	a.nextID++
	writeJSON(w, http.StatusOK, map[string]any{
		"kind": "compute#operation", "name": fmt.Sprintf("operation-17630934%05d-delete", a.nextID), "operationType": "delete", "status": "RUNNING",
		"zone": a.address("zones", zone), "targetLink": a.address("zones", zone, "instances", name),
	})
}

// list answers with every zone, as Compute Engine does: the instances of
// the ones that have some on this page, and a warning for the others.
func (a *gcpAPI) list(w http.ResponseWriter, r *http.Request) {
	start, end, next, ok := a.page(r, len(a.instances))
	if !ok {
		a.fail(w, gcpReply{http.StatusBadRequest, "invalid", "Invalid value for field 'maxResults'"})
		return
	}
	items := map[string]any{}
	for zone := range a.machineTypes {
		items["zones/"+zone] = map[string]any{"warning": map[string]any{"code": "NO_RESULTS_ON_PAGE", "message": "There are no results for scope 'zones/" + zone + "' on this page."}}
	}
	for _, instance := range a.instances[start:end] {
		scope := "zones/" + instance["zone"].(string)[strings.LastIndex(instance["zone"].(string), "/")+1:]
		listed, _ := items[scope].(map[string]any)["instances"].([]any)
		items[scope] = map[string]any{"instances": append(listed, instance)}
	}
	body := map[string]any{"kind": "compute#instanceAggregatedList", "items": items}
	if next != "" {
		body["nextPageToken"] = next
	}
	writeJSON(w, http.StatusOK, body)
}

// state makes Compute Engine report an instance, named as this client names
// it, with the status.
func (a *gcpAPI) state(id, status string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	zone, name, _ := strings.Cut(id, "/")
	i := a.find(zone, name)
	require.GreaterOrEqual(a.t, i, 0, "no instance %s", id)
	a.instances[i]["status"] = status
}

func (a *gcpAPI) lose(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	zone, name, _ := strings.Cut(id, "/")
	if i := a.find(zone, name); i >= 0 {
		a.instances = slices.Delete(a.instances, i, i+1)
	}
}

// sent counts the requests of the method to paths of the project that begin
// with path.
func (a *gcpAPI) sent(method, path string) int {
	return a.apiStandIn.sent(method, "/compute/v1/projects/"+gcpTestProject+path)
}

// set changes the stand-in under its lock.
func (a *gcpAPI) set(change func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	change()
}

// How a create that Compute Engine accepted ends when there is no machine
// for it, or no quota.
var gcpFailures = map[error]*gcpReply{
	ErrOutOfStock: {http.StatusServiceUnavailable, "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS", "The zone 'projects/beam-test/zones/europe-west4-a' does not have enough resources available to fulfill the request. '(resource type:compute)'."},
	ErrQuota:      {http.StatusForbidden, "QUOTA_EXCEEDED", "Quota 'PREEMPTIBLE_NVIDIA_B200_GPUS' exceeded.  Limit: 0.0 in region europe-west4."},
}

func newGCPSite(t *testing.T) site {
	api := newGCPAPI(t)
	return site{
		provider: api.client(testOwner),
		statuses: map[string]Status{
			"PROVISIONING": StatusPending,
			"STAGING":      StatusPending,
			"PENDING":      StatusPending,
			"REPAIRING":    StatusPending,
			"RUNNING":      StatusActive,
			// Compute Engine has no status for an instance it is deleting.
			// It is stopping, like one that is being stopped.
			"STOPPING":       StatusDeleted,
			"PENDING_STOP":   StatusDeleted,
			"DEPROVISIONING": StatusDeleted,
			// The site's instance is a Spot VM, as all this client buys.
			"STOPPED":    StatusInterrupted,
			"TERMINATED": StatusInterrupted,
			"SUSPENDING": StatusInterrupted,
			"SUSPENDED":  StatusInterrupted,
		},
		report: api.state,
		refuse: func(reason error) { api.set(func() { api.failure = gcpFailures[reason] }) },
		lose:   api.lose,
		bought: func() (string, string) {
			insert := api.lastCreate()
			items := insert["metadata"].(map[string]any)["items"].([]any)
			require.Len(t, items, 1)
			require.Equal(t, "startup-script", items[0].(map[string]any)["key"])
			return insert["name"].(string), items[0].(map[string]any)["value"].(string)
		},
		plantForeign: func() string {
			offers, err := api.client(foreignOwner).Offers(context.Background(), OfferFilter{})
			require.NoError(t, err)
			instance, err := api.client(foreignOwner).Create(context.Background(), CreateRequest{Offer: offers[0], Name: "someone-elses"})
			require.NoError(t, err)
			return instance.ID
		},
		boughtSpot: func() bool {
			return api.lastCreate()["scheduling"].(map[string]any)["provisioningModel"] == "SPOT"
		},
		// Google stops a Spot VM it takes back, when the VM was made to be
		// stopped and not deleted.
		takeBack: func(id string) { api.state(id, "TERMINATED") },
	}
}

// What a machine's 200 GB disk costs an hour, at $0.10 a gigabyte a month.
const gcpTestDisk = 27_397

func gcpOffer(zone, machineType, gpu string, gpus uint32, vcpus, memoryGB, micros int64) Offer {
	return Offer{
		ID: zone + "/" + machineType, Provider: "gcp", Cloud: "gcp", InstanceType: machineType, Region: zone[:strings.LastIndex(zone, "-")],
		GPU: gpu, GPUCount: gpus, CPUMillicores: vcpus * 1000, MemoryMB: memoryGB * 1024, StorageMB: 200 * 1024,
		HourlyCostMicros: micros + gcpTestDisk, DiskHourlyMicros: gcpTestDisk, Spot: true,
	}
}

var (
	gcpL4InA    = gcpOffer("us-central1-a", "g2-standard-4", "L4", 1, 4, 16, 423_000)
	gcpL4InB    = gcpOffer("us-central1-b", "g2-standard-4", "L4", 1, 4, 16, 423_000)
	gcpL4Europe = gcpOffer("europe-west4-a", "g2-standard-4", "L4", 1, 4, 16, 467_000)
	gcpH200     = gcpOffer("us-central1-b", "a3-ultragpu-8g", "H200", 8, 224, 2952, 50_871_000)
	gcpH100     = gcpOffer("us-central1-b", "a3-highgpu-1g", "H100", 1, 26, 234, 6_588_000)
)

func TestGCPOffersEachGPUMachineTypeInEachZoneThatHasIt(t *testing.T) {
	api := newGCPAPI(t)
	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)

	// Google's name for the A100 with 40 GB does not say so. The machine
	// types with two and with four L4s have no Spot price in dollars, the
	// one with eight H100s is not sold as a Spot VM, and the price of the
	// one L4 is the last of its history.
	require.Equal(t, []Offer{
		gcpOffer("europe-west4-a", "a4-highgpu-8g", "B200", 8, 224, 3968, 46_200_000),
		gcpL4Europe,
		gcpOffer("us-central1-a", "a2-highgpu-1g", "A100-40", 1, 12, 85, 2_188_000),
		gcpOffer("us-central1-c", "a2-ultragpu-1g", "A100-80", 1, 12, 170, 3_036_000),
		gcpH100,
		gcpOffer("us-central1-b", "a3-highgpu-2g", "H100", 2, 52, 468, 13_176_000),
		gcpH200,
		gcpL4InA,
		gcpL4InB,
		gcpOffer("us-central1-c", "g4-standard-48", "RTXPro6000", 1, 48, 180, 1_718_000),
	}, offers)

	// The price of a machine type is asked for once for a region, and not
	// at all for what is not for this client.
	require.Equal(t, 10, api.sent("POST", "/regions/us-central1/advice/capacityHistory"))
	require.Equal(t, 2, api.sent("POST", "/regions/europe-west4/advice/capacityHistory"))
}

func TestGCPOffersOnlyTheRegionsAskedFor(t *testing.T) {
	api := newGCPAPI(t)
	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{Regions: []string{"europe-west4"}, GPUs: []string{"L4"}})
	require.NoError(t, err)
	require.Equal(t, []Offer{gcpL4Europe}, offers)
	require.Zero(t, api.sent("GET", "/regions/us-central1"), "a region that was not asked for is not read")
}

func TestGCPReadsEveryPageOfAList(t *testing.T) {
	api := newGCPAPI(t)
	api.pageOf = 2
	client := api.client(testOwner)
	ctx := context.Background()

	offers, err := client.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.Len(t, offers, 10)

	for i, offer := range []Offer{gcpL4InA, gcpL4InA, gcpL4InB, gcpL4Europe, gcpH100} {
		_, err := client.Create(ctx, CreateRequest{Offer: offer, Name: fmt.Sprintf("beam-machine-%d", i)})
		require.NoError(t, err)
	}
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 5)
	require.Equal(t, 3, api.sent("GET", "/aggregated/instances"))
}

func TestGCPReadsARegionOnceAnHour(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner)
	for range 3 {
		_, err := client.Offers(context.Background(), OfferFilter{})
		require.NoError(t, err)
	}
	require.Equal(t, 1, api.sent("GET", "/regions/us-central1"))
	require.Equal(t, 10, api.sent("POST", "/regions/us-central1/advice/capacityHistory"))

	api.after(time.Hour)
	_, err := client.Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, api.sent("GET", "/regions/us-central1"))
	require.Equal(t, 20, api.sent("POST", "/regions/us-central1/advice/capacityHistory"))
}

// A question that Google cannot answer is not the same as a machine type
// without a price: the region's offers are not known, and are not made up.
// Asked for that region alone, the client says what it could not learn.
func TestGCPSaysWhatItCouldNotLearn(t *testing.T) {
	for name, tc := range map[string]struct {
		change func(*gcpAPI, *types.GCPProviderConfig)
		region string
		want   string
	}{
		"a region Google does not have": {
			change: func(_ *gcpAPI, config *types.GCPProviderConfig) {
				config.Regions = append(config.Regions, types.GCPProviderRegionConfig{Name: "mars-north1"})
			},
			region: "mars-north1",
			want:   "GCP region mars-north1: GET /projects/beam-test/regions/mars-north1: status 404",
		},
		// One machine type without a Spot price is not sold as a Spot VM.
		// A region of them is a question Google does not take, or an
		// answer this client cannot read.
		"no price for any machine type of a region": {
			change: func(_ *gcpAPI, config *types.GCPProviderConfig) {
				config.Regions = append(config.Regions, types.GCPProviderRegionConfig{Name: "asia-east1"})
			},
			region: "asia-east1",
			want:   "GCP region asia-east1: none of its 1 GPU machine types has a Spot price in dollars: g2-standard-4 has no Spot price: POST /projects/beam-test/regions/asia-east1/advice/capacityHistory: status 400",
		},
		"prices in another currency": {
			change: func(api *gcpAPI, config *types.GCPProviderConfig) {
				config.Regions = []types.GCPProviderRegionConfig{{Name: "asia-east1"}}
				api.prices["asia-east1"] = map[string][]map[string]any{"g2-standard-4": api.prices["us-central1"]["g2-standard-48"]}
			},
			region: "asia-east1",
			want:   "g2-standard-4 has no Spot price: Compute Engine prices it at 1.500000 EUR",
		},
		"a region whose prices have no history": {
			change: func(api *gcpAPI, config *types.GCPProviderConfig) {
				config.Regions = []types.GCPProviderRegionConfig{{Name: "asia-east1"}}
				api.prices["asia-east1"] = map[string][]map[string]any{"g2-standard-4": {}}
			},
			region: "asia-east1",
			want:   "g2-standard-4 has no Spot price: Compute Engine has no history of one",
		},
	} {
		t.Run(name, func(t *testing.T) {
			api := newGCPAPI(t)
			client := api.client(testOwner, func(config *types.GCPProviderConfig) { tc.change(api, config) })
			_, err := client.Offers(context.Background(), OfferFilter{Regions: []string{tc.region}})
			require.ErrorContains(t, err, tc.want)
		})
	}

	// Only one answer says that a machine type is not sold as a Spot VM.
	// After any other, its price is not known, and the region's offers
	// without it would be wrong for as long as they are kept.
	for name, refusal := range map[string]gcpReply{
		"the service account may not ask for prices": {http.StatusForbidden, "forbidden", "Required 'compute.advice.capacityHistory' permission for 'projects/beam-test'"},
		"Google does not know what is asked about":   {http.StatusNotFound, "notFound", "The resource 'projects/beam-test/regions/us-central1' was not found"},
		"Google is not well":                         {http.StatusServiceUnavailable, "backendError", "Backend Error"},
	} {
		t.Run(name, func(t *testing.T) {
			api := newGCPAPI(t)
			api.priceRefusals = map[string]gcpReply{"a2-highgpu-1g": refusal}
			client := api.client(testOwner)
			_, err := client.Offers(context.Background(), OfferFilter{Regions: []string{"us-central1"}})
			require.ErrorContains(t, err, "GCP region us-central1: the Spot price of a2-highgpu-1g: POST /projects/beam-test/regions/us-central1/advice/capacityHistory: status "+strconv.Itoa(refusal.status))
			require.ErrorContains(t, err, refusal.message)
			_, err = client.List(context.Background())
			require.NoError(t, err, "what is bought can still be listed")

			// The other region is offered meanwhile.
			offers, err := client.Offers(context.Background(), OfferFilter{})
			require.NoError(t, err)
			require.Len(t, offers, 2)

			// The region is asked again the next time, and not remembered.
			api.set(func() { api.priceRefusals = nil })
			offers, err = client.Offers(context.Background(), OfferFilter{})
			require.NoError(t, err)
			require.Len(t, offers, 10)
		})
	}
}

func TestGCPBuysInAnotherProjectWhenToldTo(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner, func(config *types.GCPProviderConfig) { config.Project = "another-project" })
	_, err := client.Offers(context.Background(), OfferFilter{})
	require.ErrorContains(t, err, "/projects/another-project/regions/us-central1: status 404")
}

// A service account signs in by signing a request for a token, which
// Google's stand-in here checks as Google documents it.
func TestGCPTradesASignedRequestForATokenAndRenewsIt(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	for range 3 {
		_, err := client.List(ctx)
		require.NoError(t, err)
	}
	require.Equal(t, 1, api.tokens)

	// A token lasts an hour, and is replaced before it runs out.
	api.after(59 * time.Minute)
	_, err := client.List(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, api.tokens)

	// A token Google no longer takes is replaced at once.
	api.set(func() { api.tokens = 7 })
	api.server.Config.Handler = func(next http.Handler) http.Handler {
		refused := false
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !refused && r.URL.Path != "/token" {
				refused = true
				api.fail(w, gcpReply{http.StatusUnauthorized, "authError", "Invalid Credentials"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}(api.server.Config.Handler)
	_, err = client.List(ctx)
	require.NoError(t, err)
	require.Equal(t, 8, api.tokens)
}

func TestGCPCreateSendsWhatComputeEngineNeeds(t *testing.T) {
	api := newGCPAPI(t)
	created, err := api.client(testOwner).Create(context.Background(), CreateRequest{
		Offer:         gcpL4InB,
		Name:          "beam-l4-a1b2c3d4",
		StartupScript: "curl -fsSL https://gateway.example/join | sh",
		Tags:          map[string]string{"beam-machine": "a1b2c3d4"},
		// Google sells a Spot VM at its price for one, and takes no limit.
		MaxHourlyCostMicros: 500_000,
	})
	require.NoError(t, err)
	require.Equal(t, Instance{ID: "us-central1-b/beam-l4-a1b2c3d4", Status: StatusPending, Tags: map[string]string{"beam-machine": "a1b2c3d4"}}, created)

	require.Equal(t, map[string]any{
		"name":        "beam-l4-a1b2c3d4",
		"machineType": "zones/us-central1-b/machineTypes/g2-standard-4",
		"labels":      map[string]any{"beam-machine": "a1b2c3d4", OwnerTag: testOwner},
		"disks": []any{map[string]any{
			"boot":       true,
			"autoDelete": true,
			"initializeParams": map[string]any{
				"sourceImage": "projects/ubuntu-os-accelerator-images/global/images/family/ubuntu-accelerator-2404-amd64-with-nvidia-580",
				"diskSizeGb":  "200",
				"diskType":    "zones/us-central1-b/diskTypes/pd-balanced",
			},
		}},
		"networkInterfaces": []any{map[string]any{
			"network":       "global/networks/default",
			"accessConfigs": []any{map[string]any{"name": "External NAT", "type": "ONE_TO_ONE_NAT"}},
		}},
		"scheduling": map[string]any{
			"provisioningModel":         "SPOT",
			"instanceTerminationAction": "STOP",
			"automaticRestart":          false,
			"onHostMaintenance":         "TERMINATE",
			"preemptionNoticeDuration":  map[string]any{"seconds": "120"},
		},
		"metadata": map[string]any{"items": []any{map[string]any{
			"key":   "startup-script",
			"value": "#!/usr/bin/env bash\nset -euo pipefail\ncurl -fsSL https://gateway.example/join | sh\n",
		}}},
	}, api.lastCreate())
}

// The two older GPU series boot from Persistent Disk and not from
// Hyperdisk, and the newest from Hyperdisk only. The stand-in turns down a
// disk a series does not boot from, as Compute Engine does.
func TestGCPBootsEachSeriesFromADiskItTakes(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner)
	offers, err := client.Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	kinds := map[string]string{}
	for i, offer := range offers {
		_, err := client.Create(context.Background(), CreateRequest{Offer: offer, Name: fmt.Sprintf("beam-machine-%d", i)})
		require.NoError(t, err, offer.ID)
		disk := api.lastCreate()["disks"].([]any)[0].(map[string]any)["initializeParams"].(map[string]any)["diskType"].(string)
		kinds[offer.InstanceType] = disk[strings.LastIndex(disk, "/")+1:]
	}
	require.Equal(t, map[string]string{
		"g2-standard-4":  "pd-balanced",
		"a2-highgpu-1g":  "pd-balanced",
		"a2-ultragpu-1g": "pd-balanced",
		"a3-highgpu-1g":  "hyperdisk-balanced",
		"a3-highgpu-2g":  "hyperdisk-balanced",
		"a3-ultragpu-8g": "hyperdisk-balanced",
		"a4-highgpu-8g":  "hyperdisk-balanced",
		"g4-standard-48": "hyperdisk-balanced",
	}, kinds)
}

func TestGCPUsesTheSubnetImageAndDiskItIsGiven(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner, func(config *types.GCPProviderConfig) {
		config.Regions = []types.GCPProviderRegionConfig{
			{Name: "us-central1", Subnetwork: "beam-workers"},
			// Another project's, by its path.
			{Name: "europe-west4", Subnetwork: "projects/beam-network/regions/europe-west4/subnetworks/shared"},
		}
		config.Image = "projects/beam-test/global/images/beam-worker-v7"
		config.DiskGB = 500
	})

	offers, err := client.Offers(context.Background(), OfferFilter{GPUs: []string{"L4"}})
	require.NoError(t, err)
	require.Len(t, offers, 3)
	require.Equal(t, int64(500*1024), offers[0].StorageMB)
	require.Equal(t, int64(467_000+68_493), offers[0].HourlyCostMicros, "with the price of the disk")

	network := func() map[string]any { return api.lastCreate()["networkInterfaces"].([]any)[0].(map[string]any) }
	_, err = client.Create(context.Background(), CreateRequest{Offer: gcpL4InA, Name: "beam-l4-a1b2c3d4"})
	require.NoError(t, err)
	require.Equal(t, "regions/us-central1/subnetworks/beam-workers", network()["subnetwork"])
	require.NotContains(t, network(), "network")
	params := api.lastCreate()["disks"].([]any)[0].(map[string]any)["initializeParams"].(map[string]any)
	require.Equal(t, "projects/beam-test/global/images/beam-worker-v7", params["sourceImage"])
	require.Equal(t, "500", params["diskSizeGb"])
	require.NotContains(t, api.lastCreate(), "metadata", "no script was asked for")

	_, err = client.Create(context.Background(), CreateRequest{Offer: gcpL4Europe, Name: "beam-l4-e5f6a7b8"})
	require.NoError(t, err)
	require.Equal(t, "projects/beam-network/regions/europe-west4/subnetworks/shared", network()["subnetwork"])
}

// Compute Engine accepts a create before it has looked for a machine. Why
// it found none comes a few seconds later, and a create waits for that.
func TestGCPCreateWaitsToHearWhetherThereWasAMachine(t *testing.T) {
	ctx := context.Background()
	req := CreateRequest{Offer: gcpL4InA, Name: "beam-l4-a1b2c3d4"}

	t.Run("there was none", func(t *testing.T) {
		api := newGCPAPI(t)
		api.failure, api.answersAfter = gcpFailures[ErrOutOfStock], 2
		_, err := api.client(testOwner).Create(ctx, req)
		require.ErrorIs(t, err, ErrOutOfStock)
		require.ErrorContains(t, err, "does not have enough resources")
		require.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}, api.slept)

		_, err = api.client(testOwner).Get(ctx, "us-central1-a/beam-l4-a1b2c3d4")
		require.ErrorIs(t, err, ErrNotFound, "nothing was bought")
	})

	t.Run("the account may have no more", func(t *testing.T) {
		api := newGCPAPI(t)
		api.failure = gcpFailures[ErrQuota]
		_, err := api.client(testOwner).Create(ctx, req)
		require.ErrorIs(t, err, ErrQuota)
	})

	t.Run("it failed for another reason", func(t *testing.T) {
		api := newGCPAPI(t)
		api.failure = &gcpReply{http.StatusBadRequest, "CONDITION_NOT_MET", "Constraint constraints/compute.vmExternalIpAccess violated"}
		_, err := api.client(testOwner).Create(ctx, req)
		require.ErrorContains(t, err, "CONDITION_NOT_MET: Constraint constraints/compute.vmExternalIpAccess violated")
		require.NotErrorIs(t, err, ErrOutOfStock)
		require.NotErrorIs(t, err, ErrQuota)
	})

	t.Run("there was one", func(t *testing.T) {
		api := newGCPAPI(t)
		created, err := api.client(testOwner).Create(ctx, req)
		require.NoError(t, err)
		require.Equal(t, StatusPending, created.Status)
		require.Equal(t, []time.Duration{time.Second}, api.slept)
	})

	// The machine may be bought. The next look at it tells.
	t.Run("the answer does not come in time", func(t *testing.T) {
		api := newGCPAPI(t)
		api.failure, api.answersAfter = gcpFailures[ErrOutOfStock], -1
		created, err := api.client(testOwner).Create(ctx, req)
		require.NoError(t, err)
		require.Equal(t, Instance{ID: "us-central1-a/beam-l4-a1b2c3d4", Status: StatusPending}, created)
		require.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}, api.slept)
		require.Equal(t, 4, api.sent("GET", "/zones/us-central1-a/operations/"))
	})

	t.Run("the question gets no answer", func(t *testing.T) {
		api := newGCPAPI(t)
		api.losesOperations = true
		created, err := api.client(testOwner).Create(ctx, req)
		require.NoError(t, err)
		require.Equal(t, StatusPending, created.Status)
		found, err := api.client(testOwner).Get(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, StatusPending, found.Status)
	})
}

func TestGCPNamesWhyACreateWasTurnedDownAtOnce(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner)
	req := CreateRequest{Offer: gcpL4InA, Name: "beam-l4-a1b2c3d4"}

	api.refusal = &gcpReply{http.StatusForbidden, "quotaExceeded", "Quota 'GPUS_ALL_REGIONS' exceeded. Limit: 0.0 globally."}
	_, err := client.Create(context.Background(), req)
	require.ErrorIs(t, err, ErrQuota)
	require.ErrorContains(t, err, "GPUS_ALL_REGIONS")

	api.refusal = &gcpReply{http.StatusForbidden, "forbidden", "Required 'compute.instances.create' permission"}
	_, err = client.Create(context.Background(), req)
	require.ErrorContains(t, err, "compute.instances.create")
	require.NotErrorIs(t, err, ErrQuota)
	require.NotErrorIs(t, err, ErrOutOfStock)
	require.Empty(t, api.slept, "there is no operation to ask about")
}

// A create that got no answer is sent again. It carries a request ID that
// Compute Engine runs one create for, however often it arrives.
func TestGCPACreateSentTwiceBuysOneMachine(t *testing.T) {
	api := newGCPAPI(t)
	answered := false
	api.server.Config.Handler = func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/instances") || answered {
				next.ServeHTTP(w, r)
				return
			}
			// The create arrives, and its answer is lost on the way back.
			answered = true
			next.ServeHTTP(httptest.NewRecorder(), r)
			w.WriteHeader(http.StatusBadGateway)
		})
	}(api.server.Config.Handler)

	created, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: gcpL4InA, Name: "beam-l4-a1b2c3d4"})
	require.NoError(t, err)
	require.Equal(t, "us-central1-a/beam-l4-a1b2c3d4", created.ID)
	require.Equal(t, 2, api.sent("POST", "/zones/us-central1-a/instances"))
	require.Len(t, api.instances, 1)
	require.Len(t, api.creates, 1)
}

func TestGCPBuysNothingButSpotVMsInTheRegionsItIsGiven(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner)
	onDemand := gcpL4InA
	onDemand.Spot = false
	_, err := client.Create(context.Background(), CreateRequest{Offer: onDemand, Name: "beam-l4-a1b2c3d4"})
	require.ErrorContains(t, err, "Spot")

	for name, offer := range map[string]Offer{
		"a region not configured":  gcpOffer("asia-east1-a", "g2-standard-4", "L4", 1, 4, 16, 423_000),
		"a zone of another region": {ID: "us-central1-a/g2-standard-4", Region: "asia-east1", Spot: true},
		"no zone":                  {ID: "g2-standard-4", Region: "us-central1", Spot: true},
	} {
		_, err := client.Create(context.Background(), CreateRequest{Offer: offer, Name: "beam-l4-a1b2c3d4"})
		require.Error(t, err, name)
	}
	require.Zero(t, api.sent("POST", "/zones/"))

	// An offer is this client's own and is not looked up again. A machine
	// type its zone does not have is for Compute Engine to turn down.
	elsewhere := gcpOffer("us-central1-c", "g2-standard-4", "L4", 1, 4, 16, 423_000)
	_, err = client.Create(context.Background(), CreateRequest{Offer: elsewhere, Name: "beam-l4-a1b2c3d4"})
	require.ErrorContains(t, err, "Machine type with name 'g2-standard-4' does not exist in zone 'us-central1-c'")
	require.Equal(t, 1, api.sent("POST", "/zones/"))
	require.Empty(t, api.creates, "nothing was bought")
}

// Google stops a Spot VM to take it back, and nothing here stops one.
func TestGCPReadsAStoppedSpotVMAsTakenBack(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: gcpL4InA, Name: "beam-l4-a1b2c3d4"})
	require.NoError(t, err)

	api.state(created.ID, "TERMINATED")
	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusInterrupted, StatusDetail: "TERMINATED"}, found)

	api.set(func() { api.instances[0]["statusMessage"] = "Instance was preempted." })
	found, err = client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, "TERMINATED Instance was preempted.", found.StatusDetail)

	// A machine that was not bought as a Spot VM, and stopped, has failed.
	for name, scheduling := range map[string]map[string]any{
		"preemptible, as a Spot VM used to be called": {"preemptible": true, "provisioningModel": "STANDARD"},
		"on demand": {"provisioningModel": "STANDARD"},
	} {
		api.set(func() { api.instances[0]["scheduling"] = scheduling })
		found, err = client.Get(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, scheduling["preemptible"] == true, found.Status == StatusInterrupted, name)
		require.Equal(t, scheduling["preemptible"] != true, found.Status == StatusFailed, name)
	}

	api.state(created.ID, "A_NEW_STATUS")
	found, err = client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusUnknown, StatusDetail: "A_NEW_STATUS Instance was preempted."}, found)
}

func TestGCPReportsWhereAMachineCanBeReached(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner)
	created, err := client.Create(context.Background(), CreateRequest{Offer: gcpL4InA, Name: "beam-l4-a1b2c3d4"})
	require.NoError(t, err)

	found, err := client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Empty(t, found.PublicIP, "a machine that is not up has no address")

	api.state(created.ID, "RUNNING")
	api.set(func() {
		access := api.instances[0]["networkInterfaces"].([]any)[0].(map[string]any)["accessConfigs"].([]any)[0].(map[string]any)
		access["natIP"] = "34.72.10.20"
	})
	found, err = client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusActive, PublicIP: "34.72.10.20", SSHHost: "34.72.10.20", SSHPort: 22}, found)
}

// What this control plane bought is listed wherever in the project it is,
// so that a machine in a region that is no longer configured is not lost.
func TestGCPListsItsMachinesInEveryZoneOfTheProject(t *testing.T) {
	api := newGCPAPI(t)
	wide := api.client(testOwner, func(config *types.GCPProviderConfig) {
		config.Regions = append(config.Regions, types.GCPProviderRegionConfig{Name: "asia-east1"})
	})
	api.prices["asia-east1"] = map[string][]map[string]any{"g2-standard-4": {gcpMoney(0.51, "2026-10-01T07:00:00Z")}}
	ctx := context.Background()

	// A name is a zone's, so two zones may each have a machine by it.
	asia := gcpOffer("asia-east1-a", "g2-standard-4", "L4", 1, 4, 16, 510_000)
	a100 := gcpOffer("us-central1-c", "a2-ultragpu-1g", "A100-80", 1, 12, 170, 3_036_000)
	for _, offer := range []Offer{gcpL4InB, a100, asia, gcpL4Europe, gcpL4InA} {
		_, err := wide.Create(ctx, CreateRequest{Offer: offer, Name: "beam-" + offer.Region, Tags: map[string]string{"beam-machine": offer.Region}})
		require.NoError(t, err)
	}

	// They are listed in the order of their IDs, whichever was bought first.
	_, err := wide.Create(ctx, CreateRequest{Offer: gcpH100, Name: "beam-ahead"})
	require.NoError(t, err)

	listed, err := api.client(testOwner).List(ctx)
	require.NoError(t, err)
	require.Equal(t, []Instance{
		{ID: "asia-east1-a/beam-asia-east1", Status: StatusPending, Tags: map[string]string{"beam-machine": "asia-east1"}},
		{ID: "europe-west4-a/beam-europe-west4", Status: StatusPending, Tags: map[string]string{"beam-machine": "europe-west4"}},
		{ID: "us-central1-a/beam-us-central1", Status: StatusPending, Tags: map[string]string{"beam-machine": "us-central1"}},
		{ID: "us-central1-b/beam-ahead", Status: StatusPending},
		{ID: "us-central1-b/beam-us-central1", Status: StatusPending, Tags: map[string]string{"beam-machine": "us-central1"}},
		{ID: "us-central1-c/beam-us-central1", Status: StatusPending, Tags: map[string]string{"beam-machine": "us-central1"}},
	}, listed)

	// One in a region this client is not told about can be read and
	// deleted all the same.
	client := api.client(testOwner)
	require.NoError(t, client.Delete(ctx, "asia-east1-a/beam-asia-east1"))
	found, err := client.Get(ctx, "asia-east1-a/beam-asia-east1")
	require.NoError(t, err)
	require.Equal(t, StatusDeleted, found.Status)
}

func TestGCPFindsAnInstanceByTheZoneInItsID(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: gcpL4InA, Name: "beam-l4-a1b2c3d4"})
	require.NoError(t, err)

	// None of these is the machine, and some are not IDs of this client's
	// at all, which Compute Engine is not asked about.
	for _, id := range []string{"us-central1-b/beam-l4-a1b2c3d4", "beam-l4-a1b2c3d4", "us-central1-a/", "us-central1-a/Not A Name", "../us-central1-a/beam-l4-a1b2c3d4"} {
		_, err := client.Get(ctx, id)
		require.ErrorIs(t, err, ErrNotFound, id)
		require.NoError(t, client.Delete(ctx, id), id)
	}
	require.Equal(t, 1, api.sent("DELETE", "/zones/"))
	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusPending, found.Status, "none of those deleted it")
}

// Only an instance Google no longer has is gone. One that is stopping may be
// one Google is taking back, which it leaves stopped, with a disk that is
// paid for: a delete that is refused meanwhile has deleted nothing.
func TestGCPDeleteSucceedsOnWhatIsGoneAndFailsOnARefusal(t *testing.T) {
	api := newGCPAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: gcpL4InA, Name: "beam-l4-a1b2c3d4"})
	require.NoError(t, err)

	api.deleteRefusal = &gcpReply{http.StatusForbidden, "forbidden", "Required 'compute.instances.delete' permission"}
	require.ErrorContains(t, client.Delete(ctx, created.ID), "compute.instances.delete")

	// Google takes the machine back, and turns a delete down as it stops.
	api.state(created.ID, "STOPPING")
	api.set(func() {
		api.deleteRefusal = &gcpReply{http.StatusBadRequest, "resourceNotReady", "The resource 'projects/beam-test/zones/us-central1-a/instances/beam-l4-a1b2c3d4' is not ready"}
	})
	require.ErrorContains(t, client.Delete(ctx, created.ID), "is not ready")

	// What is left of it, once it has stopped, is deleted when asked again.
	api.state(created.ID, "TERMINATED")
	api.set(func() { api.deleteRefusal = nil })
	require.NoError(t, client.Delete(ctx, created.ID))
	require.NoError(t, client.Delete(ctx, created.ID), "Google is asked again while it deletes")
	require.Equal(t, 4, api.sent("DELETE", "/zones/us-central1-a/instances/beam-l4-a1b2c3d4"))

	api.lose(created.ID)
	require.NoError(t, client.Delete(ctx, created.ID), "what is gone needs no deleting")
}

// What a region sells is read to make offers of it. A create has its offer
// already: it asks Compute Engine for the machine, and nothing about the
// region first, so it neither waits for those answers nor fails with them.
func TestGCPBuysWithoutReadingWhatTheRegionSells(t *testing.T) {
	api := newGCPAPI(t)
	// A gateway that has just started has read nothing, and now the price
	// of one machine type cannot be read.
	api.priceRefusals = map[string]gcpReply{"a2-highgpu-1g": {http.StatusServiceUnavailable, "backendError", "Backend Error"}}
	client := api.client(testOwner)

	created, err := client.Create(context.Background(), CreateRequest{Offer: gcpL4InA, Name: "beam-l4-a1b2c3d4"})
	require.NoError(t, err)
	require.Equal(t, "us-central1-a/beam-l4-a1b2c3d4", created.ID)
	require.Zero(t, api.sent("GET", "/regions/"))
	require.Zero(t, api.sent("GET", "/zones/us-central1-a/machineTypes"))
	require.Zero(t, api.sent("POST", "/regions/"))
}

const hetznerTestToken = "hetzner-test-token"

// hetznerAPI stands in for Hetzner Cloud. It answers with the shapes of
// Hetzner's published API description (https://docs.hetzner.cloud/cloud.spec.json).
type hetznerAPI struct {
	apiStandIn

	nextID  int64
	servers []map[string]any
	refusal *hetznerError
	// listsEverything makes the server list ignore the labels asked for.
	listsEverything bool
}

type hetznerError struct {
	status  int
	code    string
	message string
}

func newHetznerAPI(t *testing.T) *hetznerAPI {
	api := &hetznerAPI{apiStandIn: apiStandIn{t: t}, nextID: 41}
	mux := http.NewServeMux()
	for _, list := range []string{"server_types", "locations", "networks"} {
		mux.HandleFunc("GET /"+list, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("page") != "1" || r.URL.Query().Get("per_page") != "50" {
				api.fail(w, hetznerError{http.StatusBadRequest, "invalid_input", "expected the first page of 50"})
				return
			}
			w.Write(sample(t, "hetzner/"+list))
		})
	}
	mux.HandleFunc("POST /servers", api.create)
	mux.HandleFunc("GET /servers", api.list)
	mux.HandleFunc("GET /servers/{id}", api.get)
	mux.HandleFunc("DELETE /servers/{id}", api.delete)
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.mu.Lock()
		defer api.mu.Unlock()
		api.requests = append(api.requests, r.Method+" "+r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer "+hetznerTestToken {
			api.fail(w, hetznerError{http.StatusUnauthorized, "unauthorized", "unable to authenticate"})
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(api.server.Close)
	return api
}

// client returns a client configured the way a deployment is: with the
// private network its servers join.
func (a *hetznerAPI) client(owner string, configure ...func(*types.HetznerProviderConfig)) *Hetzner {
	config := types.HetznerProviderConfig{
		ApiToken:       hetznerTestToken,
		BaseURL:        a.server.URL,
		PrivateNetwork: types.HetznerPrivateNetworkConfig{Name: "beam-workers"},
	}
	for _, change := range configure {
		change(&config)
	}
	return NewHetzner(config, owner)
}

func (a *hetznerAPI) fail(w http.ResponseWriter, e hetznerError) {
	writeJSON(w, e.status, map[string]any{"error": map[string]any{"code": e.code, "message": e.message, "details": nil}})
}

func (a *hetznerAPI) create(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	assert.NoError(a.t, json.NewDecoder(r.Body).Decode(&body))
	if a.refusal != nil {
		a.fail(w, *a.refusal)
		return
	}
	a.creates = append(a.creates, body)

	a.nextID++
	server := map[string]any{}
	assert.NoError(a.t, json.Unmarshal(sample(a.t, "hetzner/server"), &server))
	server["id"] = a.nextID
	server["name"] = body["name"]
	server["status"] = "initializing"
	server["labels"] = body["labels"]
	a.servers = append(a.servers, server)
	writeJSON(w, http.StatusCreated, map[string]any{
		"server":        server,
		"action":        map[string]any{"id": 1, "command": "create_server", "status": "running", "progress": 0},
		"next_actions":  []any{},
		"root_password": nil,
	})
}

func (a *hetznerAPI) list(w http.ResponseWriter, r *http.Request) {
	matching := a.servers
	if selector := r.URL.Query().Get("label_selector"); selector != "" && !a.listsEverything {
		key, value, ok := strings.Cut(selector, "==")
		if !ok {
			a.fail(w, hetznerError{http.StatusBadRequest, "invalid_input", "this stand-in only knows key==value selectors"})
			return
		}
		matching = nil
		for _, server := range a.servers {
			if labels, _ := server["labels"].(map[string]any); labels[key] == value {
				matching = append(matching, server)
			}
		}
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	if page < 1 || perPage < 1 || perPage > 50 {
		a.fail(w, hetznerError{http.StatusBadRequest, "invalid_input", "page and per_page out of range"})
		return
	}
	start := min((page-1)*perPage, len(matching))
	end := min(start+perPage, len(matching))
	var nextPage any
	if end < len(matching) {
		nextPage = page + 1
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"servers": matching[start:end],
		"meta":    map[string]any{"pagination": map[string]any{"page": page, "per_page": perPage, "next_page": nextPage, "total_entries": len(matching)}},
	})
}

func (a *hetznerAPI) find(id string) int {
	return slices.IndexFunc(a.servers, func(server map[string]any) bool { return fmt.Sprint(server["id"]) == id })
}

func (a *hetznerAPI) notFound(w http.ResponseWriter, id string) {
	a.fail(w, hetznerError{http.StatusNotFound, "not_found", fmt.Sprintf("server with ID '%s' not found", id)})
}

func (a *hetznerAPI) get(w http.ResponseWriter, r *http.Request) {
	i := a.find(r.PathValue("id"))
	if i < 0 {
		a.notFound(w, r.PathValue("id"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"server": a.servers[i]})
}

func (a *hetznerAPI) delete(w http.ResponseWriter, r *http.Request) {
	i := a.find(r.PathValue("id"))
	if i < 0 {
		a.notFound(w, r.PathValue("id"))
		return
	}
	a.servers = slices.Delete(a.servers, i, i+1)
	writeJSON(w, http.StatusOK, map[string]any{"action": map[string]any{"id": 2, "command": "delete_server", "status": "running", "progress": 0}})
}

func (a *hetznerAPI) set(id, field string, value any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := a.find(id)
	require.GreaterOrEqual(a.t, i, 0, "no server %s", id)
	a.servers[i][field] = value
}

func (a *hetznerAPI) lose(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if i := a.find(id); i >= 0 {
		a.servers = slices.Delete(a.servers, i, i+1)
	}
}

func (a *hetznerAPI) refuse(with *hetznerError) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refusal = with
}

var hetznerRefusals = map[error]*hetznerError{
	ErrOutOfStock: {http.StatusPreconditionFailed, "resource_unavailable", "server location disabled"},
	ErrQuota:      {http.StatusForbidden, "resource_limit_exceeded", "server limit exceeded"},
}

func newHetznerSite(t *testing.T) site {
	api := newHetznerAPI(t)
	return site{
		provider: api.client(testOwner),
		statuses: map[string]Status{
			"initializing": StatusPending,
			"starting":     StatusPending,
			"running":      StatusActive,
			"stopping":     StatusPending,
			"migrating":    StatusPending,
			"rebuilding":   StatusPending,
			"off":          StatusFailed,
			"deleting":     StatusDeleted,
			"unknown":      StatusUnknown,
		},
		report: func(id, status string) { api.set(id, "status", status) },
		refuse: func(reason error) { api.refuse(hetznerRefusals[reason]) },
		lose:   api.lose,
		bought: func() (string, string) {
			create := api.lastCreate()
			return create["name"].(string), create["user_data"].(string)
		},
		plantForeign: func() string {
			instance, err := api.client(foreignOwner).Create(context.Background(), CreateRequest{Offer: Offer{ID: "cpx31", Region: "ash"}, Name: "someone-elses"})
			require.NoError(t, err)
			return instance.ID
		},
	}
}

func TestHetznerOffersServerTypesInItsDefaultRegions(t *testing.T) {
	api := newHetznerAPI(t)
	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)

	// cpx31 is unavailable in hil, ccx23 is being withdrawn from hil, cpx21
	// is deprecated everywhere, and fsn1 is not a default region.
	require.Equal(t, []Offer{
		{
			ID: "cpx31", Provider: "hetzner", Cloud: "hetzner", InstanceType: "cpx31", Region: "ash",
			CPUMillicores: 4000, MemoryMB: 8 * 1024, StorageMB: 160 * 1024, HourlyCostMicros: 31_200,
			DisplayName: "CPX31", Category: "Shared vCPU", RegionDisplayName: "Ashburn, VA", Latitude: 39.045821, Longitude: -77.487073,
		},
		{
			ID: "ccx23", Provider: "hetzner", Cloud: "hetzner", InstanceType: "ccx23", Region: "ash",
			CPUMillicores: 4000, MemoryMB: 16 * 1024, StorageMB: 80 * 1024, HourlyCostMicros: 57_700,
			DisplayName: "CCX23", Category: "Dedicated vCPU", RegionDisplayName: "Ashburn, VA", Latitude: 39.045821, Longitude: -77.487073,
		},
	}, offers)
	require.Equal(t, []string{"GET /locations", "GET /server_types"}, api.asked())
}

func TestHetznerOffersTheRegionsAsked(t *testing.T) {
	api := newHetznerAPI(t)
	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{Regions: []string{"fsn1"}})
	require.NoError(t, err)

	serverTypes := []string{}
	for _, offer := range offers {
		require.Equal(t, "fsn1", offer.Region)
		serverTypes = append(serverTypes, offer.ID)
	}
	require.Equal(t, []string{"cpx31", "cax21"}, serverTypes)
}

func TestHetznerHasNoGPUsAndDoesNotAsk(t *testing.T) {
	api := newHetznerAPI(t)
	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{GPUs: []string{"A10G"}})
	require.NoError(t, err)
	require.Empty(t, offers)
	require.Empty(t, api.asked())
}

func TestHetznerOffersUseConfiguredPricesCategoriesAndRegionNames(t *testing.T) {
	api := newHetznerAPI(t)
	client := api.client(testOwner, func(config *types.HetznerProviderConfig) {
		config.DefaultRegions = []string{"ash", "fsn1"}
		config.ServerTypePrices = map[string]float64{"cpx31": 0.05, "cpx31:fsn1": 0.04}
		config.ServerTypeCategories = map[string]string{"cpx31": "General purpose", "Dedicated vCPU": "Dedicated"}
		config.RegionMetadata = map[string]types.HetznerRegionConfig{"ash": {DisplayName: "US East", Latitude: 39, Longitude: -77}}
	})
	offers, err := client.Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)

	type listing struct {
		id, region, category, regionName string
		micros                           int64
		latitude                         float64
	}
	listings := []listing{}
	for _, offer := range offers {
		listings = append(listings, listing{offer.ID, offer.Region, offer.Category, offer.RegionDisplayName, offer.HourlyCostMicros, offer.Latitude})
	}
	require.Equal(t, []listing{
		{"cpx31", "fsn1", "General purpose", "Falkenstein DC Park 1", 40_000, 50.47612},
		{"cpx31", "ash", "General purpose", "US East", 50_000, 39},
		{"ccx23", "ash", "Dedicated", "US East", 57_700, 39},
		{"cax21", "fsn1", "Shared vCPU", "Falkenstein DC Park 1", 10_400, 50.47612},
	}, listings)
}

func TestHetznerCreateStartsAServerOnThePrivateNetwork(t *testing.T) {
	api := newHetznerAPI(t)
	client := api.client(testOwner, func(config *types.HetznerProviderConfig) {
		config.SSHKeys = []string{"ops"}
		config.SSHKeysByRegion = map[string][]string{"hil": {"ops-west"}}
		config.ImageByRegion = map[string]string{"hil": "ubuntu-22.04"}
	})

	instance, err := client.Create(context.Background(), CreateRequest{
		Offer:         Offer{ID: "cpx31", Region: "ash"},
		Name:          "beam-cpu-a1b2c3d4",
		StartupScript: "echo join",
		Tags:          map[string]string{"beam-machine": "a1b2c3d4"},
	})
	require.NoError(t, err)
	require.Equal(t, Instance{
		ID: "42", Status: StatusPending, PublicIP: "1.2.3.4", SSHHost: "1.2.3.4", SSHPort: 22,
		Tags: map[string]string{"beam-machine": "a1b2c3d4"},
	}, instance)
	require.Equal(t, map[string]any{
		"name":               "beam-cpu-a1b2c3d4",
		"server_type":        "cpx31",
		"image":              "ubuntu-24.04",
		"location":           "ash",
		"start_after_create": true,
		"networks":           []any{float64(4711)},
		"labels":             map[string]any{"beam-machine": "a1b2c3d4", "beam-owner": testOwner},
		"ssh_keys":           []any{"ops"},
		"user_data":          "#!/usr/bin/env bash\nset -euo pipefail\necho join\n",
	}, api.lastCreate())

	_, err = client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "cpx31", Region: "hil"}, Name: "beam-cpu-e5f6a7b8"})
	require.NoError(t, err)
	west := api.lastCreate()
	require.Equal(t, "ubuntu-22.04", west["image"])
	require.Equal(t, []any{"ops-west"}, west["ssh_keys"])
	require.NotContains(t, west, "user_data")

	// A deployment's own image boots wherever no region names another.
	debian := api.client(testOwner, func(config *types.HetznerProviderConfig) { config.Image = "debian-12" })
	_, err = debian.Create(context.Background(), CreateRequest{Offer: Offer{ID: "cpx31", Region: "ash"}, Name: "beam-cpu-c9d0e1f2"})
	require.NoError(t, err)
	require.Equal(t, "debian-12", api.lastCreate()["image"])
}

func TestHetznerCreateFindsThePrivateNetwork(t *testing.T) {
	no := false
	cases := map[string]struct {
		network types.HetznerPrivateNetworkConfig
		region  string
		wantID  float64
		wantErr string
	}{
		"by name":                       {network: types.HetznerPrivateNetworkConfig{Name: "beam-workers"}, region: "ash", wantID: 4711},
		"by id":                         {network: types.HetznerPrivateNetworkConfig{ID: 4711}, region: "hil", wantID: 4711},
		"by name, when the id is wrong": {network: types.HetznerPrivateNetworkConfig{ID: 9, Name: "beam-workers"}, region: "ash", wantID: 4711},
		"by the region's name":          {network: types.HetznerPrivateNetworkConfig{Name: "elsewhere", RegionNames: map[string]string{"ash": "beam-workers"}}, region: "ash", wantID: 4711},
		"by the region's id":            {network: types.HetznerPrivateNetworkConfig{Name: "elsewhere", RegionIDs: map[string]int64{"ash": 4711}}, region: "ash", wantID: 4711},
		"none configured":               {network: types.HetznerPrivateNetworkConfig{}, region: "ash", wantErr: `privateNetwork has no id or name for location "ash"`},
		"none of that name":             {network: types.HetznerPrivateNetworkConfig{Name: "elsewhere"}, region: "ash", wantErr: `private network "elsewhere" was not found`},
		"none of that id":               {network: types.HetznerPrivateNetworkConfig{ID: 9}, region: "ash", wantErr: `private network "9" was not found`},
		"no subnet in the region":       {network: types.HetznerPrivateNetworkConfig{Name: "beam-workers"}, region: "fsn1", wantErr: `has no cloud subnet in network zone "eu-central" for location "fsn1"`},
		"no subnet, and none required":  {network: types.HetznerPrivateNetworkConfig{Name: "beam-workers", RequireSubnet: &no}, region: "fsn1", wantID: 4711},
		"unlisted id, none required":    {network: types.HetznerPrivateNetworkConfig{ID: 9, RequireSubnet: &no}, region: "ash", wantID: 9},
		"a location Hetzner lacks":      {network: types.HetznerPrivateNetworkConfig{Name: "beam-workers"}, region: "mars", wantErr: `location "mars" was not found`},
		"unlisted name, none required":  {network: types.HetznerPrivateNetworkConfig{Name: "elsewhere", RequireSubnet: &no}, region: "ash", wantErr: `private network "elsewhere" was not found`},
		"region id wins over the name":  {network: types.HetznerPrivateNetworkConfig{Name: "elsewhere", RegionIDs: map[string]int64{"ash": 4711}, RegionNames: map[string]string{"ash": "nowhere"}}, region: "ash", wantID: 4711},
		"other regions use the default": {network: types.HetznerPrivateNetworkConfig{Name: "beam-workers", RegionNames: map[string]string{"hil": "elsewhere"}}, region: "ash", wantID: 4711},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			api := newHetznerAPI(t)
			client := api.client(testOwner, func(config *types.HetznerProviderConfig) { config.PrivateNetwork = tc.network })

			_, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "cpx31", Region: tc.region}, Name: "beam-cpu-a1b2c3d4"})
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.NotContains(t, api.asked(), "POST /servers", "nothing is bought without a network")
				return
			}
			require.NoError(t, err)
			require.Equal(t, []any{tc.wantID}, api.lastCreate()["networks"])
		})
	}
}

func TestHetznerReadsLocationsAndNetworksOnceForManyCreates(t *testing.T) {
	api := newHetznerAPI(t)
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	client := api.client(testOwner)
	client.now = func() time.Time { return now }
	create := func() {
		_, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "cpx31", Region: "ash"}, Name: "beam-cpu-a1b2c3d4"})
		require.NoError(t, err)
	}

	create()
	require.Equal(t, []string{"GET /locations", "GET /networks", "POST /servers"}, api.asked())

	now = now.Add(hetznerCacheFor - time.Second)
	create()
	require.Equal(t, []string{"POST /servers"}, api.asked())

	now = now.Add(time.Second)
	create()
	require.Equal(t, []string{"GET /locations", "GET /networks", "POST /servers"}, api.asked())
}

func TestHetznerLooksForAMissingNetworkAgainOnTheNextCreate(t *testing.T) {
	api := newHetznerAPI(t)
	client := api.client(testOwner, func(config *types.HetznerProviderConfig) {
		config.PrivateNetwork = types.HetznerPrivateNetworkConfig{Name: "elsewhere"}
	})

	for range 2 {
		_, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "cpx31", Region: "ash"}, Name: "beam-cpu-a1b2c3d4"})
		require.ErrorContains(t, err, "was not found")
	}
	require.Equal(t, []string{"GET /locations", "GET /networks", "GET /networks"}, api.asked())
}

func TestHetznerPlacementErrorIsOutOfStock(t *testing.T) {
	api := newHetznerAPI(t)
	api.refuse(&hetznerError{http.StatusUnprocessableEntity, "placement_error", "error during placement"})

	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: Offer{ID: "cpx31", Region: "ash"}, Name: "beam-cpu-a1b2c3d4"})
	require.ErrorIs(t, err, ErrOutOfStock)
}

func TestHetznerRefusalItDoesNotRecogniseIsNeitherStockNorQuota(t *testing.T) {
	api := newHetznerAPI(t)
	api.refuse(&hetznerError{http.StatusConflict, "uniqueness_error", "server name is already used"})

	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: Offer{ID: "cpx31", Region: "ash"}, Name: "beam-cpu-a1b2c3d4"})
	require.ErrorContains(t, err, "uniqueness_error")
	require.NotErrorIs(t, err, ErrOutOfStock)
	require.NotErrorIs(t, err, ErrQuota)
}

func TestHetznerServerWithoutAPublicAddressHasNone(t *testing.T) {
	api := newHetznerAPI(t)
	client := api.client(testOwner)
	created, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "cpx31", Region: "ash"}, Name: "beam-cpu-a1b2c3d4"})
	require.NoError(t, err)

	api.set(created.ID, "public_net", map[string]any{"ipv4": nil, "ipv6": nil, "floating_ips": []any{}})
	found, err := client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusPending}, found)
}

func TestHetznerListReadsEveryPage(t *testing.T) {
	api := newHetznerAPI(t)
	client := api.client(testOwner)
	for i := range 65 {
		owner := client
		if i%13 == 0 {
			owner = api.client(foreignOwner)
		}
		_, err := owner.Create(context.Background(), CreateRequest{Offer: Offer{ID: "cpx31", Region: "ash"}, Name: fmt.Sprintf("beam-cpu-%08d", i)})
		require.NoError(t, err)
	}
	api.asked()

	listed, err := client.List(context.Background())
	require.NoError(t, err)
	require.Len(t, listed, 60)
	require.Equal(t, []string{"GET /servers", "GET /servers"}, api.asked())
}

const (
	nebiusTestAccount = "serviceaccount-e00beamtest"
	nebiusTestKeyID   = "publickey-e00beamtest"
	nebiusTestTenant  = "tenant-e00beamtest"
	// The account's two projects, in eu-north1 and us-central1.
	nebiusNorth   = "project-e00beamtest"
	nebiusCentral = "project-u00beamtest"
)

// The key pair of the test service accounts. A stand-in holds the public
// half, as the provider does.
var testServiceKey = sync.OnceValue(func() *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return key
})

func testServiceKeyPEM(t *testing.T) string {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(testServiceKey())
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

// nebiusAPI stands in for Nebius. It answers with the shapes of Nebius's
// published API description (https://api.nebius.cloud/openapi.json) and
// keeps the rules its documentation gives for a preemptible VM. The spot
// prices are Nebius's public ones as read on 2026-11-14. What the account
// can start is made up, because Nebius tells that to its customers only, and
// so is the status its errors come with, which the description leaves out.
type nebiusAPI struct {
	apiStandIn

	nextID int
	// tokens counts the access tokens given out.
	tokens int
	// subnets are each project's.
	subnets   map[string][]string
	instances []map[string]any
	policies  []map[string]any
	deletes   []string
	refusal   *nebiusReply

	// pageOf, when set, is the most a list returns at a time.
	pageOf int
	// prices, when set, are listed in place of the recorded spot prices.
	prices string
	// leavesAStoppedVM makes the next create fail after Nebius has made the
	// VM, as its documentation says a create can.
	leavesAStoppedVM *nebiusReply
	// policyMadeFirst makes the next pricing policy that is asked for one
	// that someone else's request made a moment before.
	policyMadeFirst bool
	// policyRefusal, when set, is the answer to every new pricing policy.
	policyRefusal *nebiusReply
}

// nebiusReply is an error of Nebius's: an HTTP status, the gRPC code it
// stands for, a message and one detail.
type nebiusReply struct {
	status  int
	code    int
	message string
	detail  map[string]any
}

var nebiusRegions = map[string]string{nebiusNorth: "eu-north1", nebiusCentral: "us-central1"}

func newNebiusAPI(t *testing.T) *nebiusAPI {
	api := &nebiusAPI{
		apiStandIn: apiStandIn{t: t, now: time.Date(2026, 11, 14, 2, 30, 0, 0, time.UTC)},
		subnets: map[string][]string{
			nebiusNorth:   {"vpcsubnet-e00beamtest"},
			nebiusCentral: {"vpcsubnet-u00beamtest"},
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /iam/v1/projects/{id}", api.project)
	mux.HandleFunc("GET /vpc/v1/subnets", api.listSubnets)
	mux.HandleFunc("GET /billing/v1alpha1/spot-price", func(w http.ResponseWriter, r *http.Request) {
		if api.prices != "" {
			w.Write([]byte(api.prices))
			return
		}
		api.page(w, r, api.listed("nebius/spot_price"))
	})
	mux.HandleFunc("GET /capacity/v1/resource-advice", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("parentId") != nebiusTestTenant {
			api.fail(w, nebiusReply{status: http.StatusForbidden, code: 7, message: "permission denied"})
			return
		}
		api.page(w, r, api.listed("nebius/resource_advice"))
	})
	mux.HandleFunc("GET /billing/v1/pricing-policies", api.listPolicies)
	mux.HandleFunc("POST /billing/v1/pricing-policies", api.createPolicy)
	mux.HandleFunc("POST /compute/v1/instances", api.create)
	mux.HandleFunc("GET /compute/v1/instances", api.list)
	mux.HandleFunc("GET /compute/v1/instances/{id}", api.get)
	mux.HandleFunc("DELETE /compute/v1/instances/{id}", api.delete)
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.mu.Lock()
		defer api.mu.Unlock()
		api.requests = append(api.requests, r.Method+" "+r.URL.RequestURI())
		if r.Method == http.MethodPost && r.URL.Path == "/oauth2/token/exchange" {
			api.token(w, r)
			return
		}
		var token int
		_, err := fmt.Sscanf(r.Header.Get("Authorization"), "Bearer nebius-token-%d", &token)
		if err != nil || token < 1 || token > api.tokens {
			api.fail(w, nebiusReply{status: http.StatusUnauthorized, code: 16, message: "unauthenticated"})
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(api.server.Close)
	return api
}

// client returns a client of the stand-in's Nebius, configured as a
// deployment is unless the test changes it.
func (a *nebiusAPI) client(owner string, configure ...func(*types.NebiusProviderConfig)) *Nebius {
	config := types.NebiusProviderConfig{
		ServiceAccountID: nebiusTestAccount,
		PublicKeyID:      nebiusTestKeyID,
		PrivateKey:       testServiceKeyPEM(a.t),
		Projects:         []types.NebiusProjectConfig{{ID: nebiusNorth}, {ID: nebiusCentral}},
		BaseURL:          a.server.URL,
		TokenURL:         a.server.URL + "/oauth2/token/exchange",
	}
	for _, change := range configure {
		change(&config)
	}
	client, err := NewNebius(config, owner)
	require.NoError(a.t, err)
	// The stand-in's clock, and no waiting between two tries of a call.
	client.now = a.clock
	client.api.sleep = func(context.Context, time.Duration) error { return nil }
	client.login.sleep = client.api.sleep
	return client
}

// listed returns the items of a sample list.
func (a *nebiusAPI) listed(name string) []any {
	var list struct {
		Items []any `json:"items"`
	}
	assert.NoError(a.t, json.Unmarshal(sample(a.t, name), &list))
	return list.Items
}

// page answers a list call the way Nebius does: a page, and the token that
// asks for the next one.
func (a *nebiusAPI) page(w http.ResponseWriter, r *http.Request, items []any) {
	size, err := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if err != nil || size < 1 {
		a.fail(w, nebiusReply{status: http.StatusBadRequest, code: 3, message: "pageSize is not a positive number"})
		return
	}
	if a.pageOf > 0 {
		size = min(size, a.pageOf)
	}
	start, _ := strconv.Atoi(r.URL.Query().Get("pageToken"))
	end := min(start+size, len(items))
	body := map[string]any{"items": items[min(start, end):end]}
	if end < len(items) {
		body["nextPageToken"] = strconv.Itoa(end)
	}
	writeJSON(w, http.StatusOK, body)
}

func (a *nebiusAPI) fail(w http.ResponseWriter, reply nebiusReply) {
	writeJSON(w, reply.status, reply.body())
}

func (reply nebiusReply) body() map[string]any {
	body := map[string]any{"code": reply.code, "message": reply.message}
	if reply.detail != nil {
		detail := map[string]any{"@type": "type.googleapis.com/nebius.common.v1.ServiceError", "service": "compute"}
		for key, value := range reply.detail {
			detail[key] = value
		}
		body["details"] = []any{detail}
	}
	return body
}

// token trades a request signed by the service account for an access token,
// and turns down one that is not as Nebius's SDK makes it.
func (a *nebiusAPI) token(w http.ResponseWriter, r *http.Request) {
	refuse := func(why string) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"status": 400, "message": why, "error": "", "error_description": ""})
	}
	if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" || r.ParseForm() != nil {
		refuse("the request is not a form")
		return
	}
	for field, want := range map[string]string{
		"grant_type":           "urn:ietf:params:oauth:grant-type:token-exchange",
		"requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
		"subject_token_type":   "urn:ietf:params:oauth:token-type:jwt",
	} {
		if r.PostForm.Get(field) != want {
			refuse("Unsupported " + strings.ReplaceAll(field, "_", " ") + ": " + r.PostForm.Get(field))
			return
		}
	}

	parts := strings.Split(r.PostForm.Get("subject_token"), ".")
	if len(parts) != 3 {
		refuse("the subject token is not a JWT")
		return
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err != nil || rsa.VerifyPKCS1v15(&testServiceKey().PublicKey, crypto.SHA256, digest[:], signature) != nil {
		refuse("the subject token is not signed with the service account's key")
		return
	}
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	var claims struct {
		Issuer   string   `json:"iss"`
		Subject  string   `json:"sub"`
		Audience []string `json:"aud"`
		IssuedAt int64    `json:"iat"`
		Expires  int64    `json:"exp"`
	}
	for part, into := range map[string]any{parts[0]: &header, parts[1]: &claims} {
		data, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil || json.Unmarshal(data, into) != nil {
			refuse("the subject token cannot be read")
			return
		}
	}
	lasts := time.Unix(claims.Expires, 0).Sub(a.now)
	age := a.now.Sub(time.Unix(claims.IssuedAt, 0))
	switch {
	case header.Algorithm != "RS256" || header.KeyID != nebiusTestKeyID:
		refuse("the subject token names another key")
	case claims.Issuer != nebiusTestAccount || claims.Subject != nebiusTestAccount:
		refuse("the subject token is another account's")
	case !slices.Equal(claims.Audience, []string{"token-service.iam.new.nebiuscloud.net"}):
		refuse("the subject token is addressed to someone else")
	case age < -time.Second || age > time.Minute || lasts <= 0 || lasts > time.Minute:
		refuse("the subject token is not good now, or for too long")
	default:
		a.tokens++
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token":      fmt.Sprintf("nebius-token-%d", a.tokens),
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
			"token_type":        "Bearer",
			"expires_in":        12 * 60 * 60,
		})
	}
}

func (a *nebiusAPI) project(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	region, ok := nebiusRegions[id]
	if !ok {
		a.fail(w, nebiusReply{status: http.StatusNotFound, code: 5, message: "project " + id + " not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"metadata": map[string]any{"id": id, "parentId": nebiusTestTenant, "name": "beam-" + region},
		"spec":     map[string]any{"region": region},
		"status":   map[string]any{"region": region, "containerState": "ACTIVE"},
	})
}

func (a *nebiusAPI) listSubnets(w http.ResponseWriter, r *http.Request) {
	items := []any{}
	for _, id := range a.subnets[r.URL.Query().Get("parentId")] {
		items = append(items, map[string]any{
			"metadata": map[string]any{"id": id, "parentId": r.URL.Query().Get("parentId"), "name": "default-subnet"},
			"spec":     map[string]any{"networkId": "vpcnetwork-" + id},
			"status":   map[string]any{"state": "READY"},
		})
	}
	a.page(w, r, items)
}

func (a *nebiusAPI) listPolicies(w http.ResponseWriter, r *http.Request) {
	items := []any{}
	for _, policy := range a.policies {
		if policy["metadata"].(map[string]any)["parentId"] == r.URL.Query().Get("parentId") {
			items = append(items, policy)
		}
	}
	a.page(w, r, items)
}

func (a *nebiusAPI) createPolicy(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	assert.NoError(a.t, json.NewDecoder(r.Body).Decode(&body))
	metadata := body["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	if _, known := nebiusRegions[fmt.Sprint(metadata["parentId"])]; !known {
		a.fail(w, nebiusReply{status: http.StatusNotFound, code: 5, message: "no such project"})
		return
	}
	// "It can only contain Latin letters and numbers."
	if name == "" || strings.ContainsFunc(name, func(r rune) bool { return !strings.ContainsRune("abcdefghijklmnopqrstuvwxyz0123456789", r) }) {
		a.fail(w, nebiusReply{status: http.StatusBadRequest, code: 3, message: "a policy's name has letters and digits only"})
		return
	}
	if a.policyRefusal != nil {
		a.fail(w, *a.policyRefusal)
		return
	}
	if a.policyMadeFirst {
		a.policyMadeFirst = false
		var theirs map[string]any
		sent, err := json.Marshal(body)
		assert.NoError(a.t, err)
		assert.NoError(a.t, json.Unmarshal(sent, &theirs))
		a.keepPolicy(theirs)
	}
	for _, other := range a.policies {
		if other["metadata"].(map[string]any)["name"] == name && other["metadata"].(map[string]any)["parentId"] == metadata["parentId"] {
			a.fail(w, nebiusReply{status: http.StatusConflict, code: 6, message: "a policy named " + name + " exists"})
			return
		}
	}
	id := a.keepPolicy(body)
	writeJSON(w, http.StatusOK, map[string]any{"id": fmt.Sprintf("billingoperation-e00%04d", a.nextID), "resourceId": id})
}

// keepPolicy makes a pricing policy of what was asked for, and returns its
// ID.
func (a *nebiusAPI) keepPolicy(body map[string]any) any {
	a.nextID++
	metadata := body["metadata"].(map[string]any)
	metadata["id"] = fmt.Sprintf("pricingpolicy-e00%04d", a.nextID)
	body["status"] = map[string]any{"state": "STATE_ACTIVE", "schedulingState": "SCHEDULING_STATE_ALLOWED", "currency": "USD", "runningVmCount": "0"}
	a.policies = append(a.policies, body)
	return metadata["id"]
}

func (a *nebiusAPI) create(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	assert.NoError(a.t, json.NewDecoder(r.Body).Decode(&body))
	if a.refusal != nil {
		a.fail(w, *a.refusal)
		return
	}
	metadata, _ := body["metadata"].(map[string]any)
	spec, _ := body["spec"].(map[string]any)
	if why := a.wrong(metadata, spec); why != "" {
		a.fail(w, nebiusReply{status: http.StatusBadRequest, code: 3, message: why})
		return
	}
	a.creates = append(a.creates, body)

	a.nextID++
	id := fmt.Sprintf("computeinstance-e00%04d", a.nextID)
	instance := map[string]any{
		"metadata": map[string]any{"id": id, "parentId": metadata["parentId"], "name": metadata["name"], "labels": metadata["labels"], "resourceVersion": "1"},
		"spec":     spec,
		"status":   map[string]any{"state": "CREATING", "reconciling": true},
	}
	a.instances = append(a.instances, instance)
	operation := map[string]any{"id": fmt.Sprintf("computeoperation-e00%04d", a.nextID), "resourceId": id}
	if a.leavesAStoppedVM != nil {
		instance["status"].(map[string]any)["state"] = "STOPPED"
		instance["status"].(map[string]any)["reconciling"] = false
		operation["status"] = a.leavesAStoppedVM.body()
		a.leavesAStoppedVM = nil
	}
	writeJSON(w, http.StatusOK, operation)
}

// wrong says what about a create Nebius would turn down, by the rules its
// documentation gives.
func (a *nebiusAPI) wrong(metadata, spec map[string]any) string {
	project := fmt.Sprint(metadata["parentId"])
	if _, known := nebiusRegions[project]; !known {
		return "no such project"
	}
	networks, _ := spec["networkInterfaces"].([]any)
	if len(networks) != 1 || !slices.Contains(a.subnets[project], fmt.Sprint(networks[0].(map[string]any)["subnetId"])) {
		return "the VM needs one network interface in a subnet of its project"
	}
	if _, preemptible := spec["preemptible"]; !preemptible {
		return ""
	}
	if spec["recoveryPolicy"] != "FAIL" {
		return "a preemptible VM supports only the FAIL recovery policy"
	}
	_, follows := spec["followsSpotPrice"]
	limit, limited := spec["spotPricingPolicy"].(map[string]any)
	if follows == limited {
		return "a preemptible VM follows the spot price or has a pricing policy"
	}
	if limited {
		platform := spec["resources"].(map[string]any)["platform"]
		fits := func(policy map[string]any) bool {
			metadata := policy["metadata"].(map[string]any)
			priced := policy["spec"].(map[string]any)["computeInstanceSpec"].(map[string]any)["v1"].(map[string]any)["platform"]
			return metadata["id"] == limit["id"] && metadata["parentId"] == project && priced == platform
		}
		if !slices.ContainsFunc(a.policies, fits) {
			return "the pricing policy is not one of the project's for the platform"
		}
	}
	return ""
}

func (a *nebiusAPI) list(w http.ResponseWriter, r *http.Request) {
	if project := r.URL.Query().Get("parentId"); nebiusRegions[project] == "" {
		a.fail(w, nebiusReply{status: http.StatusNotFound, code: 5, message: "project " + project + " not found"})
		return
	}
	items := []any{}
	for _, instance := range a.instances {
		if instance["metadata"].(map[string]any)["parentId"] == r.URL.Query().Get("parentId") {
			items = append(items, instance)
		}
	}
	a.page(w, r, items)
}

func (a *nebiusAPI) find(id string) int {
	return slices.IndexFunc(a.instances, func(instance map[string]any) bool { return instance["metadata"].(map[string]any)["id"] == id })
}

func (a *nebiusAPI) get(w http.ResponseWriter, r *http.Request) {
	i := a.find(r.PathValue("id"))
	if i < 0 {
		a.fail(w, nebiusReply{status: http.StatusNotFound, code: 5, message: "instance not found"})
		return
	}
	writeJSON(w, http.StatusOK, a.instances[i])
}

func (a *nebiusAPI) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	i := a.find(id)
	if i < 0 {
		a.fail(w, nebiusReply{status: http.StatusNotFound, code: 5, message: "instance not found"})
		return
	}
	status := a.instances[i]["status"].(map[string]any)
	if status["state"] == "DELETING" {
		a.fail(w, nebiusReply{status: http.StatusBadRequest, code: 9, message: "the instance is being deleted"})
		return
	}
	a.deletes = append(a.deletes, id)
	status["state"] = "DELETING"
	a.nextID++
	writeJSON(w, http.StatusOK, map[string]any{"id": fmt.Sprintf("computeoperation-e00%04d", a.nextID), "resourceId": id})
}

// state makes Nebius report an instance in the state.
func (a *nebiusAPI) state(id, state string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := a.find(id)
	require.GreaterOrEqual(a.t, i, 0, "no instance %s", id)
	status := a.instances[i]["status"].(map[string]any)
	status["state"] = state
	if state == "STOPPED" || state == "RUNNING" || state == "ERROR" {
		status["reconciling"] = false
	}
}

func (a *nebiusAPI) refuse(with *nebiusReply) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refusal = with
}

func (a *nebiusAPI) lose(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if i := a.find(id); i >= 0 {
		a.instances = slices.Delete(a.instances, i, i+1)
	}
}

// limits returns the pricing policies there are, as "project name limit".
func (a *nebiusAPI) limits() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	limits := []string{}
	for _, policy := range a.policies {
		metadata := policy["metadata"].(map[string]any)
		limit := policy["spec"].(map[string]any)["pricing"].(map[string]any)["maxPriceV1"].(map[string]any)["maxPrice"]
		limits = append(limits, fmt.Sprintf("%s %s %s", metadata["parentId"], metadata["name"], limit))
	}
	return limits
}

// Nebius documents what tells these two apart: which field of an error's
// detail is set. It does not document the status they come with. This one is
// what its gateway makes of "resource exhausted", and is the one this client
// would send a call again for.
var nebiusRefusals = map[error]*nebiusReply{
	ErrOutOfStock: {http.StatusTooManyRequests, 8, "not enough resources", map[string]any{
		"code": "NotEnoughResources",
		"notEnoughResources": map[string]any{"violations": []any{
			map[string]any{"resourceType": "compute.instance.gpu.h200", "message": "not enough capacity for 1 H200 in eu-north1", "requested": "1"},
		}},
	}},
	ErrQuota: {http.StatusTooManyRequests, 8, "quota exceeded", map[string]any{
		"code": "QuotaFailure",
		"quotaFailure": map[string]any{"violations": []any{
			map[string]any{"quota": "compute.instance.preemptible.count", "message": "limit of preemptible VMs reached", "limit": "2", "requested": "3", "region": "eu-north1"},
		}},
	}},
}

func newNebiusSite(t *testing.T) site {
	api := newNebiusAPI(t)
	return site{
		provider: api.client(testOwner),
		statuses: map[string]Status{
			"CREATING": StatusPending,
			"STARTING": StatusPending,
			"UPDATING": StatusPending,
			"RUNNING":  StatusActive,
			// The site's instance is preemptible, as all this client buys.
			"STOPPING": StatusInterrupted,
			"STOPPED":  StatusInterrupted,
			"ERROR":    StatusFailed,
			"DELETING": StatusDeleted,
		},
		report: api.state,
		refuse: func(reason error) { api.refuse(nebiusRefusals[reason]) },
		lose:   api.lose,
		bought: func() (string, string) {
			name := api.lastCreate()["metadata"].(map[string]any)["name"].(string)
			return name, nebiusStartupCommand(t, nebiusUserData(api))[2]
		},
		plantForeign: func() string {
			offers, err := api.client(foreignOwner).Offers(context.Background(), OfferFilter{})
			require.NoError(t, err)
			instance, err := api.client(foreignOwner).Create(context.Background(), CreateRequest{Offer: offers[0], Name: "someone-elses"})
			require.NoError(t, err)
			return instance.ID
		},
		boughtSpot: func() bool {
			_, preemptible := api.lastCreate()["spec"].(map[string]any)["preemptible"]
			return preemptible
		},
		takeBack: func(id string) { api.state(id, "STOPPED") },
	}
}

// What a machine's 200 GB disk costs an hour: $0.071 a gibibyte for 730
// hours.
const nebiusTestDisk = 19_452

var (
	nebiusH200 = Offer{
		ID: "gpu-h200-sxm/1gpu-16vcpu-200gb", Provider: "nebius", Cloud: "nebius", InstanceType: "gpu-h200-sxm/1gpu-16vcpu-200gb",
		Region: "eu-north1", GPU: "H200", GPUCount: 1, CPUMillicores: 16_000, MemoryMB: 200 * 1024, StorageMB: 200 * 1024,
		HourlyCostMicros: 3_380_000 + nebiusTestDisk, DiskHourlyMicros: nebiusTestDisk, Spot: true, Available: 2,
	}
	nebiusH200x8 = Offer{
		ID: "gpu-h200-sxm/8gpu-128vcpu-1600gb", Provider: "nebius", Cloud: "nebius", InstanceType: "gpu-h200-sxm/8gpu-128vcpu-1600gb",
		Region: "eu-north1", GPU: "H200", GPUCount: 8, CPUMillicores: 128_000, MemoryMB: 1600 * 1024, StorageMB: 200 * 1024,
		HourlyCostMicros: 8*3_380_000 + nebiusTestDisk, DiskHourlyMicros: nebiusTestDisk, Spot: true, Available: 1,
	}
	nebiusRTXPro6000 = Offer{
		ID: "gpu-rtx6000/1gpu-24vcpu-218gb", Provider: "nebius", Cloud: "nebius", InstanceType: "gpu-rtx6000/1gpu-24vcpu-218gb",
		Region: "us-central1", GPU: "RTXPro6000", GPUCount: 1, CPUMillicores: 24_000, MemoryMB: 218 * 1024, StorageMB: 200 * 1024,
		HourlyCostMicros: 1_789_000 + nebiusTestDisk, DiskHourlyMicros: nebiusTestDisk, Spot: true, Available: 2,
	}
)

// Of the nine things the account is advised about, three can be bought: the
// others have none to start as preemptible, have no spot price, or are in a
// region the account has no project in.
func TestNebiusOffersThePreemptibleVMsItsProjectsCanStart(t *testing.T) {
	api := newNebiusAPI(t)
	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)

	// The price is the spot price of a GPU, times the GPUs, plus the disk.
	// The H200 is counted in two parts of eu-north1, two VMs in one and one
	// in the other. The RTX PRO 6000 is named by Nebius's title for it: its
	// platform's ID alone would read as another GPU.
	require.Equal(t, []Offer{nebiusH200, nebiusH200x8, nebiusRTXPro6000}, offers)
}

// Nebius also bills in shekels. A price in them is not a price in dollars,
// and the spend caps count on every machine having one.
func TestNebiusDoesNotOfferWhatItCannotPriceInDollars(t *testing.T) {
	api := newNebiusAPI(t)
	price := func(region, platform, value, currency string) string {
		return fmt.Sprintf(`{"productTitle": "Preemptible NVIDIA® H200 NVLink", "region": %q, "computeInstance": {"platform": %q}, "price": {"value": %q, "currency": %q, "unitTitle": "GPU hour"}}`, region, platform, value, currency)
	}
	api.prices = `{"items": [` + strings.Join([]string{
		price("eu-north1", "gpu-h200-sxm", "10.478", "ils"),
		price("us-central1", "gpu-rtx6000", "0", "usd"),
		price("us-central1", "gpu-rtx6000", "", "usd"),
	}, ",") + `]}`

	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Empty(t, offers)

	api.prices = `{"items": [` + price("eu-north1", "gpu-h200-sxm", "3.38", "USD") + `]}`
	offers, err = api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Equal(t, []Offer{nebiusH200, nebiusH200x8}, offers)
}

func TestNebiusReadsEveryPageOfAList(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	for _, name := range []string{"beam-h200-a1b2c3d4", "beam-h200-e5f6a7b8", "beam-h200-c9d0e1f2"} {
		_, err := client.Create(ctx, CreateRequest{Offer: nebiusH200, Name: name})
		require.NoError(t, err)
	}

	api.mu.Lock()
	api.pageOf = 2
	api.mu.Unlock()
	offers, err := client.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.Equal(t, []Offer{nebiusH200, nebiusH200x8, nebiusRTXPro6000}, offers)

	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 3)
	require.Equal(t, 6, api.sent("GET", "/billing/v1alpha1/spot-price"), "twelve prices, two at a time")
}

// The stand-in checks the request as Nebius would: signed with the service
// account's key, in its name, addressed to the token service, good for a
// minute.
func TestNebiusTradesASignedRequestForItsToken(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	for range 3 {
		_, err := client.List(context.Background())
		require.NoError(t, err)
	}
	require.Equal(t, 1, api.sent("POST", "/oauth2/token/exchange"), "one token serves every call")

	// Nebius's tokens last twelve hours.
	api.after(12*time.Hour - renewTokenBefore)
	_, err := client.List(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, api.sent("POST", "/oauth2/token/exchange"))
}

// A key that is a key, and not the service account's, is turned down where
// it is traded for a token. One that cannot be read at all never gets as far
// as a client: the registry leaves the provider out.
func TestNebiusWithTheWrongKeySaysSo(t *testing.T) {
	api := newNebiusAPI(t)
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	client := api.client(testOwner, func(config *types.NebiusProviderConfig) {
		config.PrivateKey = string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(other)}))
	})

	_, err = client.Offers(context.Background(), OfferFilter{})
	require.ErrorContains(t, err, "get an access token")
	require.ErrorContains(t, err, "not signed with the service account's key")
	require.Zero(t, api.sent("GET", "/"), "nothing is asked without a token")
}

func TestNebiusCreateSendsWhatNebiusNeeds(t *testing.T) {
	api := newNebiusAPI(t)
	created, err := api.client(testOwner).Create(context.Background(), CreateRequest{
		Offer:         nebiusH200,
		Name:          "beam-h200-a1b2c3d4",
		StartupScript: "curl -fsSL https://gateway.example/join | sh",
		Tags:          map[string]string{"beam-machine": "a1b2c3d4"},
	})
	require.NoError(t, err)
	require.Equal(t, Instance{ID: "computeinstance-e000001", Status: StatusPending, Tags: map[string]string{"beam-machine": "a1b2c3d4"}}, created)
	userData := nebiusUserData(api)
	require.Equal(t, "#!/usr/bin/env bash\nset -euo pipefail\ncurl -fsSL https://gateway.example/join | sh\n", nebiusStartupCommand(t, userData)[2])

	require.Equal(t, map[string]any{
		"metadata": map[string]any{
			// The project that is in the offer's region.
			"parentId": nebiusNorth,
			"name":     "beam-h200-a1b2c3d4",
			"labels":   map[string]any{"beam-machine": "a1b2c3d4", OwnerTag: testOwner},
		},
		"spec": map[string]any{
			"resources": map[string]any{"platform": "gpu-h200-sxm", "preset": "1gpu-16vcpu-200gb"},
			"bootDisk": map[string]any{
				"attachMode": "READ_WRITE",
				// Managed, so that Nebius deletes it with the VM.
				"managedDisk": map[string]any{
					"name": "beam-h200-a1b2c3d4",
					"spec": map[string]any{
						"sizeGibibytes":     "200",
						"type":              "NETWORK_SSD",
						"sourceImageFamily": map[string]any{"imageFamily": "ubuntu24.04-cuda13.0"},
					},
				},
			},
			"networkInterfaces": []any{map[string]any{
				"name":            "eth0",
				"subnetId":        "vpcsubnet-e00beamtest",
				"ipAddress":       map[string]any{},
				"publicIpAddress": map[string]any{},
			}},
			"cloudInitUserData": userData,
			"preemptible":       map[string]any{"onPreemption": "STOP"},
			"recoveryPolicy":    "FAIL",
			// No price limit was asked for.
			"followsSpotPrice": map[string]any{},
		},
	}, api.lastCreate())
	require.Empty(t, api.limits())
}

func TestNebiusBuysInTheProjectOfTheOffersRegion(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)

	_, err := client.Create(context.Background(), CreateRequest{Offer: nebiusRTXPro6000, Name: "beam-rtx-a1b2c3d4"})
	require.NoError(t, err)
	create := api.lastCreate()
	require.Equal(t, nebiusCentral, create["metadata"].(map[string]any)["parentId"])
	require.Equal(t, "vpcsubnet-u00beamtest", create["spec"].(map[string]any)["networkInterfaces"].([]any)[0].(map[string]any)["subnetId"])
	require.NotContains(t, create["spec"], "cloudInitUserData", "no script was asked for")

	elsewhere := nebiusH200
	elsewhere.Region = "eu-west1"
	_, err = client.Create(context.Background(), CreateRequest{Offer: elsewhere, Name: "beam-h200-a1b2c3d4"})
	require.ErrorContains(t, err, "no project in eu-west1")
	require.Len(t, api.creates, 1)
}

// Nebius has no API for the prices of its regular VMs, so this client has
// no offer for one, and does not buy one for the price of another.
func TestNebiusBuysNothingButPreemptibleVMs(t *testing.T) {
	api := newNebiusAPI(t)
	onDemand := nebiusH200
	onDemand.Spot = false

	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: onDemand, Name: "beam-h200-a1b2c3d4"})
	require.ErrorContains(t, err, "preemptible")
	require.Zero(t, api.sent("POST", "/compute/v1/instances"))
}

// Nebius limits what a VM pays with a pricing policy: the most a GPU of a
// platform may cost an hour, in a project. A VM without one pays the spot
// price whatever it rises to.
func TestNebiusLimitsThePriceWithAPricingPolicy(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	buy := func(offer Offer, name string, limit int64) map[string]any {
		t.Helper()
		_, err := client.Create(ctx, CreateRequest{Offer: offer, Name: name, MaxHourlyCostMicros: limit})
		require.NoError(t, err)
		spec := api.lastCreate()["spec"].(map[string]any)
		require.NotContains(t, spec, "followsSpotPrice")
		return spec["spotPricingPolicy"].(map[string]any)
	}

	// $4 an hour for the machine leaves $3.980548 for its GPU once the disk
	// is paid, which Nebius takes in thousandths, rounded down here.
	first := buy(nebiusH200, "beam-h200-a1b2c3d4", 4_000_000)
	require.Equal(t, []string{nebiusNorth + " beamgpuh200sxmmax3980 3.980"}, api.limits())

	// The next machine with the same limit shares the policy.
	require.Equal(t, first, buy(nebiusH200, "beam-h200-e5f6a7b8", 4_000_000))
	require.Len(t, api.limits(), 1)

	// The limit is the machine's, and the policy's is one GPU's.
	eight := buy(nebiusH200x8, "beam-h200-c9d0e1f2", 30_000_000)
	require.NotEqual(t, first, eight)
	require.Equal(t, nebiusNorth+" beamgpuh200sxmmax3747 3.747", api.limits()[1])

	// A policy is for one platform in one project.
	buy(nebiusRTXPro6000, "beam-rtx-a1b2c3d4", 4_000_000)
	require.Equal(t, nebiusCentral+" beamgpurtx6000max3980 3.980", api.limits()[2])
	h100 := Offer{ID: "gpu-h100-sxm/1gpu-16vcpu-200gb", Region: "eu-north1", GPUCount: 1, Spot: true}
	require.NotEqual(t, first, buy(h100, "beam-h100-a1b2c3d4", 4_000_000))
	require.Equal(t, nebiusNorth+" beamgpuh100sxmmax3980 3.980", api.limits()[3])

	// One that is being deleted takes no more VMs.
	api.mu.Lock()
	api.policies[0]["status"].(map[string]any)["state"] = "STATE_DELETING"
	api.policies[0]["metadata"].(map[string]any)["name"] = "renamed"
	api.mu.Unlock()
	require.NotEqual(t, first, buy(nebiusH200, "beam-h200-a3b4c5d6", 4_000_000))
	require.Len(t, api.limits(), 5)
}

func TestNebiusRefusesAPriceLimitThatLeavesNothingForTheGPU(t *testing.T) {
	api := newNebiusAPI(t)
	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4", MaxHourlyCostMicros: nebiusTestDisk + 999})
	require.ErrorContains(t, err, "leaves nothing for the GPUs")
	require.Empty(t, api.limits())
	require.Empty(t, api.creates)
}

// "Creating a VM whose pricing policy is blocked returns an error but still
// creates a stopped VM", whose disk is paid for.
func TestNebiusDeletesWhatAFailedCreateLeaves(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	api.mu.Lock()
	api.leavesAStoppedVM = &nebiusReply{code: 8, message: "not enough resources", detail: nebiusRefusals[ErrOutOfStock].detail}
	api.mu.Unlock()

	created, err := client.Create(context.Background(), CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4"})
	require.ErrorIs(t, err, ErrOutOfStock)
	require.ErrorContains(t, err, "not enough resources")
	require.Empty(t, created.ID)
	require.Equal(t, []string{"computeinstance-e000001"}, api.deletes)

	// A failure that names no reason is a plain error.
	api.mu.Lock()
	api.leavesAStoppedVM = &nebiusReply{code: 9, message: "the pricing policy is blocked"}
	api.mu.Unlock()
	_, err = client.Create(context.Background(), CreateRequest{Offer: nebiusH200, Name: "beam-h200-e5f6a7b8"})
	require.ErrorContains(t, err, "the pricing policy is blocked")
	require.NotErrorIs(t, err, ErrOutOfStock)
	require.NotErrorIs(t, err, ErrQuota)
	require.Len(t, api.deletes, 2)
}

// Nebius stops a preemptible VM to take it back and says no more about it.
// Nothing here stops a VM, so a stopped one was taken back.
func TestNebiusReadsAStoppedPreemptibleVMAsTakenBack(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4"})
	require.NoError(t, err)

	api.state(created.ID, "STOPPED")
	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusInterrupted, StatusDetail: "STOPPED"}, found)

	// It is still there, and is listed so that it gets deleted: it counts
	// towards the quota and its disk is paid for.
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []Instance{found}, listed)

	// A regular VM that someone gave this owner's label was not taken back.
	api.mu.Lock()
	delete(api.instances[0]["spec"].(map[string]any), "preemptible")
	api.mu.Unlock()
	found, err = client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusFailed, found.Status)

	api.state(created.ID, "UNSPECIFIED")
	found, err = client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusUnknown, found.Status)
}

func TestNebiusReportsWhereAMachineCanBeReached(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	created, err := client.Create(context.Background(), CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4"})
	require.NoError(t, err)

	found, err := client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Empty(t, found.PublicIP, "a machine that is not up has no address")

	// Nebius writes an address with its prefix length.
	api.mu.Lock()
	api.instances[0]["status"] = map[string]any{
		"state": "RUNNING",
		"networkInterfaces": []any{map[string]any{
			"name":            "eth0",
			"ipAddress":       map[string]any{"address": "192.168.0.12/32"},
			"publicIpAddress": map[string]any{"address": "195.242.10.162/32"},
		}},
	}
	api.mu.Unlock()
	found, err = client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusActive, PublicIP: "195.242.10.162", SSHHost: "195.242.10.162", SSHPort: 22}, found)
}

// The stand-in refuses to delete what it is deleting, as Nebius may.
func TestNebiusDeleteSucceedsWhileNebiusIsDeleting(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4"})
	require.NoError(t, err)

	require.NoError(t, client.Delete(ctx, created.ID))
	require.NoError(t, client.Delete(ctx, created.ID))
	require.Equal(t, []string{created.ID}, api.deletes)

	// A delete that Nebius refuses for another reason is an error.
	other, err := client.Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-e5f6a7b8"})
	require.NoError(t, err)
	api.mu.Lock()
	api.revokeAll()
	api.mu.Unlock()
	require.Error(t, client.Delete(ctx, other.ID))
}

// revokeAll makes every call fail from here on: no token is good, and none
// is given out.
func (a *nebiusAPI) revokeAll() {
	a.tokens = -1 << 30
}

func TestNebiusNeedsToKnowWhichSubnetToUse(t *testing.T) {
	api := newNebiusAPI(t)
	api.subnets[nebiusCentral] = append(api.subnets[nebiusCentral], "vpcsubnet-u00second")

	_, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.ErrorContains(t, err, `Nebius project "project-u00beamtest" has 2 subnets`)
	require.ErrorContains(t, err, "subnetId under providers.nebius.projects")

	client := api.client(testOwner, func(config *types.NebiusProviderConfig) { config.Projects[1].SubnetID = "vpcsubnet-u00second" })
	_, err = client.Create(context.Background(), CreateRequest{Offer: nebiusRTXPro6000, Name: "beam-rtx-a1b2c3d4"})
	require.NoError(t, err)
	require.Equal(t, "vpcsubnet-u00second", api.lastCreate()["spec"].(map[string]any)["networkInterfaces"].([]any)[0].(map[string]any)["subnetId"])

	api.subnets[nebiusNorth] = nil
	_, err = api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.ErrorContains(t, err, `Nebius project "project-e00beamtest" has 0 subnets`)
}

func TestNebiusNeedsOneProjectForARegion(t *testing.T) {
	api := newNebiusAPI(t)
	for name, tc := range map[string]struct {
		projects []types.NebiusProjectConfig
		want     string
		// unlisted is why what was bought cannot be listed either, when it
		// cannot.
		unlisted string
	}{
		"none":           {nil, "providers.nebius.projects names no project", "providers.nebius.projects names no project"},
		"one twice":      {[]types.NebiusProjectConfig{{ID: nebiusNorth}, {ID: nebiusNorth}}, "are both in eu-north1", ""},
		"one not there":  {[]types.NebiusProjectConfig{{ID: "project-e00nosuch"}}, `Nebius project "project-e00nosuch"`, "project project-e00nosuch not found"},
		"one and a typo": {[]types.NebiusProjectConfig{{ID: nebiusNorth}, {ID: "project-e00nosuch"}}, "not found", "not found"},
	} {
		t.Run(name, func(t *testing.T) {
			client := api.client(testOwner, func(config *types.NebiusProviderConfig) { config.Projects = tc.projects })
			_, err := client.Offers(context.Background(), OfferFilter{})
			require.ErrorContains(t, err, tc.want)
			_, err = client.List(context.Background())
			if tc.unlisted == "" {
				require.NoError(t, err, "what was bought there can still be listed")
			} else {
				require.ErrorContains(t, err, tc.unlisted, "what cannot be listed must not read as nothing bought")
			}
		})
	}
}

func TestNebiusReadsItsProjectsRarelyAndItsStockEveryTime(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	for range 3 {
		_, err := client.Offers(context.Background(), OfferFilter{})
		require.NoError(t, err)
	}
	require.Equal(t, 2, api.sent("GET", "/iam/v1/projects/"))
	require.Equal(t, 2, api.sent("GET", "/vpc/v1/subnets"))
	require.Equal(t, 3, api.sent("GET", "/billing/v1alpha1/spot-price"))
	// The two projects are one tenant's, which is asked once each time.
	require.Equal(t, 3, api.sent("GET", "/capacity/v1/resource-advice"))

	api.after(nebiusProjectsFor)
	_, err := client.Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Equal(t, 4, api.sent("GET", "/iam/v1/projects/"))
}

func TestNebiusUsesTheImageFamilyAndDiskItIsGiven(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner, func(config *types.NebiusProviderConfig) {
		config.ImageFamily = "ubuntu24.04-cuda12"
		config.DiskGB = 500
	})
	ctx := context.Background()

	// The price is the GPU's plus the disk's: $0.071 a gibibyte for 730
	// hours, which is $0.04863 an hour for 500.
	offers, err := client.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	want := nebiusH200
	want.StorageMB, want.HourlyCostMicros, want.DiskHourlyMicros = 500*1024, 3_380_000+48_630, 48_630
	require.Equal(t, want, offers[0])

	_, err = client.Create(ctx, CreateRequest{Offer: offers[0], Name: "beam-h200-a1b2c3d4"})
	require.NoError(t, err)
	disk := api.lastCreate()["spec"].(map[string]any)["bootDisk"].(map[string]any)["managedDisk"].(map[string]any)["spec"].(map[string]any)
	require.Equal(t, "500", disk["sizeGibibytes"])
	require.Equal(t, map[string]any{"imageFamily": "ubuntu24.04-cuda12"}, disk["sourceImageFamily"])
}

// Two machines bought at once with the same limit both look for its policy,
// find none, and both make it. Nebius keeps the first and turns the second
// down, which then buys with the first's.
func TestNebiusUsesAPricingPolicyThatWasMadeWhileItMadeItsOwn(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	api.mu.Lock()
	api.policyMadeFirst = true
	api.mu.Unlock()

	_, err := client.Create(context.Background(), CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4", MaxHourlyCostMicros: 4_000_000})
	require.NoError(t, err)
	require.Equal(t, []string{nebiusNorth + " beamgpuh200sxmmax3980 3.980"}, api.limits())
	require.Equal(t, map[string]any{"id": "pricingpolicy-e000001"}, api.lastCreate()["spec"].(map[string]any)["spotPricingPolicy"])
	require.Equal(t, 1, api.sent("POST", "/billing/v1/pricing-policies"))
	require.Equal(t, 2, api.sent("GET", "/billing/v1/pricing-policies"), "it looked again once")
}

// A policy that cannot be made, and is not there, buys nothing: a machine
// without it would pay the spot price whatever it rose to.
func TestNebiusBuysNothingWhenItCannotMakeThePricingPolicy(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	api.mu.Lock()
	api.policyRefusal = &nebiusReply{status: http.StatusForbidden, code: 7, message: "permission denied"}
	api.mu.Unlock()

	_, err := client.Create(context.Background(), CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4", MaxHourlyCostMicros: 4_000_000})
	require.ErrorContains(t, err, "make a Nebius pricing policy for gpu-h200-sxm at $3.980 a GPU-hour")
	require.ErrorContains(t, err, "permission denied")
	require.NotErrorIs(t, err, ErrOutOfStock)
	require.NotErrorIs(t, err, ErrQuota)
	require.Zero(t, api.sent("POST", "/compute/v1/instances"))
}

// An error of Nebius's says what kind it is by which field of a detail is
// set. It is read the same way where it answers a call and where it is the
// status of an operation that failed. A field that is null is not set.
func TestNebiusReadsTheKindOfAnErrorFromTheDetailThatIsSet(t *testing.T) {
	for name, tc := range map[string]struct {
		detail map[string]any
		want   error
	}{
		"not enough resources": {map[string]any{"notEnoughResources": map[string]any{"violations": []any{}}}, ErrOutOfStock},
		"a quota":              {map[string]any{"quotaFailure": map[string]any{}}, ErrQuota},
		"fields that are null": {map[string]any{"notEnoughResources": nil, "quotaFailure": nil}, nil},
		"another kind":         {map[string]any{"badRequest": map[string]any{"violations": []any{}}}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			check := func(err error) {
				t.Helper()
				require.ErrorContains(t, err, "the reason in words")
				for _, kind := range []error{ErrOutOfStock, ErrQuota} {
					require.Equal(t, kind == tc.want, errors.Is(err, kind), "%v", err)
				}
			}
			api := newNebiusAPI(t)
			client := api.client(testOwner)
			req := CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4"}

			api.refuse(&nebiusReply{status: http.StatusBadRequest, code: 9, message: "the reason in words", detail: tc.detail})
			_, err := client.Create(context.Background(), req)
			check(err)

			api.refuse(nil)
			api.mu.Lock()
			api.leavesAStoppedVM = &nebiusReply{code: 9, message: "the reason in words", detail: tc.detail}
			api.mu.Unlock()
			_, err = client.Create(context.Background(), req)
			check(err)
		})
	}
}

// Nebius says how an operation ended in its status. One that this client
// cannot read may have failed, and is not taken for one that went well.
func TestNebiusDoesNotTakeAnOperationItCannotReadForOneThatSucceeded(t *testing.T) {
	api := newNebiusAPI(t)
	next := api.server.Config.Handler
	api.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/compute/v1/instances" {
			next.ServeHTTP(w, r)
			return
		}
		// The create arrives, and its answer is in no shape Nebius documents.
		next.ServeHTTP(httptest.NewRecorder(), r)
		writeJSON(w, http.StatusOK, map[string]any{"id": "computeoperation-e000001", "resourceId": "computeinstance-e000001", "status": "it did not go well"})
	})
	client := api.client(testOwner)
	ctx := context.Background()
	tags := map[string]string{"beam-machine": "a1b2c3d4"}

	created, err := client.Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4", Tags: tags})
	require.ErrorContains(t, err, "POST /compute/v1/instances: decode response")
	require.Empty(t, created.ID)

	// What the create made is found by its tags, as after any create that
	// got no answer.
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, tags, listed[0].Tags)
}

// A create that Nebius fails can leave a stopped VM, which is deleted at
// once. When that delete fails too, the create still says why it failed,
// and the VM is there to be found and deleted later.
func TestNebiusACreateThatFailedSaysWhyEvenWhenWhatItLeftCannotBeDeleted(t *testing.T) {
	api := newNebiusAPI(t)
	api.mu.Lock()
	api.leavesAStoppedVM = &nebiusReply{code: 9, message: "the pricing policy is blocked"}
	api.mu.Unlock()
	nebius := api.server.Config.Handler
	api.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			http.Error(w, "this key may not delete", http.StatusForbidden)
			return
		}
		nebius.ServeHTTP(w, r)
	})
	client := api.client(testOwner)
	ctx := context.Background()

	created, err := client.Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4"})
	require.ErrorContains(t, err, "the pricing policy is blocked")
	require.NotContains(t, err.Error(), "may not delete")
	require.Empty(t, created.ID)

	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, StatusInterrupted, listed[0].Status, "stopped, and paid for until it is deleted")
}

const shadeformTestKey = "shadeform-test-key"

// shadeformAPI stands in for Shadeform. It answers with the shapes of
// Shadeform's published API description (https://api.shadeform.ai/v1/openapi.json).
type shadeformAPI struct {
	apiStandIn

	nextID    int
	instances []map[string]any
	updates   []map[string]any
	refusal   map[string]string
	// catalogue, when set, is listed in place of the recorded instance
	// types.
	catalogue string
}

func newShadeformAPI(t *testing.T) *shadeformAPI {
	api := &shadeformAPI{apiStandIn: apiStandIn{t: t}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /instances/types", api.types)
	mux.HandleFunc("POST /instances/create", api.create)
	mux.HandleFunc("GET /instances", api.list)
	mux.HandleFunc("GET /instances/{id}/info", api.info)
	mux.HandleFunc("POST /instances/{id}/update", api.update)
	mux.HandleFunc("POST /instances/{id}/delete", api.delete)
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.mu.Lock()
		defer api.mu.Unlock()
		api.requests = append(api.requests, r.Method+" "+r.URL.RequestURI())
		if r.Header.Get("X-API-KEY") != shadeformTestKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(api.server.Close)
	return api
}

func (a *shadeformAPI) client(owner string) *Shadeform {
	return NewShadeform(types.ShadeformProviderConfig{ApiKey: shadeformTestKey, BaseURL: a.server.URL}, owner)
}

func (a *shadeformAPI) types(w http.ResponseWriter, r *http.Request) {
	if a.catalogue != "" {
		w.Write([]byte(a.catalogue))
		return
	}
	w.Write(sample(a.t, "shadeform/instance_types"))
}

func (a *shadeformAPI) create(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	assert.NoError(a.t, json.NewDecoder(r.Body).Decode(&body))
	if a.refusal != nil {
		writeJSON(w, http.StatusConflict, a.refusal)
		return
	}
	a.creates = append(a.creates, body)

	a.nextID++
	instance := map[string]any{}
	assert.NoError(a.t, json.Unmarshal(sample(a.t, "shadeform/instance"), &instance))
	instance["id"] = fmt.Sprintf("d290f1ee-6c54-4b01-90e6-%012d", a.nextID)
	instance["status"] = "creating"
	instance["status_details"] = ""
	instance["ip"] = nil
	for _, field := range []string{"name", "cloud", "region", "shade_instance_type", "tags"} {
		instance[field] = body[field]
	}
	rentalType, _ := body["rental_type"].(string)
	if rentalType == "" {
		rentalType = "on_demand"
	}
	instance["rental_terms"] = map[string]any{"rental_type": rentalType, "ending_at": ""}
	a.instances = append(a.instances, instance)
	writeJSON(w, http.StatusOK, map[string]any{"id": instance["id"], "status": "creating"})
}

func (a *shadeformAPI) list(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if limit < 1 || limit > 100 {
		http.Error(w, "limit must be between 1 and 100", http.StatusBadRequest)
		return
	}
	page := a.instances[min(offset, len(a.instances)):]
	page = page[:min(limit, len(page))]
	writeJSON(w, http.StatusOK, map[string]any{"instances": page})
}

func (a *shadeformAPI) find(id string) int {
	return slices.IndexFunc(a.instances, func(instance map[string]any) bool { return instance["id"] == id })
}

const shadeformNoSuchInstance = "No such instance on this account"

func (a *shadeformAPI) info(w http.ResponseWriter, r *http.Request) {
	i := a.find(r.PathValue("id"))
	if i < 0 {
		http.Error(w, shadeformNoSuchInstance, http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, a.instances[i])
}

func (a *shadeformAPI) update(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	assert.NoError(a.t, json.NewDecoder(r.Body).Decode(&body))
	if a.find(r.PathValue("id")) < 0 {
		http.Error(w, shadeformNoSuchInstance, http.StatusNotFound)
		return
	}
	body["id"] = r.PathValue("id")
	a.updates = append(a.updates, body)
}

func (a *shadeformAPI) delete(w http.ResponseWriter, r *http.Request) {
	i := a.find(r.PathValue("id"))
	if i < 0 {
		http.Error(w, shadeformNoSuchInstance, http.StatusNotFound)
		return
	}
	a.instances[i]["status"] = "deleting"
}

func (a *shadeformAPI) set(id, field string, value any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := a.find(id)
	require.GreaterOrEqual(a.t, i, 0, "no instance %s", id)
	a.instances[i][field] = value
}

func (a *shadeformAPI) refuse(with map[string]string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refusal = with
}

func (a *shadeformAPI) lose(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if i := a.find(id); i >= 0 {
		a.instances = slices.Delete(a.instances, i, i+1)
	}
}

func shadeformScript(t *testing.T, create map[string]any) string {
	launch, _ := create["launch_configuration"].(map[string]any)
	require.Equal(t, "script", launch["type"])
	encoded := launch["script_configuration"].(map[string]any)["base64_script"].(string)
	script, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)
	return string(script)
}

// Shadeform publishes the shape of a refusal, not its codes. These are the
// reasons this client is written to recognise; a live smoke test has to
// confirm them.
var shadeformRefusals = map[error]map[string]string{
	ErrOutOfStock: {"error": "The requested instance type is not available in the selected region.", "error_code": "INSTANCE_UNAVAILABLE"},
	ErrQuota:      {"error": "You have reached your instance quota.", "error_code": "QUOTA_EXCEEDED"},
}

func newShadeformSite(t *testing.T) site {
	api := newShadeformAPI(t)
	return site{
		provider: api.client(testOwner),
		statuses: map[string]Status{
			"creating":         StatusPending,
			"pending_provider": StatusPending,
			"pending":          StatusPending,
			"active":           StatusActive,
			"error":            StatusFailed,
			"deleting":         StatusDeleted,
			"deleted":          StatusDeleted,
		},
		report: func(id, status string) { api.set(id, "status", status) },
		refuse: func(reason error) { api.refuse(shadeformRefusals[reason]) },
		lose:   api.lose,
		bought: func() (string, string) {
			create := api.lastCreate()
			return create["name"].(string), shadeformScript(t, create)
		},
		plantForeign: func() string {
			offers, err := api.client(foreignOwner).Offers(context.Background(), OfferFilter{})
			require.NoError(t, err)
			instance, err := api.client(foreignOwner).Create(context.Background(), CreateRequest{Offer: offers[0], Name: "someone-elses"})
			require.NoError(t, err)
			return instance.ID
		},
		boughtSpot: func() bool { return api.lastCreate()["rental_type"] == "spot" },
		takeBack: func(id string) {
			api.set(id, "status", "deleting")
			api.set(id, "status_details", "preempted")
		},
	}
}

func TestShadeformOffersOneForEachRegionThatHasStock(t *testing.T) {
	api := newShadeformAPI(t)
	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)

	// The catalogue holds an L4 with stock in two regions of three, an H100
	// two clouds sell under one name, a sold-out A100, a bare-metal machine,
	// a CPU machine, and a B200 that has spot stock only. The B200 costs
	// $76 an hour on demand and $44 as spot: the spot price is the one in
	// the region's own entry, in dollars.
	require.Equal(t, []Offer{
		{ID: "L4", Provider: "shadeform", Cloud: "scaleway", InstanceType: "L4", Region: "paris-france-5", GPU: "L4", GPUCount: 1, CPUMillicores: 8000, MemoryMB: 48 * 1024, StorageMB: 500 * 1024, HourlyCostMicros: 950_000},
		{ID: "L4", Provider: "shadeform", Cloud: "scaleway", InstanceType: "L4", Region: "warsaw-poland-1", GPU: "L4", GPUCount: 1, CPUMillicores: 8000, MemoryMB: 48 * 1024, StorageMB: 500 * 1024, HourlyCostMicros: 950_000},
		{ID: "B200_sxm5x8_NVLINK", Provider: "shadeform", Cloud: "nebius", InstanceType: "B200_sxm5x8_NVLINK", Region: "kansascity-usa-3", GPU: "B200", GPUCount: 8, CPUMillicores: 160_000, MemoryMB: 1792 * 1024, StorageMB: 1250 * 1024, HourlyCostMicros: 44_000_000, Spot: true},
		{ID: "H100", Provider: "shadeform", Cloud: "paperspace", InstanceType: "H100", Region: "newyork-usa-1", GPU: "H100", GPUCount: 1, CPUMillicores: 16000, MemoryMB: 268 * 1024, StorageMB: 500 * 1024, HourlyCostMicros: 5_990_000},
		{ID: "H100", Provider: "shadeform", Cloud: "scaleway", InstanceType: "H100", Region: "paris-france-1", GPU: "H100", GPUCount: 1, CPUMillicores: 24000, MemoryMB: 240 * 1024, StorageMB: 1000 * 1024, HourlyCostMicros: 3_300_000},
	}, offers)
	require.Equal(t, []string{"GET /instances/types"}, api.asked())
}

func TestShadeformCreateBuysTheOfferInItsCloudAndRegion(t *testing.T) {
	api := newShadeformAPI(t)
	until := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)

	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{
		Offer:         Offer{ID: "H100", Cloud: "scaleway", Region: "paris-france-1"},
		Name:          "beam-h100-a1b2c3d4",
		StartupScript: "echo join",
		Tags:          map[string]string{"beam-machine": "a1b2c3d4"},
		Until:         until,
	})
	require.NoError(t, err)

	create := api.lastCreate()
	require.Equal(t, "#!/usr/bin/env bash\nset -euo pipefail\necho join\n", shadeformScript(t, create))
	delete(create, "launch_configuration")
	require.Equal(t, map[string]any{
		"cloud":               "scaleway",
		"region":              "paris-france-1",
		"shade_instance_type": "H100",
		"shade_cloud":         true,
		"name":                "beam-h100-a1b2c3d4",
		"tags":                []any{"beam-machine=a1b2c3d4", "beam-owner=" + testOwner},
		// Shadeform's own timer is set to the time it is given, and there
		// is no spend threshold to end a renewed machine early.
		"auto_delete": map[string]any{"date_threshold": "2026-11-14T12:00:00Z"},
	}, create)
}

func TestShadeformBuysASpotOfferAsSpot(t *testing.T) {
	api := newShadeformAPI(t)
	client := api.client(testOwner)
	spot := Offer{ID: "B200_sxm5x8_NVLINK", Cloud: "nebius", Region: "kansascity-usa-3", Spot: true}

	_, err := client.Create(context.Background(), CreateRequest{Offer: spot, Name: "beam-b200-a1b2c3d4"})
	require.NoError(t, err)
	create := api.lastCreate()
	require.Equal(t, "spot", create["rental_type"])
	require.Equal(t, true, create["shade_cloud"], "spot runs on Shade Cloud only")

	// On demand is Shadeform's default, and is left to it.
	spot.Spot = false
	_, err = client.Create(context.Background(), CreateRequest{Offer: spot, Name: "beam-b200-e5f6a7b8"})
	require.NoError(t, err)
	require.NotContains(t, api.lastCreate(), "rental_type")
}

func TestShadeformCreateFitsTheNameIntoFiftyCharacters(t *testing.T) {
	api := newShadeformAPI(t)
	long := "beam-a-pool-with-a-very-long-name-that-goes-on-and-on-a1b2c3d4"

	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{
		Offer: Offer{ID: "H100", Cloud: "scaleway", Region: "paris-france-1"},
		Name:  long,
	})
	require.NoError(t, err)

	name := api.lastCreate()["name"].(string)
	require.Len(t, name, 50)
	require.Equal(t, "beam-a-pool-with-a-very-long-name-that-go-a1b2c3d4", name)
}

func TestShadeformCreateNeedsACloudAndRegion(t *testing.T) {
	api := newShadeformAPI(t)
	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: Offer{ID: "H100"}, Name: "beam-h100-a1b2c3d4"})
	require.ErrorContains(t, err, "needs an instance type, cloud and region")
	require.Empty(t, api.asked())
}

func TestShadeformRefusalItDoesNotRecogniseIsNeitherStockNorQuota(t *testing.T) {
	api := newShadeformAPI(t)
	api.refuse(map[string]string{"error": "The name is already in use.", "error_code": "NAME_TAKEN"})

	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: Offer{ID: "H100", Cloud: "scaleway", Region: "paris-france-1"}, Name: "beam-h100-a1b2c3d4"})
	require.ErrorContains(t, err, "NAME_TAKEN")
	require.NotErrorIs(t, err, ErrOutOfStock)
	require.NotErrorIs(t, err, ErrQuota)
}

func TestShadeformReportsAddressesOnceActive(t *testing.T) {
	api := newShadeformAPI(t)
	client := api.client(testOwner)
	created, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "H100", Cloud: "scaleway", Region: "paris-france-1"}, Name: "beam-h100-a1b2c3d4"})
	require.NoError(t, err)

	booting, err := client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusPending, SSHPort: 22}, booting)

	api.set(created.ID, "status", "active")
	api.set(created.ID, "ip", "1.0.0.1")
	active, err := client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusActive, PublicIP: "1.0.0.1", SSHHost: "1.0.0.1", SSHPort: 22}, active)

	// Some clouds behind Shadeform give a DNS name where others give an IP.
	api.set(created.ID, "ip", "node-7.cloud.example")
	named, err := client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusActive, SSHHost: "node-7.cloud.example", SSHPort: 22}, named)
}

// A spot instance that Shadeform takes back passes through deleting to
// deleted, with its details saying why. It is interrupted in both, and is
// told apart from an instance that was deleted on request.
func TestShadeformReportsASpotInstanceItTookBack(t *testing.T) {
	api := newShadeformAPI(t)
	client := api.client(testOwner)
	created, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "B200_sxm5x8_NVLINK", Cloud: "nebius", Region: "kansascity-usa-3", Spot: true}, Name: "beam-b200-a1b2c3d4"})
	require.NoError(t, err)

	for _, status := range []string{"deleting", "deleted"} {
		api.set(created.ID, "status", status)
		api.set(created.ID, "status_details", "preempted")
		found, err := client.Get(context.Background(), created.ID)
		require.NoError(t, err)
		require.Equal(t, StatusInterrupted, found.Status, status)
		require.Equal(t, "preempted", found.StatusDetail)

		listed, err := client.List(context.Background())
		require.NoError(t, err)
		require.Len(t, listed, 1)
		require.Equal(t, StatusInterrupted, listed[0].Status, status)
	}

	api.set(created.ID, "status_details", "")
	found, err := client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusDeleted, found.Status)
}

func TestShadeformExtendMovesItsDeletionTimer(t *testing.T) {
	api := newShadeformAPI(t)
	client := api.client(testOwner)
	created, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "H100", Cloud: "scaleway", Region: "paris-france-1"}, Name: "beam-h100-a1b2c3d4"})
	require.NoError(t, err)

	// The time is the caller's, and Shadeform is given it in UTC.
	until := time.Date(2026, 12, 1, 1, 0, 0, 0, time.FixedZone("CET", 3600))
	require.NoError(t, client.Extend(context.Background(), created.ID, until))
	require.Equal(t, []map[string]any{{
		"id":          created.ID,
		"auto_delete": map[string]any{"date_threshold": "2026-12-01T00:00:00Z"},
	}}, api.updates)

	// An instance Shadeform does not have has no timer to move. What it
	// answers is passed on as it is: no caller asks why an Extend failed.
	err = client.Extend(context.Background(), "424242", time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC))
	require.EqualError(t, err, "POST /instances/424242/update: status 404: No such instance on this account")
}

func TestShadeformListReadsEveryPage(t *testing.T) {
	api := newShadeformAPI(t)
	client := api.client(testOwner)
	offer := Offer{ID: "H100", Cloud: "scaleway", Region: "paris-france-1"}
	for i := range 130 {
		owner := client
		if i%13 == 0 {
			owner = api.client(foreignOwner)
		}
		_, err := owner.Create(context.Background(), CreateRequest{Offer: offer, Name: fmt.Sprintf("beam-h100-%08d", i)})
		require.NoError(t, err)
	}
	api.asked()

	listed, err := client.List(context.Background())
	require.NoError(t, err)
	require.Len(t, listed, 120)
	require.Equal(t, []string{"GET /instances?limit=100&offset=0", "GET /instances?limit=100&offset=100"}, api.asked())
}

// Shadeform answers a create with the instance's ID and status, and tells
// the rest when it is asked about the instance. Nothing else in the answer
// is read, so that a machine which was bought is not reported as a failure
// over a field this client has no use for.
func TestShadeformCreateReadsOnlyTheIDAndStatusOfItsAnswer(t *testing.T) {
	api := newShadeformAPI(t)
	shadeform := api.server.Config.Handler
	api.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/instances/create" {
			shadeform.ServeHTTP(w, r)
			return
		}
		// The create goes through, and its answer says more than it used
		// to, in shapes of its own.
		recorded := httptest.NewRecorder()
		shadeform.ServeHTTP(recorded, r)
		var answer map[string]any
		assert.NoError(t, json.Unmarshal(recorded.Body.Bytes(), &answer))
		answer["ip"], answer["ssh_port"], answer["tags"] = 0, "22", "none"
		writeJSON(w, http.StatusOK, answer)
	})

	created, err := api.client(testOwner).Create(context.Background(), CreateRequest{
		Offer: Offer{ID: "H100", Cloud: "scaleway", Region: "paris-france-1"}, Name: "beam-h100-a1b2c3d4",
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.ID)
	require.Equal(t, StatusPending, created.Status)
}

const vastTestKey = "vast-test-key"

// vastAPI stands in for Vast.
//
// What it rents out is recorded: the sample vast/bundles is part of what
// Vast's search, which needs no account, answered to this client's own
// question on 2026-11-14, each offer cut down to the fields read here. The
// search itself follows what that endpoint was seen to do: the filters and
// their operators, one offer for a machine and a number of its GPUs, the
// price of the disk that is asked about.
//
// The rest follows Vast's published API description, and is made up where
// that says nothing, since Vast shows it to its customers only: how an
// instance that was outbid reads, what a create answers to an account
// without credit, and what is said of an instance that does not exist.
type vastAPI struct {
	apiStandIn

	nextID int64
	// searches are the searches for offers that were asked for, in order.
	searches []map[string]any
	// asks are what hosts ask for their machines.
	asks      []map[string]any
	instances []map[string]any

	// sshKeys is how many SSH keys the account has.
	sshKeys int
	// outbidAt, when set, is what someone else bids for every machine.
	outbidAt float64
	// refusal, when set, is the answer to every create.
	refusal *vastReply
	// deleteAnswer, when set, is the answer to every delete.
	deleteAnswer *vastReply
	// missingIs404 makes a question about an instance that does not exist
	// an error, where Vast otherwise answers with no instance.
	missingIs404 bool
}

// vastReply is an answer of Vast's that is not the usual one.
type vastReply struct {
	status int
	body   map[string]any
}

func vastError(status int, code, message string) *vastReply {
	return &vastReply{status, map[string]any{"success": false, "error": code, "msg": message}}
}

func newVastAPI(t *testing.T) *vastAPI {
	api := &vastAPI{apiStandIn: apiStandIn{t: t}, nextID: 31337000, sshKeys: 1}

	var catalogue struct {
		Offers []map[string]any `json:"offers"`
	}
	require.NoError(t, json.Unmarshal(sample(t, "vast/bundles"), &catalogue))
	api.asks = catalogue.Offers

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v0/bundles/", api.search)
	mux.HandleFunc("PUT /api/v0/asks/{id}/", api.authorized(api.create))
	mux.HandleFunc("GET /api/v0/instances/{id}/", api.authorized(api.show))
	mux.HandleFunc("DELETE /api/v0/instances/{id}/", api.authorized(api.destroy))
	mux.HandleFunc("GET /api/v1/instances/", api.authorized(api.list))
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.mu.Lock()
		defer api.mu.Unlock()
		api.requests = append(api.requests, r.Method+" "+r.URL.Path)
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(api.server.Close)
	return api
}

// client returns a client of the stand-in's Vast, configured as a deployment
// is unless the test changes it.
func (a *vastAPI) client(owner string, configure ...func(*types.VastProviderConfig)) *Vast {
	config := types.VastProviderConfig{ApiKey: vastTestKey, BaseURL: a.server.URL}
	for _, change := range configure {
		change(&config)
	}
	return NewVast(config, owner)
}

func (a *vastAPI) authorized(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+vastTestKey {
			a.answer(w, vastError(http.StatusUnauthorized, "auth_error", "Invalid user key"))
			return
		}
		next(w, r)
	}
}

func (a *vastAPI) answer(w http.ResponseWriter, reply *vastReply) {
	writeJSON(w, reply.status, reply.body)
}

// vastAskFields are what a search can narrow by, and where an ask has it.
var vastAskFields = map[string]func(ask map[string]any) any{
	"verified":    func(ask map[string]any) any { return ask["verification"] == "verified" },
	"rentable":    func(ask map[string]any) any { return ask["rentable"] },
	"rented":      func(ask map[string]any) any { return ask["rented"] },
	"vms_enabled": func(ask map[string]any) any { return ask["vms_enabled"] },
	"gpu_arch":    func(ask map[string]any) any { return ask["gpu_arch"] },
	"cpu_arch":    func(ask map[string]any) any { return ask["cpu_arch"] },
	"disk_space":  func(ask map[string]any) any { return ask["disk_space"] },
	"machine_id":  func(ask map[string]any) any { return ask["machine_id"] },
	"num_gpus":    func(ask map[string]any) any { return ask["num_gpus"] },
}

// vastHolds reports whether a value passes a filter such as {"gte": 200}.
func vastHolds(have any, filter any) (bool, error) {
	conditions, ok := filter.(map[string]any)
	if !ok {
		return false, fmt.Errorf("Input should be a valid dictionary")
	}
	for operator, want := range conditions {
		number, isNumber := have.(float64)
		wanted, wantsNumber := want.(float64)
		var holds bool
		switch operator {
		case "eq":
			holds = have == want
		case "neq":
			holds = have != want
		case "gt", "gte", "lt", "lte":
			if !isNumber || !wantsNumber {
				return false, fmt.Errorf("Input should be a valid number")
			}
			holds = map[string]bool{"gt": number > wanted, "gte": number >= wanted, "lt": number < wanted, "lte": number <= wanted}[operator]
		case "in", "notin":
			among, ok := want.([]any)
			if !ok {
				return false, fmt.Errorf("Input should be a valid list")
			}
			holds = slices.Contains(among, have) == (operator == "in")
		default:
			return false, fmt.Errorf("%s.[key]: Input should be 'eq', 'neq', 'gte', 'gt', 'lte', 'lt', 'in' or 'notin'", operator)
		}
		if !holds {
			return false, nil
		}
	}
	return true, nil
}

// search answers as Vast's does: what matches every filter, priced with the
// disk that is asked about, in the order asked for.
func (a *vastAPI) search(w http.ResponseWriter, r *http.Request) {
	var question map[string]any
	if err := json.NewDecoder(r.Body).Decode(&question); err != nil {
		a.answer(w, vastError(http.StatusBadRequest, "invalid_request", "Invalid json body"))
		return
	}
	a.searches = append(a.searches, maps.Clone(question))
	bad := func(why string) { a.answer(w, vastError(http.StatusBadRequest, "bad_request", why)) }

	// What Vast assumes when it is not told.
	for field, assumed := range map[string]bool{"verified": true, "rentable": true, "rented": false} {
		if _, told := question[field]; !told {
			question[field] = map[string]any{"eq": assumed}
		}
	}
	kind, disk, limit := "ondemand", 8.0, len(a.asks)
	var order [][]string
	found := []map[string]any{}
	for field, value := range question {
		switch field {
		case "type":
			kind, _ = value.(string)
			if !slices.Contains([]string{"ondemand", "bid", "reserved"}, kind) {
				bad("type: Input should be 'ondemand', 'bid' or 'reserved'")
				return
			}
		case "allocated_storage":
			disk, _ = value.(float64)
		case "limit":
			number, _ := value.(float64)
			limit = int(number)
		case "order":
			encoded, _ := json.Marshal(value)
			if err := json.Unmarshal(encoded, &order); err != nil {
				bad("order: Input should be a valid list")
				return
			}
		default:
			if _, known := vastAskFields[field]; !known {
				bad(field + ": Extra inputs are not permitted")
				return
			}
		}
	}
	for _, ask := range a.asks {
		matches := true
		for field, read := range vastAskFields {
			filter, filtered := question[field]
			if !filtered {
				continue
			}
			holds, err := vastHolds(read(ask), filter)
			if err != nil {
				bad(field + "." + err.Error())
				return
			}
			matches = matches && holds
		}
		if !matches {
			continue
		}
		offer := maps.Clone(ask)
		offer["dph_base"] = ask["min_bid"]
		if kind != "bid" {
			// Made up: what its host asks of someone who rents it on demand.
			offer["dph_base"] = ask["min_bid"].(float64) * 2.5
		}
		offer["storage_total_cost"] = ask["storage_cost"].(float64) * disk / 720
		offer["dph_total"] = offer["dph_base"].(float64) + offer["storage_total_cost"].(float64)
		found = append(found, offer)
	}
	slices.SortStableFunc(found, func(x, y map[string]any) int {
		for _, by := range order {
			if len(by) != 2 {
				continue
			}
			result := cmp.Compare(x[by[0]].(float64), y[by[0]].(float64))
			if by[1] == "desc" {
				result = -result
			}
			if result != 0 {
				return result
			}
		}
		return 0
	})
	if limit < len(found) {
		found = found[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"offers": found, "truncated": false})
}

func (a *vastAPI) ask(id float64) map[string]any {
	for _, ask := range a.asks {
		if ask["id"] == id {
			return ask
		}
	}
	return nil
}

// vastCreateFields are what this stand-in knows of a create. Vast takes
// more.
var vastCreateFields = []string{"image", "vm", "disk", "label", "runtype", "target_state", "price", "cancel_unavail", "onstart"}

// wrong says why Vast would turn the create down, by the rules its
// documentation gives.
func (a *vastAPI) wrong(ask, body map[string]any) *vastReply {
	invalid := func(why string) *vastReply {
		return vastError(http.StatusBadRequest, "invalid_args", "error 400/3467: Invalid args: "+why)
	}
	for field := range body {
		if !slices.Contains(vastCreateFields, field) {
			return invalid("the stand-in does not know '" + field + "'")
		}
	}
	image, _ := body["image"].(string)
	if image == "" {
		return invalid("'image' is required")
	}
	if vm, _ := body["vm"].(bool); vm {
		switch {
		case !strings.HasPrefix(image, "docker.io/vastai/kvm:"):
			return invalid("a VM boots one of docker.io/vastai/kvm")
		case ask["vms_enabled"] != true:
			return invalid("the machine runs no VMs")
		case a.sshKeys == 0:
			return vastError(http.StatusBadRequest, "no_ssh_key_for_vm", "error 400/3524: no_ssh_key_for_vm You must add an SSH key to your account to create a VM.")
		case body["runtype"] != nil && body["runtype"] != "ssh":
			return invalid("a VM is started with 'ssh' only")
		}
	}
	if disk, ok := body["disk"].(float64); body["disk"] != nil && (!ok || disk <= 0 || disk > ask["disk_space"].(float64)) {
		return invalid("'disk' is more than the machine has")
	}
	if price, bids := body["price"]; bids {
		bid, ok := price.(float64)
		if !ok || bid < 0.001 || bid > 128 || bid < ask["min_bid"].(float64) {
			return vastError(http.StatusBadRequest, "invalid_price", fmt.Sprintf("error 400/3531: invalid_price The bid is below the minimum of %v.", ask["min_bid"]))
		}
	}
	if state := body["target_state"]; state != nil && state != "running" && state != "stopped" {
		return invalid("'target_state' is 'running' or 'stopped'")
	}
	if script, _ := body["onstart"].(string); len(script) > 4048 {
		return invalid("'onstart' is longer than 4048 characters")
	}
	if label, _ := body["label"].(string); len(label) > 1024 {
		return invalid("'label' is longer than 1024 characters")
	}
	return nil
}

func (a *vastAPI) create(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseFloat(r.PathValue("id"), 64)
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		a.answer(w, vastError(http.StatusBadRequest, "invalid_args", "error 400/3467: Invalid args: the body is not JSON"))
		return
	}
	asked := maps.Clone(body)
	asked["ask"] = id
	a.creates = append(a.creates, asked)

	if a.refusal != nil {
		a.answer(w, a.refusal)
		return
	}
	ask := a.ask(id)
	if ask == nil || ask["rentable"] != true {
		reply := vastError(http.StatusNotFound, "invalid_args", fmt.Sprintf("error 404/3603: no_such_ask Instance type by id %v is not available.", id))
		reply.body["ask_id"] = id
		a.answer(w, reply)
		return
	}
	if reply := a.wrong(ask, body); reply != nil {
		a.answer(w, reply)
		return
	}
	price, bids := body["price"].(float64)
	state := "running"
	if bids && a.outbidAt > price {
		// The machine is someone else's for now. A renter who would
		// rather not wait gets nothing.
		if body["cancel_unavail"] == true {
			a.answer(w, vastError(http.StatusGone, "no_such_ask", fmt.Sprintf("error 410/3907: no_such_ask Instance type %v is no longer available.", id)))
			return
		}
		state = "stopped"
	}

	a.nextID++
	if !bids {
		price = ask["min_bid"].(float64) * 2.5
	}
	instance := map[string]any{
		"id":              a.nextID,
		"actual_status":   nil,
		"intended_status": "running",
		"cur_state":       state,
		"next_state":      "running",
		"status_msg":      nil,
		"label":           body["label"],
		"is_bid":          bids,
		"min_bid":         ask["min_bid"],
		"dph_base":        price,
		"machine_id":      ask["machine_id"],
		"gpu_name":        ask["gpu_name"],
		"num_gpus":        ask["num_gpus"],
		"image_uuid":      body["image"],
		"image_runtype":   body["runtype"],
		"onstart":         body["onstart"],
		"disk_space":      body["disk"],
		"public_ipaddr":   "",
		"ssh_host":        "",
		"ssh_port":        nil,
		"ask":             id,
	}
	a.instances = append(a.instances, instance)
	ask["rentable"] = false
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "new_contract": a.nextID})
}

func (a *vastAPI) find(id string) int {
	number, _ := strconv.ParseInt(id, 10, 64)
	return slices.IndexFunc(a.instances, func(instance map[string]any) bool { return instance["id"] == number })
}

func (a *vastAPI) show(w http.ResponseWriter, r *http.Request) {
	i := a.find(r.PathValue("id"))
	switch {
	case i >= 0:
		writeJSON(w, http.StatusOK, map[string]any{"instances": a.instances[i]})
	case a.missingIs404:
		a.answer(w, vastError(http.StatusNotFound, "no_such_instance", "Instance not found"))
	default:
		writeJSON(w, http.StatusOK, map[string]any{"instances": nil})
	}
}

func (a *vastAPI) destroy(w http.ResponseWriter, r *http.Request) {
	if a.deleteAnswer != nil {
		a.answer(w, a.deleteAnswer)
		return
	}
	i := a.find(r.PathValue("id"))
	if i < 0 {
		a.answer(w, vastError(http.StatusNotFound, "not_found", "Instance not found"))
		return
	}
	// Its machine is for rent again.
	if ask := a.ask(a.instances[i]["ask"].(float64)); ask != nil {
		ask["rentable"] = true
	}
	a.instances = slices.Delete(a.instances, i, i+1)
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "msg": "Instance destroyed successfully"})
}

// list answers a page of at most 25 instances, and a token for the next.
func (a *vastAPI) list(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := 25
	if asked := query.Get("limit"); asked != "" {
		number, err := strconv.Atoi(asked)
		if err != nil || number > 25 {
			a.answer(w, vastError(http.StatusBadRequest, "invalid_args", "the stand-in gives at most 25 instances at a time"))
			return
		}
		limit = number
	}
	start := 0
	if token := query.Get("after_token"); token != "" {
		decoded, err := base64.StdEncoding.DecodeString(token)
		start, _ = strconv.Atoi(string(decoded))
		if err != nil || start <= 0 || start > len(a.instances) {
			a.answer(w, vastError(http.StatusBadRequest, "invalid_args", "Invalid after_token"))
			return
		}
	}
	end := min(start+limit, len(a.instances))
	body := map[string]any{
		"success":         true,
		"instances_found": end - start,
		"total_instances": len(a.instances),
		"next_token":      nil,
		"instances":       a.instances[start:end],
	}
	if end < len(a.instances) {
		body["next_token"] = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}
	writeJSON(w, http.StatusOK, body)
}

// set changes the stand-in under its lock.
func (a *vastAPI) set(change func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	change()
}

// change changes what Vast says of an instance.
func (a *vastAPI) change(id string, fields map[string]any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := a.find(id)
	require.GreaterOrEqual(a.t, i, 0, "no instance %s", id)
	maps.Copy(a.instances[i], fields)
}

// outbid makes Vast stop an instance for someone who pays more. How Vast
// then describes it is not documented; this is the stand-in's guess.
func (a *vastAPI) outbid(id string) {
	a.change(id, map[string]any{"actual_status": "exited", "cur_state": "stopped", "intended_status": "stopped", "status_msg": "outbid"})
}

func (a *vastAPI) lose(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if i := a.find(id); i >= 0 {
		a.instances = slices.Delete(a.instances, i, i+1)
	}
}

// plant adds an instance that was not rented through this stand-in, with
// the label, and returns its ID.
func (a *vastAPI) plant(label any) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nextID++
	a.instances = append(a.instances, map[string]any{
		"id": a.nextID, "actual_status": "running", "intended_status": "running", "cur_state": "running", "next_state": "running",
		"label": label, "is_bid": true, "ask": 0.0,
	})
	return strconv.FormatInt(a.nextID, 10)
}

// offer adds a made-up ask: the recorded one of the same machine and
// number of GPUs, with the fields changed.
func (a *vastAPI) offer(machine, gpus float64, fields map[string]any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nextID++
	made := map[string]any{}
	for _, ask := range a.asks {
		if ask["machine_id"] == 35231.0 && ask["num_gpus"] == 1.0 {
			made = maps.Clone(ask)
		}
	}
	made["id"], made["ask_contract_id"] = float64(a.nextID), float64(a.nextID)
	made["machine_id"], made["num_gpus"] = machine, gpus
	maps.Copy(made, fields)
	a.asks = append(a.asks, made)
}

func (a *vastAPI) lastSearch() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NotEmpty(a.t, a.searches)
	return a.searches[len(a.searches)-1]
}

func newVastSite(t *testing.T) site {
	api := newVastAPI(t)
	return site{
		provider: api.client(testOwner),
		// The words of actual_status. Vast has none for an instance that
		// is being destroyed: it is gone when the delete returns.
		statuses: map[string]Status{
			"created":    StatusPending,
			"loading":    StatusPending,
			"connecting": StatusPending,
			"rebooting":  StatusPending,
			"recycling":  StatusPending,
			"running":    StatusActive,
			"exited":     StatusInterrupted,
			"scheduling": StatusInterrupted,
			"offline":    StatusFailed,
			"unknown":    StatusFailed,
		},
		forgetsAtOnce: true,
		report:        func(id, status string) { api.change(id, map[string]any{"actual_status": status}) },
		refuse: func(reason error) {
			api.set(func() {
				switch reason {
				case nil:
					api.refusal = nil
				case ErrOutOfStock:
					api.refusal = vastError(http.StatusGone, "no_such_ask", "error 410/3907: no_such_ask Instance type 59566615 is no longer available.")
				case ErrQuota:
					// Made up: Vast does not document this refusal.
					api.refusal = vastError(http.StatusBadRequest, "insufficient_credit", "error 400/3477: Your account has insufficient credit to rent this instance.")
				}
			})
		},
		lose: api.lose,
		bought: func() (string, string) {
			create := api.lastCreate()
			name, _, _ := strings.Cut(create["label"].(string), " ")
			script, _ := create["onstart"].(string)
			return name, script
		},
		plantForeign: func() string { return api.plant("beam-other " + OwnerTag + "=" + foreignOwner) },
		boughtSpot: func() bool {
			create := api.lastCreate()
			return create["price"] != nil && create["vm"] == true
		},
		takeBack: api.outbid,
	}
}

// vastOffer is an offer of the recorded catalogue, with a 200 GB disk.
func vastOffer(machine int64, gpus uint32, name, gpu, region, place string, micros, millicores, memoryMB int64, reliability float64) Offer {
	return Offer{
		ID: fmt.Sprintf("%d/%d", machine, gpus), Provider: "vast", Cloud: "vast", InstanceType: fmt.Sprintf("%dx %s", gpus, name), Region: region,
		GPU: gpu, GPUCount: gpus, CPUMillicores: millicores, MemoryMB: memoryMB, StorageMB: 200 * 1024,
		HourlyCostMicros: micros, Spot: true, Available: 1, Reliability: reliability, RegionDisplayName: place,
	}
}

var (
	vast5090      = vastOffer(35231, 1, "RTX 5090", "RTX5090", "CZ", "Czechia, CZ", 455_556, 16_000, 48_345, 0.9523005)
	vast5090Twice = vastOffer(35231, 2, "RTX 5090", "RTX5090", "CZ", "Czechia, CZ", 855_556, 32_000, 96_691, 0.9523005)
)

func TestVastOffersWhatCanBeRentedNowAsAnInterruptibleVM(t *testing.T) {
	api := newVastAPI(t)
	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)

	// A country is a region, and a machine is offered once for each number
	// of GPUs it gives. Beam has no name for an RTX 3090, which keeps
	// Vast's. A share of a machine's CPUs need not be a whole number.
	require.Equal(t, []Offer{
		vastOffer(55395, 1, "RTX 3090", "RTX 3090", "CA", "Quebec, CA", 253_037, 12_000, 128_487, 0.9892778),
		vastOffer(148193, 2, "RTX 4090", "RTX4090", "CA", "Quebec, CA", 687_407, 36_000, 128_899, 0.9985652),
		vastOffer(151137, 1, "RTX 5090", "RTX5090", "CA", "Ontario, CA", 405_556, 64_000, 128_918, 0.9924823),
		vastOffer(57583, 1, "RTX 4090", "RTX4090", "CH", "Switzerland, CH", 322_222, 24_000, 64_130, 0.9922031),
		vast5090,
		vast5090Twice,
		vastOffer(35231, 4, "RTX 5090", "RTX5090", "CZ", "Czechia, CZ", 1_655_556, 64_000, 193_383, 0.9523005),
		vastOffer(155673, 1, "RTX PRO 6000 WS", "RTXPro6000", "CZ", "Czechia, CZ", 1_122_222, 27_428, 110_523, 0.9383145),
		vastOffer(55541, 1, "RTX 6000Ada", "RTX6000Ada", "FI", "Finland, FI", 692_593, 32_000, 64_466, 0.9845749),
		vastOffer(154848, 1, "RTX PRO 6000 WS", "RTXPro6000", "US", "New York, US", 1_388_889, 16_000, 95_658, 0.9929403),
		vastOffer(144714, 1, "RTX PRO 6000 WS", "RTXPro6000", "US", "Texas, US", 1_877_778, 14_000, 128_326, 0.9974215),
	}, offers)

	require.Equal(t, map[string]any{
		"type":              "bid",
		"verified":          map[string]any{"eq": true},
		"rentable":          map[string]any{"eq": true},
		"rented":            map[string]any{"eq": false},
		"vms_enabled":       map[string]any{"eq": true},
		"gpu_arch":          map[string]any{"eq": "nvidia"},
		"cpu_arch":          map[string]any{"eq": "amd64"},
		"disk_space":        map[string]any{"gte": 200.0},
		"allocated_storage": 200.0,
		"order":             []any{[]any{"dph_total", "asc"}},
		"limit":             1000.0,
	}, api.lastSearch())
	require.Equal(t, 1, api.sent("POST", "/api/v0/bundles/"))
}

// The stand-in works a price out the way Vast did when the catalogue was
// recorded, for the disk of 200 GB that was asked about then.
func TestTheVastStandInPricesAsVastDid(t *testing.T) {
	api := newVastAPI(t)
	recorded := map[float64]float64{}
	for _, ask := range api.asks {
		recorded[ask["id"].(float64)] = ask["dph_total"].(float64)
	}
	require.Len(t, recorded, 11)

	response, err := http.Post(api.server.URL+"/api/v0/bundles/", "application/json", strings.NewReader(`{"type": "bid", "allocated_storage": 200}`))
	require.NoError(t, err)
	defer response.Body.Close()
	var answered struct {
		Offers []map[string]any `json:"offers"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&answered))
	require.Len(t, answered.Offers, 11)
	for _, offer := range answered.Offers {
		require.InDelta(t, recorded[offer["id"].(float64)], offer["dph_total"].(float64), 1e-9, "ask %v", offer["id"])
	}
}

// Vast's usual instance is a container, which a worker cannot run in, and
// most of its machines run nothing else.
func TestVastLeavesOutWhatAWorkerCannotRunOn(t *testing.T) {
	for name, fields := range map[string]map[string]any{
		"a machine that runs containers only":        {"vms_enabled": false},
		"a host Vast has not verified":               {"verification": "unverified"},
		"a host Vast no longer vouches for":          {"verification": "deverified"},
		"a machine someone has":                      {"rentable": false},
		"a machine this account has GPUs of":         {"rented": true},
		"an Arm machine":                             {"cpu_arch": "arm64"},
		"a GPU the image has no driver for":          {"gpu_arch": "amd", "gpu_name": "RX 7900 XTX"},
		"a machine with less disk than a worker has": {"disk_space": 150.0},
		"a machine Vast cannot place":                {"geolocation": nil},
	} {
		t.Run(name, func(t *testing.T) {
			api := newVastAPI(t)
			api.offer(90001, 1, fields)
			offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
			require.NoError(t, err)
			require.Len(t, offers, 11)
			for _, offer := range offers {
				require.NotEqual(t, "90001/1", offer.ID)
			}
		})
	}

	// The made-up machine is offered when nothing is wrong with it.
	api := newVastAPI(t)
	api.offer(90001, 1, nil)
	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Len(t, offers, 12)
}

// Vast names a GPU as its driver does, which for two kinds leaves out how
// much memory it has.
func TestVastNamesAGPUAsBeamDoes(t *testing.T) {
	api := newVastAPI(t)
	want := map[string]string{}
	for i, gpu := range []struct {
		name     string
		memoryMB float64
		want     string
	}{
		{"A100 PCIE", 81920, "A100-80"},
		{"A100 SXM4", 40960, "A100-40"},
		{"A100X", 81920, "A100-80"},
		{"Tesla V100", 16384, "V100"},
		{"Tesla V100", 32768, "V100-32"},
		{"H100 SXM", 81559, "H100"},
		{"H200 NVL", 143771, "H200"},
		{"Tesla T4", 15360, "T4"},
		{"L40S", 46068, "L40S"},
		{"RTX A6000", 49140, "A6000"},
	} {
		machine := float64(90001 + i)
		api.offer(machine, 1, map[string]any{"gpu_name": gpu.name, "gpu_ram": gpu.memoryMB})
		want[fmt.Sprintf("%v/1", machine)] = gpu.want
	}
	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	got := map[string]string{}
	for _, offer := range offers {
		if _, made := want[offer.ID]; made {
			got[offer.ID] = offer.GPU
		}
	}
	require.Equal(t, want, got)
}

func TestVastUsesTheImageAndDiskItIsGiven(t *testing.T) {
	api := newVastAPI(t)
	client := api.client(testOwner, func(config *types.VastProviderConfig) {
		config.Image, config.DiskGB = "docker.io/vastai/kvm:ubuntu_desktop", 500
	})
	ctx := context.Background()

	// The price is for the bigger disk, and a machine with less is not
	// offered: two of the recorded ones have less than 500 GB.
	offers, err := client.Offers(ctx, OfferFilter{GPUs: []string{"RTX5090"}, Regions: []string{"CZ"}})
	require.NoError(t, err)
	require.Len(t, offers, 3)
	want := vast5090
	want.StorageMB, want.HourlyCostMicros = 500*1024, 538_889
	require.Equal(t, want, offers[0])
	search := api.lastSearch()
	require.Equal(t, map[string]any{"gte": 500.0}, search["disk_space"])
	require.Equal(t, 500.0, search["allocated_storage"])
	all, err := client.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.Len(t, all, 9)

	_, err = client.Create(ctx, CreateRequest{Offer: offers[0], Name: "beam-rtx5090-a1b2c3d4"})
	require.NoError(t, err)
	create := api.lastCreate()
	require.Equal(t, "docker.io/vastai/kvm:ubuntu_desktop", create["image"])
	require.Equal(t, 500.0, create["disk"])
	require.NotContains(t, create, "onstart", "no script, none sent")
}

func TestVastReadsAnAddressThatEndsInTheVersionOfTheAPI(t *testing.T) {
	api := newVastAPI(t)
	for _, written := range []string{api.server.URL + "/api/v0", api.server.URL + "/api/v0/"} {
		client := api.client(testOwner, func(config *types.VastProviderConfig) { config.BaseURL = written })

		offers, err := client.Offers(context.Background(), OfferFilter{})
		require.NoError(t, err, written)
		require.Len(t, offers, 11, written)
		instances, err := client.List(context.Background())
		require.NoError(t, err, "the list is of another version: %s", written)
		require.Empty(t, instances)
	}
}

func TestVastCreateSendsWhatVastNeeds(t *testing.T) {
	api := newVastAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()

	created, err := client.Create(ctx, CreateRequest{
		Offer:         vast5090Twice,
		Name:          "beam-rtx5090-a1b2c3d4",
		StartupScript: "curl -fsSL https://gateway.example/join | sh",
		Tags:          map[string]string{"beam-pool-machine": "machine-a1b2c3d4", "beam-pool": "rtx5090"},
	})
	require.NoError(t, err)
	require.Equal(t, Instance{ID: "31337001", Status: StatusPending, Tags: map[string]string{"beam-pool-machine": "machine-a1b2c3d4", "beam-pool": "rtx5090"}}, created)

	// The machine is asked about first: what Vast calls the ask, and what
	// its host takes now.
	search := api.lastSearch()
	require.Equal(t, map[string]any{"eq": 35231.0}, search["machine_id"])
	require.Equal(t, map[string]any{"eq": 2.0}, search["num_gpus"])
	require.Equal(t, 1.0, search["limit"])
	require.Equal(t, "bid", search["type"])
	require.Equal(t, map[string]any{"eq": true}, search["vms_enabled"])
	require.Equal(t, 200.0, search["allocated_storage"])

	require.Equal(t, map[string]any{
		"ask":            59566609.0,
		"image":          "docker.io/vastai/kvm:ubuntu_terminal",
		"vm":             true,
		"disk":           200.0,
		"label":          "beam-rtx5090-a1b2c3d4 beam-owner=beam-test-1a2b3c beam-pool=rtx5090 beam-pool-machine=machine-a1b2c3d4",
		"runtype":        "ssh",
		"target_state":   "running",
		"cancel_unavail": false,
		"price":          0.7999999999999999,
		"onstart":        "#!/usr/bin/env bash\nset -euo pipefail\ncurl -fsSL https://gateway.example/join | sh\n",
	}, api.lastCreate())
	require.Equal(t, 1, api.sent("PUT", "/api/v0/asks/59566609/"))

	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, created, found)
}

// The price named is the price paid, so it is the host's lowest of the
// moment. The buyer has counted on the offer's price, in its limits and in
// what it records the machine to cost, so a host that has raised its price
// since rents nothing.
func TestVastRentsAtTheHostsPriceOfTheMomentWhenItIsNoMoreThanTheOffers(t *testing.T) {
	api := newVastAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	offers, err := client.Offers(ctx, OfferFilter{GPUs: []string{"RTX5090"}, Regions: []string{"CZ"}})
	require.NoError(t, err)
	require.Equal(t, vast5090, offers[0])

	// The host lowers its price, and Vast gives the ask another number.
	api.set(func() {
		ask := api.ask(59566615)
		ask["id"], ask["min_bid"] = 59570001.0, 0.3
	})
	created, err := client.Create(ctx, CreateRequest{Offer: offers[0], Name: "beam-rtx5090-a1b2c3d4"})
	require.NoError(t, err)
	create := api.lastCreate()
	require.Equal(t, 0.3, create["price"])
	require.Equal(t, 59570001.0, create["ask"])
	require.NoError(t, client.Delete(ctx, created.ID))

	// The host raises its price over the offer's. However much the buyer
	// would pay for a machine, this one was not offered at that.
	api.set(func() { api.ask(59570001)["min_bid"] = 0.5 })
	for _, limit := range []int64{0, 555_556, 10_000_000} {
		_, err = client.Create(ctx, CreateRequest{Offer: offers[0], Name: "beam-rtx5090-a1b2c3d4", MaxHourlyCostMicros: limit})
		require.ErrorIs(t, err, ErrOutOfStock)
		require.ErrorContains(t, err, "Vast machine 35231 now costs 555556 micros an hour, and was offered at 455556")
	}
	require.Equal(t, 1, api.sent("PUT", "/api/v0/asks/"), "nothing more was bid")
}

func TestVastCreateSaysWhenTheMachineCannotBeHad(t *testing.T) {
	ctx := context.Background()
	request := CreateRequest{Offer: vast5090, Name: "beam-rtx5090-a1b2c3d4"}

	t.Run("someone has rented it since it was offered", func(t *testing.T) {
		api := newVastAPI(t)
		api.set(func() { api.ask(59566615)["rentable"] = false })
		_, err := api.client(testOwner).Create(ctx, request)
		require.ErrorIs(t, err, ErrOutOfStock)
		require.ErrorContains(t, err, "Vast machine 35231 has no 1 GPUs to rent as a VM now")
		require.Zero(t, api.sent("PUT", "/api/v0/asks/"))
	})

	t.Run("someone rents it between the question and the bid", func(t *testing.T) {
		api := newVastAPI(t)
		api.set(func() {
			api.refusal = vastError(http.StatusNotFound, "invalid_args", "error 404/3603: no_such_ask Instance type by id 59566615 is not available.")
		})
		_, err := api.client(testOwner).Create(ctx, request)
		require.ErrorIs(t, err, ErrOutOfStock)
		require.ErrorContains(t, err, "no_such_ask")
	})

	t.Run("its host asks for more between the question and the bid", func(t *testing.T) {
		api := newVastAPI(t)
		api.set(func() {
			api.refusal = vastError(http.StatusBadRequest, "invalid_price", "error 400/3531: invalid_price The bid is below the minimum of 0.5.")
		})
		_, err := api.client(testOwner).Create(ctx, request)
		require.ErrorIs(t, err, ErrOutOfStock)
	})

	// A queued VM bid is still an owned contract. Keep its ID so startup
	// can be observed and the contract deleted when its deadline expires.
	t.Run("a bid waits for its machine to become available", func(t *testing.T) {
		api := newVastAPI(t)
		api.set(func() { api.outbidAt = 0.45 })
		client := api.client(testOwner)
		created, err := client.Create(ctx, request)
		require.NoError(t, err)
		require.Equal(t, StatusPending, created.Status)
		listed, err := client.List(ctx)
		require.NoError(t, err)
		require.Len(t, listed, 1)
		require.Equal(t, created.ID, listed[0].ID)
		require.NoError(t, client.Delete(ctx, created.ID))
		listed, err = client.List(ctx)
		require.NoError(t, err)
		require.Empty(t, listed)
	})
}

func TestVastNamesWhyACreateWasTurnedDown(t *testing.T) {
	ctx := context.Background()
	request := CreateRequest{Offer: vast5090, Name: "beam-rtx5090-a1b2c3d4"}
	plain := func(t *testing.T, err error) {
		for _, kind := range []error{ErrOutOfStock, ErrQuota, ErrNotFound} {
			require.NotErrorIs(t, err, kind)
		}
	}

	t.Run("an account without an SSH key", func(t *testing.T) {
		api := newVastAPI(t)
		api.set(func() { api.sshKeys = 0 })
		_, err := api.client(testOwner).Create(ctx, request)
		require.ErrorContains(t, err, "Vast starts a VM only for an account with an SSH key; add one in Vast's console: PUT /api/v0/asks/59566615/: status 400")
		plain(t, err)
	})

	t.Run("an account without credit", func(t *testing.T) {
		api := newVastAPI(t)
		api.set(func() {
			api.refusal = vastError(http.StatusBadRequest, "insufficient_credit", "error 400/3477: Your account has insufficient credit to rent this instance.")
		})
		_, err := api.client(testOwner).Create(ctx, request)
		require.ErrorIs(t, err, ErrQuota)
		require.NotErrorIs(t, err, ErrOutOfStock)
	})

	// Not every 404 is a machine that is gone.
	t.Run("an address Vast does not have", func(t *testing.T) {
		api := newVastAPI(t)
		api.set(func() { api.refusal = vastError(http.StatusNotFound, "not_found", "Not Found") })
		_, err := api.client(testOwner).Create(ctx, request)
		require.ErrorContains(t, err, "status 404")
		plain(t, err)
	})

	t.Run("a key Vast does not know", func(t *testing.T) {
		api := newVastAPI(t)
		_, err := api.client(testOwner, func(config *types.VastProviderConfig) { config.ApiKey = "another" }).Create(ctx, request)
		require.ErrorContains(t, err, "status 401")
		plain(t, err)
	})

	t.Run("an answer that is no instance", func(t *testing.T) {
		api := newVastAPI(t)
		api.set(func() {
			api.refusal = &vastReply{http.StatusOK, map[string]any{"success": false, "msg": "the machine is in maintenance"}}
		})
		_, err := api.client(testOwner).Create(ctx, request)
		require.EqualError(t, err, "no answer from the provider: Vast rented no instance: the machine is in maintenance")
		require.True(t, Unanswered(err))

		api.set(func() { api.refusal = &vastReply{http.StatusOK, map[string]any{"success": true}} })
		_, err = api.client(testOwner).Create(ctx, request)
		require.EqualError(t, err, "no answer from the provider: Vast rented no instance: it gave no reason")
		require.True(t, Unanswered(err))
	})
}

func TestVastRefusesWhatItCannotSend(t *testing.T) {
	api := newVastAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()

	onDemand := vast5090
	onDemand.Spot = false
	_, err := client.Create(ctx, CreateRequest{Offer: onDemand, Name: "beam-rtx5090-a1b2c3d4"})
	require.ErrorContains(t, err, "this client rents interruptible instances from Vast")

	for _, id := range []string{"59566615", "35231", "35231/", "/1", "35231/0", "0/1", "machine/1", "35231/one", "-35231/1"} {
		offer := vast5090
		offer.ID = id
		_, err := client.Create(ctx, CreateRequest{Offer: offer, Name: "beam-rtx5090-a1b2c3d4"})
		require.ErrorContains(t, err, "a Vast offer needs a machine and a number of GPUs", id)
	}

	// Vast would cut a script short, or refuse it; neither starts a worker.
	fits := strings.Repeat("x", 4048-len(bootScript("")))
	_, err = client.Create(ctx, CreateRequest{Offer: vast5090, Name: "beam-rtx5090-a1b2c3d4", StartupScript: fits + "x"})
	require.EqualError(t, err, "Vast takes a startup script of 4048 characters, and this one has 4049")

	tags := map[string]string{}
	for i := range 13 {
		tags[fmt.Sprintf("tag-%02d", i)] = strings.Repeat("v", 63)
	}
	tags["pad"] = strings.Repeat("v", 48)
	_, err = client.Create(ctx, CreateRequest{Offer: vast5090, Name: "beam-rtx5090-a1b2c3d4", Tags: tags})
	require.EqualError(t, err, "Vast keeps 1024 characters with an instance, and its name and tags are 1025")

	require.Zero(t, api.sent("POST", "/api/v0/bundles/"), "none of them was asked about")
	require.Zero(t, api.sent("PUT", "/api/v0/asks/"))

	// What just fits is sent.
	tags["pad"] = strings.Repeat("v", 47)
	created, err := client.Create(ctx, CreateRequest{Offer: vast5090, Name: "beam-rtx5090-a1b2c3d4", StartupScript: fits, Tags: tags})
	require.NoError(t, err)
	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, tags, found.Tags)
}

// Nothing here stops an instance. Vast stops an interruptible one to give
// its machine to someone who pays more.
func TestVastReadsAStoppedInstanceAsTakenBack(t *testing.T) {
	api := newVastAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: vast5090, Name: "beam-rtx5090-a1b2c3d4"})
	require.NoError(t, err)

	running := map[string]any{"actual_status": "running", "intended_status": "running", "cur_state": "running", "status_msg": nil, "is_bid": true}
	for name, tc := range map[string]struct {
		fields map[string]any
		want   Instance
	}{
		"running": {
			fields: map[string]any{},
			want:   Instance{Status: StatusActive},
		},
		"it has stopped": {
			fields: map[string]any{"actual_status": "exited", "status_msg": "Exited (137)"},
			want:   Instance{Status: StatusInterrupted, StatusDetail: "exited Exited (137)"},
		},
		"Vast means it to stop, and it still runs": {
			fields: map[string]any{"intended_status": "stopped"},
			want:   Instance{Status: StatusInterrupted, StatusDetail: "running"},
		},
		"its rental is stopped, and it still runs": {
			fields: map[string]any{"cur_state": "stopped"},
			want:   Instance{Status: StatusInterrupted, StatusDetail: "running"},
		},
		"its new bid is stopped before Vast knows what it does": {
			fields: map[string]any{"cur_state": "stopped", "actual_status": nil},
			want:   Instance{Status: StatusPending},
		},
		"it waits for its machine to come free": {
			fields: map[string]any{"actual_status": "scheduling"},
			want:   Instance{Status: StatusInterrupted, StatusDetail: "scheduling"},
		},
		// An instance that was rented on demand is not taken back.
		"one rented on demand has stopped": {
			fields: map[string]any{"actual_status": "exited", "is_bid": false},
			want:   Instance{Status: StatusFailed, StatusDetail: "exited"},
		},
		"Vast has lost its machine": {
			fields: map[string]any{"actual_status": "offline", "status_msg": "machine offline"},
			want:   Instance{Status: StatusFailed, StatusDetail: "offline machine offline"},
		},
		"a status nobody documented": {
			fields: map[string]any{"actual_status": "hibernating", "status_msg": "zzz"},
			want:   Instance{Status: StatusUnknown, StatusDetail: "hibernating zzz"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			api.change(created.ID, running)
			api.change(created.ID, tc.fields)
			found, err := client.Get(ctx, created.ID)
			require.NoError(t, err)
			tc.want.ID = created.ID
			require.Equal(t, tc.want, found)
		})
	}
}

func TestVastReportsWhereAMachineCanBeReached(t *testing.T) {
	api := newVastAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: vast5090, Name: "beam-rtx5090-a1b2c3d4"})
	require.NoError(t, err)

	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusPending}, found, "a machine that is not up has no address")

	api.change(created.ID, map[string]any{"actual_status": "running", "public_ipaddr": "80.188.223.200\n", "ssh_host": "ssh7.vast.ai", "ssh_port": 31337})
	found, err = client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusActive, PublicIP: "80.188.223.200", SSHHost: "ssh7.vast.ai", SSHPort: 31337}, found)
}

// Vast is not known to say the same thing of every instance it does not
// have, and an ID that is not a number is not one of Vast's at all.
func TestVastFindsNoInstanceItDoesNotHave(t *testing.T) {
	ctx := context.Background()
	for name, missingIs404 := range map[string]bool{"answered with no instance": false, "answered with an error": true} {
		t.Run(name, func(t *testing.T) {
			api := newVastAPI(t)
			api.missingIs404 = missingIs404
			_, err := api.client(testOwner).Get(ctx, "424242")
			require.ErrorIs(t, err, ErrNotFound)
			require.Equal(t, 1, api.sent("GET", "/api/v0/instances/424242/"))
		})
	}

	api := newVastAPI(t)
	client := api.client(testOwner)
	for _, id := range []string{"", "0", "-5", "machine-1", "31337001/", "../instances", "1e3"} {
		_, err := client.Get(ctx, id)
		require.ErrorIs(t, err, ErrNotFound, id)
		require.NoError(t, client.Delete(ctx, id), id)
	}
	require.Empty(t, api.requests, "Vast is not asked about them")

	// Nor is an answer about nothing an instance.
	api.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"instances": map[string]any{}})
	})
	_, err := client.Get(ctx, "424242")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestVastListReadsEveryPageAndKeepsWhatThisControlPlaneRented(t *testing.T) {
	api := newVastAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()

	mine := []string{}
	for i := range 55 {
		mine = append(mine, api.plant(fmt.Sprintf("beam-machine-%d beam-machine=m%d %s=%s", i, i, OwnerTag, testOwner)))
		// Other people's, and other control planes', in between.
		api.plant([]any{nil, "My ML Training Job", "", "beam-other " + OwnerTag + "=" + foreignOwner, OwnerTag + "=", "name=value"}[i%6])
	}
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 55)
	ids := []string{}
	for _, instance := range listed {
		ids = append(ids, instance.ID)
		require.Equal(t, StatusActive, instance.Status)
	}
	require.Equal(t, mine, ids, "in the order of their IDs")
	require.Equal(t, map[string]string{"beam-machine": "m0"}, listed[0].Tags)

	// 110 instances, 25 at a time.
	require.Equal(t, 5, api.sent("GET", "/api/v1/instances/"))
}

func TestVastDeleteSucceedsOnWhatIsGoneAndFailsOnARefusal(t *testing.T) {
	api := newVastAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: vast5090, Name: "beam-rtx5090-a1b2c3d4"})
	require.NoError(t, err)

	api.set(func() {
		api.deleteAnswer = vastError(http.StatusForbidden, "forbidden", "This key may not destroy instances")
	})
	require.ErrorContains(t, client.Delete(ctx, created.ID), "This key may not destroy instances")

	// Vast answers some refusals as if they were not.
	api.set(func() {
		api.deleteAnswer = &vastReply{http.StatusOK, map[string]any{"success": false, "msg": "the instance is being copied"}}
	})
	require.EqualError(t, client.Delete(ctx, created.ID), "Vast did not destroy instance "+created.ID+": the instance is being copied")
	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err, "it is still there")
	require.Equal(t, StatusPending, found.Status)

	api.set(func() { api.deleteAnswer = nil })
	require.NoError(t, client.Delete(ctx, created.ID))
	_, err = client.Get(ctx, created.ID)
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, client.Delete(ctx, created.ID), "what is gone needs no deleting")
	require.Equal(t, 4, api.sent("DELETE", "/api/v0/instances/"+created.ID+"/"))

	// Its machine can be rented again.
	offers, err := client.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.Contains(t, offers, vast5090)

	// An answer that says nothing of success is one.
	api.set(func() { api.deleteAnswer = &vastReply{http.StatusOK, map[string]any{}} })
	require.NoError(t, client.Delete(ctx, "424242"))
}

// Vast's regions are countries, which it names after a place in them.
func TestVastReadsTheCountryOfAPlace(t *testing.T) {
	assert.Equal(t, "CA", vastCountry("Quebec, CA"))
	assert.Equal(t, "US", vastCountry("New York, US"))
	assert.Equal(t, "Singapore", vastCountry("Singapore"))
	assert.Equal(t, "", vastCountry(""))
	assert.Equal(t, "", vastCountry("Nowhere, "))
}

const (
	verdaTestClientID = "verda-test-client"
	verdaTestSecret   = "verda-test-secret"

	// The IDs the stand-in gives the scripts and instances it makes, which
	// it numbers together, from one.
	verdaScriptIDFormat   = "4f74ddc3-0293-4f7e-80e6-%012d"
	verdaInstanceIDFormat = "0a6842d1-bc59-4e8d-abd7-%012d"
)

// verdaAPI stands in for Verda. It answers with the shapes of Verda's
// published API description (https://api.verda.com/v1/openapi.json). The
// instance types and volume prices are cut from Verda's public catalogue as
// read on 2026-11-13; which of them are in stock is made up, because Verda
// tells that to its customers only.
type verdaAPI struct {
	apiStandIn

	nextID int
	// tokens counts the access tokens given out, and revoked those of them
	// that are no longer good.
	tokens    int
	revoked   int
	instances []map[string]any
	scripts   []map[string]any
	actions   []map[string]any
	refusal   *verdaReply

	// catalogue, when set, is listed in place of the recorded instance
	// types.
	catalogue string
	// hangUpOnCreate makes the next create go through and its answer get
	// lost.
	hangUpOnCreate bool
	// failsAfterCreate makes the next create go through and Verda answer
	// that it failed.
	failsAfterCreate bool
	// listsEverything makes the instance list ignore the tags asked for.
	listsEverything bool
}

type verdaReply struct {
	status  int
	code    string
	message string
}

// What the made-up stock holds. On demand: an L40S in two locations and a
// CPU machine. As spot: an L40S, an A100 and a V100. Verda also reports
// stock of two types this client must not offer, a GB300 that boots no
// image and a confidential-computing machine that boots special ones.
var verdaStockOnDemand = []verdaStock{
	{LocationCode: "FIN-01", Availabilities: []string{"1GB300.32V", "1L40S.20V", "CPU.4V.16G"}},
	{LocationCode: "FIN-02", Availabilities: []string{"1L40S.20V"}},
	{LocationCode: "FIN-03", Availabilities: []string{}},
}

var verdaStockSpot = []verdaStock{
	{LocationCode: "FIN-01", Availabilities: []string{"1L40S.20V", "1V100.4V", "1RTXPRO6000.30V.CC"}},
	{LocationCode: "FIN-02", Availabilities: []string{}},
	{LocationCode: "FIN-03", Availabilities: []string{"1A100.22V"}},
}

func newVerdaAPI(t *testing.T) *verdaAPI {
	api := &verdaAPI{apiStandIn: apiStandIn{t: t, now: time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /instance-types", func(w http.ResponseWriter, r *http.Request) {
		if api.catalogue != "" {
			w.Write([]byte(api.catalogue))
			return
		}
		w.Write(sample(t, "verda/instance_types"))
	})
	mux.HandleFunc("GET /volume-types", func(w http.ResponseWriter, r *http.Request) { w.Write(sample(t, "verda/volume_types")) })
	mux.HandleFunc("GET /instance-availability", api.availability)
	mux.HandleFunc("POST /scripts", api.addScript)
	mux.HandleFunc("GET /scripts", api.listScripts)
	mux.HandleFunc("DELETE /scripts/{id}", api.deleteScript)
	mux.HandleFunc("POST /instances", api.create)
	mux.HandleFunc("GET /instances", api.list)
	mux.HandleFunc("GET /instances/{id}", api.get)
	mux.HandleFunc("PUT /instances", api.act)
	api.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.mu.Lock()
		defer api.mu.Unlock()
		api.requests = append(api.requests, r.Method+" "+r.URL.RequestURI())
		if r.Method == http.MethodPost && r.URL.Path == "/oauth2/token" {
			api.token(w, r)
			return
		}
		var token int
		_, err := fmt.Sscanf(r.Header.Get("Authorization"), "Bearer verda-token-%d", &token)
		if err != nil || token <= api.revoked || token > api.tokens {
			api.fail(w, verdaReply{http.StatusUnauthorized, "unauthorized_request", "Access token is missing or invalid"})
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(api.server.Close)
	return api
}

// client returns a client of the stand-in's Verda, configured as a
// deployment is unless the test changes it.
func (a *verdaAPI) client(owner string, configure ...func(*types.VerdaProviderConfig)) *Verda {
	config := types.VerdaProviderConfig{
		ClientID:     verdaTestClientID,
		ClientSecret: verdaTestSecret,
		BaseURL:      a.server.URL,
		SSHKeyIDs:    []string{"1d57010e-7d75-488e-9e91-711565030d58"},
	}
	for _, change := range configure {
		change(&config)
	}
	client, err := NewVerda(config, owner)
	require.NoError(a.t, err)
	client.now = a.clock
	return client
}

func (a *verdaAPI) fail(w http.ResponseWriter, reply verdaReply) {
	writeJSON(w, reply.status, map[string]string{"code": reply.code, "message": reply.message})
}

func (a *verdaAPI) token(w http.ResponseWriter, r *http.Request) {
	var body map[string]string
	assert.NoError(a.t, json.NewDecoder(r.Body).Decode(&body))
	if body["grant_type"] != "client_credentials" || body["client_id"] != verdaTestClientID || body["client_secret"] != verdaTestSecret {
		a.fail(w, verdaReply{http.StatusUnauthorized, "unauthorized_request", "Invalid client id or client secret"})
		return
	}
	a.tokens++
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":  fmt.Sprintf("verda-token-%d", a.tokens),
		"token_type":    "Bearer",
		"expires_in":    3600,
		"refresh_token": "verda-refresh",
		"scope":         "cloud-api-v1",
	})
}

func (a *verdaAPI) availability(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Query().Get("is_spot") {
	case "true":
		writeJSON(w, http.StatusOK, verdaStockSpot)
	case "false":
		writeJSON(w, http.StatusOK, verdaStockOnDemand)
	default:
		a.fail(w, verdaReply{http.StatusBadRequest, "invalid_request", "is_spot must be true or false"})
	}
}

func (a *verdaAPI) addScript(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	assert.NoError(a.t, json.NewDecoder(r.Body).Decode(&body))
	a.nextID++
	body["id"] = fmt.Sprintf(verdaScriptIDFormat, a.nextID)
	body["created_at"] = a.now.Format(time.RFC3339Nano)
	a.scripts = append(a.scripts, body)
	writeJSON(w, http.StatusCreated, body["id"])
}

// listScripts answers as Verda does: newest first, a page at a time, and
// narrowed to the names that contain the filter.
func (a *verdaAPI) listScripts(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	size, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if page < 1 || size < 1 || size > 100 {
		a.fail(w, verdaReply{http.StatusBadRequest, "invalid_request", "page starts from 1 and pageSize is at most 100"})
		return
	}
	var matching []map[string]any
	for _, script := range slices.Backward(a.scripts) {
		if strings.Contains(script["name"].(string), r.URL.Query().Get("name")) {
			matching = append(matching, script)
		}
	}
	matching = matching[min((page-1)*size, len(matching)):]
	writeJSON(w, http.StatusOK, append([]map[string]any{}, matching[:min(size, len(matching))]...))
}

func (a *verdaAPI) deleteScript(w http.ResponseWriter, r *http.Request) {
	i := slices.IndexFunc(a.scripts, func(script map[string]any) bool { return script["id"] == r.PathValue("id") })
	if i < 0 {
		a.fail(w, verdaReply{http.StatusNotFound, "not_found", "Script not found"})
		return
	}
	a.scripts = slices.Delete(a.scripts, i, i+1)
}

func (a *verdaAPI) create(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	assert.NoError(a.t, json.NewDecoder(r.Body).Decode(&body))
	if a.refusal != nil {
		a.fail(w, *a.refusal)
		return
	}
	a.creates = append(a.creates, body)

	a.nextID++
	instance := map[string]any{}
	assert.NoError(a.t, json.Unmarshal(sample(a.t, "verda/instance"), &instance))
	instance["id"] = fmt.Sprintf(verdaInstanceIDFormat, a.nextID)
	instance["status"] = "ordered"
	instance["ip"] = nil
	instance["os_volume_id"] = fmt.Sprintf("95cec4c3-af69-42eb-a790-%012d", a.nextID)
	instance["volume_ids"] = []any{instance["os_volume_id"]}
	instance["location"] = body["location_code"]
	instance["startup_script_id"] = body["startup_script_id"]
	for _, field := range []string{"hostname", "description", "instance_type", "image", "is_spot", "ssh_key_ids"} {
		instance[field] = body[field]
	}
	// Verda gives each tag an id of its own.
	tags := []any{}
	for i, tag := range body["tags"].([]any) {
		sent := tag.(map[string]any)
		tags = append(tags, map[string]any{"id": fmt.Sprintf("tag_%d_%d", a.nextID, i), "key": sent["key"], "value": sent["value"]})
	}
	instance["tags"] = tags
	a.instances = append(a.instances, instance)
	if a.hangUpOnCreate {
		a.hangUpOnCreate = false
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			conn.Close()
		}
		return
	}
	if a.failsAfterCreate {
		a.failsAfterCreate = false
		a.fail(w, verdaReply{http.StatusInternalServerError, "internal_server_error", "Something went wrong"})
		return
	}
	writeJSON(w, http.StatusAccepted, instance["id"])
}

// list narrows the instances to those that carry every tag asked for, each
// written key=value.
func (a *verdaAPI) list(w http.ResponseWriter, r *http.Request) {
	listed := []map[string]any{}
	for _, instance := range a.instances {
		carries := func(want string) bool {
			return slices.ContainsFunc(instance["tags"].([]any), func(tag any) bool {
				stored := tag.(map[string]any)
				return fmt.Sprintf("%s=%s", stored["key"], stored["value"]) == want
			})
		}
		if a.listsEverything || !slices.ContainsFunc(r.URL.Query()["tag"], func(want string) bool { return !carries(want) }) {
			listed = append(listed, instance)
		}
	}
	writeJSON(w, http.StatusOK, listed)
}

func (a *verdaAPI) find(id string) int {
	return slices.IndexFunc(a.instances, func(instance map[string]any) bool { return instance["id"] == id })
}

func (a *verdaAPI) get(w http.ResponseWriter, r *http.Request) {
	i := a.find(r.PathValue("id"))
	if i < 0 {
		a.fail(w, verdaReply{http.StatusNotFound, "not_found", "Instance not found"})
		return
	}
	writeJSON(w, http.StatusOK, a.instances[i])
}

func (a *verdaAPI) act(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	assert.NoError(a.t, json.NewDecoder(r.Body).Decode(&body))
	id, _ := body["id"].(string)
	i := a.find(id)
	if i < 0 {
		a.fail(w, verdaReply{http.StatusNotFound, "not_found", "Instance not found"})
		return
	}
	if body["action"] != "delete" {
		a.fail(w, verdaReply{http.StatusBadRequest, "invalid_request", "the stand-in only deletes"})
		return
	}
	a.actions = append(a.actions, body)
	a.instances[i]["status"] = "deleting"
	writeJSON(w, http.StatusAccepted, []map[string]any{{"instanceId": id, "action": "delete", "status": "success"}})
}

func (a *verdaAPI) set(id, field string, value any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := a.find(id)
	require.GreaterOrEqual(a.t, i, 0, "no instance %s", id)
	a.instances[i][field] = value
}

func (a *verdaAPI) refuse(with *verdaReply) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.refusal = with
}

func (a *verdaAPI) lose(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if i := a.find(id); i >= 0 {
		a.instances = slices.Delete(a.instances, i, i+1)
	}
}

func (a *verdaAPI) lastAction() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NotEmpty(a.t, a.actions)
	return a.actions[len(a.actions)-1]
}

// storedScripts returns the stored scripts by name.
func (a *verdaAPI) storedScripts() map[string]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	byName := map[string]string{}
	for _, script := range a.scripts {
		byName[script["name"].(string)] = script["script"].(string)
	}
	return byName
}

// scriptOf returns the script an instance was created to run.
func (a *verdaAPI) scriptOf(create map[string]any) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, script := range a.scripts {
		if script["id"] == create["startup_script_id"] {
			return script["script"].(string)
		}
	}
	return ""
}

// Verda documents the first two. It documents no answer for an account at
// its quota, so the third is this client's guess; a live smoke test has to
// confirm it.
var verdaRefusals = map[error]*verdaReply{
	ErrOutOfStock: {http.StatusServiceUnavailable, "service_unavailable", "No capacity available"},
	ErrQuota:      {http.StatusPaymentRequired, "insufficient_funds", "Insufficient funds"},
}

var verdaQuotaGuess = &verdaReply{http.StatusForbidden, "forbidden_action", "Spot GPU quota exceeded"}

func newVerdaSite(t *testing.T) site {
	api := newVerdaAPI(t)
	return site{
		provider: api.client(testOwner),
		statuses: map[string]Status{
			"new":                 StatusPending,
			"ordered":             StatusPending,
			"validating":          StatusPending,
			"provisioning":        StatusPending,
			"running":             StatusActive,
			"offline":             StatusFailed,
			"error":               StatusFailed,
			"no_capacity":         StatusFailed,
			"installation_failed": StatusFailed,
			"deleting":            StatusDeleted,
			// The site's instance is bought on demand.
			"discontinued": StatusDeleted,
			"notfound":     StatusDeleted,
		},
		report: func(id, status string) { api.set(id, "status", status) },
		refuse: func(reason error) { api.refuse(verdaRefusals[reason]) },
		lose:   api.lose,
		bought: func() (string, string) {
			create := api.lastCreate()
			return create["hostname"].(string), api.scriptOf(create)
		},
		plantForeign: func() string {
			offers, err := api.client(foreignOwner).Offers(context.Background(), OfferFilter{})
			require.NoError(t, err)
			instance, err := api.client(foreignOwner).Create(context.Background(), CreateRequest{Offer: offers[0], Name: "someone-elses"})
			require.NoError(t, err)
			return instance.ID
		},
		boughtSpot: func() bool { return api.lastCreate()["is_spot"] == true },
		takeBack:   func(id string) { api.set(id, "status", "discontinued") },
	}
}

// What a machine's 200 GB disk costs an hour: Verda sells it apart, at $0.20
// a gigabyte a month.
const verdaTestDisk = 54_795

// verdaOffer is an offer at a price that includes verdaTestDisk.
func verdaOffer(instanceType, region, gpu string, cores, memoryGB, micros int64, spot bool) Offer {
	offer := Offer{
		ID: instanceType, Provider: "verda", Cloud: "verda", InstanceType: instanceType, Region: region,
		GPU: gpu, CPUMillicores: cores * 1000, MemoryMB: memoryGB * 1024, StorageMB: 200 * 1024,
		HourlyCostMicros: micros, DiskHourlyMicros: verdaTestDisk, Spot: spot,
	}
	if gpu != "" {
		offer.GPUCount = 1
	}
	return offer
}

func TestVerdaOffersWhatEachLocationHasOnDemandAndAsSpot(t *testing.T) {
	api := newVerdaAPI(t)
	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)

	// Every price is the machine's plus its disk's. The spot price is half
	// the on-demand price.
	const disk = verdaTestDisk
	require.Equal(t, []Offer{
		verdaOffer("1L40S.20V", "FIN-01", "L40S", 20, 60, 1_998_000+disk, false),
		verdaOffer("1L40S.20V", "FIN-02", "L40S", 20, 60, 1_998_000+disk, false),
		verdaOffer("1L40S.20V", "FIN-01", "L40S", 20, 60, 999_000+disk, true),
		verdaOffer("1A100.22V", "FIN-03", "A100-80", 22, 120, 1_187_000+disk, true),
		verdaOffer("1V100.4V", "FIN-01", "V100", 4, 18, 122_000+disk, true),
		verdaOffer("CPU.4V.16G", "FIN-01", "", 4, 16, 48_000+disk, false),
	}, offers)
}

func TestVerdaPricesAMachineByItsLocation(t *testing.T) {
	var instanceType verdaInstanceType
	require.NoError(t, json.Unmarshal([]byte(`{
		"instance_type": "1H100.80S.30V", "price_per_hour": "5.189", "spot_price": "2.595",
		"location_prices": [{"location_code": "FIN-02", "price_per_hour": "4.90", "spot_price": "1.25"}]
	}`), &instanceType))

	require.Equal(t, int64(4_900_000), instanceType.price("FIN-02", false))
	require.Equal(t, int64(1_250_000), instanceType.price("FIN-02", true))
	// A location without a price of its own costs the default.
	require.Equal(t, int64(5_189_000), instanceType.price("ICE-01", false))
	require.Equal(t, int64(2_595_000), instanceType.price("ICE-01", true))
}

// The spend caps count on every machine having a price.
func TestVerdaDoesNotOfferWhatItCannotPrice(t *testing.T) {
	api := newVerdaAPI(t)
	api.catalogue = `[{
		"instance_type": "1L40S.20V", "name": "L40S 48GB",
		"cpu": {"number_of_cores": 20}, "gpu": {"number_of_gpus": 1}, "memory": {"size_in_gigabytes": 60},
		"price_per_hour": "1.998", "spot_price": "",
		"location_prices": [{"location_code": "FIN-02", "price_per_hour": "0", "spot_price": "0"}],
		"supported_os": ["ubuntu-24.04-cuda-12.8-open-docker"]
	}]`

	offers, err := api.client(testOwner).Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	// It is in stock on demand in FIN-01 and FIN-02 and as spot in FIN-01.
	require.Equal(t, []Offer{verdaOffer("1L40S.20V", "FIN-01", "L40S", 20, 60, 1_998_000+54_795, false)}, offers)
}

func TestVerdaReadsTheCatalogueRarelyAndTheStockEveryTime(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	for range 3 {
		_, err := client.Offers(context.Background(), OfferFilter{})
		require.NoError(t, err)
	}
	require.Equal(t, 1, api.sent("POST", "/oauth2/token"), "one login serves every call")
	require.Equal(t, 1, api.sent("GET", "/instance-types"))
	require.Equal(t, 1, api.sent("GET", "/volume-types"))
	require.Equal(t, 6, api.sent("GET", "/instance-availability"), "stock is asked on demand and as spot, each time")

	api.after(verdaCatalogueFor)
	_, err := client.Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, api.sent("GET", "/instance-types"))
}

// A token can stop being good before its hour is up. The call that finds out
// is sent again with a new one.
func TestVerdaLogsInAgainWhenItsTokenIsTurnedDown(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	_, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-a1b2c3d4"})
	require.NoError(t, err)

	api.mu.Lock()
	api.revoked = api.tokens
	api.mu.Unlock()

	// A purchase is not sent twice as a rule. This one was turned away at
	// the door, so sending it again buys one machine.
	_, err = client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-e5f6a7b8"})
	require.NoError(t, err)
	require.Equal(t, 2, api.sent("POST", "/oauth2/token"))
	require.Len(t, api.creates, 2)
}

func TestVerdaWithWrongCredentialsSaysSo(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner, func(config *types.VerdaProviderConfig) { config.ClientSecret = "wrong" })

	_, err := client.Offers(context.Background(), OfferFilter{})
	require.ErrorContains(t, err, "get an access token")
	require.ErrorContains(t, err, "Invalid client id or client secret")
	require.Zero(t, api.sent("GET", "/"), "nothing is asked without a token")
}

func TestVerdaCreateSendsWhatVerdaNeeds(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)

	_, err := client.Create(context.Background(), CreateRequest{
		Offer:         Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true},
		Name:          "beam-l40s-a1b2c3d4",
		StartupScript: "curl -fsSL https://gateway.example/join | sh",
		Tags:          map[string]string{"beam-machine": "a1b2c3d4"},
	})
	require.NoError(t, err)

	create := api.lastCreate()
	scriptID := create["startup_script_id"]
	require.NotEmpty(t, scriptID)
	require.Equal(t, map[string]any{
		"instance_type": "1L40S.20V",
		"image":         "ubuntu-24.04-cuda-12.8-open-docker",
		"hostname":      "beam-l40s-a1b2c3d4",
		"description":   "beam-l40s-a1b2c3d4",
		"location_code": "FIN-01",
		"is_spot":       true,
		// Verda keeps the disk of a spot machine it takes back, and goes on
		// charging for it, unless told otherwise.
		"os_volume":         map[string]any{"name": "beam-l40s-a1b2c3d4", "size": float64(200), "on_spot_discontinue": "delete_permanently"},
		"ssh_key_ids":       []any{"1d57010e-7d75-488e-9e91-711565030d58"},
		"startup_script_id": scriptID,
		"tags": []any{
			map[string]any{"key": "beam-machine", "value": "a1b2c3d4"},
			map[string]any{"key": OwnerTag, "value": testOwner},
		},
	}, create)
	require.Equal(t, map[string]string{
		"beam-owner=" + testOwner + " beam-l40s-a1b2c3d4": "#!/usr/bin/env bash\nset -euo pipefail\ncurl -fsSL https://gateway.example/join | sh\n",
	}, api.storedScripts())
}

// An offer is priced with the disk its provider is configured with. With
// another, it costs what the provider charges for that one instead.
func TestAnOfferWithAnotherDiskCostsTheDifference(t *testing.T) {
	offer := awsOffer("us-east-1a", "g5.xlarge", "A10G", 1, 4, 16, 350_000)

	// 1000 GB cost five times what 200 do.
	bigger := offer.WithDisk(1000)
	require.Equal(t, int64(1000*1024), bigger.StorageMB)
	require.Equal(t, int64(5*awsTestDisk), bigger.DiskHourlyMicros)
	require.Equal(t, int64(350_000+5*awsTestDisk), bigger.HourlyCostMicros)

	require.Equal(t, offer, bigger.WithDisk(200))
	require.Equal(t, offer, offer.WithDisk(0), "no disk asked for")

	// A machine that comes with its disk is sold as it is.
	whole := Offer{ID: "H100", StorageMB: 1000 * 1024, HourlyCostMicros: 3_300_000}
	require.Equal(t, whole, whole.WithDisk(2000))
}

// A purchase that asks for a disk gets that one instead of the provider's,
// where the disk is sold apart from the machine. A price limit is the
// machine's, so the bigger disk leaves less of it for what AWS and Nebius
// are given a limit for.
func TestAPurchaseBuysTheDiskItAsksFor(t *testing.T) {
	ctx := context.Background()

	verda := newVerdaAPI(t)
	_, err := verda.client(testOwner).Create(ctx, CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-a1b2c3d4", DiskGB: 1000})
	require.NoError(t, err)
	require.Equal(t, float64(1000), verda.lastCreate()["os_volume"].(map[string]any)["size"])

	gcp := newGCPAPI(t)
	_, err = gcp.client(testOwner).Create(ctx, CreateRequest{Offer: gcpL4InA, Name: "beam-l4-a1b2c3d4", DiskGB: 1000})
	require.NoError(t, err)
	require.Equal(t, "1000", gcp.lastCreate()["disks"].([]any)[0].(map[string]any)["initializeParams"].(map[string]any)["diskSizeGb"])

	// 1000 GB cost AWS $0.109589 an hour, of a limit of $0.50.
	cloud := newAWSCloud(t)
	_, err = cloud.client(testOwner).Create(ctx, CreateRequest{Offer: awsA10GInA, Name: "beam-a10g-a1b2c3d4", DiskGB: 1000, MaxHourlyCostMicros: 500_000})
	require.NoError(t, err)
	run := cloud.lastRun("us-east-1")
	require.Equal(t, int32(1000), aws.ToInt32(run.BlockDeviceMappings[0].Ebs.VolumeSize))
	require.Equal(t, "0.3904", aws.ToString(run.InstanceMarketOptions.SpotOptions.MaxPrice))

	// And Nebius $0.09726, of a limit of $4.
	nebius := newNebiusAPI(t)
	_, err = nebius.client(testOwner).Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-h200-a1b2c3d4", DiskGB: 1000, MaxHourlyCostMicros: 4_000_000})
	require.NoError(t, err)
	disk := nebius.lastCreate()["spec"].(map[string]any)["bootDisk"].(map[string]any)["managedDisk"].(map[string]any)["spec"].(map[string]any)
	require.Equal(t, "1000", disk["sizeGibibytes"])
	require.Equal(t, []string{nebiusNorth + " beamgpuh200sxmmax3902 3.902"}, nebius.limits())
}

func TestVerdaBuysOnDemandWithoutTheSpotSettings(t *testing.T) {
	api := newVerdaAPI(t)
	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-a1b2c3d4"})
	require.NoError(t, err)

	create := api.lastCreate()
	require.Equal(t, false, create["is_spot"])
	require.Equal(t, map[string]any{"name": "beam-l40s-a1b2c3d4", "size": float64(200)}, create["os_volume"])
	require.NotContains(t, create, "startup_script_id", "no script was asked for")
	require.Empty(t, api.storedScripts())
}

// Verda's older GPUs run its older driver only, and so boot another image.
func TestVerdaBootsTheFirstImageTheInstanceTypeSupports(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)

	_, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "1V100.4V", Region: "FIN-01", Spot: true}, Name: "beam-v100-a1b2c3d4"})
	require.NoError(t, err)
	require.Equal(t, "ubuntu-24.04-cuda-12.6-docker", api.lastCreate()["image"])

	// A type that boots none of the images is not offered, and not bought.
	for _, unbootable := range []string{"1GB300.32V", "1RTXPRO6000.30V.CC", "no-such-type"} {
		_, err = client.Create(context.Background(), CreateRequest{Offer: Offer{ID: unbootable, Region: "FIN-01"}, Name: "beam-a1b2c3d4"})
		require.Error(t, err, unbootable)
	}
	require.Len(t, api.creates, 1)
}

// A create that was turned down has stored a script that no machine runs.
// It goes the one way a script goes: List removes it once it is old.
func TestVerdaARefusedCreatesScriptGoesWithALaterList(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	req := CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true}, Name: "beam-l40s-a1b2c3d4", StartupScript: "join"}

	for reason, reply := range map[error]*verdaReply{ErrOutOfStock: verdaRefusals[ErrOutOfStock], ErrQuota: verdaQuotaGuess} {
		api.refuse(reply)
		_, err := client.Create(context.Background(), req)
		require.ErrorIs(t, err, reason)
	}

	// A create turned down for another reason is a plain error.
	api.refuse(&verdaReply{http.StatusBadRequest, "invalid_request", "hostname is already in use"})
	_, err := client.Create(context.Background(), req)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrOutOfStock)
	require.NotErrorIs(t, err, ErrQuota)

	require.Equal(t, 3, api.sent("POST", "/scripts"))
	require.Zero(t, api.sent("DELETE", "/scripts/"))
	api.after(verdaScriptGrace)
	_, err = client.List(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, api.sent("DELETE", "/scripts/"))
	require.Empty(t, api.storedScripts())
}

// The machine may exist, and then it needs its script. The caller finds it
// by its tags.
func TestVerdaACreateThatGotNoAnswerKeepsItsScript(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	api.mu.Lock()
	api.hangUpOnCreate = true
	api.mu.Unlock()

	_, err := client.Create(context.Background(), CreateRequest{
		Offer: Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true}, Name: "beam-l40s-a1b2c3d4", StartupScript: "join",
		Tags: map[string]string{"beam-machine": "a1b2c3d4"},
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrOutOfStock)
	require.Equal(t, 1, api.sent("POST", "/instances"), "a purchase is not sent twice")
	require.Len(t, api.storedScripts(), 1)

	listed, err := client.List(context.Background())
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, map[string]string{"beam-machine": "a1b2c3d4"}, listed[0].Tags)
}

func TestVerdaListReturnsOnlyItsOwnEvenIfVerdaReturnsMore(t *testing.T) {
	api := newVerdaAPI(t)
	ctx := context.Background()
	mine, err := api.client(testOwner).Create(ctx, CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-a1b2c3d4"})
	require.NoError(t, err)
	_, err = api.client(foreignOwner).Create(ctx, CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "someone-elses"})
	require.NoError(t, err)

	// What is listed here may be deleted as an orphan, so the filter is
	// not left to the provider alone.
	api.mu.Lock()
	api.listsEverything = true
	api.mu.Unlock()
	listed, err := api.client(testOwner).List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, mine.ID, listed[0].ID)
}

func TestVerdaDeleteRemovesTheDisk(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	created, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-a1b2c3d4", StartupScript: "join"})
	require.NoError(t, err)
	require.Len(t, api.storedScripts(), 1)

	require.NoError(t, client.Delete(context.Background(), created.ID))
	// Left to Verda, the disk would go to its trash for four days.
	require.Equal(t, map[string]any{
		"action":             "delete",
		"id":                 created.ID,
		"volume_ids":         []any{"95cec4c3-af69-42eb-a790-000000000002"},
		"delete_permanently": true,
	}, api.lastAction())

	// Verda is deleting it by now, and is not asked twice.
	require.NoError(t, client.Delete(context.Background(), created.ID))
	require.Len(t, api.actions, 1)

	// Its script is left for List, which removes it once the machine is
	// gone.
	require.Len(t, api.storedScripts(), 1)
	api.lose(created.ID)
	api.after(verdaScriptGrace)
	_, err = client.List(context.Background())
	require.NoError(t, err)
	require.Empty(t, api.storedScripts())
}

// Verda takes a spot machine back without notice, and what it leaves is the
// machine's startup script. With the machine gone nothing names the script,
// so listing looks for scripts that no instance runs.
func TestVerdaListRemovesTheScriptsOfMachinesThatAreGone(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	spot := Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true}

	taken, err := client.Create(ctx, CreateRequest{Offer: spot, Name: "beam-l40s-taken", StartupScript: "join taken"})
	require.NoError(t, err)
	_, err = client.Create(ctx, CreateRequest{Offer: spot, Name: "beam-l40s-running", StartupScript: "join running"})
	require.NoError(t, err)
	_, err = api.client(foreignOwner).Create(ctx, CreateRequest{Offer: spot, Name: "beam-l40s-foreign", StartupScript: "join foreign"})
	require.NoError(t, err)
	// The account has scripts of its own. Verda's name filter matches any
	// part of a name, so one of them comes back with this client's.
	api.mu.Lock()
	for i, name := range []string{"install monitoring", "notes on " + OwnerTag + "=" + testOwner + " machines"} {
		api.scripts = append(api.scripts, map[string]any{"id": fmt.Sprintf("the-accounts-own-%d", i), "name": name, "script": "#!/bin/bash", "created_at": api.now.Format(time.RFC3339Nano)})
	}
	api.mu.Unlock()

	scripts := func() []string {
		names := []string{}
		for name := range api.storedScripts() {
			names = append(names, name[strings.LastIndex(name, " ")+1:])
		}
		slices.Sort(names)
		return names
	}
	everything := []string{"beam-l40s-foreign", "beam-l40s-running", "beam-l40s-taken", "machines", "monitoring"}

	// Verda takes one back and, as it may, forgets it.
	api.lose(taken.ID)
	api.after(verdaScriptGrace - time.Second)
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, everything, scripts(), "a script this young may belong to a create that is under way")

	api.after(time.Second)
	_, err = client.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"beam-l40s-foreign", "beam-l40s-running", "machines", "monitoring"}, scripts())
}

func TestVerdaDeleteOfAMachineVerdaTookBackAsksNothingOfVerda(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	created, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true}, Name: "beam-l40s-a1b2c3d4", StartupScript: "join"})
	require.NoError(t, err)

	api.set(created.ID, "status", "discontinued")
	require.NoError(t, client.Delete(context.Background(), created.ID))
	require.Empty(t, api.actions, "there is no instance left to act on")
	require.Zero(t, api.sent("DELETE", "/scripts/"), "its script goes with a later List")
}

func TestVerdaListLeavesOutAnInstanceThatIsNoMore(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true}, Name: "beam-l40s-a1b2c3d4"})
	require.NoError(t, err)

	// Verda may keep showing an instance it took back. Nothing is left of
	// it to delete or to pay for.
	api.set(created.ID, "status", "discontinued")
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Empty(t, listed)

	found, err := client.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, StatusInterrupted, found.Status)
	require.Equal(t, "discontinued", found.StatusDetail)
}

func TestVerdaReportsWhereAMachineCanBeReached(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	created, err := client.Create(context.Background(), CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-a1b2c3d4"})
	require.NoError(t, err)

	found, err := client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Empty(t, found.PublicIP, "a machine that is not up has no address")

	api.set(created.ID, "status", "running")
	api.set(created.ID, "ip", "1.2.3.4")
	found, err = client.Get(context.Background(), created.ID)
	require.NoError(t, err)
	require.Equal(t, Instance{ID: created.ID, Status: StatusActive, PublicIP: "1.2.3.4", SSHHost: "1.2.3.4", SSHPort: 22}, found)
}

func TestVerdaUsesTheImagesKeysAndDiskItIsGiven(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner, func(config *types.VerdaProviderConfig) {
		config.Images = []string{"ubuntu-24.04"}
		config.SSHKeyIDs = []string{"ops"}
		config.DiskGB = 500
	})
	ctx := context.Background()

	// Verda sells the disk apart, at $0.20 a gigabyte a month: $0.136986
	// an hour for 500 GB.
	offers, err := client.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.Equal(t, int64(500*1024), offers[0].StorageMB)
	require.Equal(t, int64(1_998_000+136_986), offers[0].HourlyCostMicros)

	_, err = client.Create(ctx, CreateRequest{Offer: offers[0], Name: "beam-l40s-a1b2c3d4"})
	require.NoError(t, err)
	create := api.lastCreate()
	require.Equal(t, "ubuntu-24.04", create["image"])
	require.Equal(t, []any{"ops"}, create["ssh_key_ids"])
	require.Equal(t, float64(500), create["os_volume"].(map[string]any)["size"])
}

// A machine that Verda does not delete goes on costing money. However Verda
// says no, Delete fails, so that its caller keeps the machine's record and
// asks again.
func TestVerdaDeleteFailsWhenVerdaDoesNotDelete(t *testing.T) {
	for name, tc := range map[string]struct {
		answer func(w http.ResponseWriter, id string)
		want   string
	}{
		"the call is turned down": {
			answer: func(w http.ResponseWriter, _ string) {
				writeJSON(w, http.StatusForbidden, map[string]string{"code": "forbidden_action", "message": "The instance is locked"})
			},
			want: "PUT /instances: status 403",
		},
		// Verda answers a delete with what became of each instance.
		"the call is taken and the instance is not deleted": {
			answer: func(w http.ResponseWriter, id string) {
				writeJSON(w, http.StatusAccepted, []map[string]any{{"instanceId": id, "action": "delete", "status": "error", "error": "The instance is locked", "statusCode": http.StatusConflict}})
			},
			want: "Verda did not delete instance",
		},
	} {
		t.Run(name, func(t *testing.T) {
			api := newVerdaAPI(t)
			client := api.client(testOwner)
			ctx := context.Background()
			created, err := client.Create(ctx, CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-a1b2c3d4"})
			require.NoError(t, err)

			verda := api.server.Config.Handler
			api.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut && r.URL.Path == "/instances" {
					tc.answer(w, created.ID)
					return
				}
				verda.ServeHTTP(w, r)
			})
			err = client.Delete(ctx, created.ID)
			require.ErrorContains(t, err, tc.want)
			require.ErrorContains(t, err, "The instance is locked")
			listed, err := client.List(ctx)
			require.NoError(t, err)
			require.Len(t, listed, 1, "it is still there")

			// Asked again once Verda takes the delete, it goes.
			api.server.Config.Handler = verda
			require.NoError(t, client.Delete(ctx, created.ID))
			require.Len(t, api.actions, 1)
		})
	}
}

// An instance can go between the question about it and the delete. Verda
// then has nothing to delete, which is what was asked for.
func TestVerdaDeleteOfAnInstanceThatWentMeanwhileSucceeds(t *testing.T) {
	for name, answer := range map[string]func(w http.ResponseWriter, id string){
		"Verda does not know the instance": func(w http.ResponseWriter, _ string) {
			writeJSON(w, http.StatusNotFound, map[string]string{"code": "not_found", "message": "Instance not found"})
		},
		"Verda says so of the instance": func(w http.ResponseWriter, id string) {
			writeJSON(w, http.StatusAccepted, []map[string]any{{"instanceId": id, "action": "delete", "status": "error", "error": "Instance not found", "statusCode": http.StatusNotFound}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			api := newVerdaAPI(t)
			client := api.client(testOwner)
			ctx := context.Background()
			created, err := client.Create(ctx, CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-a1b2c3d4"})
			require.NoError(t, err)

			verda := api.server.Config.Handler
			api.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut && r.URL.Path == "/instances" {
					answer(w, created.ID)
					return
				}
				verda.ServeHTTP(w, r)
			})
			require.NoError(t, client.Delete(ctx, created.ID))
		})
	}
}

// Verda can take an order and then answer that it failed. The machine boots
// all the same, and without its script it never joins.
func TestVerdaACreateThatFailedOnVerdasSideKeepsItsScript(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	api.mu.Lock()
	api.failsAfterCreate = true
	api.mu.Unlock()

	_, err := client.Create(context.Background(), CreateRequest{
		Offer: Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true}, Name: "beam-l40s-a1b2c3d4", StartupScript: "join",
		Tags: map[string]string{"beam-machine": "a1b2c3d4"},
	})
	require.ErrorContains(t, err, "status 500")
	require.NotErrorIs(t, err, ErrOutOfStock)
	require.Equal(t, 1, api.sent("POST", "/instances"), "a purchase is not sent twice")

	// The caller finds the machine by its tags, and its script is there.
	listed, err := client.List(context.Background())
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, map[string]string{"beam-machine": "a1b2c3d4"}, listed[0].Tags)
	require.Equal(t, map[string]string{
		"beam-owner=" + testOwner + " beam-l40s-a1b2c3d4": "#!/usr/bin/env bash\nset -euo pipefail\njoin\n",
	}, api.storedScripts())
}

// Only an old script is removed: a young one may belong to a create that is
// under way. One whose age Verda does not tell cannot be called old.
func TestVerdaKeepsAScriptWhoseAgeItDoesNotKnow(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	api.mu.Lock()
	api.scripts = append(api.scripts, map[string]any{"id": "undated", "name": "beam-owner=" + testOwner + " beam-l40s-a1b2c3d4", "script": "#!/bin/bash"})
	api.mu.Unlock()

	api.after(verdaScriptGrace)
	_, err := client.List(context.Background())
	require.NoError(t, err)
	require.Len(t, api.storedScripts(), 1)
	require.Zero(t, api.sent("DELETE", "/scripts/"))
}

// Verda lists scripts a page at a time. The scripts that no machine runs are
// found on every page before one is removed, which would shift the pages
// and leave some of them unread.
func TestVerdaListRemovesUnusedScriptsFromEveryPage(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	_, err := client.Create(ctx, CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-running", StartupScript: "join"})
	require.NoError(t, err)
	api.mu.Lock()
	for i := range 130 {
		api.scripts = append(api.scripts, map[string]any{
			"id": fmt.Sprintf("left-behind-%03d", i), "name": fmt.Sprintf("beam-owner=%s beam-l40s-%03d", testOwner, i),
			"script": "#!/bin/bash", "created_at": api.now.Format(time.RFC3339Nano),
		})
	}
	api.mu.Unlock()
	api.after(verdaScriptGrace)

	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, 2, api.sent("GET", "/scripts"), "131 scripts, a hundred at a time")
	require.Equal(t, 130, api.sent("DELETE", "/scripts/"), "all of them, in one pass")
	require.Equal(t, map[string]string{
		"beam-owner=" + testOwner + " beam-l40s-running": "#!/usr/bin/env bash\nset -euo pipefail\njoin\n",
	}, api.storedScripts(), "the one a machine runs is kept")
}

// Verda runs a script that it has stored. A machine bought without its
// script would boot and never join, so a create that cannot store the script
// buys nothing.
func TestVerdaBuysNothingWhenItCannotStoreTheStartupScript(t *testing.T) {
	api := newVerdaAPI(t)
	verda := api.server.Config.Handler
	api.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/scripts" {
			writeJSON(w, http.StatusForbidden, map[string]string{"code": "forbidden_action", "message": "This key may not store scripts"})
			return
		}
		verda.ServeHTTP(w, r)
	})

	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{
		Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-a1b2c3d4", StartupScript: "join",
	})
	require.ErrorContains(t, err, "store the startup script: POST /scripts: status 403")
	require.ErrorContains(t, err, "This key may not store scripts")
	require.Zero(t, api.sent("POST", "/instances"))
}

// The scripts are tidied when the machines are listed. A list that cannot
// tidy is still the list: without it no machine could be checked or deleted.
func TestVerdaListsItsMachinesWhenItCannotLookForUnusedScripts(t *testing.T) {
	api := newVerdaAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: Offer{ID: "1L40S.20V", Region: "FIN-01"}, Name: "beam-l40s-a1b2c3d4", StartupScript: "join"})
	require.NoError(t, err)

	verda := api.server.Config.Handler
	api.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/scripts" {
			writeJSON(w, http.StatusForbidden, map[string]string{"code": "forbidden_action", "message": "This key may not list scripts"})
			return
		}
		verda.ServeHTTP(w, r)
	})
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, created.ID, listed[0].ID)
}

// TestLive buys a real machine. It checks a provider's client against the
// provider itself, without the rest of Beam: it buys the cheapest spot
// machine on offer, reads its status, deletes it, and confirms that nothing
// is left.
//
// It runs for the providers named in BEAM_LIVE_PROVIDERS, a comma-separated
// list, and nowhere else: credentials alone do not turn it on, because a
// shell often holds cloud credentials for other reasons. Each provider reads
// its credentials from the variables in liveProviders.
//
//	BEAM_LIVE_PROVIDERS=verda VERDA_CLIENT_ID=... VERDA_CLIENT_SECRET=... \
//	  go test ./pkg/providers -run TestLive -v -count=1 -timeout 30m
//
// The price limit is also the most the machine may come to cost, where the
// provider can be told: this is what makes Nebius's pricing policies.
//
// Optional:
//
//	BEAM_LIVE_GPU               buy a machine with this GPU, such as L40S
//	BEAM_LIVE_MAX_HOURLY_CENTS  never buy above this price (default 200)
//	BEAM_LIVE_ON_DEMAND=1       buy on demand where there is no spot stock
//	BEAM_LIVE_WAIT              wait this long for the machine to run, such
//	                            as 10m (default: read the status once)
func TestLive(t *testing.T) {
	names := strings.FieldsFunc(os.Getenv("BEAM_LIVE_PROVIDERS"), func(r rune) bool { return r == ',' || r == ' ' })
	if len(names) == 0 {
		t.Skip("BEAM_LIVE_PROVIDERS names no provider; this test buys real machines")
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			open, known := liveProviders[name]
			require.True(t, known, "no live test for provider %q", name)
			// The owner is this run's alone, so that what the provider lists
			// for it is what this run bought.
			owner := "beam-live-" + randomHex(t, 4)
			provider := open(t, owner)
			require.Equal(t, name, provider.Name())
			liveCheck{provider: provider, owner: owner, poll: 15 * time.Second}.run(t)
		})
	}
}

// liveProviders builds each provider's client from the environment.
var liveProviders = map[string]func(t *testing.T, owner string) Provider{
	"shadeform": func(t *testing.T, owner string) Provider {
		return NewShadeform(types.ShadeformProviderConfig{ApiKey: liveEnv(t, "SHADEFORM_API_KEY")}, owner)
	},
	// The account needs an SSH key: Vast starts no VM without one.
	"vast": func(t *testing.T, owner string) Provider {
		return NewVast(types.VastProviderConfig{ApiKey: liveEnv(t, "VAST_API_KEY")}, owner)
	},
	"verda": func(t *testing.T, owner string) Provider {
		config := types.VerdaProviderConfig{ClientID: liveEnv(t, "VERDA_CLIENT_ID"), ClientSecret: liveEnv(t, "VERDA_CLIENT_SECRET")}
		if keys := os.Getenv("VERDA_SSH_KEY_IDS"); keys != "" {
			config.SSHKeyIDs = strings.Split(keys, ",")
		}
		client, err := NewVerda(config, owner)
		require.NoError(t, err)
		return client
	},
	// NEBIUS_PRIVATE_KEY_FILE is the PEM file of the service account's
	// authorized key, and NEBIUS_PROJECT_IDS the projects to buy in, one for
	// a region, separated by commas.
	"nebius": func(t *testing.T, owner string) Provider {
		key, err := os.ReadFile(liveEnv(t, "NEBIUS_PRIVATE_KEY_FILE"))
		require.NoError(t, err)
		config := types.NebiusProviderConfig{
			ServiceAccountID: liveEnv(t, "NEBIUS_SERVICE_ACCOUNT_ID"),
			PublicKeyID:      liveEnv(t, "NEBIUS_PUBLIC_KEY_ID"),
			PrivateKey:       string(key),
		}
		for _, project := range strings.Split(liveEnv(t, "NEBIUS_PROJECT_IDS"), ",") {
			config.Projects = append(config.Projects, types.NebiusProjectConfig{ID: strings.TrimSpace(project)})
		}
		client, err := NewNebius(config, owner)
		require.NoError(t, err)
		return client
	},
	// The keys are the ones the AWS SDK finds by itself, such as
	// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or AWS_PROFILE.
	// AWS_LIVE_REGIONS are the regions to buy in, separated by commas; each
	// needs a default VPC.
	"aws": func(t *testing.T, owner string) Provider {
		config := types.AWSProviderConfig{}
		for _, region := range strings.Split(liveEnv(t, "AWS_LIVE_REGIONS"), ",") {
			config.Regions = append(config.Regions, types.AWSProviderRegionConfig{Name: strings.TrimSpace(region)})
		}
		client, err := NewAWS(config, owner)
		require.NoError(t, err)
		return client
	},
	// GCP_SERVICE_ACCOUNT_KEY_FILE is the service account's JSON key file,
	// and GCP_LIVE_REGIONS the regions to buy in, separated by commas. The
	// project is the service account's unless GCP_PROJECT names another,
	// and needs a network named default.
	"gcp": func(t *testing.T, owner string) Provider {
		key, err := os.ReadFile(liveEnv(t, "GCP_SERVICE_ACCOUNT_KEY_FILE"))
		require.NoError(t, err)
		config := types.GCPProviderConfig{ServiceAccountKey: string(key), Project: os.Getenv("GCP_PROJECT")}
		for _, region := range strings.Split(liveEnv(t, "GCP_LIVE_REGIONS"), ",") {
			config.Regions = append(config.Regions, types.GCPProviderRegionConfig{Name: strings.TrimSpace(region)})
		}
		client, err := NewGCP(config, owner)
		require.NoError(t, err)
		return client
	},
}

func liveEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	require.NotEmpty(t, value, "%s is not set", name)
	return value
}

func randomHex(t *testing.T, bytes int) string {
	t.Helper()
	raw := make([]byte, bytes)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	return hex.EncodeToString(raw)
}

// liveCheck is one purchase and its undoing.
type liveCheck struct {
	provider Provider
	owner    string
	// poll is the wait between two reads of something that is to change.
	poll time.Duration
	// finishDelete is set where a stand-in plays the provider, which then
	// has to be told to finish deleting.
	finishDelete func(id string)
}

func (c liveCheck) run(t *testing.T) {
	ctx := context.Background()
	provider, owner := c.provider, c.owner
	maxCents := int64(200)
	if text := os.Getenv("BEAM_LIVE_MAX_HOURLY_CENTS"); text != "" {
		var err error
		maxCents, err = strconv.ParseInt(text, 10, 64)
		require.NoError(t, err, "BEAM_LIVE_MAX_HOURLY_CENTS")
	}
	var wait time.Duration
	if text := os.Getenv("BEAM_LIVE_WAIT"); text != "" {
		var err error
		wait, err = time.ParseDuration(text)
		require.NoError(t, err, "BEAM_LIVE_WAIT")
	}

	before, err := provider.List(ctx)
	require.NoError(t, err)
	require.Empty(t, before, "owner %s is new and already has instances", owner)

	filter := OfferFilter{}
	if gpu := os.Getenv("BEAM_LIVE_GPU"); gpu != "" {
		filter.GPUs = []string{gpu}
	}
	offers, err := provider.Offers(ctx, filter)
	require.NoError(t, err)
	t.Logf("%d offers, %d of them spot", len(offers), len(slices.DeleteFunc(slices.Clone(offers), func(o Offer) bool { return !o.Spot })))

	offer, ok := cheapest(offers, true, maxCents)
	if !ok && os.Getenv("BEAM_LIVE_ON_DEMAND") != "" {
		offer, ok = cheapest(offers, false, maxCents)
	}
	if !ok {
		t.Skipf("nothing on offer at or under %d cents an hour (spot only unless BEAM_LIVE_ON_DEMAND is set)", maxCents)
	}
	t.Logf("buying %s in %s (%s), spot=%t, $%.4f an hour", offer.InstanceType, offer.Region, offer.Cloud, offer.Spot, float64(offer.HourlyCostMicros)/1e6)

	name := owner + "-" + randomHex(t, 4)
	tags := map[string]string{"beam-live": "smoke"}
	created, err := provider.Create(ctx, CreateRequest{
		Offer:         offer,
		Name:          name,
		StartupScript: "echo beam live smoke test",
		Tags:          tags,
		// A provider that deletes by itself does, should this test die.
		Until:               time.Now().Add(time.Hour),
		MaxHourlyCostMicros: maxCents * 10_000,
	})
	if errors.Is(err, ErrOutOfStock) || errors.Is(err, ErrQuota) {
		// The client named the refusal, which is what it is for.
		t.Skipf("the provider would not sell: %v", err)
	}
	require.NoError(t, err)
	require.NotEmpty(t, created.ID)
	t.Logf("bought %s, status %s", created.ID, created.Status)

	deleted := false
	t.Cleanup(func() {
		if deleted {
			return
		}
		// Whatever failed, the machine must not be left running.
		for attempt := 1; attempt <= 5; attempt++ {
			err := provider.Delete(context.Background(), created.ID)
			if err == nil {
				return
			}
			t.Logf("delete %s, attempt %d: %v", created.ID, attempt, err)
			time.Sleep(c.poll)
		}
		t.Errorf("INSTANCE %s OF %s MAY STILL BE RUNNING: delete it by hand", created.ID, provider.Name())
	})

	found, err := provider.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, created.ID, found.ID)
	require.Equal(t, tags, found.Tags)
	require.Contains(t, []Status{StatusPending, StatusActive}, found.Status, "detail: %s", found.StatusDetail)
	for deadline := time.Now().Add(wait); found.Status == StatusPending && time.Now().Before(deadline); {
		time.Sleep(c.poll)
		found, err = provider.Get(ctx, created.ID)
		require.NoError(t, err)
		t.Logf("status %s %s", found.Status, found.StatusDetail)
	}
	if wait > 0 {
		require.Equal(t, StatusActive, found.Status, "not running after %s; detail: %s", wait, found.StatusDetail)
		require.NotEmpty(t, found.PublicIP)
	}

	listed, err := provider.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, created.ID, listed[0].ID)
	require.Equal(t, tags, listed[0].Tags)

	require.NoError(t, provider.Extend(ctx, created.ID, time.Now().Add(2*time.Hour)))

	require.NoError(t, provider.Delete(ctx, created.ID))
	deleted = true
	require.NoError(t, provider.Delete(ctx, created.ID), "deleting twice succeeds")
	if c.finishDelete != nil {
		c.finishDelete(created.ID)
	}

	// A provider may show the instance for a while as it deletes it. Some
	// cannot tell a spot machine they took back from one that was deleted.
	ended := []Status{StatusDeleted, StatusInterrupted}
	for deadline := time.Now().Add(10 * time.Minute); ; {
		listed, err = provider.List(ctx)
		require.NoError(t, err)
		if len(listed) == 0 {
			break
		}
		for _, instance := range listed {
			require.Contains(t, ended, instance.Status, "instance %s is listed and is not being deleted", instance.ID)
		}
		require.True(t, time.Now().Before(deadline), "the provider still lists %d instances ten minutes after the delete", len(listed))
		time.Sleep(c.poll)
	}

	if found, err := provider.Get(ctx, created.ID); err == nil {
		t.Logf("after the delete the provider reports %s %s", found.Status, found.StatusDetail)
		require.Contains(t, ended, found.Status)
	} else {
		require.ErrorIs(t, err, ErrNotFound)
	}
}

// cheapest returns the offer that costs least among those bought the given
// way, at or under the price limit.
func cheapest(offers []Offer, spot bool, maxCents int64) (Offer, bool) {
	var best Offer
	found := false
	for _, offer := range offers {
		if offer.Spot != spot || offer.HourlyCostMicros > maxCents*10_000 {
			continue
		}
		if !found || offer.HourlyCostMicros < best.HourlyCostMicros {
			best, found = offer, true
		}
	}
	return best, found
}

// The live check is run rarely, and with money. This runs it against the
// stand-ins, so that a mistake in the check itself shows up here.
func TestTheLiveCheckPassesAgainstEveryStandIn(t *testing.T) {
	t.Setenv("BEAM_LIVE_MAX_HOURLY_CENTS", "10000")
	t.Setenv("BEAM_LIVE_ON_DEMAND", "1")
	for name, open := range conformanceSites {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			liveCheck{provider: s.provider, owner: testOwner, poll: time.Millisecond, finishDelete: s.lose}.run(t)
		})
	}
}

func TestCheapestOfferRespectsTheWayOfBuyingAndThePriceLimit(t *testing.T) {
	offers := []Offer{
		{ID: "spot-dear", Spot: true, HourlyCostMicros: 3_000_000},
		{ID: "spot-cheap", Spot: true, HourlyCostMicros: 500_000},
		{ID: "on-demand", HourlyCostMicros: 100_000},
	}
	best, ok := cheapest(offers, true, 200)
	require.True(t, ok)
	require.Equal(t, "spot-cheap", best.ID)

	_, ok = cheapest(offers, true, 40)
	require.False(t, ok, "no spot offer costs 40 cents or less")

	best, ok = cheapest(offers, false, 200)
	require.True(t, ok)
	require.Equal(t, "on-demand", best.ID)
}

// sampleAnswers holds the answers that sample returns, by provider and name.
const sampleAnswers = `{
  "hetzner/locations": {
    "locations": [
      {"id": 1, "name": "fsn1", "description": "Falkenstein DC Park 1", "country": "DE", "city": "Falkenstein", "latitude": 50.47612, "longitude": 12.370071, "network_zone": "eu-central"},
      {"id": 4, "name": "ash", "description": "Ashburn, VA", "country": "US", "city": "Ashburn, VA", "latitude": 39.045821, "longitude": -77.487073, "network_zone": "us-east"},
      {"id": 5, "name": "hil", "description": "Hillsboro, OR", "country": "US", "city": "Hillsboro, OR", "latitude": 45.54222, "longitude": -122.951924, "network_zone": "us-west"}
    ],
    "meta": {
      "pagination": {"page": 1, "per_page": 50, "previous_page": null, "next_page": null, "last_page": 1, "total_entries": 3}
    }
  },
  "hetzner/networks": {
    "networks": [
      {
        "id": 4711,
        "name": "beam-workers",
        "ip_range": "10.0.0.0/16",
        "subnets": [
          {"type": "cloud", "ip_range": "10.0.1.0/24", "network_zone": "us-east", "gateway": "10.0.0.1", "vswitch_id": null},
          {"type": "cloud", "ip_range": "10.0.2.0/24", "network_zone": "us-west", "gateway": "10.0.0.1", "vswitch_id": null}
        ],
        "routes": [],
        "servers": [42],
        "load_balancers": [],
        "protection": {"delete": false},
        "labels": {},
        "created": "2016-01-30T23:55:00Z",
        "expose_routes_to_vswitch": false
      }
    ],
    "meta": {
      "pagination": {"page": 1, "per_page": 50, "previous_page": null, "next_page": null, "last_page": 1, "total_entries": 1}
    }
  },
  "hetzner/server": {
    "id": 42,
    "name": "my-resource",
    "status": "running",
    "created": "2016-01-30T23:55:00Z",
    "public_net": {
      "ipv4": {
        "id": 42,
        "ip": "1.2.3.4",
        "blocked": false,
        "dns_ptr": "server01.example.com"
      },
      "ipv6": {
        "id": 42,
        "ip": "2001:db8::/64",
        "blocked": false,
        "dns_ptr": [
          {
            "ip": "2001:db8::1",
            "dns_ptr": "server.example.com"
          }
        ]
      },
      "floating_ips": [
        478
      ],
      "firewalls": [
        {
          "id": 42,
          "status": "applied"
        }
      ]
    },
    "private_net": [
      {
        "network": 4711,
        "ip": "10.0.0.2",
        "alias_ips": [
          "10.0.0.3",
          "10.0.0.4"
        ],
        "mac_address": "86:00:ff:2a:7d:e1"
      }
    ],
    "server_type": {
      "id": 1,
      "name": "cpx22",
      "description": "CPX22",
      "cores": 2,
      "memory": 4,
      "disk": 80,
      "prices": [
        {
          "location": "fsn1",
          "price_hourly": {
            "net": "1.0000",
            "gross": "1.1900"
          },
          "price_monthly": {
            "net": "1.0000",
            "gross": "1.1900"
          },
          "included_traffic": 654321,
          "price_per_tb_traffic": {
            "net": "1.0000",
            "gross": "1.1900"
          }
        }
      ],
      "storage_type": "local",
      "cpu_type": "shared",
      "category": "Shared vCPU",
      "architecture": "x86",
      "deprecation": {
        "unavailable_after": "2023-09-01T00:00:00Z",
        "announced": "2023-06-01T00:00:00Z"
      },
      "locations": [
        {
          "id": 42,
          "name": "fsn1",
          "deprecation": {
            "unavailable_after": "2023-09-01T00:00:00Z",
            "announced": "2023-06-01T00:00:00Z"
          },
          "recommended": false,
          "available": true
        }
      ]
    },
    "location": {
      "id": 42,
      "name": "fsn1",
      "description": "Falkenstein DC Park 1",
      "country": "DE",
      "city": "Falkenstein",
      "latitude": 50.47612,
      "longitude": 12.370071,
      "network_zone": "eu-central"
    },
    "image": {
      "id": 42,
      "type": "snapshot",
      "status": "available",
      "name": "ubuntu-24.04",
      "description": "Ubuntu 24.04 Standard 64 bit",
      "image_size": 2.3,
      "disk_size": 10,
      "created": "2016-01-30T23:55:00Z",
      "created_from": {
        "id": 1,
        "name": "Server"
      },
      "bound_to": null,
      "os_flavor": "ubuntu",
      "os_version": "24.04",
      "rapid_deploy": false,
      "protection": {
        "delete": false
      },
      "deprecation": {
        "unavailable_after": "2023-09-01T00:00:00Z",
        "announced": "2023-06-01T00:00:00Z"
      },
      "deleted": null,
      "labels": {
        "environment": "prod",
        "example.com/my": "label",
        "just-a-key": ""
      },
      "architecture": "x86"
    },
    "iso": {
      "id": 42,
      "name": "FreeBSD-11.0-RELEASE-amd64-dvd1",
      "description": "FreeBSD 11.0 x64",
      "type": "public",
      "deprecation": {
        "unavailable_after": "2023-09-01T00:00:00Z",
        "announced": "2023-06-01T00:00:00Z"
      },
      "architecture": "x86"
    },
    "rescue_enabled": false,
    "locked": false,
    "backup_window": "22-02",
    "outgoing_traffic": 123456,
    "ingoing_traffic": 123456,
    "included_traffic": 654321,
    "protection": {
      "delete": false,
      "rebuild": false
    },
    "labels": {},
    "volumes": [
      0
    ],
    "load_balancers": [
      0
    ],
    "primary_disk_size": 50,
    "placement_group": {
      "id": 42,
      "name": "my-resource",
      "labels": {
        "environment": "prod",
        "example.com/my": "label",
        "just-a-key": ""
      },
      "type": "spread",
      "created": "2016-01-30T23:55:00Z",
      "servers": [
        42
      ]
    }
  },
  "hetzner/server_types": {
    "server_types": [
      {
        "id": 45,
        "name": "cpx31",
        "description": "CPX31",
        "cores": 4,
        "memory": 8,
        "disk": 160,
        "prices": [
          {
            "location": "fsn1",
            "price_hourly": {"net": "0.0208", "gross": "0.0248"},
            "price_monthly": {"net": "12.9900", "gross": "15.4581"},
            "included_traffic": 21990232555520,
            "price_per_tb_traffic": {"net": "1.0000", "gross": "1.1900"}
          },
          {
            "location": "ash",
            "price_hourly": {"net": "0.0312", "gross": "0.0371"},
            "price_monthly": {"net": "19.4900", "gross": "23.1931"},
            "included_traffic": 3298534883328,
            "price_per_tb_traffic": {"net": "1.0000", "gross": "1.1900"}
          },
          {
            "location": "hil",
            "price_hourly": {"net": "0.0312", "gross": "0.0371"},
            "price_monthly": {"net": "19.4900", "gross": "23.1931"},
            "included_traffic": 3298534883328,
            "price_per_tb_traffic": {"net": "1.0000", "gross": "1.1900"}
          }
        ],
        "storage_type": "local",
        "cpu_type": "shared",
        "category": "Shared vCPU",
        "architecture": "x86",
        "deprecation": null,
        "locations": [
          {"id": 1, "name": "fsn1", "deprecation": null, "recommended": false, "available": true},
          {"id": 4, "name": "ash", "deprecation": null, "recommended": false, "available": true},
          {"id": 5, "name": "hil", "deprecation": null, "recommended": false, "available": false}
        ]
      },
      {
        "id": 97,
        "name": "ccx23",
        "description": "CCX23",
        "cores": 4,
        "memory": 16,
        "disk": 80,
        "prices": [
          {
            "location": "ash",
            "price_hourly": {"net": "0.0577", "gross": "0.0687"},
            "price_monthly": {"net": "35.9900", "gross": "42.8281"},
            "included_traffic": 2199023255552,
            "price_per_tb_traffic": {"net": "1.0000", "gross": "1.1900"}
          },
          {
            "location": "hil",
            "price_hourly": {"net": "0.0577", "gross": "0.0687"},
            "price_monthly": {"net": "35.9900", "gross": "42.8281"},
            "included_traffic": 2199023255552,
            "price_per_tb_traffic": {"net": "1.0000", "gross": "1.1900"}
          }
        ],
        "storage_type": "local",
        "cpu_type": "dedicated",
        "category": "Dedicated vCPU",
        "architecture": "x86",
        "deprecation": null,
        "locations": [
          {"id": 4, "name": "ash", "deprecation": null, "recommended": true, "available": true},
          {"id": 5, "name": "hil", "deprecation": {"unavailable_after": "2027-01-01T00:00:00Z", "announced": "2026-10-01T00:00:00Z"}, "recommended": false, "available": true}
        ]
      },
      {
        "id": 22,
        "name": "cpx21",
        "description": "CPX21",
        "cores": 3,
        "memory": 4,
        "disk": 80,
        "prices": [
          {
            "location": "ash",
            "price_hourly": {"net": "0.0152", "gross": "0.0181"},
            "price_monthly": {"net": "9.4900", "gross": "11.2931"},
            "included_traffic": 2199023255552,
            "price_per_tb_traffic": {"net": "1.0000", "gross": "1.1900"}
          }
        ],
        "storage_type": "local",
        "cpu_type": "shared",
        "category": "Shared vCPU",
        "architecture": "x86",
        "deprecation": {"unavailable_after": "2027-01-01T00:00:00Z", "announced": "2026-10-01T00:00:00Z"},
        "locations": [
          {"id": 4, "name": "ash", "deprecation": {"unavailable_after": "2027-01-01T00:00:00Z", "announced": "2026-10-01T00:00:00Z"}, "recommended": false, "available": true}
        ]
      },
      {
        "id": 104,
        "name": "cax21",
        "description": "CAX21",
        "cores": 4,
        "memory": 8,
        "disk": 80,
        "prices": [
          {
            "location": "fsn1",
            "price_hourly": {"net": "0.0104", "gross": "0.0124"},
            "price_monthly": {"net": "6.4900", "gross": "7.7231"},
            "included_traffic": 21990232555520,
            "price_per_tb_traffic": {"net": "1.0000", "gross": "1.1900"}
          }
        ],
        "storage_type": "local",
        "cpu_type": "shared",
        "category": "Shared vCPU",
        "architecture": "arm",
        "deprecation": null,
        "locations": [
          {"id": 1, "name": "fsn1", "deprecation": null, "recommended": false, "available": true},
          {"id": 4, "name": "ash", "deprecation": null, "recommended": false, "available": true}
        ]
      }
    ],
    "meta": {
      "pagination": {"page": 1, "per_page": 50, "previous_page": null, "next_page": null, "last_page": 1, "total_entries": 4}
    }
  },
  "nebius/resource_advice": {
    "items": [
      {
        "metadata": {
          "id": "resourceadvice-e000001",
          "parentId": "tenant-e00beamtest"
        },
        "spec": {
          "region": "eu-north1",
          "fabric": "fabric-2",
          "computeInstance": {
            "platform": "gpu-h200-sxm",
            "preset": {
              "name": "1gpu-16vcpu-200gb",
              "resources": {
                "vcpuCount": 16,
                "memoryGibibytes": 200,
                "gpuCount": 1
              }
            },
            "gpuMemoryGigabytes": 141
          }
        },
        "status": {
          "onDemand": {
            "available": 4,
            "limit": 8,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "preemptible": {
            "available": 2,
            "limit": 2,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "reserved": {
            "available": 0,
            "limit": 0,
            "availabilityLevel": "AVAILABILITY_LEVEL_LIMIT_REACHED",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          }
        }
      },
      {
        "metadata": {
          "id": "resourceadvice-e000002",
          "parentId": "tenant-e00beamtest"
        },
        "spec": {
          "region": "eu-north1",
          "fabric": "fabric-3",
          "computeInstance": {
            "platform": "gpu-h200-sxm",
            "preset": {
              "name": "1gpu-16vcpu-200gb",
              "resources": {
                "vcpuCount": 16,
                "memoryGibibytes": 200,
                "gpuCount": 1
              }
            },
            "gpuMemoryGigabytes": 141
          }
        },
        "status": {
          "onDemand": {
            "available": "4",
            "limit": "8",
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "preemptible": {
            "available": "1",
            "limit": "2",
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "reserved": {
            "available": 0,
            "limit": 0,
            "availabilityLevel": "AVAILABILITY_LEVEL_LIMIT_REACHED",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          }
        }
      },
      {
        "metadata": {
          "id": "resourceadvice-e000003",
          "parentId": "tenant-e00beamtest"
        },
        "spec": {
          "region": "eu-north1",
          "fabric": "fabric-2",
          "computeInstance": {
            "platform": "gpu-h200-sxm",
            "preset": {
              "name": "8gpu-128vcpu-1600gb",
              "resources": {
                "vcpuCount": 128,
                "memoryGibibytes": 1600,
                "gpuCount": 8
              }
            },
            "gpuMemoryGigabytes": 141
          }
        },
        "status": {
          "onDemand": {
            "available": 1,
            "limit": 1,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "preemptible": {
            "available": 1,
            "limit": 2,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "reserved": {
            "available": 0,
            "limit": 0,
            "availabilityLevel": "AVAILABILITY_LEVEL_LIMIT_REACHED",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          }
        }
      },
      {
        "metadata": {
          "id": "resourceadvice-e000004",
          "parentId": "tenant-e00beamtest"
        },
        "spec": {
          "region": "eu-north1",
          "fabric": "fabric-2",
          "computeInstance": {
            "platform": "gpu-h100-sxm",
            "preset": {
              "name": "1gpu-16vcpu-200gb",
              "resources": {
                "vcpuCount": 16,
                "memoryGibibytes": 200,
                "gpuCount": 1
              }
            },
            "gpuMemoryGigabytes": 80
          }
        },
        "status": {
          "onDemand": {
            "available": 2,
            "limit": 8,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "preemptible": {
            "available": 0,
            "limit": 2,
            "availabilityLevel": "AVAILABILITY_LEVEL_LIMIT_REACHED",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "reserved": {
            "available": 0,
            "limit": 0,
            "availabilityLevel": "AVAILABILITY_LEVEL_LIMIT_REACHED",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          }
        }
      },
      {
        "metadata": {
          "id": "resourceadvice-e000005",
          "parentId": "tenant-e00beamtest"
        },
        "spec": {
          "region": "eu-north1",
          "fabric": "fabric-4",
          "computeInstance": {
            "platform": "gpu-l40s-a",
            "preset": {
              "name": "1gpu-8vcpu-32gb",
              "resources": {
                "vcpuCount": 8,
                "memoryGibibytes": 32,
                "gpuCount": 1
              }
            },
            "gpuMemoryGigabytes": 48
          }
        },
        "status": {
          "onDemand": {
            "available": 2,
            "limit": 8,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "preemptible": {
            "available": 2,
            "limit": 2,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "reserved": {
            "available": 0,
            "limit": 0,
            "availabilityLevel": "AVAILABILITY_LEVEL_LIMIT_REACHED",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          }
        }
      },
      {
        "metadata": {
          "id": "resourceadvice-e000006",
          "parentId": "tenant-e00beamtest"
        },
        "spec": {
          "region": "eu-north1",
          "fabric": "",
          "computeInstance": {
            "platform": "cpu-d3",
            "preset": {
              "name": "4vcpu-16gb",
              "resources": {
                "vcpuCount": 4,
                "memoryGibibytes": 16
              }
            }
          }
        },
        "status": {
          "onDemand": {
            "available": 20,
            "limit": 20,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "preemptible": {
            "available": 2,
            "limit": 2,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "reserved": {
            "available": 0,
            "limit": 0,
            "availabilityLevel": "AVAILABILITY_LEVEL_LIMIT_REACHED",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          }
        }
      },
      {
        "metadata": {
          "id": "resourceadvice-e000007",
          "parentId": "tenant-e00beamtest"
        },
        "spec": {
          "region": "us-central1",
          "fabric": "fabric-7",
          "computeInstance": {
            "platform": "gpu-rtx6000",
            "preset": {
              "name": "1gpu-24vcpu-218gb",
              "resources": {
                "vcpuCount": 24,
                "memoryGibibytes": 218,
                "gpuCount": 1
              }
            },
            "gpuMemoryGigabytes": 96
          }
        },
        "status": {
          "onDemand": {
            "available": 3,
            "limit": 8,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "preemptible": {
            "available": 2,
            "limit": 2,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "reserved": {
            "available": 0,
            "limit": 0,
            "availabilityLevel": "AVAILABILITY_LEVEL_LIMIT_REACHED",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          }
        }
      },
      {
        "metadata": {
          "id": "resourceadvice-e000008",
          "parentId": "tenant-e00beamtest"
        },
        "spec": {
          "region": "us-central1",
          "fabric": "fabric-7",
          "computeInstance": {
            "platform": "gpu-b200-sxm",
            "preset": {
              "name": "1gpu-20vcpu-224gb",
              "resources": {
                "vcpuCount": 20,
                "memoryGibibytes": 224,
                "gpuCount": 1
              }
            },
            "gpuMemoryGigabytes": 180
          }
        },
        "status": {
          "onDemand": {
            "available": 3,
            "limit": 8,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "preemptible": {
            "available": 0,
            "limit": 2,
            "availabilityLevel": "AVAILABILITY_LEVEL_LIMIT_REACHED",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "reserved": {
            "available": 0,
            "limit": 0,
            "availabilityLevel": "AVAILABILITY_LEVEL_LIMIT_REACHED",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          }
        }
      },
      {
        "metadata": {
          "id": "resourceadvice-e000009",
          "parentId": "tenant-e00beamtest"
        },
        "spec": {
          "region": "eu-west1",
          "fabric": "fabric-5",
          "computeInstance": {
            "platform": "gpu-h200-sxm",
            "preset": {
              "name": "1gpu-16vcpu-200gb",
              "resources": {
                "vcpuCount": 16,
                "memoryGibibytes": 200,
                "gpuCount": 1
              }
            },
            "gpuMemoryGigabytes": 141
          }
        },
        "status": {
          "onDemand": {
            "available": 2,
            "limit": 8,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "preemptible": {
            "available": 2,
            "limit": 2,
            "availabilityLevel": "AVAILABILITY_LEVEL_HIGH",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          },
          "reserved": {
            "available": 0,
            "limit": 0,
            "availabilityLevel": "AVAILABILITY_LEVEL_LIMIT_REACHED",
            "dataState": "DATA_STATE_FRESH",
            "effectiveAt": "2026-11-14T02:20:00Z"
          }
        }
      }
    ]
  },
  "nebius/spot_price": {
    "items": [
      {
        "skuId": "sku-e00agrh84hf7fwlrgsfsv",
        "productTitle": "Preemptible NVIDIA® H200 NVLink",
        "region": "eu-north1",
        "computeInstance": {
          "platform": "gpu-h200-sxm"
        },
        "price": {
          "value": "3.38",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-14T02:01:00Z"
      },
      {
        "skuId": "sku-e00ljkedv5qtbwhikuoct",
        "productTitle": "Preemptible NVIDIA® H100 NVLink",
        "region": "eu-north1",
        "computeInstance": {
          "platform": "gpu-h100-sxm"
        },
        "price": {
          "value": "2.15",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-14T01:41:00Z"
      },
      {
        "skuId": "sku-e00g61h0323jih8tkg5jv",
        "productTitle": "Preemptible NVIDIA® RTX PRO 6000",
        "region": "eu-south1",
        "computeInstance": {
          "platform": "gpu-rtx6000-a"
        },
        "price": {
          "value": "1.38",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-12T04:11:00Z"
      },
      {
        "skuId": "sku-e00dh1moiqg9mc5ix6wj2",
        "productTitle": "Preemptible NVIDIA® H200 NVLink",
        "region": "eu-west1",
        "computeInstance": {
          "platform": "gpu-h200-sxm"
        },
        "price": {
          "value": "4.7",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-14T01:41:00Z"
      },
      {
        "skuId": "sku-e00vd4x28cozliwb6g4vh",
        "productTitle": "Preemptible NVIDIA® B300 NVLink",
        "region": "eu-west2",
        "computeInstance": {
          "platform": "gpu-b300-sxm"
        },
        "price": {
          "value": "5.75",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-13T19:31:00Z"
      },
      {
        "skuId": "sku-e00k58wq19wjszct65fwm",
        "productTitle": "Preemptible NVIDIA® B200 NVLink",
        "region": "me-west1",
        "computeInstance": {
          "platform": "gpu-b200-sxm-a"
        },
        "price": {
          "value": "2.4",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-14T02:11:00Z"
      },
      {
        "skuId": "sku-e00fmto201j24eyp0841g",
        "productTitle": "Preemptible NVIDIA® B300 NVLink",
        "region": "uk-south1",
        "computeInstance": {
          "platform": "gpu-b300-sxm"
        },
        "price": {
          "value": "3.15",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-14T01:46:00Z"
      },
      {
        "skuId": "sku-e00lxawt0lavf9i0dutiz",
        "productTitle": "Preemptible NVIDIA® RTX PRO 6000",
        "region": "uk-south2",
        "computeInstance": {
          "platform": "gpu-rtx6000-a"
        },
        "price": {
          "value": "1.08",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-14T02:01:00Z"
      },
      {
        "skuId": "sku-e00em8l461xmyihec7sr8",
        "productTitle": "Preemptible NVIDIA® H200 NVLink",
        "region": "us-central1",
        "computeInstance": {
          "platform": "gpu-h200-sxm"
        },
        "price": {
          "value": "2.9",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-14T01:51:00Z"
      },
      {
        "skuId": "sku-e00fjjc4gwdx48pck9b46",
        "productTitle": "Preemptible NVIDIA® B200 NVLink",
        "region": "us-central1",
        "computeInstance": {
          "platform": "gpu-b200-sxm"
        },
        "price": {
          "value": "2.7",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-14T01:56:00Z"
      },
      {
        "skuId": "sku-e00otwkvc6gea6nmvvsda",
        "productTitle": "Preemptible NVIDIA® RTX PRO 6000",
        "region": "us-central1",
        "computeInstance": {
          "platform": "gpu-rtx6000"
        },
        "price": {
          "value": "1.789",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-13T22:56:00Z"
      },
      {
        "skuId": "sku-e00hfg6dlxtt9ji6fqo9u",
        "productTitle": "Preemptible NVIDIA® B300 NVLink",
        "region": "us-north1",
        "computeInstance": {
          "platform": "gpu-b300-sxm"
        },
        "price": {
          "value": "6.1",
          "currency": "usd",
          "unitTitle": "GPU hour"
        },
        "effectiveTime": "2026-11-14T02:06:00Z"
      }
    ],
    "snapshotTime": "2026-11-14T02:25:37.862467Z"
  },
  "shadeform/instance": {
    "active_at": "2026-08-29T09:12:33.001Z",
    "boot_time": {
      "max_boot_in_sec": 300,
      "min_boot_in_sec": 180
    },
    "cloud": "hyperstack",
    "cloud_assigned_id": "13b057d7-e266-4869-985f-760fe75a78b3",
    "cloud_instance_type": "gpu_1x_a6000",
    "configuration": {
      "gpu_manufacturer": "nvidia",
      "gpu_type": "A6000",
      "interconnect": "pcie",
      "memory_in_gb": 64,
      "num_gpus": 1,
      "nvlink": false,
      "storage_in_gb": 500,
      "vcpus": 16,
      "vram_per_gpu_in_gb": 48
    },
    "cost_estimate": "103.40",
    "created_at": "2026-08-29T09:10:01.000Z",
    "deleted_at": null,
    "flex_vm": true,
    "hourly_price": 210,
    "id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
    "ip": "1.0.0.1",
    "name": "cool-gpu-server",
    "region": "canada-1",
    "rental_terms": {
      "ending_at": "",
      "rental_type": "on_demand"
    },
    "shade_cloud": true,
    "shade_instance_type": "A6000",
    "ssh_key_id": "78a0dd5a-dbb1-4568-b55c-5e7e0a8b0c40",
    "ssh_port": 22,
    "ssh_user": "shadeform",
    "status": "active",
    "status_details": "downloading",
    "tags": []
  },
  "shadeform/instance_types": {
    "instance_types": [
      {
        "cloud": "scaleway",
        "shade_instance_type": "L4",
        "cloud_instance_type": "L4-1-24G",
        "configuration": {
          "memory_in_gb": 48,
          "storage_in_gb": 500,
          "vcpus": 8,
          "num_gpus": 1,
          "gpu_type": "L4",
          "interconnect": "pcie",
          "nvlink": false,
          "os_options": [
            "ubuntu22.04_cuda12.4_shade_os"
          ],
          "vram_per_gpu_in_gb": 24,
          "gpu_manufacturer": "nvidia"
        },
        "memory_in_gb": 48,
        "storage_in_gb": 500,
        "vcpus": 8,
        "num_gpus": 1,
        "gpu_type": "L4",
        "interconnect": "pcie",
        "nvlink": false,
        "hourly_price": 95,
        "availability": [
          {
            "region": "paris-france-5",
            "available": true,
            "display_name": "FR, Paris",
            "rental_type": "on_demand"
          },
          {
            "region": "warsaw-poland-1",
            "available": true,
            "display_name": "PL, Warsaw",
            "rental_type": "on_demand"
          },
          {
            "region": "paris-france-1",
            "available": false,
            "display_name": "FR, Paris",
            "rental_type": "on_demand"
          }
        ],
        "boot_time": {
          "min_boot_in_sec": 300,
          "max_boot_in_sec": 480
        },
        "deployment_type": "vm",
        "flex_vm": false
      },
      {
        "cloud": "crusoe",
        "shade_instance_type": "A100_80G_sxm4x8",
        "cloud_instance_type": "a100-80gb-sxm-ib.8x",
        "configuration": {
          "memory_in_gb": 960,
          "storage_in_gb": 7808,
          "vcpus": 96,
          "num_gpus": 8,
          "gpu_type": "A100_80G",
          "interconnect": "pcie",
          "nvlink": false,
          "os_options": [
            "ubuntu22.04_cuda12.8_shade_os"
          ],
          "vram_per_gpu_in_gb": 80,
          "gpu_manufacturer": "nvidia"
        },
        "memory_in_gb": 960,
        "storage_in_gb": 7808,
        "vcpus": 96,
        "num_gpus": 8,
        "gpu_type": "A100_80G",
        "interconnect": "pcie",
        "nvlink": false,
        "hourly_price": 1840,
        "availability": [
          {
            "region": "culpeper-usa-1",
            "available": false,
            "display_name": "US, Culpeper, VA",
            "rental_type": "on_demand"
          }
        ],
        "boot_time": {
          "min_boot_in_sec": 300,
          "max_boot_in_sec": 480
        },
        "deployment_type": "vm",
        "flex_vm": false
      },
      {
        "cloud": "boostrun",
        "shade_instance_type": "RTX4090x8",
        "cloud_instance_type": "RTX4090x8",
        "configuration": {
          "memory_in_gb": 512,
          "storage_in_gb": 15360,
          "vcpus": 64,
          "num_gpus": 8,
          "gpu_type": "RTX4090",
          "interconnect": "pcie",
          "nvlink": false,
          "os_options": [
            "ubuntu22.04_cuda12.4_shade_os"
          ],
          "vram_per_gpu_in_gb": 24,
          "gpu_manufacturer": "nvidia"
        },
        "memory_in_gb": 512,
        "storage_in_gb": 15360,
        "vcpus": 64,
        "num_gpus": 8,
        "gpu_type": "RTX4090",
        "interconnect": "pcie",
        "nvlink": false,
        "hourly_price": 392,
        "availability": [
          {
            "region": "raleigh-usa-1",
            "available": true,
            "display_name": "US, Raleigh, NC",
            "rental_type": "on_demand"
          }
        ],
        "boot_time": {
          "min_boot_in_sec": 300,
          "max_boot_in_sec": 480
        },
        "deployment_type": "baremetal",
        "flex_vm": false
      },
      {
        "cloud": "denvr",
        "shade_instance_type": "cpu_8",
        "cloud_instance_type": "AMD_EPYC3_8x",
        "configuration": {
          "memory_in_gb": 32,
          "storage_in_gb": 500,
          "vcpus": 8,
          "num_gpus": 0,
          "gpu_type": "CPU",
          "interconnect": "",
          "nvlink": false,
          "os_options": [
            "ubuntu22.04",
            "ubuntu20.04"
          ],
          "vram_per_gpu_in_gb": 0,
          "gpu_manufacturer": "n/a"
        },
        "memory_in_gb": 32,
        "storage_in_gb": 500,
        "vcpus": 8,
        "num_gpus": 0,
        "gpu_type": "CPU",
        "interconnect": "",
        "nvlink": false,
        "hourly_price": 47,
        "availability": [
          {
            "region": "calgary-canada-1",
            "available": true,
            "display_name": "CA, Calgary",
            "rental_type": "on_demand"
          },
          {
            "region": "houston-usa-1",
            "available": true,
            "display_name": "US, Houston, TX",
            "rental_type": "on_demand"
          }
        ],
        "boot_time": {
          "min_boot_in_sec": 420,
          "max_boot_in_sec": 840
        },
        "deployment_type": "vm",
        "flex_vm": false
      },
      {
        "cloud": "nebius",
        "shade_instance_type": "B200_sxm5x8_NVLINK",
        "cloud_instance_type": "gpu-b200-sxm|8gpu-160vcpu-1792gb",
        "configuration": {
          "memory_in_gb": 1792,
          "storage_in_gb": 1250,
          "vcpus": 160,
          "num_gpus": 8,
          "gpu_type": "B200",
          "interconnect": "sxm5",
          "nvlink": false,
          "os_options": [
            "ubuntu24.04_cuda13.0_shade_os"
          ],
          "vram_per_gpu_in_gb": 192,
          "gpu_manufacturer": "nvidia"
        },
        "memory_in_gb": 1792,
        "storage_in_gb": 1250,
        "vcpus": 160,
        "num_gpus": 8,
        "gpu_type": "B200",
        "interconnect": "sxm5",
        "nvlink": false,
        "hourly_price": 7600,
        "availability": [
          {
            "region": "kansascity-usa-3",
            "available": false,
            "display_name": "US, Kansas City, MO",
            "rental_type": "on_demand"
          },
          {
            "region": "kansascity-usa-3",
            "available": true,
            "display_name": "US, Kansas City, MO",
            "rental_type": "spot",
            "hourly_price": "44"
          }
        ],
        "boot_time": {
          "min_boot_in_sec": 300,
          "max_boot_in_sec": 480
        },
        "deployment_type": "vm",
        "flex_vm": false
      },
      {
        "cloud": "paperspace",
        "shade_instance_type": "H100",
        "cloud_instance_type": "H100",
        "configuration": {
          "memory_in_gb": 268,
          "storage_in_gb": 500,
          "vcpus": 16,
          "num_gpus": 1,
          "gpu_type": "H100",
          "interconnect": "pcie",
          "nvlink": false,
          "os_options": [
            "ubuntu22.04_cuda12.2_shade_os"
          ],
          "vram_per_gpu_in_gb": 80,
          "gpu_manufacturer": "nvidia"
        },
        "memory_in_gb": 268,
        "storage_in_gb": 500,
        "vcpus": 16,
        "num_gpus": 1,
        "gpu_type": "H100",
        "interconnect": "pcie",
        "nvlink": false,
        "hourly_price": 599,
        "availability": [
          {
            "region": "newyork-usa-1",
            "available": true,
            "display_name": "US, New York, NY",
            "rental_type": "on_demand"
          }
        ],
        "boot_time": {
          "min_boot_in_sec": 300,
          "max_boot_in_sec": 420
        },
        "deployment_type": "vm",
        "flex_vm": false
      },
      {
        "cloud": "scaleway",
        "shade_instance_type": "H100",
        "cloud_instance_type": "H100-1-80G",
        "configuration": {
          "memory_in_gb": 240,
          "storage_in_gb": 1000,
          "vcpus": 24,
          "num_gpus": 1,
          "gpu_type": "H100",
          "interconnect": "pcie",
          "nvlink": false,
          "os_options": [
            "ubuntu24.04_cuda12.8_shade_os",
            "ubuntu24.04_cuda13.0_shade_os"
          ],
          "vram_per_gpu_in_gb": 80,
          "gpu_manufacturer": "nvidia"
        },
        "memory_in_gb": 240,
        "storage_in_gb": 1000,
        "vcpus": 24,
        "num_gpus": 1,
        "gpu_type": "H100",
        "interconnect": "pcie",
        "nvlink": false,
        "hourly_price": 330,
        "availability": [
          {
            "region": "paris-france-1",
            "available": true,
            "display_name": "FR, Paris",
            "rental_type": "on_demand"
          },
          {
            "region": "warsaw-poland-1",
            "available": false,
            "display_name": "PL, Warsaw",
            "rental_type": "on_demand"
          }
        ],
        "boot_time": {
          "min_boot_in_sec": 300,
          "max_boot_in_sec": 480
        },
        "deployment_type": "vm",
        "flex_vm": false
      }
    ]
  },
  "vast/bundles": {
    "offers": [
      {
        "id": 60202942,
        "ask_contract_id": 60202942,
        "machine_id": 55395,
        "gpu_name": "RTX 3090",
        "num_gpus": 1,
        "gpu_ram": 24576,
        "gpu_arch": "nvidia",
        "cpu_arch": "amd64",
        "cpu_cores": 12,
        "cpu_cores_effective": 12.0,
        "cpu_ram": 128487,
        "disk_space": 1263.0,
        "min_bid": 0.216,
        "dph_base": 0.216,
        "dph_total": 0.253037037037037,
        "storage_cost": 0.1333333333333333,
        "storage_total_cost": 0.03703703703703703,
        "geolocation": "Quebec, CA",
        "reliability2": 0.9892778,
        "verification": "verified",
        "vms_enabled": true,
        "rentable": true,
        "rented": false,
        "hosting_type": 0,
        "is_bid": false
      },
      {
        "id": 60342518,
        "ask_contract_id": 60342518,
        "machine_id": 57583,
        "gpu_name": "RTX 4090",
        "num_gpus": 1,
        "gpu_ram": 24564,
        "gpu_arch": "nvidia",
        "cpu_arch": "amd64",
        "cpu_cores": 24,
        "cpu_cores_effective": 24.0,
        "cpu_ram": 64130,
        "disk_space": 1313.0,
        "min_bid": 0.26666666666666666,
        "dph_base": 0.26666666666666666,
        "dph_total": 0.3222222222222222,
        "storage_cost": 0.19999999999999998,
        "storage_total_cost": 0.05555555555555555,
        "geolocation": "Switzerland, CH",
        "reliability2": 0.9922031,
        "verification": "verified",
        "vms_enabled": true,
        "rentable": true,
        "rented": false,
        "hosting_type": 0,
        "is_bid": false
      },
      {
        "id": 59239032,
        "ask_contract_id": 59239032,
        "machine_id": 151137,
        "gpu_name": "RTX 5090",
        "num_gpus": 1,
        "gpu_ram": 32607,
        "gpu_arch": "nvidia",
        "cpu_arch": "amd64",
        "cpu_cores": 384,
        "cpu_cores_effective": 64.0,
        "cpu_ram": 128918,
        "disk_space": 265.91666666666663,
        "min_bid": 0.35,
        "dph_base": 0.35,
        "dph_total": 0.40555555555555556,
        "storage_cost": 0.19999999999999998,
        "storage_total_cost": 0.05555555555555555,
        "geolocation": "Ontario, CA",
        "reliability2": 0.9924823,
        "verification": "verified",
        "vms_enabled": true,
        "rentable": true,
        "rented": false,
        "hosting_type": 0,
        "is_bid": false
      },
      {
        "id": 59566615,
        "ask_contract_id": 59566615,
        "machine_id": 35231,
        "gpu_name": "RTX 5090",
        "num_gpus": 1,
        "gpu_ram": 32607,
        "gpu_arch": "nvidia",
        "cpu_arch": "amd64",
        "cpu_cores": 128,
        "cpu_cores_effective": 16.0,
        "cpu_ram": 48345,
        "disk_space": 605.25,
        "min_bid": 0.39999999999999997,
        "dph_base": 0.39999999999999997,
        "dph_total": 0.45555555555555555,
        "storage_cost": 0.20000000000000004,
        "storage_total_cost": 0.055555555555555566,
        "geolocation": "Czechia, CZ",
        "reliability2": 0.9523005,
        "verification": "verified",
        "vms_enabled": true,
        "rentable": true,
        "rented": false,
        "hosting_type": 1,
        "is_bid": false
      },
      {
        "id": 51737447,
        "ask_contract_id": 51737447,
        "machine_id": 148193,
        "gpu_name": "RTX 4090",
        "num_gpus": 2,
        "gpu_ram": 24564,
        "gpu_arch": "nvidia",
        "cpu_arch": "amd64",
        "cpu_cores": 144,
        "cpu_cores_effective": 36.0,
        "cpu_ram": 128899,
        "disk_space": 205.5,
        "min_bid": 0.6133333333333334,
        "dph_base": 0.6133333333333334,
        "dph_total": 0.6874074074074075,
        "storage_cost": 0.2666666666666666,
        "storage_total_cost": 0.07407407407407406,
        "geolocation": "Quebec, CA",
        "reliability2": 0.9985652,
        "verification": "verified",
        "vms_enabled": true,
        "rentable": true,
        "rented": false,
        "hosting_type": 0,
        "is_bid": false
      },
      {
        "id": 58982197,
        "ask_contract_id": 58982197,
        "machine_id": 55541,
        "gpu_name": "RTX 6000Ada",
        "num_gpus": 1,
        "gpu_ram": 49140,
        "gpu_arch": "nvidia",
        "cpu_arch": "amd64",
        "cpu_cores": 256,
        "cpu_cores_effective": 32.0,
        "cpu_ram": 64466,
        "disk_space": 640.5125,
        "min_bid": 0.6,
        "dph_base": 0.6,
        "dph_total": 0.6925925925925925,
        "storage_cost": 0.3333333333333333,
        "storage_total_cost": 0.09259259259259259,
        "geolocation": "Finland, FI",
        "reliability2": 0.9845749,
        "verification": "verified",
        "vms_enabled": true,
        "rentable": true,
        "rented": false,
        "hosting_type": 0,
        "is_bid": false
      },
      {
        "id": 59566609,
        "ask_contract_id": 59566609,
        "machine_id": 35231,
        "gpu_name": "RTX 5090",
        "num_gpus": 2,
        "gpu_ram": 32607,
        "gpu_arch": "nvidia",
        "cpu_arch": "amd64",
        "cpu_cores": 128,
        "cpu_cores_effective": 32.0,
        "cpu_ram": 96691,
        "disk_space": 1210.5,
        "min_bid": 0.7999999999999999,
        "dph_base": 0.7999999999999999,
        "dph_total": 0.8555555555555555,
        "storage_cost": 0.20000000000000004,
        "storage_total_cost": 0.055555555555555566,
        "geolocation": "Czechia, CZ",
        "reliability2": 0.9523005,
        "verification": "verified",
        "vms_enabled": true,
        "rentable": true,
        "rented": false,
        "hosting_type": 1,
        "is_bid": false
      },
      {
        "id": 59159440,
        "ask_contract_id": 59159440,
        "machine_id": 155673,
        "gpu_name": "RTX PRO 6000 WS",
        "num_gpus": 1,
        "gpu_ram": 97887,
        "gpu_arch": "nvidia",
        "cpu_arch": "amd64",
        "cpu_cores": 192,
        "cpu_cores_effective": 27.428571428571427,
        "cpu_ram": 110523,
        "disk_space": 595.2640638920413,
        "min_bid": 1.0666666666666667,
        "dph_base": 1.0666666666666667,
        "dph_total": 1.1222222222222222,
        "storage_cost": 0.19999999999999998,
        "storage_total_cost": 0.05555555555555555,
        "geolocation": "Czechia, CZ",
        "reliability2": 0.9383145,
        "verification": "verified",
        "vms_enabled": true,
        "rentable": true,
        "rented": false,
        "hosting_type": 1,
        "is_bid": false
      },
      {
        "id": 60503633,
        "ask_contract_id": 60503633,
        "machine_id": 154848,
        "gpu_name": "RTX PRO 6000 WS",
        "num_gpus": 1,
        "gpu_ram": 97887,
        "gpu_arch": "nvidia",
        "cpu_arch": "amd64",
        "cpu_cores": 16,
        "cpu_cores_effective": 16.0,
        "cpu_ram": 95658,
        "disk_space": 1561.0,
        "min_bid": 1.3333333333333333,
        "dph_base": 1.3333333333333333,
        "dph_total": 1.3888888888888888,
        "storage_cost": 0.20000000000000004,
        "storage_total_cost": 0.055555555555555566,
        "geolocation": "New York, US",
        "reliability2": 0.9929403,
        "verification": "verified",
        "vms_enabled": true,
        "rentable": true,
        "rented": false,
        "hosting_type": 0,
        "is_bid": false
      },
      {
        "id": 59566611,
        "ask_contract_id": 59566611,
        "machine_id": 35231,
        "gpu_name": "RTX 5090",
        "num_gpus": 4,
        "gpu_ram": 32607,
        "gpu_arch": "nvidia",
        "cpu_arch": "amd64",
        "cpu_cores": 128,
        "cpu_cores_effective": 64.0,
        "cpu_ram": 193383,
        "disk_space": 2421.0,
        "min_bid": 1.5999999999999999,
        "dph_base": 1.5999999999999999,
        "dph_total": 1.6555555555555554,
        "storage_cost": 0.20000000000000004,
        "storage_total_cost": 0.055555555555555566,
        "geolocation": "Czechia, CZ",
        "reliability2": 0.9523005,
        "verification": "verified",
        "vms_enabled": true,
        "rentable": true,
        "rented": false,
        "hosting_type": 1,
        "is_bid": false
      },
      {
        "id": 59185714,
        "ask_contract_id": 59185714,
        "machine_id": 144714,
        "gpu_name": "RTX PRO 6000 WS",
        "num_gpus": 1,
        "gpu_ram": 97887,
        "gpu_arch": "nvidia",
        "cpu_arch": "amd64",
        "cpu_cores": 14,
        "cpu_cores_effective": 14.0,
        "cpu_ram": 128326,
        "disk_space": 1393.0,
        "min_bid": 1.5999999999999999,
        "dph_base": 1.5999999999999999,
        "dph_total": 1.8777777777777778,
        "storage_cost": 1.0,
        "storage_total_cost": 0.2777777777777778,
        "geolocation": "Texas, US",
        "reliability2": 0.9974215,
        "verification": "verified",
        "vms_enabled": true,
        "rentable": true,
        "rented": false,
        "hosting_type": 0,
        "is_bid": false
      }
    ],
    "truncated": false
  },
  "verda/instance": {
    "id": "0a6842d1-bc59-4e8d-abd7-1b0265e188cf",
    "ip": "1.2.3.4",
    "private_ip": null,
    "private_network_id": null,
    "subnet_id": null,
    "status": "running",
    "created_at": "2023-07-15T14:10:26.654Z",
    "created_by_user_id": "7dfc2631-0158-4f74-93f6-6b46dcb90fef",
    "cpu": {"description": "176 CPU", "number_of_cores": 176, "model": "AMD Genoa"},
    "gpu": {"description": "8x A100", "number_of_gpus": 8},
    "gpu_memory": {"description": "80GB GPU RAM", "size_in_gigabytes": 80},
    "memory": {"description": "640GB RAM", "size_in_gigabytes": 640},
    "storage": {"description": "dynamic"},
    "hostname": "hazy-star-swims-fin-01",
    "description": "ubuntu-22-04-cuda-12-0-docker-fin-01",
    "tags": [{"id": "tag_0aB1cD2eF3gH4iJ5kL6mN7", "key": "environment", "value": "production"}],
    "location": "FIN-01",
    "price_per_hour": 2.481,
    "is_spot": false,
    "instance_type": "8A100.176V",
    "image": "ubuntu-22-04-cuda-12-0-docker",
    "os_name": "Ubuntu 22.04",
    "startup_script_id": "95cec4c3-af69-42eb-a790-fcacebfdfcea",
    "ssh_key_ids": ["dd50b622-1c36-48c2-a485-f4d4937a3ea1"],
    "os_volume_id": "95cec4c3-af69-42eb-a790-fcacebfdfcea",
    "jupyter_token": "b9e6d8517db3a722ccfb309ca599a35f",
    "contract": "PAY_AS_YOU_GO",
    "pricing": "FIXED_PRICE",
    "volume_ids": ["95cec4c3-af69-42eb-a790-fcacebfdfcea", "dd50b622-1c36-48c2-a485-f4d4937a3ea1"]
  },
  "verda/instance_types": [
    {
      "best_for": [],
      "cpu": {
        "description": "32 CPU",
        "number_of_cores": 32
      },
      "deploy_warning": null,
      "description": "Dedicated Hardware Instance",
      "gpu": {
        "description": "1x GB300 SXM6 288GB",
        "number_of_gpus": 1
      },
      "gpu_memory": {
        "description": "288GB GPU RAM",
        "size_in_gigabytes": 288
      },
      "id": "fb300f00-0000-4000-8000-fb300f000001",
      "instance_type": "1GB300.32V",
      "memory": {
        "description": "225GB RAM",
        "size_in_gigabytes": 225
      },
      "model": "GB300",
      "name": "GB300 SXM6 288GB",
      "p2p": "1.8 TB/s",
      "price_per_hour": "14.13",
      "spot_price": "7.067",
      "location_prices": [
        {
          "location_code": "FIN-01",
          "price_per_hour": "14.13",
          "spot_price": "7.067"
        },
        {
          "location_code": "FIN-02",
          "price_per_hour": "14.13",
          "spot_price": "7.067"
        },
        {
          "location_code": "FIN-03",
          "price_per_hour": "14.13",
          "spot_price": "7.067"
        }
      ],
      "dynamic_price": null,
      "max_dynamic_price": "14.13",
      "serverless_price": "0",
      "serverless_spot_price": "0",
      "storage": {
        "description": "dynamic"
      },
      "currency": "usd",
      "manufacturer": "NVIDIA",
      "display_name": null,
      "supported_os": []
    },
    {
      "best_for": [
        "Large ML models",
        "32-16-8 bit operations",
        "Single-GPU training"
      ],
      "cpu": {
        "description": "20 CPU",
        "number_of_cores": 20
      },
      "deploy_warning": null,
      "description": "Dedicated Hardware Instance",
      "gpu": {
        "description": "1x L40S 48GB",
        "number_of_gpus": 1
      },
      "gpu_memory": {
        "description": "48GB GPU RAM",
        "size_in_gigabytes": 48
      },
      "id": "40000000-a5d2-4972-ae4e-d429115d055b",
      "instance_type": "1L40S.20V",
      "memory": {
        "description": "60GB RAM",
        "size_in_gigabytes": 60
      },
      "model": "L40S",
      "name": "L40S 48GB",
      "p2p": null,
      "price_per_hour": "1.998",
      "spot_price": "0.9990",
      "location_prices": [
        {
          "location_code": "FIN-01",
          "price_per_hour": "1.998",
          "spot_price": "0.9990"
        },
        {
          "location_code": "FIN-02",
          "price_per_hour": "1.998",
          "spot_price": "0.9990"
        },
        {
          "location_code": "FIN-03",
          "price_per_hour": "1.998",
          "spot_price": "0.9990"
        }
      ],
      "dynamic_price": null,
      "max_dynamic_price": "1.998",
      "serverless_price": "2.198",
      "serverless_spot_price": "1.099",
      "storage": {
        "description": "dynamic"
      },
      "currency": "usd",
      "manufacturer": "NVIDIA",
      "display_name": null,
      "supported_os": [
        "ubuntu-24.04",
        "ubuntu-24.04-cuda-12.6",
        "ubuntu-24.04-cuda-12.6-docker",
        "ubuntu-24.04-cuda-12.8-open",
        "ubuntu-24.04-cuda-12.8-open-docker",
        "ubuntu-24.04-cuda-13.0-open",
        "ubuntu-24.04-cuda-13.0-open-docker"
      ]
    },
    {
      "best_for": [
        "Giant ML models",
        "Multi-GPU training",
        "FP64 calculations",
        "NVLINK"
      ],
      "cpu": {
        "description": "22 CPU",
        "number_of_cores": 22
      },
      "deploy_warning": null,
      "description": "Dedicated Hardware Instance",
      "gpu": {
        "description": "1x A100 SXM4 80GB",
        "number_of_gpus": 1
      },
      "gpu_memory": {
        "description": "80GB GPU RAM",
        "size_in_gigabytes": 80
      },
      "id": "a0000000-a5d2-4972-ae4e-d429115d055b",
      "instance_type": "1A100.22V",
      "memory": {
        "description": "120GB RAM",
        "size_in_gigabytes": 120
      },
      "model": "A100 80GB",
      "name": "A100 SXM4 80GB",
      "p2p": null,
      "price_per_hour": "2.374",
      "spot_price": "1.187",
      "location_prices": [
        {
          "location_code": "FIN-01",
          "price_per_hour": "2.374",
          "spot_price": "1.187"
        },
        {
          "location_code": "FIN-02",
          "price_per_hour": "2.374",
          "spot_price": "1.187"
        },
        {
          "location_code": "FIN-03",
          "price_per_hour": "2.374",
          "spot_price": "1.187"
        }
      ],
      "dynamic_price": null,
      "max_dynamic_price": "2.374",
      "serverless_price": "2.611",
      "serverless_spot_price": "1.306",
      "storage": {
        "description": "dynamic"
      },
      "currency": "usd",
      "manufacturer": "NVIDIA",
      "display_name": null,
      "supported_os": [
        "ubuntu-24.04",
        "ubuntu-24.04-cuda-12.6",
        "ubuntu-24.04-cuda-12.6-docker",
        "ubuntu-24.04-cuda-12.8-open",
        "ubuntu-24.04-cuda-12.8-open-docker",
        "ubuntu-24.04-cuda-13.0-open",
        "ubuntu-24.04-cuda-13.0-open-docker"
      ]
    },
    {
      "best_for": [
        "Small ML models",
        "Multi-GPU training",
        "FP64 calculations",
        "NVLINK"
      ],
      "cpu": {
        "description": "4 CPU (INTEL XEON)",
        "number_of_cores": 4,
        "model": "INTEL XEON"
      },
      "deploy_warning": null,
      "description": "Dedicated Hardware Instance",
      "gpu": {
        "description": "1x Tesla V100 16GB",
        "number_of_gpus": 1
      },
      "gpu_memory": {
        "description": "16GB GPU RAM",
        "size_in_gigabytes": 16
      },
      "id": "91f291d9-2e2b-4da5-868f-d20e48572349",
      "instance_type": "1V100.4V",
      "memory": {
        "description": "18GB RAM",
        "size_in_gigabytes": 18
      },
      "model": "Tesla V100",
      "name": "Tesla V100 16GB",
      "p2p": null,
      "price_per_hour": "0.2440",
      "spot_price": "0.1220",
      "location_prices": [
        {
          "location_code": "FIN-01",
          "price_per_hour": "0.2440",
          "spot_price": "0.1220"
        },
        {
          "location_code": "FIN-02",
          "price_per_hour": "0.2440",
          "spot_price": "0.1220"
        },
        {
          "location_code": "FIN-03",
          "price_per_hour": "0.2440",
          "spot_price": "0.1220"
        }
      ],
      "dynamic_price": null,
      "max_dynamic_price": "0.2440",
      "serverless_price": "0",
      "serverless_spot_price": "0",
      "storage": {
        "description": "225GB NVME",
        "size_in_gigabytes": 225
      },
      "currency": "usd",
      "manufacturer": "NVIDIA",
      "display_name": null,
      "supported_os": [
        "ubuntu-24.04",
        "ubuntu-24.04-cuda-12.6",
        "ubuntu-24.04-cuda-12.6-docker"
      ]
    },
    {
      "best_for": [
        "Running services",
        "API server",
        "Data transfers"
      ],
      "cpu": {
        "description": "4 CPU (AMD EPYC Rome or Milan 7B12/3/4 64-Core)",
        "number_of_cores": 4,
        "model": "AMD EPYC Rome or Milan 7B12/3/4 64-Core"
      },
      "deploy_warning": null,
      "description": "Dedicated Hardware Instance",
      "gpu": {
        "description": "",
        "number_of_gpus": 0
      },
      "gpu_memory": {
        "description": "",
        "size_in_gigabytes": 0
      },
      "id": "ccc00000-a5d2-4972-ae4e-d429115d055b",
      "instance_type": "CPU.4V.16G",
      "memory": {
        "description": "16GB RAM",
        "size_in_gigabytes": 16
      },
      "model": "CPU Node",
      "name": "AMD Rome+",
      "p2p": null,
      "price_per_hour": "0.04800",
      "spot_price": "0.01920",
      "location_prices": [
        {
          "location_code": "FIN-01",
          "price_per_hour": "0.04800",
          "spot_price": "0.01920"
        },
        {
          "location_code": "FIN-02",
          "price_per_hour": "0.04800",
          "spot_price": "0.01920"
        },
        {
          "location_code": "FIN-03",
          "price_per_hour": "0.04800",
          "spot_price": "0.01920"
        }
      ],
      "dynamic_price": null,
      "max_dynamic_price": "0.04800",
      "serverless_price": "0.05280",
      "serverless_spot_price": "0.02110",
      "storage": {
        "description": "dynamic"
      },
      "currency": "usd",
      "manufacturer": "AMD",
      "display_name": "Rome/Milan",
      "supported_os": [
        "24.04.base",
        "ubuntu-24.04",
        "ubuntu-24.04-cuda-12.6",
        "ubuntu-24.04-cuda-12.6-docker",
        "ubuntu-24.04-cuda-12.8-open",
        "ubuntu-24.04-cuda-12.8-open-docker",
        "ubuntu-24.04-cuda-13.0-open",
        "ubuntu-24.04-cuda-13.0-open-docker"
      ]
    },
    {
      "best_for": [
        "Confidential Computing",
        "Secure Workloads"
      ],
      "cpu": {
        "description": "30 CPU",
        "number_of_cores": 30
      },
      "deploy_warning": null,
      "description": "Dedicated Hardware Instance",
      "gpu": {
        "description": "1x RTX PRO 6000 CC 96GB",
        "number_of_gpus": 1
      },
      "gpu_memory": {
        "description": "96GB GPU RAM",
        "size_in_gigabytes": 96
      },
      "id": "a7142910-939d-4f78-95e5-8614635f8bc1",
      "instance_type": "1RTXPRO6000.30V.CC",
      "memory": {
        "description": "90GB RAM",
        "size_in_gigabytes": 90
      },
      "model": "RTX PRO 6000 CC",
      "name": "RTX PRO 6000 CC 96GB",
      "p2p": null,
      "price_per_hour": "2.864",
      "spot_price": "1.432",
      "location_prices": [
        {
          "location_code": "FIN-01",
          "price_per_hour": "2.864",
          "spot_price": "1.432"
        },
        {
          "location_code": "FIN-02",
          "price_per_hour": "2.864",
          "spot_price": "1.432"
        },
        {
          "location_code": "FIN-03",
          "price_per_hour": "2.864",
          "spot_price": "1.432"
        }
      ],
      "dynamic_price": null,
      "max_dynamic_price": "2.864",
      "serverless_price": "0",
      "serverless_spot_price": "0",
      "storage": {
        "description": "dynamic"
      },
      "currency": "usd",
      "manufacturer": "NVIDIA",
      "display_name": null,
      "supported_os": [
        "24.04.cuda13.2.cc",
        "24.04.cuda13.4.cc",
        "26.04.cuda13.2.cc",
        "26.04.cuda13.4.cc"
      ]
    }
  ],
  "verda/volume_types": [
    {
      "type": "NVMe",
      "price": {
        "price_per_month_per_gb": 0.2,
        "cps_per_gb": 7.6103500761035E-8,
        "currency": "usd"
      },
      "is_shared_fs": false,
      "burst_bandwidth": 2500,
      "continuous_bandwidth": 2000,
      "internal_network_speed": 50,
      "iops": "100k",
      "throughput_gbps": 4
    },
    {
      "type": "HDD",
      "price": {
        "price_per_month_per_gb": 0.05,
        "cps_per_gb": 1.902587519025875E-8,
        "currency": "usd"
      },
      "is_shared_fs": false,
      "burst_bandwidth": 2000,
      "continuous_bandwidth": 250,
      "internal_network_speed": 50,
      "iops": "300",
      "throughput_gbps": 2
    }
  ]
}`
