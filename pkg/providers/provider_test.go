package providers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

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
