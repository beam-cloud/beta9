package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const hetznerTestToken = "hetzner-test-token"

// hetznerAPI stands in for Hetzner Cloud. It answers with the shapes of
// Hetzner's published API description (https://docs.hetzner.cloud/cloud.spec.json).
type hetznerAPI struct {
	t      *testing.T
	server *httptest.Server

	mu       sync.Mutex
	nextID   int64
	servers  []map[string]any
	creates  []map[string]any
	refusal  *hetznerError
	requests []string
	// listsEverything makes the server list ignore the labels asked for.
	listsEverything bool
}

type hetznerError struct {
	status  int
	code    string
	message string
}

func newHetznerAPI(t *testing.T) *hetznerAPI {
	api := &hetznerAPI{t: t, nextID: 41}
	mux := http.NewServeMux()
	for path, fixture := range map[string]string{
		"/server_types": "hetzner/server_types.json",
		"/locations":    "hetzner/locations.json",
		"/networks":     "hetzner/networks.json",
	} {
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("page") != "1" || r.URL.Query().Get("per_page") != "50" {
				api.fail(w, hetznerError{http.StatusBadRequest, "invalid_input", "expected the first page of 50"})
				return
			}
			w.Write(api.fixture(fixture))
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

func (a *hetznerAPI) fixture(name string) []byte {
	data, err := os.ReadFile("testdata/" + name)
	assert.NoError(a.t, err)
	return data
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
	assert.NoError(a.t, json.Unmarshal(a.fixture("hetzner/server.json"), &server))
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

func (a *hetznerAPI) lastCreate() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NotEmpty(a.t, a.creates)
	return a.creates[len(a.creates)-1]
}

// sent returns the requests received since the last call.
func (a *hetznerAPI) sent() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	requests := a.requests
	a.requests = nil
	return requests
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
	require.Equal(t, []string{"GET /locations", "GET /server_types"}, api.sent())
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
	require.Empty(t, api.sent())
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
				require.NotContains(t, api.sent(), "POST /servers", "nothing is bought without a network")
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
	require.Equal(t, []string{"GET /locations", "GET /networks", "POST /servers"}, api.sent())

	now = now.Add(hetznerCacheFor - time.Second)
	create()
	require.Equal(t, []string{"POST /servers"}, api.sent())

	now = now.Add(time.Second)
	create()
	require.Equal(t, []string{"GET /locations", "GET /networks", "POST /servers"}, api.sent())
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
	require.Equal(t, []string{"GET /locations", "GET /networks", "GET /networks"}, api.sent())
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
	api.sent()

	listed, err := client.List(context.Background())
	require.NoError(t, err)
	require.Len(t, listed, 60)
	require.Equal(t, []string{"GET /servers", "GET /servers"}, api.sent())
}
