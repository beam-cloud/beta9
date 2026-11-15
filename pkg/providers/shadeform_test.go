package providers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const shadeformTestKey = "shadeform-test-key"

// shadeformAPI stands in for Shadeform. It answers with the shapes of
// Shadeform's published API description (https://api.shadeform.ai/v1/openapi.json).
type shadeformAPI struct {
	t      *testing.T
	server *httptest.Server

	mu        sync.Mutex
	nextID    int
	instances []map[string]any
	creates   []map[string]any
	updates   []map[string]any
	refusal   map[string]string
	requests  []string
	// catalogue, when set, is listed in place of the recorded instance
	// types.
	catalogue string
}

func newShadeformAPI(t *testing.T) *shadeformAPI {
	api := &shadeformAPI{t: t}
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
	w.Write(a.fixture("shadeform/instance_types.json"))
}

// fixture reads a recorded answer. It runs on the server's goroutine, where a
// test may be failed but not stopped.
func (a *shadeformAPI) fixture(name string) []byte {
	data, err := os.ReadFile("testdata/" + name)
	assert.NoError(a.t, err)
	return data
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
	assert.NoError(a.t, json.Unmarshal(a.fixture("shadeform/instance.json"), &instance))
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

func (a *shadeformAPI) forgetRequests() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = nil
}

func (a *shadeformAPI) lose(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if i := a.find(id); i >= 0 {
		a.instances = slices.Delete(a.instances, i, i+1)
	}
}

func (a *shadeformAPI) lastCreate() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NotEmpty(a.t, a.creates)
	return a.creates[len(a.creates)-1]
}

func (a *shadeformAPI) sent() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.requests)
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
	require.Equal(t, []string{"GET /instances/types"}, api.sent())
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
	require.Empty(t, api.sent())
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
	api.forgetRequests()

	listed, err := client.List(context.Background())
	require.NoError(t, err)
	require.Len(t, listed, 120)
	require.Equal(t, []string{"GET /instances?limit=100&offset=0", "GET /instances?limit=100&offset=100"}, api.sent())
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
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
