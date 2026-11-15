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

const (
	verdaTestClientID = "verda-test-client"
	verdaTestSecret   = "verda-test-secret"
)

// verdaAPI stands in for Verda. It answers with the shapes of Verda's
// published API description (https://api.verda.com/v1/openapi.json). The
// instance types and volume prices are cut from Verda's public catalogue as
// read on 2026-11-13; which of them are in stock is made up, because Verda
// tells that to its customers only.
type verdaAPI struct {
	t      *testing.T
	server *httptest.Server

	mu     sync.Mutex
	now    time.Time
	nextID int
	// tokens counts the access tokens given out, and revoked those of them
	// that are no longer good.
	tokens    int
	revoked   int
	instances []map[string]any
	scripts   []map[string]any
	creates   []map[string]any
	actions   []map[string]any
	refusal   *verdaReply
	requests  []string

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
	api := &verdaAPI{t: t, now: time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /instance-types", func(w http.ResponseWriter, r *http.Request) {
		if api.catalogue != "" {
			w.Write([]byte(api.catalogue))
			return
		}
		w.Write(api.fixture("verda/instance_types.json"))
	})
	mux.HandleFunc("GET /volume-types", func(w http.ResponseWriter, r *http.Request) { w.Write(api.fixture("verda/volume_types.json")) })
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

func (a *verdaAPI) clock() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.now
}

func (a *verdaAPI) after(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.now = a.now.Add(d)
}

func (a *verdaAPI) fixture(name string) []byte {
	data, err := os.ReadFile("testdata/" + name)
	assert.NoError(a.t, err)
	return data
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
	body["id"] = fmt.Sprintf("4f74ddc3-0293-4f7e-80e6-%012d", a.nextID)
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
	assert.NoError(a.t, json.Unmarshal(a.fixture("verda/instance.json"), &instance))
	instance["id"] = fmt.Sprintf("0a6842d1-bc59-4e8d-abd7-%012d", a.nextID)
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

func (a *verdaAPI) lastCreate() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NotEmpty(a.t, a.creates)
	return a.creates[len(a.creates)-1]
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

func (a *verdaAPI) sent(method, path string) int {
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

func verdaOffer(instanceType, region, gpu string, cores, memoryGB, micros int64, spot bool) Offer {
	offer := Offer{
		ID: instanceType, Provider: "verda", Cloud: "verda", InstanceType: instanceType, Region: region,
		GPU: gpu, CPUMillicores: cores * 1000, MemoryMB: memoryGB * 1024, StorageMB: 200 * 1024,
		HourlyCostMicros: micros, Spot: spot,
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

	// Every price is the machine's plus $0.054795 an hour for its 200 GB
	// disk, which Verda sells apart at $0.20 a gigabyte a month. The spot
	// price is half the on-demand price.
	const disk = 54_795
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
