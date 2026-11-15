package providers

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
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
	t      *testing.T
	server *httptest.Server

	mu     sync.Mutex
	now    time.Time
	nextID int
	// tokens counts the access tokens given out.
	tokens int
	// subnets are each project's.
	subnets   map[string][]string
	instances []map[string]any
	creates   []map[string]any
	policies  []map[string]any
	deletes   []string
	refusal   *nebiusReply
	requests  []string

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
		t:   t,
		now: time.Date(2026, 11, 14, 2, 30, 0, 0, time.UTC),
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
		api.page(w, r, api.fixture("nebius/spot_price.json"))
	})
	mux.HandleFunc("GET /capacity/v1/resource-advice", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("parentId") != nebiusTestTenant {
			api.fail(w, nebiusReply{status: http.StatusForbidden, code: 7, message: "permission denied"})
			return
		}
		api.page(w, r, api.fixture("nebius/resource_advice.json"))
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

func (a *nebiusAPI) clock() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.now
}

func (a *nebiusAPI) after(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.now = a.now.Add(d)
}

// fixture returns the items of a recorded list.
func (a *nebiusAPI) fixture(name string) []any {
	data, err := os.ReadFile("testdata/" + name)
	assert.NoError(a.t, err)
	var list struct {
		Items []any `json:"items"`
	}
	assert.NoError(a.t, json.Unmarshal(data, &list))
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
	a.instances[i]["status"].(map[string]any)["state"] = state
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

func (a *nebiusAPI) lastCreate() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NotEmpty(a.t, a.creates)
	return a.creates[len(a.creates)-1]
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

func (a *nebiusAPI) sent(method, path string) int {
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
			create := api.lastCreate()
			script, _ := create["spec"].(map[string]any)["cloudInitUserData"].(string)
			return create["metadata"].(map[string]any)["name"].(string), script
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
		HourlyCostMicros: 3_380_000 + nebiusTestDisk, Spot: true, Available: 2,
	}
	nebiusH200x8 = Offer{
		ID: "gpu-h200-sxm/8gpu-128vcpu-1600gb", Provider: "nebius", Cloud: "nebius", InstanceType: "gpu-h200-sxm/8gpu-128vcpu-1600gb",
		Region: "eu-north1", GPU: "H200", GPUCount: 8, CPUMillicores: 128_000, MemoryMB: 1600 * 1024, StorageMB: 200 * 1024,
		HourlyCostMicros: 8*3_380_000 + nebiusTestDisk, Spot: true, Available: 1,
	}
	nebiusRTXPro6000 = Offer{
		ID: "gpu-rtx6000/1gpu-24vcpu-218gb", Provider: "nebius", Cloud: "nebius", InstanceType: "gpu-rtx6000/1gpu-24vcpu-218gb",
		Region: "us-central1", GPU: "RTXPro6000", GPUCount: 1, CPUMillicores: 24_000, MemoryMB: 218 * 1024, StorageMB: 200 * 1024,
		HourlyCostMicros: 1_789_000 + nebiusTestDisk, Spot: true, Available: 2,
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
			"cloudInitUserData": "#!/usr/bin/env bash\nset -euo pipefail\ncurl -fsSL https://gateway.example/join | sh\n",
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
	want.StorageMB, want.HourlyCostMicros = 500*1024, 3_380_000+48_630
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
