package providers

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	t      *testing.T
	server *httptest.Server

	mu     sync.Mutex
	now    time.Time
	nextID int
	// tokens counts the access tokens given out.
	tokens   int
	requests []string
	// slept is every wait the client asked for.
	slept []time.Duration

	// machineTypes are each zone's.
	machineTypes map[string][]map[string]any
	// prices are the price histories of each region's machine types.
	prices     map[string]map[string][]map[string]any
	subnets    []string
	instances  []map[string]any
	inserts    []map[string]any
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
		t:          t,
		now:        time.Date(2026, 11, 14, 4, 0, 0, 0, time.UTC),
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

func (a *gcpAPI) clock() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.now
}

func (a *gcpAPI) after(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.now = a.now.Add(d)
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
	a.inserts = append(a.inserts, body)

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

func (a *gcpAPI) lastInsert() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NotEmpty(a.t, a.inserts)
	return a.inserts[len(a.inserts)-1]
}

func (a *gcpAPI) sent(method, path string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	sent := 0
	for _, request := range a.requests {
		if strings.HasPrefix(request, method+" /compute/v1/projects/"+gcpTestProject+path) {
			sent++
		}
	}
	return sent
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
			insert := api.lastInsert()
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
			return api.lastInsert()["scheduling"].(map[string]any)["provisioningModel"] == "SPOT"
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
		HourlyCostMicros: micros + gcpTestDisk, Spot: true,
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
	}, api.lastInsert())
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
		disk := api.lastInsert()["disks"].([]any)[0].(map[string]any)["initializeParams"].(map[string]any)["diskType"].(string)
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

	network := func() map[string]any { return api.lastInsert()["networkInterfaces"].([]any)[0].(map[string]any) }
	_, err = client.Create(context.Background(), CreateRequest{Offer: gcpL4InA, Name: "beam-l4-a1b2c3d4"})
	require.NoError(t, err)
	require.Equal(t, "regions/us-central1/subnetworks/beam-workers", network()["subnetwork"])
	require.NotContains(t, network(), "network")
	params := api.lastInsert()["disks"].([]any)[0].(map[string]any)["initializeParams"].(map[string]any)
	require.Equal(t, "projects/beam-test/global/images/beam-worker-v7", params["sourceImage"])
	require.Equal(t, "500", params["diskSizeGb"])
	require.NotContains(t, api.lastInsert(), "metadata", "no script was asked for")

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
	require.Len(t, api.inserts, 1)
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
	require.Empty(t, api.inserts, "nothing was bought")
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
