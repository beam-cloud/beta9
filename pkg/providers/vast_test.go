package providers

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const vastTestKey = "vast-test-key"

// vastAPI stands in for Vast.
//
// What it rents out is recorded: testdata/vast/bundles.json is part of what
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
	t      *testing.T
	server *httptest.Server

	mu       sync.Mutex
	nextID   int64
	requests []string
	// searches and creates are what was asked, in order.
	searches []map[string]any
	creates  []map[string]any
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
	api := &vastAPI{t: t, nextID: 31337000, sshKeys: 1}

	recorded, err := os.ReadFile("testdata/vast/bundles.json")
	require.NoError(t, err)
	var catalogue struct {
		Offers []map[string]any `json:"offers"`
	}
	require.NoError(t, json.Unmarshal(recorded, &catalogue))
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

func (a *vastAPI) lastCreate() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NotEmpty(a.t, a.creates)
	return a.creates[len(a.creates)-1]
}

func (a *vastAPI) lastSearch() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	require.NotEmpty(a.t, a.searches)
	return a.searches[len(a.searches)-1]
}

func (a *vastAPI) sent(method, path string) int {
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
		"cancel_unavail": true,
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

	// Vast would rent an instance that waits, stopped, for the other bid to
	// go away. The pool wants a machine now or none.
	t.Run("someone bids more for it", func(t *testing.T) {
		api := newVastAPI(t)
		api.set(func() { api.outbidAt = 0.45 })
		client := api.client(testOwner)
		_, err := client.Create(ctx, request)
		require.ErrorIs(t, err, ErrOutOfStock)
		listed, err := client.List(ctx)
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
		require.EqualError(t, err, "Vast rented no instance: the machine is in maintenance")

		api.set(func() { api.refusal = &vastReply{http.StatusOK, map[string]any{"success": true}} })
		_, err = api.client(testOwner).Create(ctx, request)
		require.EqualError(t, err, "Vast rented no instance: it gave no reason")
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
		"its rental is stopped before Vast knows what it does": {
			fields: map[string]any{"cur_state": "stopped", "actual_status": nil},
			want:   Instance{Status: StatusInterrupted},
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
