package providers

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	vastName    = "vast"
	vastBaseURL = "https://console.vast.ai"

	// Vast's Ubuntu 22.04 with the NVIDIA driver and Docker, as a VM.
	vastImage = "docker.io/vastai/kvm:ubuntu_terminal"

	// The cheapest offers, when Vast has more.
	vastOffersLimit = 1000
	// Vast lists at most this many instances at a time.
	vastPageSize = 25
	// What Vast takes: a label of this many characters, and a startup
	// script of this many.
	vastLabelLimit  = 1024
	vastScriptLimit = 4048

	// Vast's names for the A100 and the V100 do not say how much memory
	// one has. A GPU with more than this, in MB, is the larger kind.
	vastA100LargeFromMB = 60_000
	vastV100LargeFromMB = 24_000
)

// Vast is a marketplace of other people's machines. This client rents its
// interruptible instances: the renter names a price, and has the machine
// until someone names a higher one or rents it on demand. Vast then stops
// the instance, without notice, and keeps its disk.
//
// It rents virtual machines only. Vast's usual instance is a container
// without the rights a worker needs to run containers of its own, and a
// small part of its machines can run a VM.
//
// The account needs an SSH key, added in Vast's console: Vast starts no VM
// without one, though nothing here logs in to a machine.
type Vast struct {
	// An instance lasts until the day its host's offer ends, which a renter
	// cannot move. Vast stops it then.
	noDeletionTimer

	api    *api
	owner  string
	config types.VastProviderConfig
}

// NewVast makes a client that rents as owner: the name this control plane
// puts on the instances it rents.
func NewVast(config types.VastProviderConfig, owner string) *Vast {
	// The address used to end in the version of the API, which each call
	// now names. A config written that way is still read.
	config.BaseURL = strings.TrimSuffix(strings.TrimRight(cmp.Or(config.BaseURL, vastBaseURL), "/"), "/api/v0")
	config.Image = cmp.Or(config.Image, vastImage)
	if config.DiskGB <= 0 {
		config.DiskGB = defaultDiskGB
	}
	return &Vast{
		api:    newAPI(config.BaseURL, withHeader("Authorization", "Bearer "+config.ApiKey)),
		owner:  owner,
		config: config,
	}
}

func (c *Vast) Name() string { return vastName }

// vastAsk is what a host asks for part of a machine: some of its GPUs, with
// their share of its CPUs, memory and disk.
type vastAsk struct {
	ID          int64   `json:"id"`
	MachineID   int64   `json:"machine_id"`
	GPUName     string  `json:"gpu_name"`
	GPUs        uint32  `json:"num_gpus"`
	GPUMemoryMB float64 `json:"gpu_ram"` // of one GPU
	CPUs        float64 `json:"cpu_cores_effective"`
	MemoryMB    float64 `json:"cpu_ram"`
	// MinBid is the lowest price the host takes for an interruptible
	// instance, in dollars an hour, without the disk.
	MinBid float64 `json:"min_bid"`
	// DollarsPerHour is the price with the disk that was asked about.
	DollarsPerHour float64 `json:"dph_total"`
	// Geolocation is a place and its country, as in "Quebec, CA".
	Geolocation string  `json:"geolocation"`
	Reliability float64 `json:"reliability2"`
}

// search asks Vast for what can be rented now as an interruptible VM, with
// this client's disk, cheapest first. The narrowing is added to the
// question.
func (c *Vast) search(ctx context.Context, limit int, narrowing map[string]any) ([]vastAsk, error) {
	is := func(value any) map[string]any { return map[string]any{"eq": value} }
	question := map[string]any{
		// Priced at the lowest bid the host takes.
		"type":        "bid",
		"verified":    is(true),
		"rentable":    is(true),
		"rented":      is(false),
		"vms_enabled": is(true),
		"gpu_arch":    is("nvidia"),
		"cpu_arch":    is("amd64"),
		"disk_space":  map[string]any{"gte": c.config.DiskGB},
		// The price is then for a machine with a disk of this size.
		"allocated_storage": c.config.DiskGB,
		"order":             [][]string{{"dph_total", "asc"}},
		"limit":             limit,
	}
	for field, value := range narrowing {
		question[field] = value
	}
	var found struct {
		Offers []vastAsk `json:"offers"`
	}
	err := c.api.do(ctx, call{method: http.MethodPost, path: "/api/v0/bundles/", body: question, out: &found, repeatable: true})
	return found.Offers, err
}

// Offers lists what Vast rents now as an interruptible VM. An offer is some
// of a machine's GPUs: Vast has no instance types, and the same machine is
// offered once for each number of GPUs it can give.
func (c *Vast) Offers(ctx context.Context, filter OfferFilter) ([]Offer, error) {
	asks, err := c.search(ctx, vastOffersLimit, nil)
	if err != nil {
		return nil, err
	}
	offers := make([]Offer, 0, len(asks))
	for _, ask := range asks {
		region := vastCountry(ask.Geolocation)
		price := DollarsToMicros(ask.DollarsPerHour)
		// Vast cannot place it, or does not say for how much it is.
		if region == "" || ask.GPUs == 0 || price <= 0 {
			continue
		}
		offers = append(offers, Offer{
			ID:           vastOfferID(ask.MachineID, ask.GPUs),
			Provider:     vastName,
			Cloud:        vastName,
			InstanceType: fmt.Sprintf("%dx %s", ask.GPUs, ask.GPUName),
			Region:       region,

			GPU:           vastGPU(ask.GPUName, ask.GPUMemoryMB),
			GPUCount:      ask.GPUs,
			CPUMillicores: int64(ask.CPUs * 1000),
			MemoryMB:      int64(ask.MemoryMB),
			StorageMB:     c.config.DiskGB * 1024,

			HourlyCostMicros: price,
			Spot:             true,
			// An ask is for one machine.
			Available:         1,
			Reliability:       ask.Reliability,
			RegionDisplayName: ask.Geolocation,
		})
	}
	offers = filter.matching(offers)
	sortOffers(offers)
	return offers, nil
}

// vastOfferID names an offer by the machine and the number of its GPUs.
// The number Vast gives an ask changes from one search to the next.
func vastOfferID(machine int64, gpus uint32) string {
	return fmt.Sprintf("%d/%d", machine, gpus)
}

func vastOfferOf(id string) (machine int64, gpus uint32, ok bool) {
	left, right, _ := strings.Cut(id, "/")
	machine, err := strconv.ParseInt(left, 10, 64)
	count, countErr := strconv.ParseUint(right, 10, 32)
	return machine, uint32(count), err == nil && countErr == nil && machine > 0 && count > 0
}

// vastCountry is the country of one of Vast's places, as its two letters:
// "CA" of "Quebec, CA". They are what Vast's regions are.
func vastCountry(geolocation string) string {
	return strings.TrimSpace(geolocation[strings.LastIndex(geolocation, ",")+1:])
}

// vastGPU gives a GPU Beam's name for it.
func vastGPU(name string, memoryMB float64) string {
	gpu := gpuName(name)
	switch gpu {
	case types.GPU_A100:
		gpu = types.GPU_A100_40
		if memoryMB > vastA100LargeFromMB {
			gpu = types.GPU_A100_80
		}
	case types.GPU_V100:
		if memoryMB > vastV100LargeFromMB {
			gpu = types.GPU_V100_32
		}
	}
	return string(gpu)
}

// vastLabel is the one text Vast keeps with an instance: its name, then its
// tags as words. A name has neither spaces nor equals signs.
func vastLabel(name string, tags map[string]string) string {
	return strings.Join(append([]string{name}, tagWords(tags)...), " ")
}

// vastTags reads the tags back from a label. A label someone else wrote has
// none.
func vastTags(label string) map[string]string {
	return wordTags(strings.Fields(label))
}

// Create rents the offer's GPUs at the lowest price their host takes now,
// which is the offer's price unless the host has changed it. On Vast the
// price named is the price paid, so nothing is added to it to keep the
// machine for longer. A host that has raised its price since the offer
// rents nothing: the buyer chose the offer for its price, and counts the
// machine at it.
func (c *Vast) Create(ctx context.Context, req CreateRequest) (Instance, error) {
	tags, err := ownedTags(c.owner, req.Tags)
	if err != nil {
		return Instance{}, err
	}
	machine, gpus, ok := vastOfferOf(req.Offer.ID)
	if !ok {
		return Instance{}, fmt.Errorf("a Vast offer needs a machine and a number of GPUs: %+v", req.Offer)
	}
	if !req.Offer.Spot {
		return Instance{}, fmt.Errorf("this client rents interruptible instances from Vast, and the offer is not for one: %+v", req.Offer)
	}
	label := vastLabel(req.Name, tags)
	if len(label) > vastLabelLimit {
		return Instance{}, fmt.Errorf("Vast keeps %d characters with an instance, and its name and tags are %d", vastLabelLimit, len(label))
	}
	instance := map[string]any{
		"image": c.config.Image,
		"vm":    true,
		"disk":  c.config.DiskGB,
		"label": label,
		// The only way Vast starts a VM.
		"runtype":      "ssh",
		"target_state": "running",
		// A bid that does not win the machine at once rents nothing. Vast
		// would otherwise keep an instance that waits for its turn.
		"cancel_unavail": true,
	}
	if req.StartupScript != "" {
		script := bootScript(req.StartupScript)
		if len(script) > vastScriptLimit {
			return Instance{}, fmt.Errorf("Vast takes a startup script of %d characters, and this one has %d", vastScriptLimit, len(script))
		}
		instance["onstart"] = script
	}

	ask, err := c.askNow(ctx, machine, gpus, req.Offer.HourlyCostMicros)
	if err != nil {
		return Instance{}, err
	}
	instance["price"] = ask.MinBid

	var rented struct {
		Success  bool   `json:"success"`
		Contract int64  `json:"new_contract"`
		Message  string `json:"msg"`
	}
	err = c.api.do(ctx, call{method: http.MethodPut, path: fmt.Sprintf("/api/v0/asks/%d/", ask.ID), body: instance, out: &rented})
	if err != nil {
		return Instance{}, vastRefusal(err)
	}
	if !rented.Success || rented.Contract <= 0 {
		return Instance{}, fmt.Errorf("Vast rented no instance: %s", cmp.Or(rented.Message, "it gave no reason"))
	}
	return Instance{ID: strconv.FormatInt(rented.Contract, 10), Status: StatusPending, Tags: req.Tags}, nil
}

// askNow returns what the host asks now for the GPUs of an offer that was
// priced at offered micros an hour. A host that no longer rents them, or
// that has raised its price since, has nothing to rent.
func (c *Vast) askNow(ctx context.Context, machine int64, gpus uint32, offered int64) (vastAsk, error) {
	asks, err := c.search(ctx, 1, map[string]any{
		"machine_id": map[string]any{"eq": machine},
		"num_gpus":   map[string]any{"eq": gpus},
	})
	if err != nil {
		return vastAsk{}, err
	}
	if len(asks) == 0 {
		return vastAsk{}, because(ErrOutOfStock, fmt.Errorf("Vast machine %d has no %d GPUs to rent as a VM now", machine, gpus))
	}
	if price := DollarsToMicros(asks[0].DollarsPerHour); price > offered {
		return vastAsk{}, because(ErrOutOfStock, fmt.Errorf("Vast machine %d now costs %d micros an hour, and was offered at %d", machine, price, offered))
	}
	return asks[0], nil
}

// vastProblem is the body of an answer Vast turned a call down with.
type vastProblem struct {
	Error   string `json:"error"`
	Message string `json:"msg"`
}

var errVastNoSSHKey = errors.New("Vast starts a VM only for an account with an SSH key; add one in Vast's console")

// vastRefusal names why Vast turned a create down.
func vastRefusal(err error) error {
	return refusal(err, func(answer *StatusError, refused vastProblem) error {
		said := strings.ToLower(refused.Error + " " + refused.Message)
		switch {
		// Someone else has the machine, or bid more for it, or its host now
		// asks more than was bid.
		case answer.Status == http.StatusGone, containsAny(said, "no_such_ask", "invalid_price"):
			return ErrOutOfStock
		case strings.Contains(said, "no_ssh_key_for_vm"):
			return errVastNoSSHKey
		// Vast is paid in advance. It does not document how it refuses an
		// account that has run out.
		case containsAny(said, "credit", "balance"):
			return ErrQuota
		}
		return nil
	})
}

// vastInstance is what Vast says of an instance.
type vastInstance struct {
	ID int64 `json:"id"`
	// ActualStatus is what the instance is doing. It is empty until Vast
	// knows.
	ActualStatus string `json:"actual_status"`
	// IntendedStatus and State are what Vast means the instance to be doing.
	IntendedStatus string `json:"intended_status"`
	State          string `json:"cur_state"`
	Message        string `json:"status_msg"`
	Label          string `json:"label"`
	// Bid marks an interruptible instance.
	Bid      bool   `json:"is_bid"`
	PublicIP string `json:"public_ipaddr"`
	SSHHost  string `json:"ssh_host"`
	SSHPort  int64  `json:"ssh_port"`
}

func (c *Vast) instance(raw vastInstance) Instance {
	instance := Instance{
		ID:       strconv.FormatInt(raw.ID, 10),
		Status:   raw.status(),
		PublicIP: strings.TrimSpace(raw.PublicIP),
		Tags:     callerTags(vastTags(raw.Label)),
	}
	instance.StatusDetail = statusDetail(instance.Status, strings.TrimSpace(raw.ActualStatus+" "+raw.Message))
	if raw.SSHHost != "" && raw.SSHPort > 0 {
		instance.SSHHost = raw.SSHHost
		instance.SSHPort = uint32(raw.SSHPort)
	}
	return instance
}

func (i vastInstance) status() Status {
	// Nothing here stops an instance. Vast stops an interruptible one when
	// someone bids more for its machine or rents it on demand, and starts
	// it again if the machine comes free, which a pool does not wait for.
	stopped := StatusFailed
	if i.Bid {
		stopped = StatusInterrupted
	}
	if i.IntendedStatus == "stopped" || i.State == "stopped" {
		return stopped
	}
	switch i.ActualStatus {
	case "", "created", "loading", "connecting", "rebooting", "recycling":
		return StatusPending
	case "running":
		return StatusActive
	case "exited", "scheduling":
		return stopped
	case "offline", "unknown":
		// Vast has lost the machine, or cannot tell what the instance is
		// doing: neither comes back.
		return StatusFailed
	}
	return unknownStatus(vastName, i.ActualStatus)
}

// vastInstanceID reads an instance's ID, which at Vast is a number.
func vastInstanceID(id string) (int64, bool) {
	number, err := strconv.ParseInt(id, 10, 64)
	return number, err == nil && number > 0
}

func (c *Vast) Get(ctx context.Context, id string) (Instance, error) {
	number, ok := vastInstanceID(id)
	if !ok {
		return Instance{}, because(ErrNotFound, fmt.Errorf("Vast instance %q is not a number", id))
	}
	var found struct {
		Instance *vastInstance `json:"instances"`
	}
	if err := c.api.do(ctx, call{method: http.MethodGet, path: fmt.Sprintf("/api/v0/instances/%d/", number), out: &found}); err != nil {
		return Instance{}, notFound(err)
	}
	// Vast answers about an instance it does not have with none.
	if found.Instance == nil || found.Instance.ID == 0 {
		return Instance{}, because(ErrNotFound, fmt.Errorf("Vast has no instance %s", id))
	}
	return c.instance(*found.Instance), nil
}

// List returns the instances this control plane rented.
func (c *Vast) List(ctx context.Context) ([]Instance, error) {
	instances := []Instance{}
	query := url.Values{"limit": {strconv.Itoa(vastPageSize)}}
	for range maxPages {
		var page struct {
			Instances []vastInstance `json:"instances"`
			NextToken string         `json:"next_token"`
		}
		if err := c.api.do(ctx, call{method: http.MethodGet, path: "/api/v1/instances/?" + query.Encode(), out: &page}); err != nil {
			return nil, err
		}
		for _, raw := range page.Instances {
			if vastTags(raw.Label)[OwnerTag] == c.owner {
				instances = append(instances, c.instance(raw))
			}
		}
		if page.NextToken == "" {
			slices.SortFunc(instances, func(a, b Instance) int { return cmp.Compare(a.ID, b.ID) })
			return instances, nil
		}
		query.Set("after_token", page.NextToken)
	}
	return nil, tooManyPages("GET /api/v1/instances/")
}

// Delete destroys the instance and its disk.
func (c *Vast) Delete(ctx context.Context, id string) error {
	number, ok := vastInstanceID(id)
	if !ok {
		return nil
	}
	var destroyed struct {
		Success *bool  `json:"success"`
		Message string `json:"msg"`
	}
	if err := unlessGone(c.api.do(ctx, call{method: http.MethodDelete, path: fmt.Sprintf("/api/v0/instances/%d/", number), out: &destroyed, repeatable: true})); err != nil {
		return err
	}
	if destroyed.Success != nil && !*destroyed.Success {
		return fmt.Errorf("Vast did not destroy instance %s: %s", id, cmp.Or(destroyed.Message, "it gave no reason"))
	}
	return nil
}
