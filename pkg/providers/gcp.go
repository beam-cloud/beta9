package providers

import (
	"cmp"
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	gcpName     = "gcp"
	gcpBaseURL  = "https://compute.googleapis.com/compute/v1"
	gcpTokenURL = "https://oauth2.googleapis.com/token"
	gcpScope    = "https://www.googleapis.com/auth/compute"
	// Google takes a signed request for a token that is good for at most
	// an hour.
	gcpRequestLasts = time.Hour

	// Ubuntu 24.04 with the NVIDIA driver, from Canonical.
	gcpImage = "projects/ubuntu-os-accelerator-images/global/images/family/ubuntu-accelerator-2404-amd64-with-nvidia-580"
	// Compute Engine has no API for what a disk costs. This is the price
	// of a balanced disk in us-central1, $0.10 for a gigabyte for a month.
	gcpDiskMicrosPerGBMonth = 100_000

	gcpNetwork  = "global/networks/default"
	gcpPageSize = 500
	// The longest notice Google gives before it takes a Spot VM back.
	gcpNoticeSeconds = 120

	// A region's zones and machine types change rarely, and its Spot prices
	// at most once a day.
	gcpRegionFor = time.Hour
)

// Compute Engine answers a create before it has looked for a machine, and
// says a few seconds later whether it found one. A create waits this long
// for that answer, so that a zone with no stock is known at once.
var gcpCreateWaits = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}

// The names Compute Engine gives zones and instances.
var gcpNamePattern = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)

// GCP buys Compute Engine Spot VMs, which Google takes back with two
// minutes' notice.
//
// It sells nothing else here: Compute Engine tells the Spot price of a
// machine type and not its on-demand price. It does not say whether a zone
// has Spot capacity either, so every GPU machine type is offered in every
// zone that has it, and a create finds out.
//
// The machine types are the ones that come with their GPUs. A T4 or a V100
// is attached to a general-purpose machine and priced apart from it, and is
// not offered.
type GCP struct {
	// Google ends an instance only when asked, or when it takes a Spot VM
	// back.
	noDeletionTimer

	api *api
	// login talks to the service that gives out access tokens, which takes
	// no credentials.
	login *api
	// key is the service account's key file, and signer the key in it,
	// which the account signs in with.
	key     gcpKey
	signer  *rsa.PrivateKey
	owner   string
	config  types.GCPProviderConfig
	project string
	// now is the clock, which a test sets.
	now func() time.Time
	// regions holds the configured regions by name.
	regions map[string]*gcpRegion
}

// gcpKey is what this client reads of a service account's key file.
type gcpKey struct {
	ClientEmail  string `json:"client_email"`
	PrivateKey   string `json:"private_key"`
	PrivateKeyID string `json:"private_key_id"`
	ProjectID    string `json:"project_id"`
	TokenURI     string `json:"token_uri"`
}

// NewGCP makes a client that buys as owner: the name this control plane puts
// on the instances it buys. It fails when the service account's key is
// missing or cannot be read, or nothing names the project to buy in.
func NewGCP(config types.GCPProviderConfig, owner string) (*GCP, error) {
	if config.ServiceAccountKey == "" {
		return nil, errors.New("providers.gcp.serviceAccountKey is not set")
	}
	var key gcpKey
	if err := json.Unmarshal([]byte(config.ServiceAccountKey), &key); err != nil {
		return nil, fmt.Errorf("providers.gcp.serviceAccountKey is not a service account's JSON key: %w", err)
	}
	signer, err := rsaKey(key.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("providers.gcp.serviceAccountKey: %w", err)
	}
	key.TokenURI = cmp.Or(key.TokenURI, gcpTokenURL)
	project := cmp.Or(config.Project, key.ProjectID)
	if project == "" {
		return nil, errors.New("providers.gcp.project is not set, and the service account's key names no project")
	}
	if config.BaseURL == "" {
		config.BaseURL = gcpBaseURL
	}
	if config.Image == "" {
		config.Image = gcpImage
	}
	if config.DiskGB <= 0 {
		config.DiskGB = defaultDiskGB
	}
	c := &GCP{
		login:   newAPI(key.TokenURI, withoutCredentials()),
		key:     key,
		signer:  signer,
		owner:   owner,
		config:  config,
		project: project,
		now:     time.Now,
		regions: map[string]*gcpRegion{},
	}
	for _, region := range config.Regions {
		c.regions[region.Name] = &gcpRegion{config: region}
	}
	// The token reads the time from the client, whose clock a test sets.
	token := newAccessToken(func() time.Time { return c.now() }, c.signIn)
	c.api = newAPI(config.BaseURL, token.bearer())
	return c, nil
}

// signIn trades a request that the service account signs with its key for
// an access token.
func (c *GCP) signIn(ctx context.Context) (string, time.Duration, error) {
	request, err := signedJWT(c.signer, c.key.PrivateKeyID, map[string]any{
		"iss":   c.key.ClientEmail,
		"scope": gcpScope,
		"aud":   c.key.TokenURI,
		"iat":   c.now().Unix(),
		"exp":   c.now().Add(gcpRequestLasts).Unix(),
	})
	if err != nil {
		return "", 0, err
	}
	return grant(ctx, c.login, call{form: url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"assertion":  {request},
	}})
}

func (c *GCP) Name() string { return gcpName }

// do sends a call about the project. The path is the part after the
// project's.
func (c *GCP) do(ctx context.Context, call call) error {
	call.path = "/projects/" + url.PathEscape(c.project) + call.path
	return c.api.do(ctx, call)
}

// pages reads every page of one of Compute Engine's lists, and hands each
// page's items to read.
func (c *GCP) pages(ctx context.Context, path string, read func(items json.RawMessage) error) error {
	query := url.Values{"maxResults": {strconv.Itoa(gcpPageSize)}}
	for range maxPages {
		var page struct {
			Items         json.RawMessage `json:"items"`
			NextPageToken string          `json:"nextPageToken"`
		}
		if err := c.do(ctx, call{method: http.MethodGet, path: path + "?" + query.Encode(), out: &page}); err != nil {
			return err
		}
		if len(page.Items) > 0 {
			if err := read(page.Items); err != nil {
				return err
			}
		}
		if page.NextPageToken == "" {
			return nil
		}
		query.Set("pageToken", page.NextPageToken)
	}
	return tooManyPages("GET " + path)
}

// gcpRegion is a configured region with what Compute Engine says about it.
type gcpRegion struct {
	config types.GCPProviderRegionConfig
	known  cached[gcpRegionFacts]
}

type gcpRegionFacts struct {
	// zones holds the GPU machine types of each of the region's zones, by
	// name.
	zones map[string]map[string]gcpType
	// prices holds the hourly Spot price of a machine type, in micros.
	prices map[string]int64
}

type gcpType struct {
	gpu      string
	gpus     uint32
	vcpus    int64
	memoryMB int64
}

// configured finds a region by its name.
func (c *GCP) configured(name string) (*gcpRegion, error) {
	region, ok := c.regions[name]
	if !ok {
		return nil, fmt.Errorf("providers.gcp.regions does not name %q", name)
	}
	return region, nil
}

// region returns what a configured region's offers are made from.
func (c *GCP) region(ctx context.Context, name string) (gcpRegionFacts, error) {
	region, err := c.configured(name)
	if err != nil {
		return gcpRegionFacts{}, err
	}
	return region.known.get(c.now(), gcpRegionFor, func() (gcpRegionFacts, error) {
		facts, err := c.learn(ctx, name)
		if err != nil {
			return facts, fmt.Errorf("GCP region %s: %w", name, err)
		}
		return facts, nil
	})
}

// learn asks Compute Engine for what a region's offers are made from.
func (c *GCP) learn(ctx context.Context, region string) (gcpRegionFacts, error) {
	facts := gcpRegionFacts{zones: map[string]map[string]gcpType{}, prices: map[string]int64{}}
	var found struct {
		Zones []string `json:"zones"` // their addresses
	}
	if err := c.do(ctx, call{method: http.MethodGet, path: "/regions/" + url.PathEscape(region), out: &found}); err != nil {
		return facts, err
	}

	sold := map[string]bool{}
	for _, address := range found.Zones {
		zone := path.Base(address)
		facts.zones[zone] = map[string]gcpType{}
		err := c.pages(ctx, "/zones/"+url.PathEscape(zone)+"/machineTypes", func(items json.RawMessage) error {
			var machineTypes []gcpMachineType
			if err := json.Unmarshal(items, &machineTypes); err != nil {
				return err
			}
			for _, machineType := range machineTypes {
				if machine, ok := machineType.machine(); ok {
					facts.zones[zone][machineType.Name] = machine
					sold[machineType.Name] = true
				}
			}
			return nil
		})
		if err != nil {
			return facts, fmt.Errorf("the machine types of %s: %w", zone, err)
		}
	}

	var unpriced error
	for machineType := range sold {
		price, err := c.spotPrice(ctx, region, machineType)
		if errors.Is(err, errNoSpotPrice) {
			log.Debug().Str("provider", gcpName).Str("machine_type", machineType).Str("region", region).Err(err).Msg("provider has no Spot price for a machine type")
			unpriced = fmt.Errorf("%s has %w", machineType, err)
			continue
		}
		if err != nil {
			return facts, fmt.Errorf("the Spot price of %s: %w", machineType, err)
		}
		facts.prices[machineType] = price
	}
	// Some machine types are not sold as Spot VMs. When none of a region's
	// has a price, it is the question that is wrong, or the answer is not
	// one this client can read, and a region that silently offers nothing
	// would hide that.
	if len(facts.prices) == 0 && unpriced != nil {
		return facts, fmt.Errorf("none of its %d GPU machine types has a Spot price in dollars: %w", len(sold), unpriced)
	}
	return facts, nil
}

// errNoSpotPrice says that a machine type has no price to buy a Spot VM at.
var errNoSpotPrice = errors.New("no Spot price")

type gcpMachineType struct {
	Name         string `json:"name"`
	GuestCPUs    int64  `json:"guestCpus"`
	MemoryMB     int64  `json:"memoryMb"`
	Architecture string `json:"architecture"`
	Accelerators []struct {
		Type  string `json:"guestAcceleratorType"`
		Count int32  `json:"guestAcceleratorCount"`
	} `json:"accelerators"`
	Deprecated struct {
		State string `json:"state"`
	} `json:"deprecated"`
}

// machine reads what a machine type is, and whether it is one to buy: a
// machine for the image's kind of CPU that comes with whole NVIDIA GPUs.
func (t gcpMachineType) machine() (gcpType, bool) {
	retired := t.Deprecated.State != "" && t.Deprecated.State != "ACTIVE"
	if len(t.Accelerators) != 1 || t.Accelerators[0].Count <= 0 || t.Architecture == "ARM64" || retired {
		return gcpType{}, false
	}
	name := t.Accelerators[0].Type
	if !strings.HasPrefix(name, "nvidia-") {
		return gcpType{}, false
	}
	gpu := types.NormalizeGPUType(name)
	// Google's name for the A100 with 40 GB does not say so; the one with
	// 80 GB is called nvidia-a100-80gb.
	if gpu == types.GPU_A100 {
		gpu = types.GPU_A100_40
	}
	return gcpType{gpu: string(gpu), gpus: uint32(t.Accelerators[0].Count), vcpus: t.GuestCPUs, memoryMB: t.MemoryMB}, true
}

// spotPrice asks for the hourly Spot price of a machine type in a region,
// in micros. Compute Engine gives the prices it had in the past year, of
// which the last is the one in force. A machine type that is not sold as a
// Spot VM has none, and Compute Engine turns the question down.
func (c *GCP) spotPrice(ctx context.Context, region, machineType string) (int64, error) {
	var history struct {
		PriceHistory []gcpPrice `json:"priceHistory"`
	}
	err := c.do(ctx, call{
		method: http.MethodPost,
		path:   "/regions/" + url.PathEscape(region) + "/advice/capacityHistory",
		body: map[string]any{
			"types": []string{"PRICE"},
			"instanceProperties": map[string]any{
				"machineType": machineType,
				"scheduling":  map[string]any{"provisioningModel": "SPOT"},
			},
		},
		out: &history,
		// It asks a question, and buys nothing.
		repeatable: true,
	})
	if statusOf(err) == http.StatusBadRequest {
		return 0, because(errNoSpotPrice, err)
	}
	if err != nil {
		return 0, err
	}
	if len(history.PriceHistory) == 0 {
		return 0, because(errNoSpotPrice, errors.New("Compute Engine has no history of one"))
	}
	last := slices.MaxFunc(history.PriceHistory, func(a, b gcpPrice) int {
		return a.Interval.StartTime.Compare(b.Interval.StartTime)
	})
	micros := int64(last.ListPrice.Units)*1_000_000 + last.ListPrice.Nanos/1000
	if !strings.EqualFold(last.ListPrice.CurrencyCode, "usd") || micros <= 0 {
		return 0, because(errNoSpotPrice, fmt.Errorf("Compute Engine prices it at %d.%06d %s", micros/1_000_000, micros%1_000_000, last.ListPrice.CurrencyCode))
	}
	return micros, nil
}

// gcpPrice is the price a machine type had from a time on.
type gcpPrice struct {
	Interval struct {
		StartTime time.Time `json:"startTime"`
	} `json:"interval"`
	ListPrice struct {
		CurrencyCode string `json:"currencyCode"`
		Units        count  `json:"units"`
		Nanos        int64  `json:"nanos"`
	} `json:"listPrice"`
}

// gcpDiskType names the kind of disk a machine type boots from. The two
// older GPU series boot from Persistent Disk and not from Hyperdisk; the
// newest boot from Hyperdisk only, and the ones between from either.
func gcpDiskType(machineType string) string {
	if strings.HasPrefix(machineType, "a2-") || strings.HasPrefix(machineType, "g2-") {
		return "pd-balanced"
	}
	return "hyperdisk-balanced"
}

// Offers lists the GPU machine types that have a Spot price: one offer for
// each type and zone, since the stock is the zone's.
func (c *GCP) Offers(ctx context.Context, filter OfferFilter) ([]Offer, error) {
	offers, err := offersByRegion(gcpName, slices.Sorted(maps.Keys(c.regions)), filter, func(region string) ([]Offer, error) {
		return c.regionOffers(ctx, region)
	})
	if err != nil {
		return nil, err
	}
	sortOffers(offers)
	return offers, nil
}

// regionOffers lists what one region sells.
func (c *GCP) regionOffers(ctx context.Context, name string) ([]Offer, error) {
	facts, err := c.region(ctx, name)
	if err != nil {
		return nil, err
	}
	var offers []Offer
	for zone, machines := range facts.zones {
		for machineType, machine := range machines {
			price, priced := facts.prices[machineType]
			if !priced {
				continue
			}
			offers = append(offers, Offer{
				ID:               zone + "/" + machineType,
				Provider:         gcpName,
				Cloud:            gcpName,
				InstanceType:     machineType,
				Region:           name,
				GPU:              machine.gpu,
				GPUCount:         machine.gpus,
				CPUMillicores:    machine.vcpus * 1000,
				MemoryMB:         machine.memoryMB,
				StorageMB:        c.config.DiskGB * 1024,
				HourlyCostMicros: price + hourlyDiskMicros(c.config.DiskGB, gcpDiskMicrosPerGBMonth),
				Spot:             true,
			})
		}
	}
	return offers, nil
}

// Create buys a Spot VM at the price Google charges for one: there is no
// price to bid, so the request's price limit has nothing to be passed to.
// The offer is this client's own and is not looked up again: what Compute
// Engine no longer sells, it turns down.
func (c *GCP) Create(ctx context.Context, req CreateRequest) (Instance, error) {
	labels, err := ownedTags(c.owner, req.Tags)
	if err != nil {
		return Instance{}, err
	}
	zone, machineType, ok := strings.Cut(req.Offer.ID, "/")
	if !ok || zone == "" || machineType == "" || req.Offer.Region == "" {
		return Instance{}, fmt.Errorf("a GCP offer needs a zone, a machine type and a region: %+v", req.Offer)
	}
	if !req.Offer.Spot {
		return Instance{}, fmt.Errorf("this client buys Spot VMs from GCP, and the offer is not for one: %+v", req.Offer)
	}
	region, err := c.configured(req.Offer.Region)
	if err != nil {
		return Instance{}, err
	}

	// Compute Engine runs one create for a request ID however often it is
	// sent, so a create that got no answer may be sent again.
	query := url.Values{"requestId": {uuid.NewString()}}
	var started gcpOperation
	err = c.do(ctx, call{
		method:     http.MethodPost,
		path:       "/zones/" + url.PathEscape(zone) + "/instances?" + query.Encode(),
		body:       c.instanceBody(req, labels, zone, machineType, region.config.Subnetwork),
		out:        &started,
		repeatable: true,
	})
	if err != nil {
		return Instance{}, gcpRefusal(err)
	}
	if err := c.settled(ctx, zone, started).failure(); err != nil {
		return Instance{}, err
	}
	return Instance{ID: zone + "/" + req.Name, Status: StatusPending, Tags: req.Tags}, nil
}

// instanceBody describes the VM a create asks for: a Spot VM of the machine
// type, with a disk of its own and an address that can be reached from
// outside, in the subnetwork or else in the network named default.
func (c *GCP) instanceBody(req CreateRequest, labels map[string]string, zone, machineType, subnetwork string) map[string]any {
	network := map[string]any{
		// An address that can be reached from outside.
		"accessConfigs": []map[string]any{{"name": "External NAT", "type": "ONE_TO_ONE_NAT"}},
	}
	switch {
	case subnetwork == "":
		network["network"] = gcpNetwork
	case strings.Contains(subnetwork, "/"):
		network["subnetwork"] = subnetwork
	default:
		network["subnetwork"] = "regions/" + req.Offer.Region + "/subnetworks/" + subnetwork
	}
	instance := map[string]any{
		"name":        req.Name,
		"machineType": "zones/" + zone + "/machineTypes/" + machineType,
		"labels":      labels,
		"disks": []map[string]any{{
			"boot": true,
			// The disk goes when the machine does.
			"autoDelete": true,
			"initializeParams": map[string]any{
				"sourceImage": c.config.Image,
				"diskSizeGb":  strconv.FormatInt(c.config.DiskGB, 10),
				"diskType":    "zones/" + zone + "/diskTypes/" + gcpDiskType(machineType),
			},
		}},
		"networkInterfaces": []map[string]any{network},
		"scheduling": map[string]any{
			"provisioningModel": "SPOT",
			// A machine Google takes back is left stopped, which shows
			// that it was taken back, until it is deleted here.
			"instanceTerminationAction": "STOP",
			"automaticRestart":          false,
			// A machine with GPUs cannot be moved to another host.
			"onHostMaintenance":        "TERMINATE",
			"preemptionNoticeDuration": map[string]any{"seconds": strconv.Itoa(gcpNoticeSeconds)},
		},
	}
	if req.StartupScript != "" {
		instance["metadata"] = map[string]any{"items": []map[string]any{{"key": "startup-script", "value": bootScript(req.StartupScript)}}}
	}
	return instance
}

// settled waits for Compute Engine to say how an operation ended, for as
// long as a create waits, and returns the operation as it was last heard of.
// Past the create the machine may be bought, so a question that gets no
// answer leaves it to the next look at the machine.
func (c *GCP) settled(ctx context.Context, zone string, operation gcpOperation) gcpOperation {
	for _, wait := range gcpCreateWaits {
		if operation.Status == "DONE" || operation.Name == "" {
			break
		}
		if err := c.api.sleep(ctx, wait); err != nil {
			break
		}
		var now gcpOperation
		if err := c.do(ctx, call{method: http.MethodGet, path: "/zones/" + url.PathEscape(zone) + "/operations/" + url.PathEscape(operation.Name), out: &now}); err != nil {
			break
		}
		operation = now
	}
	return operation
}

// gcpOperation is something Compute Engine was asked to do, and how far it
// has got.
type gcpOperation struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  *struct {
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	} `json:"error"`
}

// failure returns why an operation failed, or nil.
func (o gcpOperation) failure() error {
	if o.Error == nil || len(o.Error.Errors) == 0 {
		return nil
	}
	first := o.Error.Errors[0]
	err := fmt.Errorf("GCP: %s: %s", first.Code, first.Message)
	for _, problem := range o.Error.Errors {
		switch {
		case strings.Contains(problem.Code, "RESOURCE_POOL_EXHAUSTED"):
			return because(ErrOutOfStock, err)
		case problem.Code == "QUOTA_EXCEEDED":
			return because(ErrQuota, err)
		}
	}
	return err
}

// gcpRefusal names why Compute Engine turned a create down at once.
func gcpRefusal(err error) error {
	var refused *StatusError
	if !errors.As(err, &refused) {
		return err
	}
	var body struct {
		Error struct {
			Errors []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	refused.decode(&body)
	for _, problem := range body.Error.Errors {
		if problem.Reason == "quotaExceeded" {
			return because(ErrQuota, err)
		}
	}
	return err
}

type gcpInstance struct {
	Name          string            `json:"name"`
	Zone          string            `json:"zone"` // its address
	Status        string            `json:"status"`
	StatusMessage string            `json:"statusMessage"`
	Labels        map[string]string `json:"labels"`
	Scheduling    struct {
		ProvisioningModel string `json:"provisioningModel"`
		Preemptible       bool   `json:"preemptible"`
	} `json:"scheduling"`
	NetworkInterfaces []struct {
		AccessConfigs []struct {
			NatIP string `json:"natIP"`
		} `json:"accessConfigs"`
	} `json:"networkInterfaces"`
}

func (c *GCP) instance(raw gcpInstance) Instance {
	instance := Instance{
		ID:     path.Base(raw.Zone) + "/" + raw.Name,
		Status: raw.status(),
		Tags:   callerTags(raw.Labels),
	}
	instance.StatusDetail = statusDetail(instance.Status, strings.TrimSpace(raw.Status+" "+raw.StatusMessage))
	for _, network := range raw.NetworkInterfaces {
		for _, access := range network.AccessConfigs {
			if instance.PublicIP == "" {
				instance.reachableAt(access.NatIP)
			}
		}
	}
	return instance
}

func (i gcpInstance) status() Status {
	switch i.Status {
	case "PROVISIONING", "STAGING", "PENDING":
		return StatusPending
	case "REPAIRING":
		// Google is mending it, and it may come back.
		return StatusPending
	case "RUNNING":
		return StatusActive
	case "STOPPING", "PENDING_STOP", "DEPROVISIONING":
		// Compute Engine does not say whether a machine that is stopping
		// was deleted or is being taken back. It is on its way out either
		// way. One that Google took back shows as much once it has
		// stopped, and is still there to delete.
		return StatusDeleted
	case "STOPPED", "TERMINATED", "SUSPENDING", "SUSPENDED":
		// Nothing here stops a machine. Google stops a Spot VM to take it
		// back.
		if i.Scheduling.ProvisioningModel == "SPOT" || i.Scheduling.Preemptible {
			return StatusInterrupted
		}
		return StatusFailed
	}
	return unknownStatus(gcpName, i.Status)
}

// gcpPlace splits an instance's ID here, "zone/name". An ID that is not one
// of this client's names nothing at GCP.
func gcpPlace(id string) (zone, name string, err error) {
	zone, name, _ = strings.Cut(id, "/")
	if !gcpNamePattern.MatchString(zone) || !gcpNamePattern.MatchString(name) {
		return "", "", because(ErrNotFound, fmt.Errorf("GCP instance %q is not a zone and a name", id))
	}
	return zone, name, nil
}

func (c *GCP) Get(ctx context.Context, id string) (Instance, error) {
	zone, name, err := gcpPlace(id)
	if err != nil {
		return Instance{}, err
	}
	var found gcpInstance
	err = c.do(ctx, call{method: http.MethodGet, path: "/zones/" + zone + "/instances/" + name, out: &found})
	if statusOf(err) == http.StatusNotFound {
		return Instance{}, because(ErrNotFound, err)
	}
	if err != nil {
		return Instance{}, err
	}
	return c.instance(found), nil
}

// List returns the instances this control plane bought, in every zone of
// the project.
func (c *GCP) List(ctx context.Context) ([]Instance, error) {
	instances := []Instance{}
	err := c.pages(ctx, "/aggregated/instances", func(items json.RawMessage) error {
		var zones map[string]struct {
			Instances []gcpInstance `json:"instances"`
		}
		if err := json.Unmarshal(items, &zones); err != nil {
			return err
		}
		for _, zone := range zones {
			for _, raw := range zone.Instances {
				if raw.Labels[OwnerTag] == c.owner {
					instances = append(instances, c.instance(raw))
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(instances, func(a, b Instance) int { return cmp.Compare(a.ID, b.ID) })
	return instances, nil
}

// Delete deletes the instance, and its disk goes with it. Only an instance
// Compute Engine no longer has is gone: one that is stopping may be one
// Google is taking back, which stays, stopped, until it is deleted.
func (c *GCP) Delete(ctx context.Context, id string) error {
	zone, name, err := gcpPlace(id)
	if err != nil {
		return nil
	}
	err = c.do(ctx, call{method: http.MethodDelete, path: "/zones/" + zone + "/instances/" + name, repeatable: true})
	if statusOf(err) == http.StatusNotFound {
		return nil
	}
	return err
}
