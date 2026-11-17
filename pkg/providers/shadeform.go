package providers

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	shadeformName    = "shadeform"
	shadeformBaseURL = "https://api.shadeform.ai/v1"

	// Shadeform takes instance names of up to 50 characters.
	shadeformNameLimit = 50
	shadeformPageSize  = 100

	shadeformOnDemand = "on_demand"
	shadeformSpot     = "spot"

	// shadeformPreempted is the status detail of a spot instance that
	// Shadeform took back.
	shadeformPreempted = "preempted"
)

// Shadeform resells GPU machines from other clouds through one API.
type Shadeform struct {
	api   *api
	owner string
}

// NewShadeform makes a client that buys as owner: the name this control
// plane puts on the instances it buys.
func NewShadeform(config types.ShadeformProviderConfig, owner string) *Shadeform {
	baseURL := cmp.Or(config.BaseURL, shadeformBaseURL)
	return &Shadeform{api: newAPI(baseURL, withHeader("X-API-KEY", config.ApiKey)), owner: owner}
}

func (c *Shadeform) Name() string { return shadeformName }

// shadeformInstanceType is an entry of GET /instances/types.
type shadeformInstanceType struct {
	Cloud             string `json:"cloud"`
	ShadeInstanceType string `json:"shade_instance_type"`
	DeploymentType    string `json:"deployment_type"`
	HourlyPriceCents  int64  `json:"hourly_price"`
	Configuration     struct {
		GPUType     string `json:"gpu_type"`
		NumGPUs     uint32 `json:"num_gpus"`
		VCPUs       int64  `json:"vcpus"`
		MemoryInGB  int64  `json:"memory_in_gb"`
		StorageInGB int64  `json:"storage_in_gb"`
	} `json:"configuration"`
	// Availability has one entry per region and rental type, so a region
	// that also sells the type as spot appears twice. Only a spot entry has
	// a price of its own, and it is in dollars where the price of the type
	// is in cents.
	Availability []struct {
		Region      string  `json:"region"`
		Available   bool    `json:"available"`
		RentalType  string  `json:"rental_type"`
		HourlyPrice dollars `json:"hourly_price"`
	} `json:"availability"`
}

func (c *Shadeform) Offers(ctx context.Context, filter OfferFilter) ([]Offer, error) {
	// Each type is read by itself, so that one this client cannot read does
	// not hide what the others sell.
	var catalogue struct {
		InstanceTypes []json.RawMessage `json:"instance_types"`
	}
	if err := c.api.do(ctx, call{method: http.MethodGet, path: "/instances/types", out: &catalogue}); err != nil {
		return nil, err
	}

	offers := []Offer{}
	unread := []string{}
	for _, written := range catalogue.InstanceTypes {
		var instanceType shadeformInstanceType
		if err := json.Unmarshal(written, &instanceType); err != nil {
			unread = append(unread, fmt.Sprintf("%s: %v", instanceType.ShadeInstanceType, err))
			continue
		}
		// Only virtual machines launch on request; bare metal is ordered.
		if instanceType.DeploymentType != "vm" || instanceType.Configuration.NumGPUs == 0 {
			continue
		}
		for _, stock := range instanceType.Availability {
			price := instanceType.hourlyMicros(stock.RentalType, stock.HourlyPrice)
			if !stock.Available || price <= 0 {
				continue
			}
			offers = append(offers, Offer{
				ID:               instanceType.ShadeInstanceType,
				Provider:         shadeformName,
				Cloud:            instanceType.Cloud,
				InstanceType:     instanceType.ShadeInstanceType,
				Region:           stock.Region,
				GPU:              string(gpuName(instanceType.Configuration.GPUType)),
				GPUCount:         instanceType.Configuration.NumGPUs,
				CPUMillicores:    instanceType.Configuration.VCPUs * 1000,
				MemoryMB:         instanceType.Configuration.MemoryInGB * 1024,
				StorageMB:        instanceType.Configuration.StorageInGB * 1024,
				HourlyCostMicros: price,
				Spot:             stock.RentalType == shadeformSpot,
			})
		}
	}
	switch {
	case len(unread) == 0:
	case len(unread) == len(catalogue.InstanceTypes):
		return nil, fmt.Errorf("Shadeform's instance types could not be read: %s", unread[0])
	default:
		log.Warn().Str("provider", shadeformName).Strs("unread", unread).Msg("could not read some instance types; their offers are left out")
	}
	return filter.matching(offers), nil
}

// hourlyMicros is what the type costs an hour when rented this way: on
// demand the price of the type, which is in cents, and as spot the price of
// the region, which is in dollars. It is zero for a way of renting that this
// client does not buy, and when Shadeform has the type and does not say for
// how much.
func (t shadeformInstanceType) hourlyMicros(rentalType string, spotPrice dollars) int64 {
	switch rentalType {
	case shadeformOnDemand:
		return t.HourlyPriceCents * 10_000
	case shadeformSpot:
		return int64(spotPrice)
	}
	return 0
}

type shadeformCreate struct {
	Cloud               string               `json:"cloud"`
	Region              string               `json:"region"`
	ShadeInstanceType   string               `json:"shade_instance_type"`
	ShadeCloud          bool                 `json:"shade_cloud"`
	RentalType          string               `json:"rental_type,omitempty"`
	Name                string               `json:"name"`
	Tags                []string             `json:"tags"`
	LaunchConfiguration *shadeformLaunch     `json:"launch_configuration,omitempty"`
	AutoDelete          *shadeformAutoDelete `json:"auto_delete,omitempty"`
}

type shadeformLaunch struct {
	Type                string `json:"type"`
	ScriptConfiguration struct {
		Base64Script string `json:"base64_script"`
	} `json:"script_configuration"`
}

// shadeformAutoDelete carries only a date. A spend threshold would end a
// machine that is renewed past its first budget, and Shadeform never raises
// it; spend is the control plane's to enforce.
type shadeformAutoDelete struct {
	DateThreshold string `json:"date_threshold"`
}

// shadeformAutoDeleteAt has Shadeform delete the machine at until.
func shadeformAutoDeleteAt(until time.Time) *shadeformAutoDelete {
	return &shadeformAutoDelete{DateThreshold: until.UTC().Format(time.RFC3339)}
}

// Create buys the offer as it is listed, on demand or as spot. Shadeform
// takes no price to bid for a spot machine, so the request's price limit has
// nothing to be passed to.
func (c *Shadeform) Create(ctx context.Context, req CreateRequest) (Instance, error) {
	tags, err := ownedTags(c.owner, req.Tags)
	if err != nil {
		return Instance{}, err
	}
	if req.Offer.ID == "" || req.Offer.Cloud == "" || req.Offer.Region == "" {
		return Instance{}, fmt.Errorf("a Shadeform offer needs an instance type, cloud and region: %+v", req.Offer)
	}

	body := shadeformCreate{
		Cloud:             req.Offer.Cloud,
		Region:            req.Offer.Region,
		ShadeInstanceType: req.Offer.ID,
		ShadeCloud:        true,
		Name:              shortenName(req.Name, shadeformNameLimit),
		Tags:              tagWords(tags),
	}
	if req.Offer.Spot {
		// Shadeform never falls back to on-demand: without spot stock the
		// create is refused.
		body.RentalType = shadeformSpot
	}
	if req.StartupScript != "" {
		body.LaunchConfiguration = &shadeformLaunch{Type: "script"}
		body.LaunchConfiguration.ScriptConfiguration.Base64Script = base64.StdEncoding.EncodeToString([]byte(bootScript(req.StartupScript)))
	}
	if !req.Until.IsZero() {
		body.AutoDelete = shadeformAutoDeleteAt(req.Until)
	}

	// A create answers with the instance's ID and status. No more of the
	// answer is read, so that nothing else in it can fail a purchase.
	var created struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := c.api.do(ctx, call{method: http.MethodPost, path: "/instances/create", body: body, out: &created}); err != nil {
		return Instance{}, shadeformRefusal(err)
	}
	if created.ID == "" {
		return Instance{}, fmt.Errorf("Shadeform accepted the create without returning an instance id")
	}
	return Instance{ID: created.ID, Status: shadeformInstance{Status: created.Status}.status(), Tags: req.Tags}, nil
}

// shadeformInstance is the answer of GET /instances/{id}/info and an entry
// of GET /instances.
type shadeformInstance struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	StatusDetails string `json:"status_details"`
	// IP is an address, or on some clouds a DNS name. Empty until active.
	IP      string   `json:"ip"`
	SSHPort uint32   `json:"ssh_port"`
	Tags    []string `json:"tags"`
}

func (c *Shadeform) instance(raw shadeformInstance) Instance {
	instance := Instance{
		ID:           raw.ID,
		Status:       raw.status(),
		StatusDetail: raw.StatusDetails,
		SSHHost:      raw.IP,
		SSHPort:      raw.SSHPort,
		Tags:         callerTags(wordTags(raw.Tags)),
	}
	if net.ParseIP(raw.IP) != nil {
		instance.PublicIP = raw.IP
	}
	return instance
}

func (i shadeformInstance) status() Status {
	// A spot instance that Shadeform takes back goes to deleting and then
	// deleted, and says why in its details.
	if i.StatusDetails == shadeformPreempted {
		return StatusInterrupted
	}
	switch i.Status {
	case "creating", "pending_provider", "pending":
		return StatusPending
	case "active":
		return StatusActive
	case "error":
		return StatusFailed
	case "deleting", "deleted":
		return StatusDeleted
	}
	return unknownStatus(shadeformName, i.Status)
}

func (c *Shadeform) Get(ctx context.Context, id string) (Instance, error) {
	var raw shadeformInstance
	if err := c.api.do(ctx, call{method: http.MethodGet, path: "/instances/" + url.PathEscape(id) + "/info", out: &raw}); err != nil {
		return Instance{}, notFound(err)
	}
	raw.ID = cmp.Or(raw.ID, id)
	return c.instance(raw), nil
}

func (c *Shadeform) List(ctx context.Context) ([]Instance, error) {
	instances := []Instance{}
	for read := range maxPages {
		var page struct {
			Instances []shadeformInstance `json:"instances"`
		}
		path := fmt.Sprintf("/instances?limit=%d&offset=%d", shadeformPageSize, read*shadeformPageSize)
		if err := c.api.do(ctx, call{method: http.MethodGet, path: path, out: &page}); err != nil {
			return nil, err
		}
		for _, raw := range page.Instances {
			if wordTags(raw.Tags)[OwnerTag] == c.owner {
				instances = append(instances, c.instance(raw))
			}
		}
		if len(page.Instances) < shadeformPageSize {
			return instances, nil
		}
	}
	return nil, tooManyPages("GET /instances")
}

func (c *Shadeform) Extend(ctx context.Context, id string, until time.Time) error {
	body := struct {
		AutoDelete *shadeformAutoDelete `json:"auto_delete"`
	}{shadeformAutoDeleteAt(until)}
	return c.api.do(ctx, call{method: http.MethodPost, path: "/instances/" + url.PathEscape(id) + "/update", body: body, repeatable: true})
}

func (c *Shadeform) Delete(ctx context.Context, id string) error {
	return unlessGone(c.api.do(ctx, call{method: http.MethodPost, path: "/instances/" + url.PathEscape(id) + "/delete", body: struct{}{}, repeatable: true}))
}

// shadeformProblem is the body of an answer Shadeform turned a call down with.
type shadeformProblem struct {
	Error     string `json:"error"`
	ErrorCode string `json:"error_code"`
}

// shadeformRefusal names why a create was turned down. Shadeform answers a
// refusal with 409 and a reason, and publishes the shape of that answer but
// not its codes, so the reason is read from its words.
func shadeformRefusal(err error) error {
	return refusal(err, func(answer *StatusError, refused shadeformProblem) error {
		reason := strings.ToLower(refused.ErrorCode + " " + refused.Error)
		switch {
		case answer.Status != http.StatusConflict:
			return nil
		case containsAny(reason, "quota", "limit", "balance", "funds", "payment"):
			return ErrQuota
		case containsAny(reason, "capacity", "available", "availability", "stock"):
			return ErrOutOfStock
		}
		return nil
	})
}

// shortenName fits a name into limit characters. The end of a name tells
// machines apart, so the cut comes out of the middle.
func shortenName(name string, limit int) string {
	if len(name) <= limit {
		return name
	}
	tail := name[strings.LastIndex(name, "-")+1:]
	if len(tail) >= limit {
		return name[len(name)-limit:]
	}
	head := strings.TrimRight(name[:limit-len(tail)-1], "-")
	return head + "-" + tail
}
