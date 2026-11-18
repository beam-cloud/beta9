package providers

import (
	"cmp"
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	nebiusName     = "nebius"
	nebiusBaseURL  = "https://api.nebius.cloud"
	nebiusTokenURL = "https://auth.eu.nebius.com/oauth2/token/exchange"

	// Who the signed request for an access token is addressed to, and how
	// long it is good for. It is used at once.
	nebiusTokenAudience = "token-service.iam.new.nebiuscloud.net"
	nebiusRequestLasts  = time.Minute

	// Ubuntu with the NVIDIA driver. Every GPU platform of Nebius boots it.
	nebiusImageFamily = "ubuntu24.04-cuda13.0"
	nebiusDiskType    = "NETWORK_SSD"
	// Nebius has no API for what a disk costs. This is its price list as
	// read on 2026-11-13: $0.071 for a gibibyte of network SSD for a month
	// of 730 hours.
	nebiusDiskMicrosPerGBMonth = 71_000

	nebiusPageSize = 100

	// A project's region and tenant do not change, and the subnet of a
	// project that has one seldom does. They are read again every hour so
	// that a subnet that was replaced is noticed. What could not be read is
	// not kept at all, and is asked for again the next time.
	nebiusProjectsFor = time.Hour

	// A billing policy is created asynchronously. Wait for it to become
	// active before Compute can use it, without holding a reservation open
	// indefinitely if billing never finishes the change.
	nebiusPolicyReadyTimeout = 45 * time.Second
)

var nebiusPolicyWaits = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second}
var nebiusPolicyPropagationWaits = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}

// Nebius buys preemptible VMs from Nebius, which takes them back with a
// minute's notice and leaves them stopped.
//
// It sells nothing else here: Nebius has an API for its spot prices and none
// for the prices of its regular VMs. A platform whose spot price Nebius does
// not publish, such as the L40S ones it bills by the part, is not offered.
type Nebius struct {
	// Nebius deletes an instance only when asked.
	noDeletionTimer

	api *api
	// login talks to the service that gives out access tokens, which takes
	// no credentials.
	login *api
	// key is the service account's, which it signs in with.
	key    *rsa.PrivateKey
	owner  string
	config types.NebiusProviderConfig
	// now is the clock, which a test sets.
	now func() time.Time

	projects cached[[]nebiusProject]
}

// NewNebius makes a client that buys as owner: the name this control plane
// puts on the instances it buys. It fails when part of what the service
// account signs in with is missing, or its key cannot be read.
func NewNebius(config types.NebiusProviderConfig, owner string) (*Nebius, error) {
	for _, needed := range []struct{ key, value string }{
		{"serviceAccountId", config.ServiceAccountID},
		{"publicKeyId", config.PublicKeyID},
		{"privateKey", config.PrivateKey},
	} {
		if needed.value == "" {
			return nil, fmt.Errorf("providers.nebius.%s is not set", needed.key)
		}
	}
	key, err := rsaKey(config.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("providers.nebius.privateKey: %w", err)
	}
	config.BaseURL = cmp.Or(config.BaseURL, nebiusBaseURL)
	config.TokenURL = cmp.Or(config.TokenURL, nebiusTokenURL)
	config.ImageFamily = cmp.Or(config.ImageFamily, nebiusImageFamily)
	if config.DiskGB <= 0 {
		config.DiskGB = defaultDiskGB
	}
	c := &Nebius{
		login:  newAPI(config.TokenURL, withoutCredentials()),
		key:    key,
		owner:  owner,
		config: config,
		now:    time.Now,
	}
	// The token reads the time from the client, whose clock a test sets.
	token := newAccessToken(func() time.Time { return c.now() }, c.signIn)
	c.api = newAPI(config.BaseURL, token.bearer())
	return c, nil
}

// signIn trades a request that the service account signs with its key for
// an access token.
func (c *Nebius) signIn(ctx context.Context) (string, time.Duration, error) {
	request, err := signedJWT(c.key, c.config.PublicKeyID, map[string]any{
		"iss": c.config.ServiceAccountID,
		"sub": c.config.ServiceAccountID,
		"aud": []string{nebiusTokenAudience},
		"iat": c.now().Unix(),
		"exp": c.now().Add(nebiusRequestLasts).Unix(),
	})
	if err != nil {
		return "", 0, err
	}
	return grant(ctx, c.login, call{form: url.Values{
		"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
		"subject_token":        {request},
		"subject_token_type":   {"urn:ietf:params:oauth:token-type:jwt"},
	}})
}

func (c *Nebius) Name() string { return nebiusName }

// nebiusPages reads every page of one of Nebius's lists: what the parent
// holds, for a list of things that have one.
func nebiusPages[T any](ctx context.Context, api *api, path, parent string) ([]T, error) {
	query := url.Values{"pageSize": {strconv.Itoa(nebiusPageSize)}}
	if parent != "" {
		query.Set("parentId", parent)
	}
	var all []T
	for range maxPages {
		var page struct {
			Items         []T    `json:"items"`
			NextPageToken string `json:"nextPageToken"`
		}
		if err := api.do(ctx, call{method: http.MethodGet, path: path + "?" + query.Encode(), out: &page}); err != nil {
			return nil, err
		}
		all = append(all, page.Items...)
		if page.NextPageToken == "" {
			return all, nil
		}
		query.Set("pageToken", page.NextPageToken)
	}
	return nil, tooManyPages("GET " + path)
}

// nebiusProject is a configured project with what Nebius says about it.
type nebiusProject struct {
	id     string
	region string
	// tenant is the account the project belongs to.
	tenant string
	subnet string
}

var errNoNebiusProject = errors.New("providers.nebius.projects names no project to buy in")

func (c *Nebius) knownProjects(ctx context.Context) ([]nebiusProject, error) {
	return c.projects.get(c.now(), nebiusProjectsFor, func() ([]nebiusProject, error) {
		if len(c.config.Projects) == 0 {
			return nil, errNoNebiusProject
		}
		var projects []nebiusProject
		for _, configured := range c.config.Projects {
			var found struct {
				Metadata struct {
					ParentID string `json:"parentId"`
				} `json:"metadata"`
				Spec struct {
					Region string `json:"region"`
				} `json:"spec"`
				Status struct {
					Region string `json:"region"`
				} `json:"status"`
			}
			if err := c.api.do(ctx, call{method: http.MethodGet, path: "/iam/v1/projects/" + url.PathEscape(configured.ID), out: &found}); err != nil {
				return nil, fmt.Errorf("Nebius project %q: %w", configured.ID, err)
			}
			project := nebiusProject{
				id:     configured.ID,
				region: cmp.Or(found.Spec.Region, found.Status.Region),
				tenant: found.Metadata.ParentID,
				subnet: configured.SubnetID,
			}
			if project.region == "" || project.tenant == "" {
				return nil, fmt.Errorf("Nebius did not say which region and tenant project %q is in", configured.ID)
			}
			if at := slices.IndexFunc(projects, func(other nebiusProject) bool { return other.region == project.region }); at >= 0 {
				return nil, fmt.Errorf("Nebius projects %q and %q are both in %s: name one project for a region", projects[at].id, project.id, project.region)
			}
			if project.subnet == "" {
				var err error
				if project.subnet, err = c.onlySubnet(ctx, project.id); err != nil {
					return nil, err
				}
			}
			projects = append(projects, project)
		}
		return projects, nil
	})
}

// onlySubnet returns the subnet of a project that has one.
func (c *Nebius) onlySubnet(ctx context.Context, project string) (string, error) {
	type subnet struct {
		Metadata struct {
			ID string `json:"id"`
		} `json:"metadata"`
	}
	subnets, err := nebiusPages[subnet](ctx, c.api, "/vpc/v1/subnets", project)
	if err != nil {
		return "", fmt.Errorf("the subnets of Nebius project %q: %w", project, err)
	}
	if len(subnets) != 1 {
		return "", fmt.Errorf("Nebius project %q has %d subnets: name the one to use as subnetId under providers.nebius.projects", project, len(subnets))
	}
	return subnets[0].Metadata.ID, nil
}

// nebiusSpotPrice is what one GPU of a platform costs per hour in a region,
// as a preemptible VM, now. Nebius changes it as often as every 15 minutes.
type nebiusSpotPrice struct {
	// ProductTitle names the GPU, such as "Preemptible NVIDIA® H200 NVLink".
	// The platform's ID does not: gpu-rtx6000 is the RTX PRO 6000.
	ProductTitle    string `json:"productTitle"`
	Region          string `json:"region"`
	ComputeInstance struct {
		Platform string `json:"platform"`
	} `json:"computeInstance"`
	Price struct {
		Value    dollars `json:"value"`
		Currency string  `json:"currency"`
	} `json:"price"`
}

// nebiusAdvice is how many VMs of one preset of a platform the account can
// start now in a region: what Nebius has, capped by the account's quota.
type nebiusAdvice struct {
	Spec struct {
		Region          string `json:"region"`
		ComputeInstance struct {
			Platform string `json:"platform"`
			Preset   struct {
				Name      string `json:"name"`
				Resources struct {
					GPUs      count `json:"gpuCount"`
					VCPUs     count `json:"vcpuCount"`
					MemoryGiB count `json:"memoryGibibytes"`
				} `json:"resources"`
			} `json:"preset"`
		} `json:"computeInstance"`
	} `json:"spec"`
	Status struct {
		Preemptible struct {
			Available count `json:"available"`
		} `json:"preemptible"`
	} `json:"status"`
}

// nebiusMarket is what Nebius sets one spot price for: a platform in a
// region.
type nebiusMarket struct{ region, platform string }

// spotPrices asks what a GPU of each platform costs now, in each region. A
// price that is none, or is not in dollars, is left out.
func (c *Nebius) spotPrices(ctx context.Context) (map[nebiusMarket]nebiusSpotPrice, error) {
	prices, err := nebiusPages[nebiusSpotPrice](ctx, c.api, "/billing/v1alpha1/spot-price", "")
	if err != nil {
		return nil, err
	}
	priced := map[nebiusMarket]nebiusSpotPrice{}
	for _, price := range prices {
		if strings.EqualFold(price.Price.Currency, "usd") && price.Price.Value > 0 {
			priced[nebiusMarket{price.Region, price.ComputeInstance.Platform}] = price
		}
	}
	return priced, nil
}

// offerOf makes an offer of what Nebius advises the account it can start:
// a preset of a platform, at the price of the platform's GPUs.
func (c *Nebius) offerOf(advised nebiusAdvice, price nebiusSpotPrice) Offer {
	machine, preset := advised.Spec.ComputeInstance, advised.Spec.ComputeInstance.Preset
	id := machine.Platform + "/" + preset.Name
	return Offer{
		ID:               id,
		Provider:         nebiusName,
		Cloud:            nebiusName,
		InstanceType:     id,
		Region:           advised.Spec.Region,
		GPU:              string(gpuName(price.ProductTitle)),
		GPUCount:         uint32(preset.Resources.GPUs),
		CPUMillicores:    int64(preset.Resources.VCPUs) * 1000,
		MemoryMB:         int64(preset.Resources.MemoryGiB) * 1024,
		StorageMB:        c.config.DiskGB * 1024,
		HourlyCostMicros: int64(price.Price.Value)*int64(preset.Resources.GPUs) + hourlyDiskMicros(c.config.DiskGB, nebiusDiskMicrosPerGBMonth),
		Spot:             true,
		Available:        uint32(advised.Status.Preemptible.Available),
	}
}

// Offers lists the preemptible VMs the account can start now in the regions
// of its projects: one offer for each preset of a platform and region.
func (c *Nebius) Offers(ctx context.Context, filter OfferFilter) ([]Offer, error) {
	projects, err := c.knownProjects(ctx)
	if err != nil {
		return nil, err
	}
	priced, err := c.spotPrices(ctx)
	if err != nil {
		return nil, err
	}

	// Nebius counts stock for a tenant, and by the part of a region the
	// machines stand in. A preset is offered once, with the most that any
	// part has.
	offers := map[string]Offer{}
	var tenants []string
	for _, project := range projects {
		if slices.Contains(tenants, project.tenant) {
			continue
		}
		tenants = append(tenants, project.tenant)
		advice, err := nebiusPages[nebiusAdvice](ctx, c.api, "/capacity/v1/resource-advice", project.tenant)
		if err != nil {
			return nil, err
		}
		for _, advised := range advice {
			machine := advised.Spec.ComputeInstance
			price, sold := priced[nebiusMarket{advised.Spec.Region, machine.Platform}]
			buysIn := slices.ContainsFunc(projects, func(p nebiusProject) bool {
				return p.region == advised.Spec.Region && p.tenant == project.tenant
			})
			if !sold || !buysIn || advised.Status.Preemptible.Available <= 0 || machine.Preset.Resources.GPUs <= 0 {
				continue
			}
			offer := c.offerOf(advised, price)
			key := offer.Region + "/" + offer.ID
			offer.Available = max(offer.Available, offers[key].Available)
			offers[key] = offer
		}
	}

	matching := filter.matching(slices.Collect(maps.Values(offers)))
	sortOffers(matching)
	return matching, nil
}

func (c *Nebius) Create(ctx context.Context, req CreateRequest) (Instance, error) {
	tags, err := ownedTags(c.owner, req.Tags)
	if err != nil {
		return Instance{}, err
	}
	platform, preset, ok := strings.Cut(req.Offer.ID, "/")
	if !ok || platform == "" || preset == "" || req.Offer.Region == "" {
		return Instance{}, fmt.Errorf("a Nebius offer needs a platform, a preset and a region: %+v", req.Offer)
	}
	if !req.Offer.Spot {
		return Instance{}, fmt.Errorf("this client buys preemptible VMs from Nebius, and the offer is not for one: %+v", req.Offer)
	}
	project, err := c.projectIn(ctx, req.Offer.Region)
	if err != nil {
		return Instance{}, err
	}
	// A price limit is a pricing policy at Nebius, found or made before the
	// VM that is to have it.
	policy := ""
	if req.MaxHourlyCostMicros > 0 {
		if policy, err = c.priceLimit(ctx, project, platform, req.Offer.GPUCount, req.MaxHourlyCostMicros); err != nil {
			return Instance{}, err
		}
	}

	body := map[string]any{
		"metadata": map[string]any{"parentId": project.id, "name": req.Name, "labels": tags},
		"spec":     c.spec(req, platform, preset, project.subnet, policy),
	}
	var started nebiusOperation
	for attempt := 0; ; attempt++ {
		err := c.api.do(ctx, call{method: http.MethodPost, path: "/compute/v1/instances", body: body, out: &started})
		if err == nil {
			break
		}
		// Even an active billing policy can take a moment to reach Compute.
		// Retry only the exact validation that proves this request was
		// rejected before creating anything. An unanswered create, another
		// validation, or an operation that started must never be repeated.
		if attempt == len(nebiusPolicyPropagationWaits) || !nebiusPolicyNotVisible(err, policy) {
			return Instance{}, nebiusRefusal(err)
		}
		if err := c.api.sleep(ctx, nebiusPolicyPropagationWaits[attempt]); err != nil {
			return Instance{}, err
		}
	}
	if err := started.failure(); err != nil {
		c.deleteLeftover(ctx, started.ResourceID)
		return Instance{}, err
	}
	if started.ResourceID == "" {
		return Instance{}, fmt.Errorf("Nebius accepted the create without returning an instance id")
	}
	return Instance{ID: started.ResourceID, Status: StatusPending, Tags: req.Tags}, nil
}

// deleteLeftover deletes what a create that failed left, if it left
// anything: Nebius can turn a create down and leave a stopped VM, whose disk
// is paid for.
func (c *Nebius) deleteLeftover(ctx context.Context, id string) {
	if id == "" {
		return
	}
	if err := c.Delete(ctx, id); err != nil {
		log.Warn().Err(err).Str("provider", nebiusName).Str("instance", id).Msg("could not delete what a failed create left")
	}
}

// projectIn returns the project that machines in a region are bought in.
func (c *Nebius) projectIn(ctx context.Context, region string) (nebiusProject, error) {
	projects, err := c.knownProjects(ctx)
	if err != nil {
		return nebiusProject{}, err
	}
	at := slices.IndexFunc(projects, func(p nebiusProject) bool { return p.region == region })
	if at < 0 {
		return nebiusProject{}, fmt.Errorf("providers.nebius.projects names no project in %s", region)
	}
	return projects[at], nil
}

// spec describes the VM a create asks for: a preemptible one of the preset,
// with a disk of its own and a public address, in the subnet. With a pricing
// policy, Nebius takes the VM back when it would cost more than the policy
// allows. Without one, the VM pays the spot price whatever it rises to.
func (c *Nebius) spec(req CreateRequest, platform, preset, subnet, policy string) map[string]any {
	empty := struct{}{}
	spec := map[string]any{
		"resources": map[string]string{"platform": platform, "preset": preset},
		"bootDisk": map[string]any{
			"attachMode": "READ_WRITE",
			// A managed disk is deleted with its instance.
			"managedDisk": map[string]any{
				"name": req.Name,
				"spec": map[string]any{
					"sizeGibibytes":     strconv.FormatInt(c.config.DiskGB, 10),
					"type":              nebiusDiskType,
					"sourceImageFamily": map[string]string{"imageFamily": c.config.ImageFamily},
				},
			},
		},
		"networkInterfaces": []any{map[string]any{
			"name":            "eth0",
			"subnetId":        subnet,
			"ipAddress":       empty,
			"publicIpAddress": empty,
		}},
		// The only choices Nebius gives a preemptible VM: it is stopped when
		// taken back, and not started again.
		"preemptible":    map[string]string{"onPreemption": "STOP"},
		"recoveryPolicy": "FAIL",
	}
	if req.StartupScript != "" {
		spec["cloudInitUserData"] = bootScript(req.StartupScript)
	}
	if policy != "" {
		spec["spotPricingPolicy"] = map[string]string{"id": policy}
	} else {
		spec["followsSpotPrice"] = empty
	}
	return spec
}

// priceLimit returns the pricing policy that has Nebius take a VM back when
// it would cost more than maxMicros an hour. Nebius limits the price of a
// GPU, with a policy for a platform in a project, so the policy is found
// among the project's by its platform and limit, and made when there is
// none.
func (c *Nebius) priceLimit(ctx context.Context, project nebiusProject, platform string, gpus uint32, maxMicros int64) (string, error) {
	if gpus == 0 {
		return "", fmt.Errorf("a Nebius price limit is for a GPU, and the offer has none")
	}
	// Nebius takes a limit in thousandths of a dollar.
	perGPU := (maxMicros - hourlyDiskMicros(c.config.DiskGB, nebiusDiskMicrosPerGBMonth)) / int64(gpus) / 1000 * 1000
	if perGPU <= 0 {
		return "", fmt.Errorf("a price limit of %d micros an hour leaves nothing for the GPUs of a Nebius VM", maxMicros)
	}

	id, err := c.findPolicy(ctx, project.id, platform, perGPU)
	if err != nil || id != "" {
		return id, err
	}
	id, err = c.makePolicy(ctx, project.id, platform, perGPU)
	if err != nil {
		// Two creates that look at the same time both find no policy and
		// both make it. Nebius keeps the first and turns the second down,
		// and the first is as good. When this look finds nothing either,
		// why the policy could not be made is what there is to say.
		if statusOf(err) == http.StatusConflict {
			if theirs, _ := c.findPolicy(ctx, project.id, platform, perGPU); theirs != "" {
				return theirs, nil
			}
		}
		return "", err
	}
	return id, nil
}

// findPolicy returns the project's pricing policy that limits a GPU of the
// platform to perGPU micros an hour, or "" when it has none.
func (c *Nebius) findPolicy(ctx context.Context, project, platform string, perGPU int64) (string, error) {
	policies, err := nebiusPages[nebiusPolicy](ctx, c.api, "/billing/v1/pricing-policies", project)
	if err != nil {
		return "", fmt.Errorf("the pricing policies of Nebius project %q: %w", project, err)
	}
	pending := ""
	for _, existing := range policies {
		if existing.matches(platform, perGPU) && existing.Status.State != "STATE_DELETING" {
			if existing.Status.State == "STATE_ACTIVE" {
				return existing.Metadata.ID, nil
			}
			pending = existing.Metadata.ID
		}
	}
	if pending != "" {
		return c.readyPolicy(ctx, project, pending, platform, perGPU)
	}
	return "", nil
}

type nebiusPolicy struct {
	Metadata struct {
		ID string `json:"id"`
	} `json:"metadata"`
	Spec struct {
		ComputeInstanceSpec struct {
			V1 struct {
				Platform string `json:"platform"`
			} `json:"v1"`
		} `json:"computeInstanceSpec"`
		Pricing struct {
			MaxPriceV1 struct {
				MaxPrice dollars `json:"maxPrice"`
			} `json:"maxPriceV1"`
		} `json:"pricing"`
	} `json:"spec"`
	Status struct {
		State string `json:"state"`
	} `json:"status"`
}

func (p nebiusPolicy) matches(platform string, perGPU int64) bool {
	return p.Spec.ComputeInstanceSpec.V1.Platform == platform && int64(p.Spec.Pricing.MaxPriceV1.MaxPrice) == perGPU
}

// readyPolicy waits for the resource's documented ready state rather than
// treating an unfinished operation's resourceId as evidence of completion.
// STATE_CREATING and STATE_UPDATING cannot yet back VMs. The policy is read
// from its project so a stale or changed price limit cannot be substituted.
func (c *Nebius) readyPolicy(ctx context.Context, project, id, platform string, perGPU int64) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, nebiusPolicyReadyTimeout)
	defer cancel()
	state := "not visible"
	for attempt := 0; ; attempt++ {
		policies, err := nebiusPages[nebiusPolicy](ctx, c.api, "/billing/v1/pricing-policies", project)
		if err != nil {
			return "", fmt.Errorf("wait for Nebius pricing policy %q: %w", id, err)
		}
		for _, policy := range policies {
			if policy.Metadata.ID != id {
				continue
			}
			if !policy.matches(platform, perGPU) {
				return "", fmt.Errorf("Nebius pricing policy %q no longer has the requested platform and price limit", id)
			}
			state = policy.Status.State
			switch state {
			case "STATE_ACTIVE":
				return id, nil
			case "", "STATE_UNSPECIFIED", "STATE_CREATING", "STATE_UPDATING":
				// A newly created resource can be absent or incomplete in a
				// list before billing finishes publishing it.
			default:
				return "", fmt.Errorf("Nebius pricing policy %q cannot back a VM in state %q", id, state)
			}
		}
		if attempt == len(nebiusPolicyWaits) {
			return "", fmt.Errorf("Nebius pricing policy %q is not ready (state %q)", id, state)
		}
		if err := c.api.sleep(ctx, nebiusPolicyWaits[attempt]); err != nil {
			return "", fmt.Errorf("wait for Nebius pricing policy %q: %w", id, err)
		}
	}
}

// makePolicy makes that pricing policy in the project and waits until it
// can back a VM. Nebius's create operation completes only once it is ready.
func (c *Nebius) makePolicy(ctx context.Context, project, platform string, perGPU int64) (string, error) {
	limit := fmt.Sprintf("%d.%03d", perGPU/1_000_000, perGPU%1_000_000/1000)
	// Nebius takes letters and digits in a policy's name, and nothing else.
	name := "beam" + lettersAndDigits(platform) + "max" + lettersAndDigits(limit)
	body := map[string]any{
		"metadata": map[string]string{"parentId": project, "name": name},
		"spec": map[string]any{
			"computeInstanceSpec": map[string]any{"v1": map[string]string{"platform": platform}},
			"pricing":             map[string]any{"maxPriceV1": map[string]string{"maxPrice": limit}},
		},
	}
	var made nebiusOperation
	err := c.api.do(ctx, call{method: http.MethodPost, path: "/billing/v1/pricing-policies", body: body, out: &made})
	if err == nil {
		err = made.failure()
	}
	if err != nil {
		return "", fmt.Errorf("make a Nebius pricing policy for %s at $%s a GPU-hour: %w", platform, limit, err)
	}
	if made.ResourceID == "" {
		return "", fmt.Errorf("Nebius made a pricing policy without returning its id")
	}
	return c.readyPolicy(ctx, project, made.ResourceID, platform, perGPU)
}

// nebiusPolicyNotVisible recognizes only the observed pre-create validation
// for the exact policy just read from billing. General errors may have bought
// a VM already and are deliberately not retryable here.
func nebiusPolicyNotVisible(err error, policy string) bool {
	var response *StatusError
	if policy == "" || !errors.As(err, &response) || response.Method != http.MethodPost || response.Path != "/compute/v1/instances" || response.Status != http.StatusBadRequest {
		return false
	}
	var status struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Details []struct {
			Service    string `json:"service"`
			Code       string `json:"code"`
			BadRequest struct {
				Violations []struct {
					Field   string `json:"field"`
					Message string `json:"message"`
				} `json:"violations"`
			} `json:"badRequest"`
		} `json:"details"`
	}
	response.decode(&status)
	if status.Code != 3 || status.Message != "spec.spot_pricing_policy.id is invalid" || len(status.Details) != 1 {
		return false
	}
	detail := status.Details[0]
	if detail.Service != "compute" || detail.Code != "BadRequest" || len(detail.BadRequest.Violations) != 1 {
		return false
	}
	violation := detail.BadRequest.Violations[0]
	return violation.Field == "spec.spot_pricing_policy.id" && violation.Message == fmt.Sprintf("pricing policy not found by id %q", policy)
}

func lettersAndDigits(text string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return r
		}
		return -1
	}, text)
}

// nebiusStatus is an error of Nebius's: its answer to a call it turned down,
// and the status of an operation that failed. Next to its message an error
// carries details, and a detail says what kind it is by which of its fields
// is set.
type nebiusStatus struct {
	// Code is zero when nothing went wrong.
	Code    int    `json:"code"`
	Message string `json:"message"`
	Details []struct {
		NotEnoughResources *struct{} `json:"notEnoughResources"`
		QuotaFailure       *struct{} `json:"quotaFailure"`
	} `json:"details"`
}

// kind is the reason the error gives, when it is one of those a create
// names.
func (s nebiusStatus) kind() error {
	for _, detail := range s.Details {
		switch {
		case detail.NotEnoughResources != nil:
			return ErrOutOfStock
		case detail.QuotaFailure != nil:
			return ErrQuota
		}
	}
	return nil
}

// nebiusOperation is Nebius's answer to a call that changes something: the
// change is under way, or has failed already.
type nebiusOperation struct {
	ResourceID string       `json:"resourceId"`
	Status     nebiusStatus `json:"status"`
}

// failure returns why the operation failed, or nil when it has not.
func (o nebiusOperation) failure() error {
	if o.Status.Code == 0 {
		return nil
	}
	err := fmt.Errorf("Nebius failed the operation: %s (code %d)", o.Status.Message, o.Status.Code)
	if kind := o.Status.kind(); kind != nil {
		return because(kind, err)
	}
	return err
}

// nebiusRefusal names why a call was turned down.
func nebiusRefusal(err error) error {
	return refusal(err, func(_ *StatusError, status nebiusStatus) error { return status.kind() })
}

type nebiusInstance struct {
	Metadata struct {
		ID     string            `json:"id"`
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		// Preemptible is set for a VM that Nebius may take back.
		Preemptible *struct{} `json:"preemptible"`
	} `json:"spec"`
	Status struct {
		State             string `json:"state"`
		NetworkInterfaces []struct {
			PublicIPAddress struct {
				Address string `json:"address"`
			} `json:"publicIpAddress"`
		} `json:"networkInterfaces"`
	} `json:"status"`
}

func (c *Nebius) instance(raw nebiusInstance) Instance {
	instance := Instance{
		ID:     raw.Metadata.ID,
		Status: raw.status(),
		Tags:   callerTags(raw.Metadata.Labels),
	}
	instance.StatusDetail = statusDetail(instance.Status, raw.Status.State)
	for _, network := range raw.Status.NetworkInterfaces {
		// Nebius writes an address with its prefix length, as 1.2.3.4/32.
		if address, _, _ := strings.Cut(network.PublicIPAddress.Address, "/"); address != "" {
			instance.reachableAt(address)
			break
		}
	}
	return instance
}

func (i nebiusInstance) status() Status {
	switch i.Status.State {
	case "CREATING", "STARTING", "UPDATING":
		return StatusPending
	case "RUNNING":
		return StatusActive
	case "STOPPING", "STOPPED":
		// Nothing here stops a machine. Nebius stops a preemptible VM to
		// take it back, and gives no other sign that it did. What is left is
		// a stopped VM that counts towards the quota, with a disk that is
		// paid for.
		if i.Spec.Preemptible != nil {
			return StatusInterrupted
		}
		return StatusFailed
	case "ERROR":
		return StatusFailed
	case "DELETING":
		return StatusDeleted
	}
	// This includes the state Nebius calls "UNSPECIFIED".
	return unknownStatus(nebiusName, i.Status.State)
}

func (c *Nebius) Get(ctx context.Context, id string) (Instance, error) {
	var found nebiusInstance
	if err := c.api.do(ctx, call{method: http.MethodGet, path: "/compute/v1/instances/" + url.PathEscape(id), out: &found}); err != nil {
		return Instance{}, notFound(err)
	}
	return c.instance(found), nil
}

// List returns the instances this control plane bought, in every configured
// project. Nebius cannot be asked for the instances with a label, so each
// project's are read and the others left out. The projects are taken as they
// are configured: what Nebius says of one is needed to buy in it, and not to
// see what was bought.
func (c *Nebius) List(ctx context.Context) ([]Instance, error) {
	if len(c.config.Projects) == 0 {
		return nil, errNoNebiusProject
	}
	instances := []Instance{}
	var read []string
	for _, project := range c.config.Projects {
		if slices.Contains(read, project.ID) {
			continue
		}
		read = append(read, project.ID)
		listed, err := nebiusPages[nebiusInstance](ctx, c.api, "/compute/v1/instances", project.ID)
		if err != nil {
			return nil, err
		}
		for _, raw := range listed {
			if raw.Metadata.Labels[OwnerTag] == c.owner {
				instances = append(instances, c.instance(raw))
			}
		}
	}
	return instances, nil
}

// Delete removes the instance and, with it, its disk.
func (c *Nebius) Delete(ctx context.Context, id string) error {
	var started nebiusOperation
	err := c.api.do(ctx, call{method: http.MethodDelete, path: "/compute/v1/instances/" + url.PathEscape(id), out: &started, repeatable: true})
	if err != nil {
		// What is gone needs no deleting, and Nebius may refuse to delete
		// what it is deleting already.
		found, getErr := c.Get(ctx, id)
		if errors.Is(getErr, ErrNotFound) || (getErr == nil && found.Status == StatusDeleted) {
			return nil
		}
		return err
	}
	return started.failure()
}
