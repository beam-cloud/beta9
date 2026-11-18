package providers

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	verdaName    = "verda"
	verdaBaseURL = "https://api.verda.com/v1"

	// A machine's disk is a volume that Verda sells apart from the machine.
	verdaDiskType = "NVMe"

	verdaPageSize = 100

	// Instance types, prices and images change rarely. Stock does not, and
	// is asked for every time.
	verdaCatalogueFor = 10 * time.Minute

	// A startup script younger than this may belong to a create that is
	// still under way.
	verdaScriptGrace = time.Hour

	// verdaDeletePermanently has Verda delete the disk of a spot machine it
	// takes back. Left to Verda, the disk is kept and paid for.
	verdaDeletePermanently = "delete_permanently"
)

// verdaImages are the images a GPU machine boots, best first: Ubuntu with the
// NVIDIA driver and Docker. Verda's older GPUs run the older driver only.
var verdaImages = []string{
	"24.04.cuda13.0.docker",
	"24.04.cuda12.9.docker",
	"24.04.cuda12.9-v100.docker",
	// Older catalogues use these names. Prefer the current aliases: the
	// instance-type catalogue can retain a retired image after /images
	// stops publishing it, and attempting that image then fails with 404.
	"ubuntu-24.04-cuda-12.8-open-docker",
	"ubuntu-24.04-cuda-12.6-docker",
}

// CPU machines use plain Ubuntu without GPU image initialization or driver
// requirements.
var verdaCPUImages = []string{"24.04.base"}

// Verda buys from Verda, formerly DataCrunch: on-demand machines, and spot
// machines that Verda takes back without notice.
type Verda struct {
	// Verda deletes an instance only when asked, or when it takes a spot
	// machine back.
	noDeletionTimer

	api *api
	// login talks to Verda where it gives out access tokens, which takes
	// no credentials.
	login  *api
	owner  string
	config types.VerdaProviderConfig
	// now is the clock, which a test sets.
	now func() time.Time

	catalogue cached[verdaCatalogue]
}

// NewVerda makes a client that buys as owner: the name this control plane
// puts on the instances it buys. It fails when the client id or its secret
// is missing.
func NewVerda(config types.VerdaProviderConfig, owner string) (*Verda, error) {
	if config.ClientID == "" {
		return nil, errors.New("providers.verda.clientId is not set")
	}
	if config.ClientSecret == "" {
		return nil, errors.New("providers.verda.clientSecret is not set")
	}
	config.BaseURL = cmp.Or(config.BaseURL, verdaBaseURL)
	if config.DiskGB <= 0 {
		config.DiskGB = defaultDiskGB
	}
	c := &Verda{login: newAPI(config.BaseURL, withoutCredentials()), owner: owner, config: config, now: time.Now}
	// The token reads the time from the client, whose clock a test sets.
	token := newAccessToken(func() time.Time { return c.now() }, c.signIn)
	c.api = newAPI(config.BaseURL, token.bearer())
	return c, nil
}

// signIn trades the client id and its secret for an access token.
func (c *Verda) signIn(ctx context.Context) (string, time.Duration, error) {
	return grant(ctx, c.login, call{path: "/oauth2/token", body: map[string]string{
		"grant_type":    "client_credentials",
		"client_id":     c.config.ClientID,
		"client_secret": c.config.ClientSecret,
	}})
}

func (c *Verda) Name() string { return verdaName }

type verdaInstanceType struct {
	InstanceType string `json:"instance_type"`
	// Name is the GPU's, such as "H100 SXM5 80GB".
	Name string `json:"name"`
	CPU  struct {
		Cores int64 `json:"number_of_cores"`
	} `json:"cpu"`
	GPU struct {
		Count uint32 `json:"number_of_gpus"`
	} `json:"gpu"`
	Memory struct {
		GB float64 `json:"size_in_gigabytes"`
	} `json:"memory"`
	// The prices are for the whole machine, whatever its number of GPUs.
	// These two are for a location that has no price of its own.
	PricePerHour   dollars `json:"price_per_hour"`
	SpotPrice      dollars `json:"spot_price"`
	LocationPrices []struct {
		LocationCode string  `json:"location_code"`
		PricePerHour dollars `json:"price_per_hour"`
		SpotPrice    dollars `json:"spot_price"`
	} `json:"location_prices"`
	SupportedOS []string `json:"supported_os"`
}

// gpu is Beam's name for the type's GPU, or "" for a machine without one.
func (t verdaInstanceType) gpu() string {
	if t.GPU.Count == 0 {
		return ""
	}
	return string(gpuName(t.Name))
}

// price is what the machine costs per hour in the location, in micros.
func (t verdaInstanceType) price(location string, spot bool) int64 {
	onDemand, asSpot := t.PricePerHour, t.SpotPrice
	for _, local := range t.LocationPrices {
		if local.LocationCode == location {
			onDemand, asSpot = local.PricePerHour, local.SpotPrice
		}
	}
	if spot {
		return int64(asSpot)
	}
	return int64(onDemand)
}

// verdaCatalogue is what Verda sells, without whether it has any.
type verdaCatalogue struct {
	types []verdaInstanceType
	// diskMicros is the hourly price of a machine's disk.
	diskMicros int64
}

func (c *Verda) knownCatalogue(ctx context.Context) (verdaCatalogue, error) {
	return c.catalogue.get(c.now(), verdaCatalogueFor, func() (verdaCatalogue, error) {
		var catalogue verdaCatalogue
		if err := c.api.do(ctx, call{method: http.MethodGet, path: "/instance-types?currency=usd", out: &catalogue.types}); err != nil {
			return catalogue, err
		}

		var volumeTypes []struct {
			Type  string `json:"type"`
			Price struct {
				// Dollars for one gigabyte for one second.
				PerSecondPerGB float64 `json:"cps_per_gb"`
			} `json:"price"`
		}
		if err := c.api.do(ctx, call{method: http.MethodGet, path: "/volume-types", out: &volumeTypes}); err != nil {
			return catalogue, err
		}
		for _, volumeType := range volumeTypes {
			if volumeType.Type == verdaDiskType {
				hourly := volumeType.Price.PerSecondPerGB * 3600 * float64(c.config.DiskGB)
				catalogue.diskMicros = DollarsToMicros(hourly)
				return catalogue, nil
			}
		}
		return catalogue, fmt.Errorf("Verda lists no price for %s volumes", verdaDiskType)
	})
}

// images preserves an explicit preference for either kind of machine. With
// no override, only GPU machines are eligible for the default CUDA images.
func (c *Verda) images(instanceType verdaInstanceType) []string {
	if len(c.config.Images) > 0 {
		return c.config.Images
	}
	if instanceType.GPU.Count == 0 {
		return verdaCPUImages
	}
	return verdaImages
}

// image returns the first supported image, or "" when none is compatible.
func (c *Verda) image(instanceType verdaInstanceType) string {
	for _, image := range c.images(instanceType) {
		if slices.Contains(instanceType.SupportedOS, image) {
			return image
		}
	}
	return ""
}

// verdaStock is what one location has now, bought one way: on demand, or as
// spot.
type verdaStock struct {
	LocationCode   string   `json:"location_code"`
	Availabilities []string `json:"availabilities"`
	spot           bool
}

// stock asks which instance types each location has now: on demand first,
// then as spot. Verda says whether it has a type, not how many.
func (c *Verda) stock(ctx context.Context) ([]verdaStock, error) {
	var all []verdaStock
	for _, spot := range []bool{false, true} {
		var stock []verdaStock
		path := fmt.Sprintf("/instance-availability?is_spot=%t", spot)
		if err := c.api.do(ctx, call{method: http.MethodGet, path: path, out: &stock}); err != nil {
			return nil, err
		}
		for i := range stock {
			stock[i].spot = spot
		}
		all = append(all, stock...)
	}
	return all, nil
}

// Offers lists what Verda has now: one offer for each instance type,
// location, and way of buying it.
func (c *Verda) Offers(ctx context.Context, filter OfferFilter) ([]Offer, error) {
	catalogue, err := c.knownCatalogue(ctx)
	if err != nil {
		return nil, err
	}
	stock, err := c.stock(ctx)
	if err != nil {
		return nil, err
	}

	var offers []Offer
	for _, instanceType := range catalogue.types {
		if c.image(instanceType) == "" {
			continue
		}
		for _, location := range stock {
			// Verda may have a type and not say for how much.
			price := instanceType.price(location.LocationCode, location.spot)
			if !slices.Contains(location.Availabilities, instanceType.InstanceType) || price <= 0 {
				continue
			}
			offers = append(offers, Offer{
				ID:               instanceType.InstanceType,
				Provider:         verdaName,
				Cloud:            verdaName,
				InstanceType:     instanceType.InstanceType,
				Region:           location.LocationCode,
				GPU:              instanceType.gpu(),
				GPUCount:         instanceType.GPU.Count,
				CPUMillicores:    instanceType.CPU.Cores * 1000,
				MemoryMB:         int64(math.Round(instanceType.Memory.GB * 1024)),
				StorageMB:        c.config.DiskGB * 1024,
				HourlyCostMicros: price + catalogue.diskMicros,
				Spot:             location.spot,
			})
		}
	}
	return filter.matching(offers), nil
}

type verdaCreate struct {
	InstanceType    string        `json:"instance_type"`
	Image           string        `json:"image"`
	Hostname        string        `json:"hostname"`
	Description     string        `json:"description"`
	LocationCode    string        `json:"location_code"`
	IsSpot          bool          `json:"is_spot"`
	OSVolume        verdaOSVolume `json:"os_volume"`
	SSHKeyIDs       []string      `json:"ssh_key_ids"`
	StartupScriptID string        `json:"startup_script_id,omitempty"`
	Tags            []verdaTag    `json:"tags"`
}

type verdaOSVolume struct {
	Name              string `json:"name"`
	Size              int64  `json:"size"` // GB
	OnSpotDiscontinue string `json:"on_spot_discontinue,omitempty"`
}

type verdaTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// verdaInstance is what Verda says of an instance. What it has not got yet,
// such as an address, it writes as null, which reads as empty.
type verdaInstance struct {
	ID              string     `json:"id"`
	IP              string     `json:"ip"`
	Status          string     `json:"status"`
	IsSpot          bool       `json:"is_spot"`
	Tags            []verdaTag `json:"tags"`
	StartupScriptID string     `json:"startup_script_id"`
	OSVolumeID      string     `json:"os_volume_id"`
}

// Create buys the offer as it is listed, on demand or as spot. Verda takes
// no price to bid for a spot machine, so the request's price limit has
// nothing to be passed to.
func (c *Verda) Create(ctx context.Context, req CreateRequest) (Instance, error) {
	tags, err := ownedTags(c.owner, req.Tags)
	if err != nil {
		return Instance{}, err
	}
	if req.Offer.ID == "" || req.Offer.Region == "" {
		return Instance{}, fmt.Errorf("a Verda offer needs an instance type and location: %+v", req.Offer)
	}
	catalogue, err := c.knownCatalogue(ctx)
	if err != nil {
		return Instance{}, err
	}
	at := slices.IndexFunc(catalogue.types, func(t verdaInstanceType) bool { return t.InstanceType == req.Offer.ID })
	if at < 0 {
		return Instance{}, fmt.Errorf("Verda has no instance type %q", req.Offer.ID)
	}
	image := c.image(catalogue.types[at])
	if image == "" {
		return Instance{}, fmt.Errorf("Verda instance type %q boots none of the images %v", req.Offer.ID, c.images(catalogue.types[at]))
	}

	body := verdaCreate{
		InstanceType: req.Offer.ID,
		Image:        image,
		Hostname:     req.Name,
		Description:  req.Name,
		LocationCode: req.Offer.Region,
		IsSpot:       req.Offer.Spot,
		OSVolume:     verdaOSVolume{Name: req.Name, Size: c.config.DiskGB},
		SSHKeyIDs:    c.config.SSHKeyIDs,
		Tags:         verdaTagList(tags),
	}
	// The live API rejects an omitted ssh_key_ids field even though it is
	// documented as optional. An explicit null requests no particular keys;
	// preserve the UUID array when keys have been configured.
	if len(body.SSHKeyIDs) == 0 {
		body.SSHKeyIDs = nil
	}
	if req.Offer.Spot {
		body.OSVolume.OnSpotDiscontinue = verdaDeletePermanently
	}
	if req.StartupScript != "" {
		// Verda runs a script it has stored, and takes none with the create.
		script := map[string]string{"name": c.scriptPrefix() + req.Name, "script": bootScript(req.StartupScript)}
		body.StartupScriptID, err = c.createID(ctx, "/scripts", script)
		if err != nil {
			return Instance{}, fmt.Errorf("store the startup script: %w", err)
		}
	}

	id, err := c.createID(ctx, "/instances", body)
	if err != nil {
		// The script stays: Verda may have taken the order all the same,
		// and the machine then needs it. List removes one nothing runs.
		return Instance{}, verdaRefusal(err)
	}
	return Instance{ID: id, Status: StatusPending, Tags: req.Tags}, nil
}

// createID reads Verda's resource ID. Its published examples encode it as a
// JSON string, but the live create endpoints also return an unquoted UUID.
// Only a UUID is accepted as plain text: an unexpected response must not be
// mistaken for the ID of an instance that may already have been bought.
func (c *Verda) createID(ctx context.Context, path string, body any) (string, error) {
	var data []byte
	if err := c.api.do(ctx, call{method: http.MethodPost, path: path, body: body, rawOut: &data}); err != nil {
		return "", err
	}
	var id string
	if err := json.Unmarshal(data, &id); err == nil && strings.TrimSpace(id) != "" {
		return id, nil
	}
	id = strings.TrimSpace(string(data))
	if len(id) == 36 {
		if _, err := uuid.Parse(id); err == nil {
			return id, nil
		}
	}
	// A successful mutation without an ID may still have created the resource.
	// Keep that uncertainty so delayed instance visibility cannot erase the
	// reservation's tag-based cleanup record or invite a blind purchase retry.
	return "", because(errNoAnswer, fmt.Errorf("Verda accepted POST %s without returning a resource ID", path))
}

func (c *Verda) instance(raw verdaInstance) Instance {
	instance := Instance{
		ID:     raw.ID,
		Status: raw.status(),
		Tags:   callerTags(verdaTagMap(raw.Tags)),
	}
	instance.StatusDetail = statusDetail(instance.Status, raw.Status)
	instance.reachableAt(raw.IP)
	return instance
}

func (raw verdaInstance) status() Status {
	switch raw.Status {
	case "new", "ordered", "validating", "provisioning":
		return StatusPending
	case "running":
		return StatusActive
	case "offline", "error", "no_capacity", "installation_failed":
		// Nothing here shuts a machine down, and one that Verda could not
		// start does not start later. A machine that is off is still paid
		// for.
		return StatusFailed
	case "discontinued":
		// Verda's word for an instance that is no more. It does not say who
		// ended it; for a spot machine that is Verda as a rule, since Beam
		// forgets a machine once it has deleted it.
		if raw.IsSpot {
			return StatusInterrupted
		}
		return StatusDeleted
	case "deleting", "notfound":
		return StatusDeleted
	}
	// This includes the status Verda itself calls "unknown".
	return unknownStatus(verdaName, raw.Status)
}

// gone reports whether nothing is left of the instance at Verda.
func (raw verdaInstance) gone() bool {
	return raw.Status == "discontinued" || raw.Status == "notfound"
}

// going reports whether Verda is deleting the instance already.
func (raw verdaInstance) going() bool {
	return raw.Status == "deleting"
}

func (c *Verda) get(ctx context.Context, id string) (verdaInstance, error) {
	var found verdaInstance
	err := c.api.do(ctx, call{method: http.MethodGet, path: "/instances/" + url.PathEscape(id), out: &found})
	return found, notFound(err)
}

func (c *Verda) Get(ctx context.Context, id string) (Instance, error) {
	found, err := c.get(ctx, id)
	if err != nil {
		return Instance{}, err
	}
	return c.instance(found), nil
}

// List returns the instances this control plane bought that Verda still has.
// It is also when startup scripts that no machine runs are removed, and the
// only time: Verda keeps a script apart from the instance that ran it, and
// only here is it known which are in use.
func (c *Verda) List(ctx context.Context) ([]Instance, error) {
	var listed []verdaInstance
	path := "/instances?tag=" + url.QueryEscape(OwnerTag+"="+c.owner)
	if err := c.api.do(ctx, call{method: http.MethodGet, path: path, out: &listed}); err != nil {
		return nil, err
	}

	instances := make([]Instance, 0, len(listed))
	scriptsInUse := map[string]bool{}
	for _, raw := range listed {
		if verdaTagMap(raw.Tags)[OwnerTag] != c.owner || raw.gone() {
			continue
		}
		instances = append(instances, c.instance(raw))
		scriptsInUse[raw.StartupScriptID] = true
	}
	if err := c.deleteUnusedScripts(ctx, scriptsInUse); err != nil {
		log.Warn().Err(err).Str("provider", verdaName).Msg("could not remove the startup scripts of machines that are gone")
	}
	return instances, nil
}

// Delete removes the instance and its disk. Its startup script goes with a
// later List.
func (c *Verda) Delete(ctx context.Context, id string) error {
	found, err := c.get(ctx, id)
	switch {
	case errors.Is(err, ErrNotFound):
		return nil
	case err != nil:
		return err
	case found.gone() || found.going():
		return nil
	}

	action := map[string]any{"action": "delete", "id": id}
	if found.OSVolumeID != "" {
		// Without this the disk goes to Verda's trash for four days, where
		// it counts towards the account's storage quota.
		action["volume_ids"] = []string{found.OSVolumeID}
		action["delete_permanently"] = true
	}
	var results []struct {
		Status     string `json:"status"`
		Error      string `json:"error"`
		StatusCode int    `json:"statusCode"`
	}
	if err := unlessGone(c.api.do(ctx, call{method: http.MethodPut, path: "/instances", body: action, out: &results, repeatable: true})); err != nil {
		return err
	}
	for _, result := range results {
		if result.Status == "error" && result.StatusCode != http.StatusNotFound {
			return fmt.Errorf("Verda did not delete instance %s: %s", id, result.Error)
		}
	}
	return nil
}

// scriptPrefix starts the name of each startup script this control plane
// stores, so that its scripts can be told from the account's others.
func (c *Verda) scriptPrefix() string { return OwnerTag + "=" + c.owner + " " }

// deleteScript removes a startup script. One that stays is removed by a later
// List.
func (c *Verda) deleteScript(ctx context.Context, id string) {
	err := unlessGone(c.api.do(ctx, call{method: http.MethodDelete, path: "/scripts/" + url.PathEscape(id), repeatable: true}))
	if err != nil {
		log.Warn().Err(err).Str("provider", verdaName).Str("script", id).Msg("could not delete a startup script")
	}
}

// deleteUnusedScripts removes this control plane's startup scripts that no
// instance runs: those of machines that were deleted or taken back, and of
// creates that Verda turned down. They are all found before one is deleted,
// which would shift the pages they are read from.
func (c *Verda) deleteUnusedScripts(ctx context.Context, inUse map[string]bool) error {
	unused, err := c.unusedScripts(ctx, inUse)
	if err != nil {
		return err
	}
	for _, id := range unused {
		c.deleteScript(ctx, id)
	}
	return nil
}

// unusedScripts finds this control plane's startup scripts that are old and
// that no instance runs. One whose age Verda does not tell is not old.
func (c *Verda) unusedScripts(ctx context.Context, inUse map[string]bool) ([]string, error) {
	type script struct {
		ID        string    `json:"id"`
		Name      string    `json:"name"`
		CreatedAt time.Time `json:"created_at"`
	}
	var unused []string
	for page := 1; page <= maxPages; page++ {
		var scripts []script
		path := fmt.Sprintf("/scripts?page=%d&pageSize=%d&name=%s", page, verdaPageSize, url.QueryEscape(c.scriptPrefix()))
		if err := c.api.do(ctx, call{method: http.MethodGet, path: path, out: &scripts}); err != nil {
			return nil, err
		}
		for _, script := range scripts {
			old := !script.CreatedAt.IsZero() && c.now().Sub(script.CreatedAt) >= verdaScriptGrace
			if strings.HasPrefix(script.Name, c.scriptPrefix()) && old && !inUse[script.ID] {
				unused = append(unused, script.ID)
			}
		}
		if len(scripts) < verdaPageSize {
			return unused, nil
		}
	}
	return nil, tooManyPages("GET /scripts")
}

// verdaProblem is the body of an answer Verda turned a call down with.
type verdaProblem struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// verdaRefusal names why a create was turned down.
func verdaRefusal(err error) error {
	return refusal(err, func(_ *StatusError, refused verdaProblem) error {
		switch {
		case refused.Code == "service_unavailable":
			// Verda's answer when the type has no capacity in the location.
			return ErrOutOfStock
		case refused.Code == "insufficient_funds":
			return ErrQuota
		case containsAny(strings.ToLower(refused.Message), "quota", "limit"):
			// Verda documents no code for an account at its quota. Its
			// message is the only sign.
			return ErrQuota
		}
		return nil
	})
}

func verdaTagList(tags map[string]string) []verdaTag {
	list := make([]verdaTag, 0, len(tags))
	for key, value := range tags {
		list = append(list, verdaTag{Key: key, Value: value})
	}
	slices.SortFunc(list, func(a, b verdaTag) int { return strings.Compare(a.Key, b.Key) })
	return list
}

func verdaTagMap(list []verdaTag) map[string]string {
	tags := make(map[string]string, len(list))
	for _, tag := range list {
		tags[tag.Key] = tag.Value
	}
	return tags
}
