package providers

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	hetznerName     = "hetzner"
	hetznerBaseURL  = "https://api.hetzner.cloud/v1"
	hetznerImage    = "ubuntu-24.04"
	hetznerPageSize = 50

	// Locations and networks change when an operator changes them, so they
	// are read from Hetzner at most this often.
	hetznerCacheFor = 10 * time.Minute
)

var hetznerDefaultRegions = []string{"ash", "hil"}

// Hetzner sells CPU machines. Every machine joins a private network.
type Hetzner struct {
	// Hetzner deletes a server only when asked.
	noDeletionTimer

	api    *api
	owner  string
	config types.HetznerProviderConfig
	// now is the clock, which a test sets.
	now func() time.Time

	locations cached[map[string]hetznerLocation]
	networks  cached[[]hetznerNetwork]
}

// NewHetzner makes a client that buys as owner: the name this control plane
// puts on the servers it buys.
func NewHetzner(config types.HetznerProviderConfig, owner string) *Hetzner {
	config.BaseURL = cmp.Or(config.BaseURL, hetznerBaseURL)
	config.Image = cmp.Or(config.Image, hetznerImage)
	if len(config.DefaultRegions) == 0 {
		config.DefaultRegions = hetznerDefaultRegions
	}
	authorize := withHeader("Authorization", "Bearer "+config.ApiToken)
	return &Hetzner{api: newAPI(config.BaseURL, authorize), owner: owner, config: config, now: time.Now}
}

func (c *Hetzner) Name() string { return hetznerName }

type hetznerServerType struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Cores       float64 `json:"cores"`
	MemoryGB    float64 `json:"memory"`
	DiskGB      float64 `json:"disk"`
	Category    string  `json:"category"`
	CPUType     string  `json:"cpu_type"`
	// Deprecation is set once Hetzner has announced the type's removal.
	Deprecation *struct{} `json:"deprecation"`
	Prices      []struct {
		Location    string `json:"location"`
		PriceHourly struct {
			Net string `json:"net"`
		} `json:"price_hourly"`
	} `json:"prices"`
	Locations []struct {
		Name        string    `json:"name"`
		Available   bool      `json:"available"`
		Deprecation *struct{} `json:"deprecation"`
	} `json:"locations"`
}

type hetznerLocation struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	City        string  `json:"city"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
	NetworkZone string  `json:"network_zone"`
}

type hetznerNetwork struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Subnets []struct {
		Type        string `json:"type"`
		NetworkZone string `json:"network_zone"`
	} `json:"subnets"`
}

func (c *Hetzner) Offers(ctx context.Context, filter OfferFilter) ([]Offer, error) {
	if len(filter.GPUs) > 0 {
		return nil, nil
	}
	// A filter that names no location is for the default ones.
	if len(filter.Regions) == 0 {
		filter.Regions = c.config.DefaultRegions
	}

	locations, err := c.knownLocations(ctx)
	if err != nil {
		return nil, err
	}
	serverTypes, err := hetznerPages[hetznerServerType](ctx, c.api, "/server_types", "server_types")
	if err != nil {
		return nil, err
	}

	offers := []Offer{}
	for _, serverType := range serverTypes {
		if serverType.Deprecation != nil || serverType.Name == "" || serverType.Cores <= 0 || serverType.MemoryGB <= 0 {
			continue
		}
		for _, where := range serverType.Locations {
			hourlyCost := c.hourlyCostMicros(serverType, where.Name)
			if !where.Available || where.Deprecation != nil || hourlyCost <= 0 {
				continue
			}
			offers = append(offers, c.offer(serverType, locations[where.Name], where.Name, hourlyCost))
		}
	}
	return filter.matching(offers), nil
}

// offer is a server type as it is sold in one location. What the config
// says of the location comes before what Hetzner says of it.
func (c *Hetzner) offer(serverType hetznerServerType, location hetznerLocation, region string, hourlyCost int64) Offer {
	configured := c.config.RegionMetadata[region]
	categories := c.config.ServerTypeCategories
	return Offer{
		ID:                serverType.Name,
		Provider:          hetznerName,
		Cloud:             hetznerName,
		InstanceType:      serverType.Name,
		Region:            region,
		CPUMillicores:     int64(serverType.Cores * 1000),
		MemoryMB:          int64(serverType.MemoryGB * 1024),
		StorageMB:         int64(serverType.DiskGB * 1024),
		HourlyCostMicros:  hourlyCost,
		DisplayName:       cmp.Or(serverType.Description, serverType.Name),
		Category:          cmp.Or(categories[serverType.Name], categories[serverType.Category], serverType.Category, serverType.CPUType),
		RegionDisplayName: cmp.Or(configured.DisplayName, location.Description, location.City, region),
		Latitude:          cmp.Or(configured.Latitude, location.Latitude),
		Longitude:         cmp.Or(configured.Longitude, location.Longitude),
	}
}

// hourlyCostMicros is what a server type costs in a location: the price the
// config gives it in dollars, under "type:location" or under the type, and
// without one Hetzner's own.
func (c *Hetzner) hourlyCostMicros(serverType hetznerServerType, location string) int64 {
	for _, key := range []string{serverType.Name + ":" + location, serverType.Name} {
		if dollars := c.config.ServerTypePrices[key]; dollars > 0 {
			return DollarsToMicros(dollars)
		}
	}
	for _, price := range serverType.Prices {
		if price.Location != location {
			continue
		}
		if dollars, err := strconv.ParseFloat(price.PriceHourly.Net, 64); err == nil {
			return DollarsToMicros(dollars)
		}
	}
	return 0
}

type hetznerCreate struct {
	Name             string            `json:"name"`
	ServerType       string            `json:"server_type"`
	Image            string            `json:"image"`
	Location         string            `json:"location"`
	StartAfterCreate bool              `json:"start_after_create"`
	Networks         []int64           `json:"networks"`
	Labels           map[string]string `json:"labels"`
	SSHKeys          []string          `json:"ssh_keys,omitempty"`
	UserData         string            `json:"user_data,omitempty"`
}

type hetznerServer struct {
	ID        int64             `json:"id"`
	Status    string            `json:"status"`
	Labels    map[string]string `json:"labels"`
	PublicNet struct {
		IPv4 *struct {
			IP string `json:"ip"`
		} `json:"ipv4"`
	} `json:"public_net"`
}

func (c *Hetzner) Create(ctx context.Context, req CreateRequest) (Instance, error) {
	labels, err := ownedTags(c.owner, req.Tags)
	if err != nil {
		return Instance{}, err
	}
	if req.Offer.ID == "" || req.Offer.Region == "" {
		return Instance{}, fmt.Errorf("a Hetzner offer needs a server type and location: %+v", req.Offer)
	}
	networkID, err := c.privateNetworkID(ctx, req.Offer.Region)
	if err != nil {
		return Instance{}, err
	}

	body := hetznerCreate{
		Name:             req.Name,
		ServerType:       req.Offer.ID,
		Image:            cmp.Or(c.config.ImageByRegion[req.Offer.Region], c.config.Image),
		Location:         req.Offer.Region,
		StartAfterCreate: true,
		Networks:         []int64{networkID},
		Labels:           labels,
		SSHKeys:          c.config.SSHKeys,
	}
	if keys := c.config.SSHKeysByRegion[req.Offer.Region]; len(keys) > 0 {
		body.SSHKeys = keys
	}
	if req.StartupScript != "" {
		body.UserData = bootScript(req.StartupScript)
	}

	var created struct {
		Server hetznerServer `json:"server"`
	}
	if err := c.api.do(ctx, call{method: http.MethodPost, path: "/servers", body: body, out: &created}); err != nil {
		return Instance{}, hetznerRefusal(err)
	}
	if created.Server.ID == 0 {
		return Instance{}, fmt.Errorf("Hetzner accepted the create without returning a server id")
	}
	return c.instance(created.Server), nil
}

func (c *Hetzner) instance(server hetznerServer) Instance {
	instance := Instance{
		ID:     strconv.FormatInt(server.ID, 10),
		Status: server.status(),
		Tags:   callerTags(server.Labels),
	}
	if ipv4 := server.PublicNet.IPv4; ipv4 != nil {
		instance.reachableAt(ipv4.IP)
	}
	return instance
}

func (s hetznerServer) status() Status {
	switch s.Status {
	case "initializing", "starting", "stopping", "migrating", "rebuilding":
		return StatusPending
	case "running":
		return StatusActive
	case "off":
		// Nothing here stops a server, and one that stopped does not come back.
		return StatusFailed
	case "deleting":
		return StatusDeleted
	}
	// This includes the status Hetzner itself calls "unknown".
	return unknownStatus(hetznerName, s.Status)
}

func (c *Hetzner) Get(ctx context.Context, id string) (Instance, error) {
	var found struct {
		Server hetznerServer `json:"server"`
	}
	if err := c.api.do(ctx, call{method: http.MethodGet, path: "/servers/" + url.PathEscape(id), out: &found}); err != nil {
		return Instance{}, notFound(err)
	}
	return c.instance(found.Server), nil
}

func (c *Hetzner) List(ctx context.Context) ([]Instance, error) {
	path := "/servers?label_selector=" + url.QueryEscape(OwnerTag+"=="+c.owner)
	servers, err := hetznerPages[hetznerServer](ctx, c.api, path, "servers")
	if err != nil {
		return nil, err
	}
	// What is listed here may be deleted as an orphan, so the filter is not
	// left to Hetzner alone.
	instances := make([]Instance, 0, len(servers))
	for _, server := range servers {
		if server.Labels[OwnerTag] == c.owner {
			instances = append(instances, c.instance(server))
		}
	}
	return instances, nil
}

func (c *Hetzner) Delete(ctx context.Context, id string) error {
	return unlessGone(c.api.do(ctx, call{method: http.MethodDelete, path: "/servers/" + url.PathEscape(id), repeatable: true}))
}

// hetznerProblem is the body of an answer Hetzner turned a call down with.
type hetznerProblem struct {
	Error struct {
		Code string `json:"code"`
	} `json:"error"`
}

// hetznerRefusal names why a create was turned down, from Hetzner's error
// code.
func hetznerRefusal(err error) error {
	return refusal(err, func(_ *StatusError, refused hetznerProblem) error {
		switch refused.Error.Code {
		case "resource_unavailable", "placement_error":
			return ErrOutOfStock
		case "resource_limit_exceeded":
			return ErrQuota
		}
		return nil
	})
}

func (c *Hetzner) knownLocations(ctx context.Context) (map[string]hetznerLocation, error) {
	return c.locations.get(c.now(), hetznerCacheFor, func() (map[string]hetznerLocation, error) {
		list, err := hetznerPages[hetznerLocation](ctx, c.api, "/locations", "locations")
		if err != nil {
			return nil, err
		}
		byName := make(map[string]hetznerLocation, len(list))
		for _, location := range list {
			byName[location.Name] = location
		}
		return byName, nil
	})
}

// hetznerNetworkName is how the config names a network: by ID, or else by
// name.
type hetznerNetworkName struct {
	id   int64
	name string
}

// hetznerNetworkFor is the network the config names for a region: the one it
// names for that region, or else the one it names for every region.
func hetznerNetworkFor(config types.HetznerPrivateNetworkConfig, region string) hetznerNetworkName {
	switch {
	case config.RegionIDs[region] > 0:
		return hetznerNetworkName{id: config.RegionIDs[region]}
	case config.RegionNames[region] != "":
		return hetznerNetworkName{name: config.RegionNames[region]}
	}
	return hetznerNetworkName{id: config.ID, name: config.Name}
}

func (n hetznerNetworkName) String() string {
	if n.id > 0 {
		return strconv.FormatInt(n.id, 10)
	}
	return n.name
}

// names reports whether the network is the one named.
func (n hetznerNetworkName) names(network hetznerNetwork) bool {
	if n.id > 0 {
		return network.ID == n.id
	}
	return network.Name == n.name
}

// privateNetworkID returns the network a new server in the region joins. A
// network that has no subnet in the region's network zone is refused, unless
// requireSubnet is set and false.
func (c *Hetzner) privateNetworkID(ctx context.Context, region string) (int64, error) {
	want := hetznerNetworkFor(c.config.PrivateNetwork, region)
	if want.id <= 0 && want.name == "" {
		return 0, fmt.Errorf("Hetzner privateNetwork has no id or name for location %q", region)
	}
	requireSubnet := c.config.PrivateNetwork.RequireSubnet == nil || *c.config.PrivateNetwork.RequireSubnet

	locations, err := c.knownLocations(ctx)
	if err != nil {
		return 0, err
	}
	location, known := locations[region]
	if !known {
		return 0, fmt.Errorf("Hetzner location %q was not found", region)
	}
	networks, err := c.networks.get(c.now(), hetznerCacheFor, func() ([]hetznerNetwork, error) {
		return hetznerPages[hetznerNetwork](ctx, c.api, "/networks", "networks")
	})
	if err != nil {
		return 0, err
	}

	at := slices.IndexFunc(networks, want.names)
	switch {
	case at < 0 && want.id > 0 && !requireSubnet:
		// A network whose subnets need no checking is taken at its ID.
		return want.id, nil
	case at < 0:
		// The operator's fix should count on the next attempt.
		c.networks.forget()
		return 0, fmt.Errorf("Hetzner private network %q was not found", want)
	case requireSubnet && !networks[at].hasSubnetIn(location.NetworkZone):
		c.networks.forget()
		return 0, fmt.Errorf("Hetzner private network %q has no cloud subnet in network zone %q for location %q", want, location.NetworkZone, region)
	}
	return networks[at].ID, nil
}

func (n hetznerNetwork) hasSubnetIn(networkZone string) bool {
	for _, subnet := range n.Subnets {
		if subnet.NetworkZone == networkZone && slices.Contains([]string{"", "cloud", "server"}, subnet.Type) {
			return true
		}
	}
	return false
}

// hetznerPages reads every page of a Hetzner list. The items are under key.
func hetznerPages[T any](ctx context.Context, api *api, path, key string) ([]T, error) {
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	items := []T{}
	page := 1
	for range maxPages {
		var raw map[string]json.RawMessage
		if err := api.do(ctx, call{method: http.MethodGet, path: fmt.Sprintf("%s%spage=%d&per_page=%d", path, separator, page, hetznerPageSize), out: &raw}); err != nil {
			return nil, err
		}
		var pageItems []T
		if err := json.Unmarshal(raw[key], &pageItems); err != nil {
			return nil, fmt.Errorf("GET %s: decode %s: %w", path, key, err)
		}
		items = append(items, pageItems...)

		// A page that does not say whether another follows is not taken
		// for the last: the list would be cut short without a word.
		var meta struct {
			Pagination struct {
				NextPage int `json:"next_page"`
			} `json:"pagination"`
		}
		if err := json.Unmarshal(raw["meta"], &meta); err != nil {
			return nil, fmt.Errorf("GET %s: decode meta: %w", path, err)
		}
		page = meta.Pagination.NextPage
		if page <= 0 {
			return items, nil
		}
	}
	return nil, tooManyPages("GET " + path)
}
