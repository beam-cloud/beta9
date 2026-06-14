package compute

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	HetznerDefaultBaseURL   = "https://api.hetzner.cloud/v1"
	hetznerProviderName     = "hetzner"
	hetznerDefaultImage     = "ubuntu-24.04"
	hetznerPageSize         = 50
	hetznerDefaultAvailable = 1
)

var hetznerDefaultRegions = []string{"ash", "hil"}

type HetznerClient struct {
	api                  HTTPClient
	image                string
	imageByRegion        map[string]string
	sshKeys              []string
	sshKeysByRegion      map[string][]string
	privateNetwork       HetznerPrivateNetworkConfig
	serverTypePrices     map[string]float64
	serverTypeCategories map[string]string
	regionMetadata       map[string]HetznerRegionMetadata
	defaultRegions       []string
	now                  func() time.Time
}

type HetznerConfig struct {
	APIToken             string
	BaseURL              string
	Image                string
	ImageByRegion        map[string]string
	SSHKeys              []string
	SSHKeysByRegion      map[string][]string
	PrivateNetwork       HetznerPrivateNetworkConfig
	ServerTypePrices     map[string]float64
	ServerTypeCategories map[string]string
	RegionMetadata       map[string]HetznerRegionMetadata
	DefaultRegions       []string
	Client               *http.Client
	Now                  func() time.Time
}

type HetznerPrivateNetworkConfig struct {
	ID            int64
	Name          string
	RegionIDs     map[string]int64
	RegionNames   map[string]string
	RequireSubnet *bool
}

type HetznerRegionMetadata struct {
	DisplayName string
	Latitude    float64
	Longitude   float64
}

type hetznerNetworkRef struct {
	ID   int64
	Name string
}

func (c HetznerPrivateNetworkConfig) configured() bool {
	return c.ID > 0 ||
		c.Name != "" ||
		len(c.RegionIDs) > 0 ||
		len(c.RegionNames) > 0
}

func (c HetznerPrivateNetworkConfig) refForRegion(region string) hetznerNetworkRef {
	if id := c.RegionIDs[region]; id > 0 {
		return hetznerNetworkRef{ID: id}
	}
	if name := c.RegionNames[region]; name != "" {
		return hetznerNetworkRef{Name: name}
	}
	return hetznerNetworkRef{ID: c.ID, Name: c.Name}
}

func (c HetznerPrivateNetworkConfig) requireSubnet() bool {
	if c.RequireSubnet == nil {
		return true
	}
	return *c.RequireSubnet
}

func NewHetzner(config HetznerConfig) *HetznerClient {
	baseURL := strings.TrimRight(config.BaseURL, "/")
	if baseURL == "" {
		baseURL = HetznerDefaultBaseURL
	}
	image := config.Image
	if image == "" {
		image = hetznerDefaultImage
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	defaultRegions := append([]string(nil), config.DefaultRegions...)
	if len(defaultRegions) == 0 {
		defaultRegions = append([]string(nil), hetznerDefaultRegions...)
	}
	return &HetznerClient{
		api: HTTPClient{
			BaseURL: baseURL,
			Token:   config.APIToken,
			Client:  config.Client,
		},
		image:                image,
		imageByRegion:        config.ImageByRegion,
		sshKeys:              append([]string(nil), config.SSHKeys...),
		sshKeysByRegion:      config.SSHKeysByRegion,
		privateNetwork:       config.PrivateNetwork,
		serverTypePrices:     config.ServerTypePrices,
		serverTypeCategories: config.ServerTypeCategories,
		regionMetadata:       config.RegionMetadata,
		defaultRegions:       defaultRegions,
		now:                  now,
	}
}

func (c *HetznerClient) Name() string {
	return hetznerProviderName
}

func (c *HetznerClient) ListOffers(ctx context.Context, req OfferRequest) ([]Offer, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if req.TotalGPUs > 0 || len(req.GPUs) > 0 {
		return nil, nil
	}

	locations, err := c.listLocations(ctx)
	if err != nil {
		return nil, err
	}
	locationByName := make(map[string]hetznerLocation, len(locations))
	for _, location := range locations {
		locationByName[location.Name] = location
	}

	serverTypes, err := c.listServerTypes(ctx)
	if err != nil {
		return nil, err
	}
	regions := req.Regions
	if len(regions) == 0 {
		regions = c.defaultRegions
	}

	offers := []Offer{}
	for _, serverType := range serverTypes {
		if serverType.Deprecated || serverType.Name == "" || serverType.Cores <= 0 || serverType.MemoryGB <= 0 {
			continue
		}
		for _, location := range serverType.Locations {
			if !location.Available || location.Deprecated || location.Name == "" {
				continue
			}
			if len(regions) > 0 && !containsString(regions, location.Name) {
				continue
			}
			hourlyCost := c.serverTypePriceMicros(serverType, location.Name)
			if hourlyCost <= 0 {
				continue
			}
			region := locationByName[location.Name]
			metadata := c.regionMetadata[location.Name]
			regionDisplayName := firstHetznerString(metadata.DisplayName, region.Description, region.City, location.Name)
			category := firstHetznerString(c.serverTypeCategories[serverType.Name], c.serverTypeCategories[serverType.Category], serverType.Category, serverType.CPUType)
			offers = append(offers, Offer{
				ID:                serverType.Name,
				Provider:          c.Name(),
				Cloud:             c.Name(),
				InstanceType:      serverType.Name,
				Region:            location.Name,
				GPU:               "",
				GPUCount:          0,
				MachineCount:      1,
				CPUMillicores:     int64(serverType.Cores * 1000),
				MemoryMB:          int64(serverType.MemoryGB * 1024),
				StorageMB:         int64(serverType.DiskGB * 1024),
				HourlyCostMicros:  hourlyCost,
				Available:         hetznerAvailableForRequest(req),
				DisplayName:       firstHetznerString(serverType.Description, serverType.Name),
				Category:          category,
				RegionDisplayName: regionDisplayName,
				Latitude:          firstNonZeroFloat(metadata.Latitude, region.Latitude),
				Longitude:         firstNonZeroFloat(metadata.Longitude, region.Longitude),
				Labels: map[string]string{
					"architecture": serverType.Architecture,
					"cpu_type":     serverType.CPUType,
					"network_zone": region.NetworkZone,
					"storage_type": serverType.StorageType,
				},
				Raw: serverType.Raw,
			})
		}
	}
	return offers, nil
}

func (c *HetznerClient) CreateReservation(ctx context.Context, req ReservationRequest) (*Reservation, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if req.Offer.ID == "" {
		return nil, fmt.Errorf("missing Hetzner server type")
	}
	if req.Offer.Region == "" {
		return nil, fmt.Errorf("missing Hetzner location")
	}
	networkID, err := c.privateNetworkIDForRegion(ctx, req.Offer.Region, req.Offer.Labels["network_zone"])
	if err != nil {
		return nil, err
	}

	body := map[string]any{
		"name":               ReservationNodeName(req),
		"server_type":        req.Offer.ID,
		"image":              c.imageForRegion(req.Offer.Region),
		"location":           req.Offer.Region,
		"start_after_create": true,
		"networks":           []int64{networkID},
		"labels": map[string]string{
			"beam_pool":       req.PoolName,
			"beam_machine_id": req.MachineID,
		},
	}
	if sshKeys := c.sshKeysForRegion(req.Offer.Region); len(sshKeys) > 0 {
		body["ssh_keys"] = sshKeys
	}
	if req.BootstrapCommand != "" {
		body["user_data"] = fmt.Sprintf("#!/usr/bin/env bash\nset -euo pipefail\n%s\n", req.BootstrapCommand)
	}

	var raw map[string]any
	if err := c.api.Do(ctx, http.MethodPost, "/servers", body, &raw); err != nil {
		return nil, err
	}

	server := hetznerServerFromResponse(raw)
	if server.ID == "" {
		return nil, fmt.Errorf("Hetzner server create response missing server id")
	}
	now := c.now().UTC()
	return &Reservation{
		ID:               server.ID,
		PoolName:         req.PoolName,
		Name:             ReservationNodeName(req),
		Selector:         req.Selector,
		Provider:         c.Name(),
		Cloud:            c.Name(),
		Region:           firstHetznerString(server.Location, req.Offer.Region),
		OfferID:          firstHetznerString(server.ServerTypeID, req.Offer.ID),
		InstanceType:     firstHetznerString(server.ServerTypeName, req.Offer.InstanceType, req.Offer.ID),
		InstanceID:       server.ID,
		MachineID:        req.MachineID,
		GPU:              "",
		GPUCount:         0,
		MachineCount:     firstNonZeroUint32(req.Offer.MachineCount, 1),
		CPUMillicores:    firstNonZeroInt64(server.CPUMillicores, req.Offer.CPUMillicores),
		MemoryMB:         firstNonZeroInt64(server.MemoryMB, req.Offer.MemoryMB),
		StorageMB:        firstNonZeroInt64(server.StorageMB, req.Offer.StorageMB),
		HourlyCostMicros: req.Offer.HourlyCostMicros,
		CommittedMicros:  req.Offer.HourlyCostMicros * WholeHours(req.TTL),
		Source:           req.Source,
		Status:           hetznerReservationStatus(server.Status),
		CreatedAt:        now,
		ExpiresAt:        now.Add(req.TTL),
		BillingRenewalAt: now.Add(time.Hour),
	}, nil
}

func (c *HetznerClient) GetReservation(ctx context.Context, id string) (*Reservation, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	var raw map[string]any
	if err := c.api.Do(ctx, http.MethodGet, "/servers/"+id, nil, &raw); err != nil {
		if statusErr, ok := err.(*HTTPStatusError); ok && statusErr.StatusCode == http.StatusNotFound {
			return &Reservation{ID: id, Provider: c.Name(), Cloud: c.Name(), InstanceID: id, Status: ReservationDeleted}, nil
		}
		return nil, err
	}
	server := hetznerServerFromResponse(raw)
	return &Reservation{
		ID:            firstHetznerString(server.ID, id),
		Provider:      c.Name(),
		Cloud:         c.Name(),
		Region:        server.Location,
		OfferID:       server.ServerTypeID,
		InstanceType:  server.ServerTypeName,
		InstanceID:    firstHetznerString(server.ID, id),
		MachineCount:  1,
		CPUMillicores: server.CPUMillicores,
		MemoryMB:      server.MemoryMB,
		StorageMB:     server.StorageMB,
		Status:        hetznerReservationStatus(server.Status),
	}, nil
}

func (c *HetznerClient) ExtendReservation(ctx context.Context, id string, expiresAt time.Time) error {
	return nil
}

func (c *HetznerClient) DeleteReservation(ctx context.Context, id string) error {
	if err := c.validate(); err != nil {
		return err
	}
	return c.api.Do(ctx, http.MethodDelete, "/servers/"+id, nil, nil)
}

func (c *HetznerClient) validate() error {
	if c == nil {
		return fmt.Errorf("Hetzner client is nil")
	}
	if c.api.Token == "" {
		return fmt.Errorf("Hetzner apiToken is required")
	}
	return nil
}

func (c *HetznerClient) listServerTypes(ctx context.Context) ([]hetznerServerType, error) {
	items, err := c.getPaginated(ctx, "/server_types", "server_types")
	if err != nil {
		return nil, err
	}
	serverTypes := make([]hetznerServerType, 0, len(items))
	for _, item := range items {
		serverTypes = append(serverTypes, hetznerServerTypeFromMap(item))
	}
	return serverTypes, nil
}

func (c *HetznerClient) listLocations(ctx context.Context) ([]hetznerLocation, error) {
	items, err := c.getPaginated(ctx, "/locations", "locations")
	if err != nil {
		return nil, err
	}
	locations := make([]hetznerLocation, 0, len(items))
	for _, item := range items {
		locations = append(locations, hetznerLocationFromMap(item))
	}
	return locations, nil
}

func (c *HetznerClient) listNetworks(ctx context.Context) ([]hetznerNetwork, error) {
	items, err := c.getPaginated(ctx, "/networks", "networks")
	if err != nil {
		return nil, err
	}
	networks := make([]hetznerNetwork, 0, len(items))
	for _, item := range items {
		networks = append(networks, hetznerNetworkFromMap(item))
	}
	return networks, nil
}

func (c *HetznerClient) getPaginated(ctx context.Context, path, key string) ([]map[string]any, error) {
	items := []map[string]any{}
	for page := int64(1); page > 0; {
		var raw map[string]any
		if err := c.api.Do(ctx, http.MethodGet, fmt.Sprintf("%s?page=%d&per_page=%d", path, page, hetznerPageSize), nil, &raw); err != nil {
			return nil, err
		}
		items = append(items, mapsFromArray(jsonArray(raw, key))...)
		page = nextPage(raw)
	}
	return items, nil
}

func (c *HetznerClient) serverTypePriceMicros(serverType hetznerServerType, location string) int64 {
	for _, key := range []string{serverType.Name + ":" + location, serverType.Name} {
		if dollars, ok := c.serverTypePrices[key]; ok && dollars > 0 {
			return DollarsToMicros(dollars)
		}
	}
	if price := serverType.Prices[location]; price > 0 {
		return price
	}
	return 0
}

func (c *HetznerClient) imageForRegion(region string) string {
	if image := c.imageByRegion[region]; image != "" {
		return image
	}
	return c.image
}

func (c *HetznerClient) sshKeysForRegion(region string) []string {
	if keys := c.sshKeysByRegion[region]; len(keys) > 0 {
		return append([]string(nil), keys...)
	}
	return append([]string(nil), c.sshKeys...)
}

func (c *HetznerClient) privateNetworkIDForRegion(ctx context.Context, region, networkZone string) (int64, error) {
	if !c.privateNetwork.configured() {
		return 0, fmt.Errorf("Hetzner privateNetwork id or name is required to launch private pool machines")
	}
	if networkZone == "" {
		location, err := c.locationForRegion(ctx, region)
		if err != nil {
			return 0, err
		}
		networkZone = location.NetworkZone
	}
	ref := c.privateNetwork.refForRegion(region)
	if ref.ID <= 0 && ref.Name == "" {
		return 0, fmt.Errorf("Hetzner privateNetwork has no id or name for location %q network zone %q", region, networkZone)
	}

	networks, err := c.listNetworks(ctx)
	if err != nil {
		return 0, err
	}
	for _, network := range networks {
		if !network.matches(ref) {
			continue
		}
		if c.privateNetwork.requireSubnet() && !network.supportsNetworkZone(networkZone) {
			return 0, fmt.Errorf("Hetzner private network %q has no cloud subnet in network zone %q for location %q", ref.String(), networkZone, region)
		}
		return network.ID, nil
	}

	if ref.ID > 0 && !c.privateNetwork.requireSubnet() {
		return ref.ID, nil
	}
	return 0, fmt.Errorf("Hetzner private network %q was not found", ref.String())
}

func (c *HetznerClient) locationForRegion(ctx context.Context, region string) (hetznerLocation, error) {
	locations, err := c.listLocations(ctx)
	if err != nil {
		return hetznerLocation{}, err
	}
	for _, location := range locations {
		if location.Name == region {
			return location, nil
		}
	}
	return hetznerLocation{}, fmt.Errorf("Hetzner location %q was not found", region)
}

type hetznerServerType struct {
	ID           int64
	Name         string
	Description  string
	Category     string
	CPUType      string
	StorageType  string
	Architecture string
	Cores        float64
	MemoryGB     float64
	DiskGB       float64
	Deprecated   bool
	Prices       map[string]int64
	Locations    []hetznerServerTypeLocation
	Raw          []byte
}

type hetznerServerTypeLocation struct {
	ID         int64
	Name       string
	Available  bool
	Deprecated bool
}

type hetznerLocation struct {
	ID          int64
	Name        string
	Description string
	Country     string
	City        string
	Latitude    float64
	Longitude   float64
	NetworkZone string
}

type hetznerNetwork struct {
	ID      int64
	Name    string
	Subnets []hetznerSubnet
}

type hetznerSubnet struct {
	Type        string
	NetworkZone string
	IPRange     string
}

func (r hetznerNetworkRef) String() string {
	if r.ID > 0 {
		return strconv.FormatInt(r.ID, 10)
	}
	return r.Name
}

func (n hetznerNetwork) matches(ref hetznerNetworkRef) bool {
	if ref.ID > 0 && n.ID == ref.ID {
		return true
	}
	return ref.Name != "" && n.Name == ref.Name
}

func (n hetznerNetwork) supportsNetworkZone(networkZone string) bool {
	for _, subnet := range n.Subnets {
		if subnet.NetworkZone != networkZone {
			continue
		}
		switch subnet.Type {
		case "", "cloud", "server":
			return true
		}
	}
	return false
}

func hetznerServerTypeFromMap(m map[string]any) hetznerServerType {
	raw, _ := json.Marshal(m)
	locations := []hetznerServerTypeLocation{}
	for _, item := range jsonArray(m, "locations") {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		locations = append(locations, hetznerServerTypeLocation{
			ID:         jsonInt64(entry, "id"),
			Name:       jsonString(entry, "name"),
			Available:  jsonBool(entry, "available"),
			Deprecated: entry["deprecation"] != nil,
		})
	}
	prices := map[string]int64{}
	for _, item := range jsonArray(m, "prices") {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		location := jsonString(entry, "location")
		hourly, _ := entry["price_hourly"].(map[string]any)
		price := DollarsToMicros(jsonFloat64(hourly, "net"))
		if location != "" && price > 0 {
			prices[location] = price
		}
	}
	return hetznerServerType{
		ID:           jsonInt64(m, "id"),
		Name:         jsonString(m, "name"),
		Description:  jsonString(m, "description"),
		Category:     jsonString(m, "category"),
		CPUType:      jsonString(m, "cpu_type"),
		StorageType:  jsonString(m, "storage_type"),
		Architecture: jsonString(m, "architecture"),
		Cores:        jsonFloat64(m, "cores"),
		MemoryGB:     jsonFloat64(m, "memory"),
		DiskGB:       jsonFloat64(m, "disk"),
		Deprecated:   jsonBool(m, "deprecated") || m["deprecation"] != nil,
		Prices:       prices,
		Locations:    locations,
		Raw:          raw,
	}
}

func hetznerLocationFromMap(m map[string]any) hetznerLocation {
	return hetznerLocation{
		ID:          jsonInt64(m, "id"),
		Name:        jsonString(m, "name"),
		Description: jsonString(m, "description"),
		Country:     jsonString(m, "country"),
		City:        jsonString(m, "city"),
		Latitude:    jsonFloat64(m, "latitude"),
		Longitude:   jsonFloat64(m, "longitude"),
		NetworkZone: jsonString(m, "network_zone"),
	}
}

func hetznerNetworkFromMap(m map[string]any) hetznerNetwork {
	subnets := []hetznerSubnet{}
	for _, item := range jsonArray(m, "subnets") {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		subnets = append(subnets, hetznerSubnet{
			Type:        jsonString(entry, "type"),
			NetworkZone: jsonString(entry, "network_zone"),
			IPRange:     jsonString(entry, "ip_range"),
		})
	}
	return hetznerNetwork{
		ID:      jsonInt64(m, "id"),
		Name:    jsonString(m, "name"),
		Subnets: subnets,
	}
}

type hetznerServer struct {
	ID             string
	Name           string
	Status         string
	Location       string
	ServerTypeID   string
	ServerTypeName string
	CPUMillicores  int64
	MemoryMB       int64
	StorageMB      int64
}

func hetznerServerFromResponse(raw map[string]any) hetznerServer {
	if server, ok := raw["server"].(map[string]any); ok {
		return hetznerServerFromMap(server)
	}
	return hetznerServerFromMap(raw)
}

func hetznerServerFromMap(m map[string]any) hetznerServer {
	serverType, _ := m["server_type"].(map[string]any)
	location, _ := m["location"].(map[string]any)
	id := jsonString(m, "id")
	if id == "" {
		if numericID := jsonInt64(m, "id"); numericID > 0 {
			id = strconv.FormatInt(numericID, 10)
		}
	}
	return hetznerServer{
		ID:             id,
		Name:           jsonString(m, "name"),
		Status:         jsonString(m, "status"),
		Location:       jsonString(location, "name"),
		ServerTypeID:   firstHetznerString(jsonString(serverType, "name"), jsonString(serverType, "id")),
		ServerTypeName: jsonString(serverType, "name"),
		CPUMillicores:  int64(jsonFloat64(serverType, "cores") * 1000),
		MemoryMB:       int64(jsonFloat64(serverType, "memory") * 1024),
		StorageMB:      int64(jsonFloat64(serverType, "disk") * 1024),
	}
}

func hetznerReservationStatus(status string) ReservationStatus {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "running":
		return ReservationActive
	case "deleting", "deleted":
		return ReservationDeleted
	case "off", "unknown":
		return ReservationFailed
	default:
		return ReservationPending
	}
}

func hetznerAvailableForRequest(req OfferRequest) uint32 {
	if req.TotalMachines > 0 {
		return maxUint32(req.TotalMachines, 1)
	}
	return hetznerDefaultAvailable
}

func nextPage(raw map[string]any) int64 {
	meta, _ := raw["meta"].(map[string]any)
	pagination, _ := meta["pagination"].(map[string]any)
	return jsonInt64(pagination, "next_page")
}

func mapsFromArray(items []any) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func firstHetznerString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstNonZeroUint32(values ...uint32) uint32 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstNonZeroFloat(values ...float64) float64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func maxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
