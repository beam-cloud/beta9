package compute

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const ShadeformDefaultBaseURL = "https://api.shadeform.ai/v1"

// shadeformAutoDeleteGrace pads the provider-side auto-delete threshold past
// our own expiry. The control plane owns reservation lifetime (renewal, final
// billing, termination); Shadeform's threshold is only a runaway backstop and
// must never fire before our reconciler has had a chance to act.
const shadeformAutoDeleteGrace = time.Hour

type ShadeformClient struct {
	api HTTPClient
}

type ShadeformConfig struct {
	APIKey  string
	BaseURL string
	Client  *http.Client
}

func NewShadeform(config ShadeformConfig) *ShadeformClient {
	baseURL := config.BaseURL
	if baseURL == "" {
		baseURL = ShadeformDefaultBaseURL
	}
	return &ShadeformClient{
		api: HTTPClient{
			BaseURL:    baseURL,
			Token:      config.APIKey,
			AuthHeader: "X-API-KEY",
			Client:     config.Client,
		},
	}
}

func (c *ShadeformClient) Name() string {
	return "shadeform"
}

func (c *ShadeformClient) ListOffers(ctx context.Context, req OfferRequest) ([]Offer, error) {
	var raw any
	if err := c.api.Do(ctx, http.MethodGet, "/instances/types", nil, &raw); err != nil {
		return nil, err
	}

	items := shadeformFlattenTypes(raw)
	offers := make([]Offer, 0, len(items))
	for _, item := range items {
		offer := shadeformOfferFromMap(item)
		if offer.ID == "" || offer.GPUCount == 0 {
			continue
		}
		// Only on-demand VM instances are instantly launchable. Baremetal types
		// are "request to launch" and shouldn't appear as ready-to-launch options.
		if deploymentType := jsonString(item, "deployment_type"); deploymentType != "" && deploymentType != "vm" {
			continue
		}
		// Only surface offers that are ready to launch right now (available in a region).
		if offer.Available == 0 {
			continue
		}
		offers = append(offers, offer)
	}
	return offers, nil
}

func (c *ShadeformClient) CreateReservation(ctx context.Context, req ReservationRequest) (*Reservation, error) {
	if req.Offer.ID == "" {
		return nil, fmt.Errorf("missing Shadeform cloud or instance type id")
	}
	cloud := req.Offer.Labels["cloud"]
	if cloud == "" {
		cloud = req.Offer.Cloud
	}
	if cloud == "" {
		cloud = req.Offer.Provider
	}
	instanceType := req.Offer.InstanceType
	if instanceType == "" {
		instanceType = req.Offer.ID
	}
	body := map[string]any{
		"cloud":               cloud,
		"shade_instance_type": instanceType,
		"shade_cloud":         true,
		"name":                ReservationNodeName(req),
	}
	if req.Offer.Region != "" {
		body["region"] = req.Offer.Region
	}
	if req.BootstrapCommand != "" {
		script := fmt.Sprintf("#!/usr/bin/env bash\nset -euo pipefail\n%s\n", req.BootstrapCommand)
		body["launch_configuration"] = map[string]any{
			"type": "script",
			"script_configuration": map[string]any{
				"base64_script": base64.StdEncoding.EncodeToString([]byte(script)),
			},
		}
	}
	if req.TTL > 0 || req.MaxSpendMicros > 0 {
		autoDelete := map[string]any{}
		if req.TTL > 0 {
			autoDelete["date_threshold"] = time.Now().Add(req.TTL + shadeformAutoDeleteGrace).UTC().Format(time.RFC3339)
		}
		if req.MaxSpendMicros > 0 {
			autoDelete["spend_threshold"] = fmt.Sprintf("%.2f", MicrosToDollars(req.MaxSpendMicros))
		}
		body["auto_delete"] = autoDelete
	}

	var raw map[string]any
	if err := c.api.Do(ctx, http.MethodPost, "/instances/create", body, &raw); err != nil {
		return nil, err
	}

	instanceID := jsonString(raw, "id", "instance_id")
	now := time.Now()
	return &Reservation{
		ID:               instanceID,
		PoolName:         req.PoolName,
		Name:             ReservationNodeName(req),
		Selector:         req.Selector,
		Provider:         c.Name(),
		Cloud:            req.Offer.Cloud,
		OfferID:          req.Offer.ID,
		InstanceType:     req.Offer.InstanceType,
		InstanceID:       instanceID,
		MachineID:        req.MachineID,
		GPU:              req.Offer.GPU,
		GPUCount:         req.Offer.GPUCount,
		CPUMillicores:    req.Offer.CPUMillicores,
		MemoryMB:         req.Offer.MemoryMB,
		HourlyCostMicros: req.Offer.HourlyCostMicros,
		CommittedMicros:  req.Offer.HourlyCostMicros * WholeHours(req.TTL),
		Source:           req.Source,
		Status:           ReservationPending,
		CreatedAt:        now,
		ExpiresAt:        now.Add(req.TTL),
		BillingRenewalAt: now.Add(time.Hour),
	}, nil
}

func (c *ShadeformClient) GetReservation(ctx context.Context, id string) (*Reservation, error) {
	var raw map[string]any
	if err := c.api.Do(ctx, http.MethodGet, fmt.Sprintf("/instances/%s/info", id), nil, &raw); err != nil {
		return nil, err
	}
	offer := shadeformOfferFromMap(raw)
	return &Reservation{
		ID:               id,
		Provider:         c.Name(),
		Cloud:            offer.Cloud,
		OfferID:          offer.ID,
		InstanceType:     offer.InstanceType,
		InstanceID:       id,
		GPU:              offer.GPU,
		GPUCount:         offer.GPUCount,
		CPUMillicores:    offer.CPUMillicores,
		MemoryMB:         offer.MemoryMB,
		HourlyCostMicros: offer.HourlyCostMicros,
		Status:           ReservationActive,
	}, nil
}

// ExtendReservation pushes the renewed expiry to Shadeform's auto-delete
// date threshold. spend_threshold is omitted so it stays unchanged.
func (c *ShadeformClient) ExtendReservation(ctx context.Context, id string, expiresAt time.Time) error {
	body := map[string]any{
		"auto_delete": map[string]any{
			"date_threshold": expiresAt.Add(shadeformAutoDeleteGrace).UTC().Format(time.RFC3339),
		},
	}
	return c.api.Do(ctx, http.MethodPost, fmt.Sprintf("/instances/%s/update", id), body, nil)
}

func (c *ShadeformClient) DeleteReservation(ctx context.Context, id string) error {
	return c.api.Do(ctx, http.MethodPost, fmt.Sprintf("/instances/%s/delete", id), map[string]any{}, nil)
}

func shadeformFlattenTypes(raw any) []map[string]any {
	switch t := raw.(type) {
	case []any:
		return shadeformMapsFromArray(t)
	case map[string]any:
		if arr := jsonArray(t, "instance_types", "types", "data"); arr != nil {
			return shadeformMapsFromArray(arr)
		}
		result := []map[string]any{}
		for cloud, value := range t {
			arr, ok := value.([]any)
			if !ok {
				continue
			}
			for _, item := range arr {
				m, ok := item.(map[string]any)
				if !ok {
					continue
				}
				if _, exists := m["cloud"]; !exists {
					m["cloud"] = cloud
				}
				result = append(result, m)
			}
		}
		return result
	default:
		return nil
	}
}

func shadeformMapsFromArray(items []any) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if ok {
			result = append(result, m)
		}
	}
	return result
}

func shadeformOfferFromMap(m map[string]any) Offer {
	raw, _ := json.Marshal(m)
	fields := shadeformOfferFields(m)
	gpuCount := uint32(jsonInt64(fields, "num_gpus", "gpu_count", "gpus"))

	// Shadeform reports hourly_price in cents.
	hourlyCents := jsonFloat64(fields, "hourly_price", "price", "cost_per_hour", "hourly_cost")

	// Memory is reported in GB (memory_in_gb); fall back to *_mb keys.
	memoryMB := jsonInt64(fields, "memory_mb", "memory", "ram")
	if memoryMB == 0 {
		memoryMB = jsonInt64(fields, "memory_in_gb") * 1024
	}

	// Storage is reported in GB (storage_in_gb); fall back to *_mb keys.
	storageMB := jsonInt64(fields, "storage_mb")
	if storageMB == 0 {
		storageMB = jsonInt64(fields, "storage_in_gb", "disk_in_gb", "ephemeral_storage_in_gb") * 1024
	}

	// Shadeform returns availability as an array of {region, available, ...}.
	region, available := shadeformAvailability(m)

	// Shadeform aggregates clouds; the "cloud" field is the underlying provider.
	cloud := jsonString(m, "cloud", "provider")
	return Offer{
		ID:               jsonString(fields, "id", "shade_instance_type", "instance_type"),
		Provider:         "shadeform",
		Cloud:            cloud,
		InstanceType:     jsonString(fields, "shade_instance_type", "cloud_instance_type", "instance_type", "name", "id"),
		Region:           region,
		GPU:              NormalizeGPU(jsonString(fields, "gpu_type", "gpu", "gpu_name")),
		GPUCount:         gpuCount,
		CPUMillicores:    int64(jsonFloat64(fields, "vcpus", "cpu", "cpus") * 1000),
		MemoryMB:         memoryMB,
		StorageMB:        storageMB,
		HourlyCostMicros: DollarsToMicros(hourlyCents / 100),
		Available:        available,
		Labels:           map[string]string{"cloud": cloud},
		Raw:              raw,
	}
}

func shadeformOfferFields(m map[string]any) map[string]any {
	config, _ := m["configuration"].(map[string]any)
	if len(config) == 0 {
		return m
	}

	fields := make(map[string]any, len(config)+len(m))
	for key, value := range config {
		fields[key] = value
	}
	for key, value := range m {
		fields[key] = value
	}
	return fields
}

// shadeformAvailability returns a representative region and the number of
// regions where the instance type is currently available, parsed from
// Shadeform's availability array ([]{region, available, display_name}).
func shadeformAvailability(m map[string]any) (string, uint32) {
	arr, ok := m["availability"].([]any)
	if !ok {
		return jsonString(m, "region", "location"), uint32(jsonInt64(m, "available", "availability_count", "capacity"))
	}
	var available uint32
	firstRegion := ""
	availableRegion := ""
	for _, item := range arr {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		region := jsonString(entry, "region", "name")
		if firstRegion == "" {
			firstRegion = region
		}
		if isAvail, _ := entry["available"].(bool); isAvail {
			available++
			if availableRegion == "" {
				availableRegion = region
			}
		}
	}
	if availableRegion != "" {
		return availableRegion, available
	}
	return firstRegion, available
}
