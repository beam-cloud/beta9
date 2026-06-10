package compute

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

const VastDefaultBaseURL = "https://console.vast.ai/api/v0"

type VastClient struct {
	api HTTPClient
}

type VastConfig struct {
	APIKey  string
	BaseURL string
	Client  *http.Client
}

func NewVast(config VastConfig) *VastClient {
	baseURL := config.BaseURL
	if baseURL == "" {
		baseURL = VastDefaultBaseURL
	}
	return &VastClient{
		api: HTTPClient{
			BaseURL: baseURL,
			Token:   config.APIKey,
			Client:  config.Client,
		},
	}
}

func (c *VastClient) Name() string {
	return "vast"
}

func (c *VastClient) ListOffers(ctx context.Context, req OfferRequest) ([]Offer, error) {
	body := map[string]any{
		"type": "on-demand",
		"q": map[string]any{
			"rentable": map[string]any{"eq": true},
			"verified": map[string]any{"eq": true},
		},
	}
	if len(req.GPUs) > 0 {
		body["q"].(map[string]any)["gpu_name"] = map[string]any{"in": req.GPUs}
	}

	var raw map[string]any
	if err := c.api.Do(ctx, http.MethodPost, "/bundles/", body, &raw); err != nil {
		return nil, err
	}

	items := jsonArray(raw, "offers", "results", "bundles")
	if items == nil {
		if arr, ok := raw["data"].([]any); ok {
			items = arr
		}
	}

	offers := make([]Offer, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		offer := vastOfferFromMap(m)
		if offer.ID == "" || offer.GPUCount == 0 {
			continue
		}
		if req.TotalGPUs > 0 && offer.Available == 0 {
			offer.Available = 1
		}
		offers = append(offers, offer)
	}
	return offers, nil
}

func (c *VastClient) CreateReservation(ctx context.Context, req ReservationRequest) (*Reservation, error) {
	if req.Offer.ID == "" {
		return nil, fmt.Errorf("missing Vast offer id")
	}
	clientID := req.Selector
	if req.MachineID != "" {
		clientID = req.MachineID
	}
	body := map[string]any{
		"label":     ReservationNodeName(req),
		"client_id": clientID,
	}
	if req.BootstrapCommand != "" {
		body["onstart"] = req.BootstrapCommand
	}

	var raw map[string]any
	path := fmt.Sprintf("/asks/%s/", req.Offer.ID)
	if err := c.api.Do(ctx, http.MethodPut, path, body, &raw); err != nil {
		return nil, err
	}

	instanceID := jsonString(raw, "new_contract", "instance_id", "id")
	now := time.Now()
	return &Reservation{
		ID:               instanceID,
		PoolName:         req.PoolName,
		Name:             ReservationNodeName(req),
		Selector:         req.Selector,
		Provider:         c.Name(),
		Cloud:            reservationCloud(req.Offer.Cloud, c.Name()),
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

func (c *VastClient) GetReservation(ctx context.Context, id string) (*Reservation, error) {
	var raw map[string]any
	if err := c.api.Do(ctx, http.MethodGet, fmt.Sprintf("/instances/%s/", id), nil, &raw); err != nil {
		return nil, err
	}
	offer := vastOfferFromMap(raw)
	return &Reservation{
		ID:               id,
		Provider:         c.Name(),
		Cloud:            reservationCloud(offer.Cloud, c.Name()),
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

func reservationCloud(cloud, fallback string) string {
	if cloud != "" {
		return cloud
	}
	return fallback
}

// ExtendReservation is a no-op: Vast has no provider-side auto-delete
// threshold; reservation lifetime is enforced by our reconciler only.
func (c *VastClient) ExtendReservation(ctx context.Context, id string, expiresAt time.Time) error {
	return nil
}

func (c *VastClient) DeleteReservation(ctx context.Context, id string) error {
	return c.api.Do(ctx, http.MethodDelete, fmt.Sprintf("/instances/%s/", id), nil, nil)
}

func vastOfferFromMap(m map[string]any) Offer {
	raw, _ := json.Marshal(m)
	id := jsonString(m, "id", "ask_contract_id", "bundle_id")
	if id == "" {
		if value := jsonInt64(m, "id", "ask_contract_id"); value > 0 {
			id = strconv.FormatInt(value, 10)
		}
	}
	gpuCount := uint32(jsonInt64(m, "num_gpus", "gpu_count", "gpus"))
	hourlyCost := jsonFloat64(m, "dph_total", "price", "hourly_cost", "cost_per_hour")
	return Offer{
		ID:               id,
		Provider:         "vast",
		Cloud:            "vast",
		InstanceType:     jsonString(m, "machine_id", "instance_type", "hostname"),
		Region:           jsonString(m, "geolocation", "region", "location"),
		GPU:              NormalizeGPU(jsonString(m, "gpu_name", "gpu", "gpu_type")),
		GPUCount:         gpuCount,
		CPUMillicores:    int64(jsonFloat64(m, "cpu_cores", "vcpus", "cpu") * 1000),
		MemoryMB:         int64(jsonFloat64(m, "cpu_ram", "memory_mb", "ram") * 1024),
		HourlyCostMicros: DollarsToMicros(hourlyCost),
		Reliability:      jsonFloat64(m, "reliability2", "reliability", "score"),
		Available:        uint32(jsonInt64(m, "available", "availability", "rentable_count")),
		Raw:              raw,
	}
}
