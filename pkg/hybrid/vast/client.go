package vast

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/hybrid"
	"github.com/beam-cloud/beta9/pkg/hybrid/httpjson"
)

const DefaultBaseURL = "https://console.vast.ai/api/v0"

type Client struct {
	api httpjson.Client
}

type Config struct {
	APIKey  string
	BaseURL string
	Client  *http.Client
}

func New(config Config) *Client {
	baseURL := config.BaseURL
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{
		api: httpjson.Client{
			BaseURL: baseURL,
			Token:   config.APIKey,
			Client:  config.Client,
		},
	}
}

func (c *Client) Name() string {
	return "vast"
}

func (c *Client) ListOffers(ctx context.Context, req hybrid.OfferRequest) ([]hybrid.Offer, error) {
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

	items := httpjson.Array(raw, "offers", "results", "bundles")
	if items == nil {
		if arr, ok := raw["data"].([]any); ok {
			items = arr
		}
	}

	offers := make([]hybrid.Offer, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		offer := offerFromMap(m)
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

func (c *Client) CreateReservation(ctx context.Context, req hybrid.ReservationRequest) (*hybrid.Reservation, error) {
	if req.Offer.ID == "" {
		return nil, fmt.Errorf("missing Vast offer id")
	}
	body := map[string]any{
		"label":     req.PoolName,
		"client_id": req.Selector,
	}
	if req.BootstrapCommand != "" {
		body["onstart"] = req.BootstrapCommand
	}

	var raw map[string]any
	path := fmt.Sprintf("/asks/%s/", req.Offer.ID)
	if err := c.api.Do(ctx, http.MethodPut, path, body, &raw); err != nil {
		return nil, err
	}

	instanceID := httpjson.String(raw, "new_contract", "instance_id", "id")
	now := time.Now()
	return &hybrid.Reservation{
		ID:               instanceID,
		PoolName:         req.PoolName,
		Selector:         req.Selector,
		Provider:         c.Name(),
		OfferID:          req.Offer.ID,
		InstanceType:     req.Offer.InstanceType,
		InstanceID:       instanceID,
		GPU:              req.Offer.GPU,
		GPUCount:         req.Offer.GPUCount,
		CPUMillicores:    req.Offer.CPUMillicores,
		MemoryMB:         req.Offer.MemoryMB,
		HourlyCostMicros: req.Offer.HourlyCostMicros,
		CommittedMicros:  req.Offer.HourlyCostMicros * hybrid.WholeHours(req.TTL),
		Source:           req.Source,
		Status:           hybrid.ReservationPending,
		CreatedAt:        now,
		ExpiresAt:        now.Add(req.TTL),
		BillingRenewalAt: now.Add(time.Hour),
	}, nil
}

func (c *Client) GetReservation(ctx context.Context, id string) (*hybrid.Reservation, error) {
	var raw map[string]any
	if err := c.api.Do(ctx, http.MethodGet, fmt.Sprintf("/instances/%s/", id), nil, &raw); err != nil {
		return nil, err
	}
	offer := offerFromMap(raw)
	return &hybrid.Reservation{
		ID:               id,
		Provider:         c.Name(),
		OfferID:          offer.ID,
		InstanceType:     offer.InstanceType,
		InstanceID:       id,
		GPU:              offer.GPU,
		GPUCount:         offer.GPUCount,
		CPUMillicores:    offer.CPUMillicores,
		MemoryMB:         offer.MemoryMB,
		HourlyCostMicros: offer.HourlyCostMicros,
		Status:           hybrid.ReservationActive,
	}, nil
}

func (c *Client) DeleteReservation(ctx context.Context, id string) error {
	return c.api.Do(ctx, http.MethodDelete, fmt.Sprintf("/instances/%s/", id), nil, nil)
}

func offerFromMap(m map[string]any) hybrid.Offer {
	raw, _ := json.Marshal(m)
	id := httpjson.String(m, "id", "ask_contract_id", "bundle_id")
	if id == "" {
		if value := httpjson.Int64(m, "id", "ask_contract_id"); value > 0 {
			id = strconv.FormatInt(value, 10)
		}
	}
	gpuCount := uint32(httpjson.Int64(m, "num_gpus", "gpu_count", "gpus"))
	hourlyCost := httpjson.Float64(m, "dph_total", "price", "hourly_cost", "cost_per_hour")
	return hybrid.Offer{
		ID:               id,
		Provider:         "vast",
		InstanceType:     httpjson.String(m, "machine_id", "instance_type", "hostname"),
		Region:           httpjson.String(m, "geolocation", "region", "location"),
		GPU:              httpjson.String(m, "gpu_name", "gpu", "gpu_type"),
		GPUCount:         gpuCount,
		CPUMillicores:    int64(httpjson.Float64(m, "cpu_cores", "vcpus", "cpu") * 1000),
		MemoryMB:         int64(httpjson.Float64(m, "cpu_ram", "memory_mb", "ram") * 1024),
		HourlyCostMicros: hybrid.DollarsToMicros(hourlyCost),
		Reliability:      httpjson.Float64(m, "reliability2", "reliability", "score"),
		Available:        uint32(httpjson.Int64(m, "available", "availability", "rentable_count")),
		Raw:              raw,
	}
}
