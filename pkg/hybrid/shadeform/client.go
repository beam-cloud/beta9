package shadeform

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/beam-cloud/beta9/pkg/hybrid"
	"github.com/beam-cloud/beta9/pkg/hybrid/httpjson"
)

const DefaultBaseURL = "https://api.shadeform.ai/v1"

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
	return "shadeform"
}

func (c *Client) ListOffers(ctx context.Context, req hybrid.OfferRequest) ([]hybrid.Offer, error) {
	var raw any
	if err := c.api.Do(ctx, http.MethodGet, "/instances/types", nil, &raw); err != nil {
		return nil, err
	}

	items := flattenTypes(raw)
	offers := make([]hybrid.Offer, 0, len(items))
	for _, item := range items {
		offer := offerFromMap(item)
		if offer.ID == "" || offer.GPUCount == 0 {
			continue
		}
		offers = append(offers, offer)
	}
	return offers, nil
}

func (c *Client) CreateReservation(ctx context.Context, req hybrid.ReservationRequest) (*hybrid.Reservation, error) {
	if req.Offer.ID == "" {
		return nil, fmt.Errorf("missing Shadeform cloud or instance type id")
	}
	cloud := req.Offer.Labels["cloud"]
	if cloud == "" {
		cloud = req.Offer.Provider
	}
	body := map[string]any{
		"cloud":         cloud,
		"instance_type": req.Offer.ID,
		"name":          req.PoolName,
	}
	if req.Offer.Region != "" {
		body["region"] = req.Offer.Region
	}

	var raw map[string]any
	if err := c.api.Do(ctx, http.MethodPost, "/instances/create", body, &raw); err != nil {
		return nil, err
	}

	instanceID := httpjson.String(raw, "id", "instance_id")
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
	if err := c.api.Do(ctx, http.MethodGet, fmt.Sprintf("/instances/%s/info", id), nil, &raw); err != nil {
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
	return c.api.Do(ctx, http.MethodPost, fmt.Sprintf("/instances/%s/delete", id), map[string]any{}, nil)
}

func flattenTypes(raw any) []map[string]any {
	switch t := raw.(type) {
	case []any:
		return mapsFromArray(t)
	case map[string]any:
		if arr := httpjson.Array(t, "instance_types", "types", "data"); arr != nil {
			return mapsFromArray(arr)
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

func mapsFromArray(items []any) []map[string]any {
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if ok {
			result = append(result, m)
		}
	}
	return result
}

func offerFromMap(m map[string]any) hybrid.Offer {
	raw, _ := json.Marshal(m)
	gpuCount := uint32(httpjson.Int64(m, "gpu_count", "num_gpus", "gpus"))
	hourlyCost := httpjson.Float64(m, "hourly_price", "price", "cost_per_hour", "hourly_cost")
	provider := httpjson.String(m, "cloud", "provider")
	if provider == "" {
		provider = "shadeform"
	}
	return hybrid.Offer{
		ID:               httpjson.String(m, "id", "instance_type", "shade_instance_type"),
		Provider:         "shadeform",
		InstanceType:     httpjson.String(m, "instance_type", "name", "id"),
		Region:           httpjson.String(m, "region", "location"),
		GPU:              httpjson.String(m, "gpu_type", "gpu", "gpu_name"),
		GPUCount:         gpuCount,
		CPUMillicores:    int64(httpjson.Float64(m, "vcpus", "cpu", "cpus") * 1000),
		MemoryMB:         httpjson.Int64(m, "memory_mb", "memory", "ram"),
		HourlyCostMicros: hybrid.DollarsToMicros(hourlyCost),
		Reliability:      httpjson.Float64(m, "reliability", "availability"),
		Available:        uint32(httpjson.Int64(m, "available", "availability_count", "capacity")),
		Labels:           map[string]string{"cloud": provider},
		Raw:              raw,
	}
}
