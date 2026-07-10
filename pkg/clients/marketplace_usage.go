package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	marketplaceUsageRoute        = "/marketplace/usage"
	managedComputeRoute          = "/managed-compute"
	managedComputeUsageSuffix    = "/usage"
	marketplaceUsageMaxBodyBytes = 1 << 20
)

type MarketplaceUsageClient struct {
	client    *http.Client
	endpoint  string
	authToken string
}

type MarketplaceUsageRequest struct {
	BuyerWorkspaceID          string                 `json:"buyer_workspace_id"`
	SellerWorkspaceID         string                 `json:"seller_workspace_id"`
	ListingID                 string                 `json:"listing_id"`
	MachineID                 string                 `json:"machine_id"`
	ContainerID               string                 `json:"container_id"`
	StubID                    string                 `json:"stub_id"`
	AppID                     string                 `json:"app_id"`
	UsageKind                 string                 `json:"usage_kind"`
	Runtime                   string                 `json:"runtime"`
	GPU                       string                 `json:"gpu"`
	GPUCount                  uint32                 `json:"gpu_count"`
	DurationSeconds           float64                `json:"duration_seconds"`
	BuyerCostCents            float64                `json:"buyer_cost_cents"`
	SellerPayoutEstimateCents float64                `json:"seller_payout_estimate_cents"`
	StartAt                   time.Time              `json:"start_at"`
	EndAt                     time.Time              `json:"end_at"`
	ContainerType             string                 `json:"container_type"`
	RuntimeMetadata           map[string]interface{} `json:"runtime_metadata"`
	OmitPrice                 bool                   `json:"-"`
}

func (u MarketplaceUsageRequest) MarshalJSON() ([]byte, error) {
	type alias MarketplaceUsageRequest
	var buyerCost, sellerPayout *float64
	if !u.OmitPrice {
		buyerCost = &u.BuyerCostCents
		sellerPayout = &u.SellerPayoutEstimateCents
	}
	return json.Marshal(struct {
		alias
		BuyerCostCents            *float64 `json:"buyer_cost_cents,omitempty"`
		SellerPayoutEstimateCents *float64 `json:"seller_payout_estimate_cents,omitempty"`
	}{
		alias:                     alias(u),
		BuyerCostCents:            buyerCost,
		SellerPayoutEstimateCents: sellerPayout,
	})
}

type marketplaceUsageResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}

func NewMarketplaceUsageClient(config types.ManagedComputeBillingConfig) *MarketplaceUsageClient {
	endpoint := marketplaceUsageEndpoint(config.Endpoint)
	if endpoint == "" || strings.EqualFold(strings.TrimSpace(config.Mode), "disabled") {
		return nil
	}

	return &MarketplaceUsageClient{
		client:    &http.Client{Timeout: config.TimeoutOrDefault()},
		endpoint:  endpoint,
		authToken: config.AuthToken,
	}
}

func (c *MarketplaceUsageClient) Record(ctx context.Context, usage MarketplaceUsageRequest) error {
	if c == nil || c.endpoint == "" {
		return nil
	}

	body, err := json.Marshal(usage)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, marketplaceUsageMaxBodyBytes))
	if err != nil {
		return err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("marketplace usage failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}

	var response marketplaceUsageResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return err
	}
	if !response.OK {
		return fmt.Errorf("marketplace usage rejected: %s", response.Message)
	}
	return nil
}

func marketplaceUsageEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}

	if parsed, err := url.Parse(endpoint); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		parsed.Path = marketplaceUsagePath(parsed.Path)
		return parsed.String()
	}
	return strings.TrimRight(endpoint, "/") + marketplaceUsageRoute + "/"
}

func marketplaceUsagePath(path string) string {
	path = strings.TrimRight(path, "/")
	// Already a marketplace usage path — keep the rewrite idempotent so a
	// pre-normalized endpoint config doesn't get mangled.
	if strings.HasSuffix(path, marketplaceUsageRoute) {
		return path + "/"
	}
	path = strings.TrimSuffix(path, managedComputeUsageSuffix)
	if strings.HasSuffix(path, managedComputeRoute) {
		path = strings.TrimSuffix(path, managedComputeRoute) + marketplaceUsageRoute
	} else {
		path += marketplaceUsageRoute
	}
	return path + "/"
}
