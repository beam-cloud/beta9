package compute

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	billingModeHTTP     = "http"
	billingModeNoop     = "noop"
	billingModeDisabled = "disabled"

	launchErrorBillingUnavailable = "billing_unavailable"
	launchErrorInsufficientCredit = "insufficient_credits"
)

var errManagedBillingUnavailable = errors.New("managed compute billing is not configured")

type managedComputeBillingClient interface {
	CheckLaunchCredit(context.Context, billingCreditRequest) (billingDecision, error)
	CheckBalance(context.Context, string) (billingDecision, error)
	RecordManagedUsage(context.Context, managedUsage) error
}

type billingCreditRequest struct {
	WorkspaceID               string `json:"workspace_id"`
	PoolName                  string `json:"pool_name"`
	RequiredCents             int64  `json:"required_cents"`
	Quantity                  uint32 `json:"quantity"`
	EstimatedHourlyCostMicros int64  `json:"estimated_hourly_cost_micros"`
	EstimatedCommittedMicros  int64  `json:"estimated_committed_micros"`
}

type billingDecision struct {
	OK             bool   `json:"ok"`
	ErrorCode      string `json:"error_code"`
	Message        string `json:"message"`
	AvailableCents int64  `json:"available_cents"`
	RequiredCents  int64  `json:"required_cents"`
}

type managedUsage struct {
	WorkspaceID        string    `json:"workspace_id"`
	PoolName           string    `json:"pool_name"`
	ReservationID      string    `json:"reservation_id"`
	Provider           string    `json:"provider"`
	Cloud              string    `json:"cloud"`
	ProviderInstanceID string    `json:"provider_instance_id"`
	MachineID          string    `json:"machine_id"`
	GPU                string    `json:"gpu"`
	GPUCount           uint32    `json:"gpu_count"`
	MachineCount       uint32    `json:"machine_count"`
	CPUMillicores      int64     `json:"cpu_millicores"`
	MemoryMB           int64     `json:"memory_mb"`
	StorageMB          int64     `json:"storage_mb"`
	HourlyCostMicros   int64     `json:"hourly_cost_micros"`
	DurationSeconds    float64   `json:"duration_seconds"`
	CostCents          float64   `json:"cost_cents"`
	StartAt            time.Time `json:"start_at"`
	EndAt              time.Time `json:"end_at"`
}

func newManagedComputeBillingClient(config types.ManagedComputeBillingConfig) managedComputeBillingClient {
	mode := strings.ToLower(strings.TrimSpace(config.Mode))
	switch mode {
	case billingModeDisabled:
		return disabledManagedBilling{}
	case billingModeNoop:
		return noopManagedBilling{minimumCents: config.MinimumCreditCentsOrDefault()}
	case billingModeHTTP:
		return newHTTPManagedBilling(config)
	}

	if config.Endpoint != "" {
		return newHTTPManagedBilling(config)
	}
	if config.Required {
		return disabledManagedBilling{}
	}
	return noopManagedBilling{minimumCents: config.MinimumCreditCentsOrDefault()}
}

type noopManagedBilling struct {
	minimumCents int64
}

func (n noopManagedBilling) CheckLaunchCredit(_ context.Context, req billingCreditRequest) (billingDecision, error) {
	required := firstNonZeroInt64(req.RequiredCents, n.minimumCents)
	return billingDecision{OK: true, RequiredCents: required, AvailableCents: required}, nil
}

func (n noopManagedBilling) CheckBalance(_ context.Context, _ string) (billingDecision, error) {
	return billingDecision{OK: true, RequiredCents: n.minimumCents, AvailableCents: n.minimumCents}, nil
}

func (noopManagedBilling) RecordManagedUsage(context.Context, managedUsage) error {
	return nil
}

type disabledManagedBilling struct{}

func (disabledManagedBilling) CheckLaunchCredit(context.Context, billingCreditRequest) (billingDecision, error) {
	return billingDecision{OK: false, ErrorCode: launchErrorBillingUnavailable}, errManagedBillingUnavailable
}

func (disabledManagedBilling) CheckBalance(context.Context, string) (billingDecision, error) {
	return billingDecision{OK: false, ErrorCode: launchErrorBillingUnavailable}, errManagedBillingUnavailable
}

func (disabledManagedBilling) RecordManagedUsage(context.Context, managedUsage) error {
	return errManagedBillingUnavailable
}

type httpManagedBilling struct {
	endpoint     string
	authToken    string
	client       *http.Client
	minimumCents int64
}

func newHTTPManagedBilling(config types.ManagedComputeBillingConfig) managedComputeBillingClient {
	return &httpManagedBilling{
		endpoint:     strings.TrimRight(config.Endpoint, "/"),
		authToken:    config.AuthToken,
		minimumCents: config.MinimumCreditCentsOrDefault(),
		client:       &http.Client{Timeout: config.TimeoutOrDefault()},
	}
}

func (h *httpManagedBilling) CheckLaunchCredit(ctx context.Context, req billingCreditRequest) (billingDecision, error) {
	if req.RequiredCents <= 0 {
		req.RequiredCents = h.minimumCents
	}
	var decision billingDecision
	if err := h.post(ctx, "launch-check/", req, &decision); err != nil {
		return billingDecision{OK: false, ErrorCode: launchErrorBillingUnavailable, RequiredCents: req.RequiredCents}, err
	}
	if decision.RequiredCents <= 0 {
		decision.RequiredCents = req.RequiredCents
	}
	return decision, nil
}

func (h *httpManagedBilling) CheckBalance(ctx context.Context, workspaceID string) (billingDecision, error) {
	req := map[string]string{"workspace_id": workspaceID}
	var decision billingDecision
	if err := h.post(ctx, "balance/", req, &decision); err != nil {
		return billingDecision{OK: false, ErrorCode: launchErrorBillingUnavailable}, err
	}
	if decision.RequiredCents <= 0 {
		decision.RequiredCents = h.minimumCents
	}
	return decision, nil
}

func (h *httpManagedBilling) RecordManagedUsage(ctx context.Context, usage managedUsage) error {
	var response struct {
		OK      bool   `json:"ok"`
		Message string `json:"message"`
	}
	if err := h.post(ctx, "usage/", usage, &response); err != nil {
		return err
	}
	if !response.OK {
		return fmt.Errorf("managed compute usage rejected: %s", response.Message)
	}
	return nil
}

func (h *httpManagedBilling) post(ctx context.Context, path string, in, out any) error {
	if h.endpoint == "" {
		return errManagedBillingUnavailable
	}

	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinBillingURL(h.endpoint, path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if h.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+h.authToken)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("managed compute billing %s failed with status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	return json.Unmarshal(data, out)
}

func joinBillingURL(base, path string) string {
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(path, "/")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/" + strings.TrimLeft(path, "/")
	return parsed.String()
}
