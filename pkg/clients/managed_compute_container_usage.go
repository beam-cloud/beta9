package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	managedComputeContainerUsageMaxBodyBytes = 1 << 20
	managedComputeContainerUsageAttempts     = 3
)

type managedComputeContainerUsageRoute struct {
	prefix    string
	endpoint  string
	authToken string
	client    *http.Client
}

// ManagedComputeContainerUsageRecorder chooses one external ledger per interval.
// Tama gives shared-pool sandbox stubs the same prefix as its private pools, so
// one PoolRoute selects Tama billing in both placement modes. Marketplace is the
// fallback only when no route matches.
type ManagedComputeContainerUsageRecorder struct {
	routes      []managedComputeContainerUsageRoute
	marketplace *ManagedComputeUsageRecorder
}

func NewManagedComputeContainerUsageRecorder(
	config types.ManagedComputeConfig,
	worker WorkerIdentity,
) *ManagedComputeContainerUsageRecorder {
	recorder := &ManagedComputeContainerUsageRecorder{
		marketplace: NewManagedComputeUsageRecorder(config, worker),
		routes:      make([]managedComputeContainerUsageRoute, 0, len(config.Billing.PoolRoutes)),
	}
	for _, configured := range config.Billing.PoolRoutes {
		prefix := strings.TrimSpace(configured.PoolNamePrefix)
		routeConfig := configured.BillingConfig(config.Billing)
		mode := strings.ToLower(strings.TrimSpace(routeConfig.Mode))
		if prefix == "" || strings.TrimSpace(routeConfig.Endpoint) == "" || mode == "noop" || mode == "disabled" {
			continue
		}
		recorder.routes = append(recorder.routes, managedComputeContainerUsageRoute{
			prefix:    prefix,
			endpoint:  strings.TrimRight(routeConfig.Endpoint, "/") + "/usage/",
			authToken: routeConfig.AuthToken,
			client:    &http.Client{Timeout: routeConfig.TimeoutOrDefault()},
		})
	}
	if len(recorder.routes) == 0 && recorder.marketplace == nil {
		return nil
	}
	return recorder
}

func (r *ManagedComputeContainerUsageRecorder) RecordContainerUsage(
	ctx context.Context,
	request *types.ContainerRequest,
	start, end time.Time,
	costCents *float64,
) error {
	if r == nil || request == nil {
		return nil
	}

	if route := r.routeFor(request); route != nil {
		body, err := managedComputeContainerUsageBody(request, start, end, costCents)
		if err != nil {
			return err
		}
		return route.post(ctx, body)
	}
	if r.marketplace != nil {
		return r.marketplace.RecordContainerUsage(ctx, request, start, end, costCents)
	}
	return nil
}

func (r *ManagedComputeContainerUsageRecorder) routeFor(request *types.ContainerRequest) *managedComputeContainerUsageRoute {
	if request.Stub.Type.Kind() != types.StubTypeSandbox {
		return nil
	}
	for i := range r.routes {
		if strings.HasPrefix(request.Stub.Name, r.routes[i].prefix) {
			return &r.routes[i]
		}
	}
	return nil
}

func managedComputeContainerUsageBody(
	request *types.ContainerRequest,
	start, end time.Time,
	costCents *float64,
) ([]byte, error) {
	if request.WorkspaceId == "" || request.ContainerId == "" || !end.After(start) {
		return nil, fmt.Errorf("managed compute usage is missing attribution or duration")
	}

	// The quote is optional: the cost hook cannot price every deployment, and
	// the ledger stops machines whose usage goes silent. An unquoted window is
	// sent with its resources and duration, and the ledger prices it from its
	// own rate card.
	var costMicros *int64
	if costCents != nil && *costCents > 0 && !math.IsNaN(*costCents) && !math.IsInf(*costCents, 0) {
		micros := math.Ceil(*costCents * 10_000)
		if math.IsInf(micros, 0) || micros >= float64(1<<63) {
			return nil, fmt.Errorf("managed compute usage cost is too large")
		}
		quoted := int64(micros)
		costMicros = &quoted
	}

	payload := struct {
		WorkspaceID   string    `json:"workspace_id"`
		ReservationID string    `json:"reservation_id"`
		CostMicros    *int64    `json:"cost_micros,omitempty"`
		StartAt       time.Time `json:"start_at"`
		EndAt         time.Time `json:"end_at"`
		GPU           string    `json:"gpu"`
		GPUCount      uint32    `json:"gpu_count"`
		CPUMillicores int64     `json:"cpu_millicores"`
		MemoryMB      int64     `json:"memory_mb"`
	}{
		WorkspaceID:   request.WorkspaceId,
		ReservationID: request.ContainerId,
		CostMicros:    costMicros,
		StartAt:       start.UTC(),
		EndAt:         end.UTC(),
		GPU:           request.Gpu,
		GPUCount:      request.GpuCount,
		CPUMillicores: request.Cpu,
		MemoryMB:      request.Memory,
	}
	return json.Marshal(payload)
}

func (r managedComputeContainerUsageRoute) post(ctx context.Context, body []byte) error {
	var lastErr error
	for attempt := 0; attempt < managedComputeContainerUsageAttempts; attempt++ {
		retry, err := r.postOnce(ctx, body)
		if err == nil || !retry {
			return err
		}
		lastErr = err
		if attempt == managedComputeContainerUsageAttempts-1 {
			break
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func (r managedComputeContainerUsageRoute) postOnce(ctx context.Context, body []byte) (bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	request.Header.Set("Content-Type", "application/json")
	if r.authToken != "" {
		request.Header.Set("Authorization", "Bearer "+r.authToken)
	}

	response, err := r.client.Do(request)
	if err != nil {
		return true, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, managedComputeContainerUsageMaxBodyBytes))
	if err != nil {
		return true, err
	}
	if response.StatusCode >= http.StatusBadRequest {
		err := fmt.Errorf("managed compute usage failed with status %d: %s", response.StatusCode, strings.TrimSpace(string(data)))
		return response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError, err
	}

	var acknowledged struct {
		OK      bool   `json:"ok"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(data, &acknowledged); err != nil {
		return true, fmt.Errorf("decode managed compute usage response: %w", err)
	}
	if !acknowledged.OK {
		return false, fmt.Errorf("managed compute usage rejected: %s", acknowledged.Message)
	}
	return false, nil
}
