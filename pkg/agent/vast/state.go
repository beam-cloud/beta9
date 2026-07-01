package vast

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

type runtimeState struct {
	GatewayURL string `json:"gateway_url"`
	MachineID  string `json:"machine_id"`
	AgentToken string `json:"agent_token"`
}

func gpuStateDir(root string, gpu GPU) string {
	return filepath.Join(root, "gpu-"+gpu.Index)
}

func loadRuntimeState(stateDir string) (*runtimeState, error) {
	data, err := os.ReadFile(filepath.Join(stateDir, types.AgentRuntimeStateFileName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var state runtimeState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.AgentToken == "" && state.MachineID == "" {
		return nil, nil
	}
	return &state, nil
}

type availabilityRequest struct {
	AgentToken  string `json:"agentToken"`
	Schedulable bool   `json:"schedulable"`
	Reason      string `json:"reason"`
	ObservedAt  int64  `json:"observedAt"`
}

type availabilityResponse struct {
	Ok     bool   `json:"ok"`
	ErrMsg string `json:"errMsg"`
}

func updateAvailability(ctx context.Context, client *http.Client, gatewayURL, agentToken string, schedulable bool, reason string) error {
	gatewayURL = strings.TrimRight(strings.TrimSpace(gatewayURL), "/")
	if gatewayURL == "" || agentToken == "" {
		return nil
	}
	httpClient := compute.HTTPClient{
		BaseURL: gatewayURL,
		Client:  client,
	}
	if httpClient.Client == nil {
		httpClient.Client = &http.Client{Timeout: 30 * time.Second}
	}
	req := availabilityRequest{
		AgentToken:  agentToken,
		Schedulable: schedulable,
		Reason:      reason,
		ObservedAt:  time.Now().UTC().UnixNano(),
	}
	var resp availabilityResponse
	if err := httpClient.Do(ctx, http.MethodPost, "/api/v1/gateway/agent/availability", req, &resp); err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("update agent availability: %s", firstNonEmpty(resp.ErrMsg, "gateway rejected request"))
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
