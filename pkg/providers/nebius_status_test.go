package providers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNebiusGetAndListFollowAnAsynchronousFirstStart(t *testing.T) {
	api := newNebiusAPI(t)
	client := api.client(testOwner)
	ctx := context.Background()
	created, err := client.Create(ctx, CreateRequest{Offer: nebiusH200, Name: "beam-startup-status-test"})
	require.NoError(t, err)
	for _, step := range []struct {
		state       string
		reconciling bool
		want        Status
	}{
		{"STOPPED", true, StatusPending},
		{"STARTING", true, StatusPending},
		{"RUNNING", false, StatusActive},
		{"STOPPED", false, StatusInterrupted},
	} {
		api.mu.Lock()
		status := api.instances[api.find(created.ID)]["status"].(map[string]any)
		status["state"], status["reconciling"] = step.state, step.reconciling
		api.mu.Unlock()
		got, err := client.Get(ctx, created.ID)
		require.NoError(t, err)
		require.Equal(t, step.want, got.Status, "Get: %s, reconciling=%v", step.state, step.reconciling)
		listed, err := client.List(ctx)
		require.NoError(t, err)
		require.Len(t, listed, 1)
		require.Equal(t, step.want, listed[0].Status, "List: %s, reconciling=%v", step.state, step.reconciling)
	}
}

func TestNebiusStoppedDuringAsyncCreateIsPending(t *testing.T) {
	for _, test := range []struct {
		name        string
		state       string
		reconciling bool
		want        Status
	}{
		{"preparing the first start", "STOPPED", true, StatusPending},
		{"completed spot stop", "STOPPED", false, StatusInterrupted},
		{"preemption stopping", "STOPPING", true, StatusInterrupted},
		{"running during reconciliation", "RUNNING", true, StatusActive},
		{"failed during reconciliation", "ERROR", true, StatusFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			body, err := json.Marshal(map[string]any{
				"spec":   map[string]any{"preemptible": map[string]any{}},
				"status": map[string]any{"state": test.state, "reconciling": test.reconciling},
			})
			require.NoError(t, err)
			var instance nebiusInstance
			require.NoError(t, json.Unmarshal(body, &instance))
			require.Equal(t, test.want, instance.status())
		})
	}
}
