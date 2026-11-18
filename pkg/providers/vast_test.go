package providers

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// vastAsksPath is where Vast takes a bid for a machine.
const vastAsksPath = "/api/v0/asks/"

// A real VM bid returned HTTP 200, success:false, and a positive contract.
// Rejecting that answer loses the purchased resource and prevents startup.
func TestVastCreateRetainsQueuedContractDespiteFalseSuccess(t *testing.T) {
	api := newVastAPI(t)
	api.set(func() {
		api.refusal = &vastReply{http.StatusOK, map[string]any{
			"success": false, "new_contract": 61274519,
			"instance_api_key": "synthetic-response-secret",
		}}
	})
	tags := map[string]string{"beam-pool": "vast"}
	created, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: vast5090, Name: "beam-vast-queued", Tags: tags})
	require.NoError(t, err)
	require.Equal(t, Instance{ID: "61274519", Status: StatusPending, Tags: tags}, created)
	require.Equal(t, false, api.lastCreate()["cancel_unavail"])
	require.Equal(t, 1, api.sent(http.MethodPut, vastAsksPath), "the purchase is never retried")
}

func TestVastCreateMissingContractRemainsUnanswered(t *testing.T) {
	for name, body := range map[string]map[string]any{
		"empty":          {},
		"success only":   {"success": true},
		"false success":  {"success": false, "msg": "not ready"},
		"zero contract":  {"success": true, "new_contract": 0},
		"wrong contract": {"success": true, "new_contract": "not-a-number"},
	} {
		t.Run(name, func(t *testing.T) {
			api := newVastAPI(t)
			api.set(func() { api.refusal = &vastReply{http.StatusOK, body} })
			_, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: vast5090, Name: "beam-vast-unknown"})
			require.Error(t, err)
			require.True(t, Unanswered(err), "an accepted response without an ID cannot prove no purchase occurred")
			require.Equal(t, 1, api.sent(http.MethodPut, vastAsksPath))
		})
	}
}

func TestVastInitialBidBootPrecedesStoppedIntent(t *testing.T) {
	for _, state := range []string{"", vastCreated, vastLoading} {
		t.Run("initial "+state, func(t *testing.T) {
			// These stopped fields and null/loading actual states were
			// observed before the first boot of a real VM bid.
			instance := vastInstance{Bid: true, ActualStatus: state, State: vastStopped, IntendedStatus: vastStopped}
			require.Equal(t, StatusPending, instance.status())
		})
	}
	for name, instance := range map[string]vastInstance{
		"running with stopped intent": {Bid: true, ActualStatus: vastRunning, IntendedStatus: vastStopped},
		"running with stopped state":  {Bid: true, ActualStatus: vastRunning, State: vastStopped},
		"exited after running":        {Bid: true, ActualStatus: vastExited},
		"waiting after interruption":  {Bid: true, ActualStatus: vastScheduling},
	} {
		t.Run(name, func(t *testing.T) { require.Equal(t, StatusInterrupted, instance.status()) })
	}
	// The initial-VM exception does not change on-demand stopped states.
	require.Equal(t, StatusFailed, (vastInstance{ActualStatus: vastLoading, State: vastStopped}).status())
}
