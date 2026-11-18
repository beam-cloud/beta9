package compute

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// An accepted Verda purchase may be missing both its response ID and immediate
// list visibility. Exercise the real adapter and launch compensation together:
// the record must survive until reconciliation can find the machine by tag.
func TestVerdaAcceptedPurchaseWithoutIDRetainsDelayedCleanup(t *testing.T) {
	const instanceID = "5266216f-3dc0-4cf1-b842-000000000001"
	var mu sync.Mutex
	var native map[string]any
	var visible bool
	var creates, scripts int
	var deletes []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		write := func(value any) { _ = json.NewEncoder(w).Encode(value) }
		switch r.Method + " " + r.URL.Path {
		case "POST /oauth2/token":
			write(map[string]any{"access_token": "test-verda-token", "expires_in": 3600})
		case "GET /instance-types":
			write([]map[string]any{{
				"instance_type": "1A10G.4V", "name": "A10G",
				"cpu": map[string]int{"number_of_cores": 4}, "gpu": map[string]int{"number_of_gpus": 1},
				"memory": map[string]int{"size_in_gigabytes": 16}, "price_per_hour": "1.0", "spot_price": "0.5",
				"supported_os": []string{"24.04.cuda13.0.docker"},
			}})
		case "GET /volume-types":
			write([]map[string]any{{"type": "NVMe", "price": map[string]int{"cps_per_gb": 0}}})
		case "GET /instance-availability":
			write([]map[string]any{{"location_code": "FIN-01", "availabilities": []string{"1A10G.4V"}}})
		case "POST /scripts":
			scripts++
			write("5266216f-3dc0-4cf1-b842-000000000002")
		case "POST /instances":
			var request map[string]any
			if json.NewDecoder(r.Body).Decode(&request) != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			creates++
			native = map[string]any{
				"id": instanceID, "status": "ordered", "tags": request["tags"],
				"hostname": request["hostname"], "startup_script_id": request["startup_script_id"],
				"os_volume_id": "5266216f-3dc0-4cf1-b842-000000000003",
			}
			w.WriteHeader(http.StatusAccepted)
			write(nil) // The purchase succeeds but its ID is absent from the answer.
		case "GET /instances":
			if visible && native != nil {
				write([]map[string]any{native})
			} else {
				write([]any{})
			}
		case "GET /scripts":
			write([]any{})
		case "GET /instances/" + instanceID:
			if native == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			write(native)
		case "PUT /instances":
			var request struct {
				ID     string `json:"id"`
				Action string `json:"action"`
			}
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.ID != instanceID || request.Action != "delete" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			deletes = append(deletes, request.ID)
			native = nil
			write([]map[string]string{{"status": "success"}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	provider, err := providers.NewVerda(types.VerdaProviderConfig{
		ClientID: "test-id", ClientSecret: "test-secret", BaseURL: server.URL,
	}, testOwner)
	require.NoError(t, err)
	repo := &fakeComputeRepo{}
	service := launchService(repo, provider)
	response := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), service, 1)
	require.False(t, response.Ok)
	require.Equal(t, launchErrorProviderFailure, response.ErrorCode)
	require.Len(t, repo.pools["workspace-1"], 1, "compensation must retain the delayed purchase")
	require.Contains(t, response.ErrMsg, "does not list it yet")
	state := repo.pools["workspace-1"][0]
	require.Len(t, state.Reservations, 1)
	reservation := state.Reservations[0]
	require.Empty(t, computeReservationInstanceID(reservation))
	require.NotEmpty(t, reservation.MachineID)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	requireEveryTokenRevoked(t, repo)

	mu.Lock()
	require.Equal(t, 1, creates, "an accepted purchase must not be repeated")
	require.Equal(t, 1, scripts)
	require.Empty(t, deletes)
	visible = true
	mu.Unlock()
	require.NoError(t, service.reconcileManagedComputeAt(context.Background(), reservation.CreatedAt.Add(time.Minute)))
	reservation = repo.pools["workspace-1"][0].Reservations[0]
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Equal(t, instanceID, reservation.InstanceID)
	mu.Lock()
	defer mu.Unlock()
	require.Nil(t, native, "reconciliation removes the real delayed instance")
	require.Equal(t, []string{instanceID}, deletes)
	require.Equal(t, 1, creates, "cleanup must never retry the purchase")
	require.Equal(t, 1, scripts)
}
