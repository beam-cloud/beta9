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
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestDisabledProviderCannotSellStaleSampledOffers(t *testing.T) {
	const disabled, selling = "aws", "shadeform"
	service := launchService(&fakeComputeRepo{})
	service.providers = providers.NewRegistry(types.ProviderConfig{
		Disabled:  []string{disabled},
		AWS:       types.AWSProviderConfig{Regions: []types.AWSProviderRegionConfig{{Name: "us-east-1"}}},
		Shadeform: types.ShadeformProviderConfig{ApiKey: "synthetic-key"},
	}, testOwner)
	service.appConfig.ManagedCompute.Reservations.Providers = []string{disabled, selling}
	stale, available := a10gOffer, a10gOffer
	stale.Provider, stale.ID = disabled, "stale-offer"
	available.Provider, available.ID = selling, "available-offer"
	service.sampledOffers = heardOffers{disabled: {stale}, selling: {available}}
	result, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{
		Pool: &pb.PoolConfig{Providers: []string{disabled, selling}, Gpu: []string{a10gOffer.GPU}},
	})
	require.NoError(t, err)
	require.True(t, result.Ok, result.ErrMsg)
	require.Len(t, result.Offers, 1)
	require.Equal(t, selling, result.Offers[0].Provider)
	require.Equal(t, available.ID, result.Offers[0].Id)
}

// lateVerda stands in for Verda where it takes an order for an instance
// without saying which instance it made, and does not list the instance
// until told to.
type lateVerda struct {
	mu sync.Mutex
	// instance is the one that was ordered, and visible whether Verda
	// lists it yet.
	instance map[string]any
	visible  bool
	// What was asked for: the instances and scripts made, and the
	// instances deleted.
	creates, scripts int
	deletes          []string
}

// What lateVerda sells and makes.
const (
	lateVerdaType     = "1A10G.4V"
	lateVerdaInstance = "5266216f-3dc0-4cf1-b842-000000000001"
	lateVerdaScript   = "5266216f-3dc0-4cf1-b842-000000000002"
	lateVerdaVolume   = "5266216f-3dc0-4cf1-b842-000000000003"
)

func (v *lateVerda) serve(w http.ResponseWriter, r *http.Request) {
	v.mu.Lock()
	defer v.mu.Unlock()
	write := func(value any) { _ = json.NewEncoder(w).Encode(value) }
	switch r.Method + " " + r.URL.Path {
	case "POST /oauth2/token":
		write(map[string]any{"access_token": "test-verda-token", "expires_in": 3600})
	case "GET /instance-types":
		write([]map[string]any{{
			"instance_type": lateVerdaType, "name": a10gOffer.GPU,
			"cpu": map[string]int{"number_of_cores": 4}, "gpu": map[string]int{"number_of_gpus": 1},
			"memory": map[string]int{"size_in_gigabytes": 16}, "price_per_hour": "1.0", "spot_price": "0.5",
			"supported_os": []string{"24.04.cuda13.0.docker"},
		}})
	case "GET /volume-types":
		write([]map[string]any{{"type": "NVMe", "price": map[string]int{"cps_per_gb": 0}}})
	case "GET /instance-availability":
		write([]map[string]any{{"location_code": "FIN-01", "availabilities": []string{lateVerdaType}}})
	case "POST /scripts":
		v.scripts++
		write(lateVerdaScript)
	case "POST /instances":
		var request map[string]any
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		v.creates++
		v.instance = map[string]any{
			"id": lateVerdaInstance, "status": "ordered", "tags": request["tags"],
			"hostname": request["hostname"], "startup_script_id": request["startup_script_id"],
			"os_volume_id": lateVerdaVolume,
		}
		w.WriteHeader(http.StatusAccepted)
		write(nil) // The order went through, and its answer names no instance.
	case "GET /instances":
		if v.visible && v.instance != nil {
			write([]map[string]any{v.instance})
		} else {
			write([]any{})
		}
	case "GET /scripts":
		write([]any{})
	case "GET /instances/" + lateVerdaInstance:
		if v.instance == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		write(v.instance)
	case "PUT /instances":
		var request struct {
			ID     string `json:"id"`
			Action string `json:"action"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.ID != lateVerdaInstance || request.Action != "delete" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		v.deletes = append(v.deletes, request.ID)
		v.instance = nil
		write([]map[string]string{{"status": "success"}})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// An accepted Verda purchase may be missing both its response ID and immediate
// list visibility. Exercise the real adapter and launch compensation together:
// the record must survive until reconciliation can find the machine by tag.
func TestVerdaAcceptedPurchaseWithoutIDRetainsDelayedCleanup(t *testing.T) {
	const workspace = "workspace-1"
	verda := &lateVerda{}
	server := httptest.NewServer(http.HandlerFunc(verda.serve))
	t.Cleanup(server.Close)
	provider, err := providers.NewVerda(types.VerdaProviderConfig{
		ClientID: "test-id", ClientSecret: "test-secret", BaseURL: server.URL,
	}, testOwner)
	require.NoError(t, err)
	repo := &fakeComputeRepo{}
	service := launchService(repo, provider)
	response := launchA10Gs(t, testAuthContext(workspace, "token-1"), service, 1)
	require.False(t, response.Ok)
	require.Equal(t, launchErrorProviderFailure, response.ErrorCode)
	require.Len(t, repo.pools[workspace], 1, "compensation must retain the delayed purchase")
	require.Contains(t, response.ErrMsg, "does not list it yet")
	state := repo.pools[workspace][0]
	require.Len(t, state.Reservations, 1)
	reservation := state.Reservations[0]
	require.Empty(t, computeReservationInstanceID(reservation))
	require.NotEmpty(t, reservation.MachineID)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	requireEveryTokenRevoked(t, repo)

	verda.mu.Lock()
	require.Equal(t, 1, verda.creates, "an accepted purchase must not be repeated")
	require.Equal(t, 1, verda.scripts)
	require.Empty(t, verda.deletes)
	verda.visible = true
	verda.mu.Unlock()
	require.NoError(t, service.reconcileManagedComputeAt(context.Background(), reservation.CreatedAt.Add(time.Minute)))
	reservation = repo.pools[workspace][0].Reservations[0]
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Equal(t, lateVerdaInstance, reservation.InstanceID)
	verda.mu.Lock()
	defer verda.mu.Unlock()
	require.Nil(t, verda.instance, "reconciliation removes the real delayed instance")
	require.Equal(t, []string{lateVerdaInstance}, verda.deletes)
	require.Equal(t, 1, verda.creates, "cleanup must never retry the purchase")
	require.Equal(t, 1, verda.scripts)
}
