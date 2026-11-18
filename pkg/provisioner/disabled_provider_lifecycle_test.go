package provisioner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type disabledLifecycleAPI struct {
	mu                           sync.Mutex
	lists, gets, offers, creates int
	deleted                      []string
}

func realDisabledLifecycleRegistry(t *testing.T) (*providers.Registry, *disabledLifecycleAPI) {
	t.Helper()
	api := &disabledLifecycleAPI{}
	instance := func(id int, machine, owner string) map[string]any {
		return map[string]any{"id": id, "actual_status": "running", "is_bid": true,
			"label": fmt.Sprintf("owned-vm %s=%s %s=%s", providers.OwnerTag, owner, MachineTag, machine)}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.mu.Lock()
		defer api.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer synthetic-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/instances/":
			api.lists++
			// The existing ID is not in the list yet, exercising the Get
			// fallback. An unanswered purchase is found only by its tag.
			_ = json.NewEncoder(w).Encode(map[string]any{"instances": []any{
				instance(102, "unanswered", testOwner), instance(103, "orphan", testOwner), instance(104, "foreign", "another-owner"),
			}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v0/instances/101/":
			api.gets++
			_ = json.NewEncoder(w).Encode(map[string]any{"instances": instance(101, "existing", testOwner)})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v0/instances/"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v0/instances/"), "/")
			api.deleted = append(api.deleted, id)
			_ = json.NewEncoder(w).Encode(map[string]bool{"success": true})
		case r.Method == http.MethodPost:
			api.offers++
			w.WriteHeader(http.StatusBadRequest)
		case r.Method == http.MethodPut:
			api.creates++
			w.WriteHeader(http.StatusBadRequest)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	registry := providers.NewRegistry(types.ProviderConfig{
		Disabled: []string{"vast"}, Vast: types.VastProviderConfig{ApiKey: "synthetic-key", BaseURL: server.URL},
	}, testOwner)
	require.Empty(t, registry.Names(), "this is a genuinely disabled registry client")
	require.Equal(t, []string{"vast"}, registry.ConfiguredNames())
	return registry, api
}

func TestDisabledProviderLifecycleStillObservesExistingAndUnansweredPurchases(t *testing.T) {
	registry, api := realDisabledLifecycleRegistry(t)
	ctx, now := context.Background(), time.Now()
	p := &Provisioner{providers: registry, sweptAll: now}
	existing := Machine{ID: "existing", InstanceID: "101"}
	existing.Provider = "vast"
	unanswered := Machine{ID: "unanswered"}
	unanswered.Provider = "vast"
	listed := p.listInstances(ctx, snapshot{}, []Machine{existing, unanswered}, now)
	require.Contains(t, listed, "vast", "disabled providers with existing records still need lifecycle inspection")
	state, known := p.instanceOf(ctx, existing, listed)
	require.Equal(t, present, state)
	require.Equal(t, "101", known.ID, "a stale list must still reach the real disabled provider's Get")
	state, found := p.instanceOf(ctx, unanswered, listed)
	require.Equal(t, present, state)
	require.Equal(t, "102", found.ID, "an unanswered purchase is rediscovered by its machine tag")
	p.deleteUnrecorded(ctx, listed, map[string]string{existing.ID: existing.InstanceID, unanswered.ID: ""})
	client, ok := registry.Get("vast")
	require.True(t, ok)
	// Once the rediscovered purchase is released, its disabled client can
	// still delete it; the orphan sweep already deleted the unrecorded VM.
	require.NoError(t, client.Delete(ctx, found.ID))
	offers, err := client.Offers(ctx, providers.OfferFilter{})
	require.NoError(t, err)
	require.Empty(t, offers)
	_, err = client.Create(ctx, providers.CreateRequest{})
	require.ErrorIs(t, err, providers.ErrDisabled)
	api.mu.Lock()
	defer api.mu.Unlock()
	require.Equal(t, 1, api.lists)
	require.Equal(t, 1, api.gets)
	require.ElementsMatch(t, []string{"103", "102"}, api.deleted)
	require.Zero(t, api.offers)
	require.Zero(t, api.creates)
}

func TestDisabledProviderWithoutPoolsIsStillSweptForOrphans(t *testing.T) {
	registry, api := realDisabledLifecycleRegistry(t)
	p := &Provisioner{providers: registry}
	ctx := context.Background()
	listed := p.listInstances(ctx, snapshot{}, nil, time.Now())
	p.deleteUnrecorded(ctx, listed, nil)
	api.mu.Lock()
	defer api.mu.Unlock()
	require.Equal(t, 1, api.lists, "the periodic full sweep includes disabled providers even without records or pools")
	require.ElementsMatch(t, []string{"102", "103"}, api.deleted, "foreign-owner instances never reach cleanup")
	require.Zero(t, api.offers)
	require.Zero(t, api.creates)
}
