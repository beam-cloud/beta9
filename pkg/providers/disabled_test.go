package providers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDisabledProvidersKeepCredentialsWithoutSellingMachines(t *testing.T) {
	var requests atomic.Int32
	vastAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer vastAPI.Close()
	registry := NewRegistry(types.ProviderConfig{
		Disabled:  []string{" AWS ", "gcp", "aws", "VAST"},
		AWS:       types.AWSProviderConfig{Regions: []types.AWSProviderRegionConfig{{Name: "us-east-1"}}},
		GCP:       types.GCPProviderConfig{ServiceAccountKey: gcpTestKeyJSON(t, "https://oauth2.googleapis.com/token")},
		Shadeform: types.ShadeformProviderConfig{ApiKey: "synthetic-key"},
		Vast:      types.VastProviderConfig{ApiKey: "synthetic-key", BaseURL: vastAPI.URL},
	}, testOwner)
	require.Equal(t, []string{"shadeform"}, registry.Names())
	for _, name := range []string{"aws", "gcp", "vast"} {
		client, ok := registry.Get(name)
		require.True(t, ok, "credentials remain available for existing-machine cleanup")
		require.False(t, registry.IsEnabled(name))
		offers, err := client.Offers(context.Background(), OfferFilter{})
		require.NoError(t, err)
		require.Empty(t, offers, "disabling must not contact a provider or expose offers")
		_, err = client.Create(context.Background(), CreateRequest{})
		require.ErrorIs(t, err, ErrDisabled)
		require.False(t, Unanswered(err), "a disabled create buys nothing")
	}
	require.Zero(t, requests.Load(), "disabled Vast discovery and purchases must consume no API quota")
}

func TestDisabledProviderStillManagesAndDeletesExistingMachines(t *testing.T) {
	fake := NewFake("aws", testOwner)
	id := fake.Plant(testOwner, StatusActive, map[string]string{"beam-machine": "existing"})
	registry := RegistryOf(disabledProvider{Provider: fake})
	client, ok := registry.Get("aws")
	require.True(t, ok)
	ctx := context.Background()
	got, err := client.Get(ctx, id)
	require.NoError(t, err)
	require.Equal(t, StatusActive, got.Status)
	listed, err := client.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.NoError(t, client.Extend(ctx, id, time.Now().Add(time.Hour)))
	require.NoError(t, client.Delete(ctx, id))
	_, err = client.Get(ctx, id)
	require.ErrorIs(t, err, ErrNotFound)
}
