package compute

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestListOffersNormalizesVastBundle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/bundles/", r.URL.Path)
		require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"offers":[{"id":123,"gpu_name":"H100","num_gpus":8,"dph_total":10.5,"cpu_cores":64,"cpu_ram":512,"geolocation":"US","rentable_count":2}]}`))
	}))
	defer server.Close()

	client := NewVast(VastConfig{APIKey: "test-key", BaseURL: server.URL})
	offers, err := client.ListOffers(context.Background(), OfferRequest{GPUs: []string{"H100"}})

	require.NoError(t, err)
	require.Len(t, offers, 1)
	require.Equal(t, "123", offers[0].ID)
	require.Equal(t, "vast", offers[0].Provider)
	require.Equal(t, "H100", offers[0].GPU)
	require.Equal(t, uint32(8), offers[0].GPUCount)
	require.Equal(t, DollarsToMicros(10.5), offers[0].HourlyCostMicros)
	require.Equal(t, uint32(2), offers[0].Available)
}

func TestCreateReservationConfiguresVastOnstart(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/asks/123/", r.URL.Path)
		require.Equal(t, http.MethodPut, r.Method)
		require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		_, _ = w.Write([]byte(`{"new_contract":"instance-123"}`))
	}))
	defer server.Close()

	client := NewVast(VastConfig{APIKey: "test-key", BaseURL: server.URL})
	reservation, err := client.CreateReservation(context.Background(), ReservationRequest{
		PoolName:         "training",
		MachineID:        "machine-123",
		Name:             "beam-workspace-training-machine-123",
		Selector:         "training",
		Offer:            Offer{ID: "123", Provider: "vast", InstanceType: "machine-1", GPU: "H100", GPUCount: 8, HourlyCostMicros: DollarsToMicros(10.5)},
		TTL:              6 * time.Hour,
		Source:           SourceCLIReservation,
		BootstrapCommand: "curl -fsSL https://app.beam.cloud/install/agent | sudo bash -s -- --gateway https://gateway.beam.cloud --join-token token",
	})

	require.NoError(t, err)
	require.Equal(t, "instance-123", reservation.ID)
	require.Equal(t, "beam-workspace-training-machine-123", body["label"])
	require.Equal(t, "machine-123", body["client_id"])
	require.Equal(t, "machine-123", reservation.MachineID)
	require.Equal(t, "beam-workspace-training-machine-123", reservation.Name)
	require.Contains(t, body["onstart"], "--join-token token")
}
