package compute

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestListOffersNormalizesShadeformInstanceTypes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/instances/types", r.URL.Path)
		require.Equal(t, "test-key", r.Header.Get("X-API-KEY"))
		_, _ = w.Write([]byte(`{"instance_types":[{"id":"sf-h100-8","cloud":"lambda","shade_instance_type":"H100x8","hourly_price":1225,"deployment_type":"vm","configuration":{"gpu_type":"H100","num_gpus":8,"vcpus":96,"memory_in_gb":1024,"storage_in_gb":2048},"availability":[{"region":"us-east","available":true},{"region":"us-west","available":false}]}]}`))
	}))
	defer server.Close()

	client := NewShadeform(ShadeformConfig{APIKey: "test-key", BaseURL: server.URL})
	offers, err := client.ListOffers(context.Background(), OfferRequest{GPUs: []string{"H100"}})

	require.NoError(t, err)
	require.Len(t, offers, 1)
	require.Equal(t, "sf-h100-8", offers[0].ID)
	require.Equal(t, "shadeform", offers[0].Provider)
	require.Equal(t, "lambda", offers[0].Labels["cloud"])
	require.Equal(t, "H100", offers[0].GPU)
	require.Equal(t, uint32(8), offers[0].GPUCount)
	require.Equal(t, DollarsToMicros(12.25), offers[0].HourlyCostMicros)
	require.Equal(t, "us-east", offers[0].Region)
	require.Equal(t, uint32(1), offers[0].Available)
}

func TestCreateReservationConfiguresShadeformStartupScript(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/instances/create", r.URL.Path)
		require.Equal(t, "test-key", r.Header.Get("X-API-KEY"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		_, _ = w.Write([]byte(`{"id":"reservation-123"}`))
	}))
	defer server.Close()

	client := NewShadeform(ShadeformConfig{APIKey: "test-key", BaseURL: server.URL})
	reservation, err := client.CreateReservation(context.Background(), ReservationRequest{
		PoolName:         "training",
		MachineID:        "machine-123",
		Name:             "beam-workspace-training-machine-123",
		Selector:         "training",
		Offer:            Offer{ID: "sf-h100-8", Provider: "shadeform", Cloud: "lambda", InstanceType: "H100x8", Region: "us-east", GPU: "H100", GPUCount: 8, HourlyCostMicros: DollarsToMicros(12.25), Labels: map[string]string{"cloud": "lambda"}},
		TTL:              6 * time.Hour,
		MaxSpendMicros:   DollarsToMicros(80),
		Source:           SourceCLIReservation,
		BootstrapCommand: "curl -fsSL https://app.beam.cloud/install/agent | sudo bash -s -- --gateway https://gateway.beam.cloud --join-token token",
	})

	require.NoError(t, err)
	require.Equal(t, "reservation-123", reservation.ID)
	require.Equal(t, "lambda", body["cloud"])
	require.Equal(t, "H100x8", body["shade_instance_type"])
	require.Equal(t, true, body["shade_cloud"])
	require.Equal(t, "beam-workspace-training-machine-123", body["name"])
	require.Equal(t, "machine-123", reservation.MachineID)
	require.Equal(t, "beam-workspace-training-machine-123", reservation.Name)

	launchConfig, ok := body["launch_configuration"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "script", launchConfig["type"])
	scriptConfig, ok := launchConfig["script_configuration"].(map[string]any)
	require.True(t, ok)
	encodedScript, ok := scriptConfig["base64_script"].(string)
	require.True(t, ok)
	script, err := base64.StdEncoding.DecodeString(encodedScript)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(script), "--join-token token"))

	autoDelete, ok := body["auto_delete"].(map[string]any)
	require.True(t, ok)
	require.NotEmpty(t, autoDelete["date_threshold"])
	require.Equal(t, "80.00", autoDelete["spend_threshold"])
}
