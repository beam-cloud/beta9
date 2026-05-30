package vast

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/stretchr/testify/require"
)

func TestListOffersNormalizesVastBundle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/bundles/", r.URL.Path)
		require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"offers":[{"id":123,"gpu_name":"H100","num_gpus":8,"dph_total":10.5,"cpu_cores":64,"cpu_ram":512,"geolocation":"US","rentable_count":2}]}`))
	}))
	defer server.Close()

	client := New(Config{APIKey: "test-key", BaseURL: server.URL})
	offers, err := client.ListOffers(context.Background(), compute.OfferRequest{GPUs: []string{"H100"}})

	require.NoError(t, err)
	require.Len(t, offers, 1)
	require.Equal(t, "123", offers[0].ID)
	require.Equal(t, "vast", offers[0].Provider)
	require.Equal(t, "H100", offers[0].GPU)
	require.Equal(t, uint32(8), offers[0].GPUCount)
	require.Equal(t, compute.DollarsToMicros(10.5), offers[0].HourlyCostMicros)
	require.Equal(t, uint32(2), offers[0].Available)
}
