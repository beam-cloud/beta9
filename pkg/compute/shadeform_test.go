package compute

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListOffersNormalizesShadeformInstanceTypes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/instances/types", r.URL.Path)
		require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"data":[{"id":"sf-h100-8","cloud":"lambda","gpu_type":"H100","gpu_count":8,"hourly_price":12.25,"vcpus":96,"memory_mb":1048576,"region":"us-east","capacity":3}]}`))
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
	require.Equal(t, uint32(3), offers[0].Available)
}
