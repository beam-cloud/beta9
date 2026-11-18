package providers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// The live instance-type catalogue still includes retired image names that
// /images no longer publishes. Prefer the current compatible Docker image,
// including the proprietary-driver image required by V100 hardware.
func TestVerdaPrefersCurrentDriverImagesOverRetiredAliases(t *testing.T) {
	for name, tc := range map[string]struct {
		instanceType string
		retired      string
		current      string
	}{
		"current GPUs": {"1L40S.20V", "ubuntu-24.04-cuda-12.8-open-docker", "24.04.cuda13.0.docker"},
		"V100":         {"1V100.4V", "ubuntu-24.04-cuda-12.6-docker", "24.04.cuda12.9-v100.docker"},
	} {
		t.Run(name, func(t *testing.T) {
			api := newVerdaAPI(t)
			catalogue, err := json.Marshal([]map[string]any{{"instance_type": tc.instanceType, "supported_os": []string{tc.retired, tc.current}}})
			require.NoError(t, err)
			api.catalogue = string(catalogue)
			_, err = api.client(testOwner).Create(context.Background(), CreateRequest{
				Offer: Offer{ID: tc.instanceType, Region: "FIN-01", Spot: true}, Name: "beam-current-image-test",
			})
			require.NoError(t, err)
			require.Equal(t, tc.current, api.lastCreate()["image"])
		})
	}
}

func TestVerdaKeepsAnExplicitImagePreference(t *testing.T) {
	api := newVerdaAPI(t)
	api.catalogue = `[{"instance_type":"1L40S.20V","supported_os":["24.04.cuda13.0.docker","custom-ubuntu-docker"]}]`
	client := api.client(testOwner, func(config *types.VerdaProviderConfig) { config.Images = []string{"custom-ubuntu-docker"} })
	_, err := client.Create(context.Background(), CreateRequest{
		Offer: Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true}, Name: "beam-custom-image-test",
	})
	require.NoError(t, err)
	require.Equal(t, "custom-ubuntu-docker", api.lastCreate()["image"])
}
