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
			catalogue, err := json.Marshal([]map[string]any{{"instance_type": tc.instanceType, "gpu": map[string]int{"number_of_gpus": 1}, "supported_os": []string{tc.retired, tc.current}}})
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
	api.catalogue = `[{"instance_type":"1L40S.20V","gpu":{"number_of_gpus":1},"supported_os":["24.04.cuda13.0.docker","custom-ubuntu-docker"]}]`
	client := api.client(testOwner, func(config *types.VerdaProviderConfig) { config.Images = []string{"custom-ubuntu-docker"} })
	_, err := client.Create(context.Background(), CreateRequest{
		Offer: Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true}, Name: "beam-custom-image-test",
	})
	require.NoError(t, err)
	require.Equal(t, "custom-ubuntu-docker", api.lastCreate()["image"])
}

func TestVerdaCPUBootsPlainUbuntuByDefault(t *testing.T) {
	api := newVerdaAPI(t)
	// The live CPU catalogue also advertises CUDA images. Supporting one must
	// not make it the default for a machine with no GPU.
	api.catalogue = `[{"instance_type":"CPU.4V.16G","gpu":{"number_of_gpus":0},"supported_os":["24.04.cuda13.0.docker","24.04.base"]}]`
	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{
		Offer: Offer{ID: "CPU.4V.16G", Region: "FIN-01"}, Name: "beam-cpu-image-test",
	})
	require.NoError(t, err)
	require.Equal(t, "24.04.base", api.lastCreate()["image"])
}

func TestVerdaCPUHonorsAnExplicitImagePreference(t *testing.T) {
	api := newVerdaAPI(t)
	api.catalogue = `[{"instance_type":"CPU.4V.16G","gpu":{"number_of_gpus":0},"supported_os":["24.04.base","custom-cpu-image","24.04.cuda13.0.docker"]}]`
	for name, images := range map[string][]string{
		"custom":        {"custom-cpu-image", "24.04.base"},
		"CUDA override": {"24.04.cuda13.0.docker", "24.04.base"},
	} {
		t.Run(name, func(t *testing.T) {
			client := api.client(testOwner, func(config *types.VerdaProviderConfig) { config.Images = images })
			_, err := client.Create(context.Background(), CreateRequest{
				Offer: Offer{ID: "CPU.4V.16G", Region: "FIN-01"}, Name: "beam-cpu-override-test",
			})
			require.NoError(t, err)
			require.Equal(t, images[0], api.lastCreate()["image"])
		})
	}
}

func TestVerdaCPUWithoutPlainImageIsNeitherOfferedNorBoughtByDefault(t *testing.T) {
	api := newVerdaAPI(t)
	api.catalogue = `[{"instance_type":"CPU.4V.16G","cpu":{"number_of_cores":4},"gpu":{"number_of_gpus":0},"memory":{"size_in_gigabytes":16},"price_per_hour":"0.048","supported_os":["24.04.cuda13.0.docker"]}]`
	client := api.client(testOwner)
	offers, err := client.Offers(context.Background(), OfferFilter{})
	require.NoError(t, err)
	require.Empty(t, offers)
	_, err = client.Create(context.Background(), CreateRequest{
		Offer: Offer{ID: "CPU.4V.16G", Region: "FIN-01"}, Name: "beam-unsupported-cpu-test", StartupScript: "echo agent",
	})
	require.ErrorContains(t, err, "boots none of the images [24.04.base]")
	require.Zero(t, api.sent("POST", "/scripts"))
	require.Zero(t, api.sent("POST", "/instances"))
}
