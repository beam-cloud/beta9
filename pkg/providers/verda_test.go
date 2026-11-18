package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// What these tests buy, where the stand-in has it in stock.
var (
	verdaSpotL40S = Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true}
	verdaSpotV100 = Offer{ID: "1V100.4V", Region: "FIN-01", Spot: true}
	verdaCPU      = Offer{ID: "CPU.4V.16G", Region: "FIN-01"}
)

// Images, as Verda's catalogue names them: the one a GPU machine is to boot,
// and plain Ubuntu.
const (
	verdaTestCUDAImage  = "24.04.cuda13.0.docker"
	verdaTestPlainImage = "24.04.base"
)

// sells has the stand-in list one instance type in place of the recorded
// catalogue: what the offer is of, with so many GPUs, booting the images.
func (a *verdaAPI) sells(offer Offer, gpus int, images ...string) {
	catalogue, err := json.Marshal([]map[string]any{{
		"instance_type": offer.ID, "price_per_hour": "0.048", "supported_os": images,
		"cpu": map[string]int{"number_of_cores": 4}, "gpu": map[string]int{"number_of_gpus": gpus},
		"memory": map[string]int{"size_in_gigabytes": 16},
	}})
	require.NoError(a.t, err)
	a.catalogue = string(catalogue)
}

// What Verda answers a create with.

// verdaCreateResponse changes the bodies of Verda's answers to the creates of
// scripts and instances that went through. The stand-in's scripts, instances
// and sign-in stay as they are.
func verdaCreateResponse(client *Verda, change func(path string, body []byte) []byte) {
	client.api.client.Transport = roundTrip(func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(req)
		created := req.Method == http.MethodPost && slices.Contains([]string{verdaScriptsPath, verdaInstancesPath}, req.URL.Path)
		if err != nil || !created || resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			return resp, err
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		body = change(req.URL.Path, body)
		resp.Body = io.NopCloser(strings.NewReader(string(body)))
		resp.ContentLength = int64(len(body))
		resp.Header.Set("Content-Type", "text/plain")
		return resp, nil
	})
}

func TestVerdaCreateAcceptsJSONAndPlainUUIDResponses(t *testing.T) {
	for name, plain := range map[string][]string{
		"JSON":           nil,
		"plain script":   {verdaScriptsPath},
		"plain instance": {verdaInstancesPath},
		"both plain":     {verdaScriptsPath, verdaInstancesPath},
	} {
		t.Run(name, func(t *testing.T) {
			api := newVerdaAPI(t)
			client := api.client(testOwner)
			verdaCreateResponse(client, func(path string, body []byte) []byte {
				if !slices.Contains(plain, path) {
					return body
				}
				var id string
				require.NoError(t, json.Unmarshal(body, &id))
				return []byte(" \n" + id + "\n")
			})

			req := CreateRequest{
				Offer:         verdaSpotL40S,
				Name:          "beam-verda-response-test",
				StartupScript: "echo starting-agent",
				Tags:          map[string]string{"beam-machine": "response-test"},
			}
			created, err := client.Create(context.Background(), req)
			require.NoError(t, err)
			// The script is the first thing the stand-in makes, and the
			// instance the second.
			require.Equal(t, fmt.Sprintf(verdaInstanceIDFormat, 2), created.ID)
			require.Equal(t, StatusPending, created.Status)
			require.Equal(t, req.Tags, created.Tags)
			create := api.lastCreate()
			require.Equal(t, fmt.Sprintf(verdaScriptIDFormat, 1), create["startup_script_id"])
			require.Equal(t, map[string]string{client.scriptPrefix() + req.Name: bootScript(req.StartupScript)}, api.storedScripts())
			require.Equal(t, bootScript(req.StartupScript), api.scriptOf(create))
			require.Equal(t, 1, api.sent(http.MethodPost, verdaScriptsPath))
			require.Equal(t, 1, api.sent(http.MethodPost, verdaInstancesPath))
		})
	}
}

func TestVerdaCreateRejectsMissingOrInvalidResourceIDsWithoutRetry(t *testing.T) {
	const private = "private-response-content"
	id := fmt.Sprintf(verdaScriptIDFormat, 1)
	for _, path := range []string{verdaScriptsPath, verdaInstancesPath} {
		for name, body := range map[string]string{
			"empty":           "",
			"empty JSON":      `""`,
			"null":            "null",
			"object":          `{"id":"` + id + `"}`,
			"malformed UUID":  id[:len(id)-1] + "z",
			"unexpected text": private,
			// Other ways to write a UUID, none of which Verda uses.
			"UUID in braces":       "{" + id + "}",
			"UUID as a URN":        "urn:uuid:" + id,
			"UUID without dashes":  strings.ReplaceAll(id, "-", ""),
			"UUID and more beside": id + " " + id,
		} {
			t.Run(path+"/"+name, func(t *testing.T) {
				api := newVerdaAPI(t)
				client := api.client(testOwner)
				verdaCreateResponse(client, func(got string, original []byte) []byte {
					if got == path {
						return []byte(body)
					}
					return original
				})

				_, err := client.Create(context.Background(), CreateRequest{
					Offer: verdaSpotL40S, Name: "beam-verda-response-test", StartupScript: "echo starting-agent",
				})
				require.ErrorContains(t, err, "Verda accepted POST "+path+" without returning a resource ID")
				require.True(t, Unanswered(err), "an accepted mutation without its resource ID remains ambiguous")
				require.NotContains(t, err.Error(), private)
				require.Equal(t, 1, api.sent(http.MethodPost, verdaScriptsPath))
				wantCreates := 0
				if path == verdaInstancesPath {
					wantCreates = 1
				}
				require.Equal(t, wantCreates, api.sent(http.MethodPost, verdaInstancesPath), "an uncertain purchase must never be repeated")
				require.Len(t, api.storedScripts(), 1, "the script must remain available if Verda accepted the order")

				// Existing cleanup still removes an unused script after its grace
				// period, while retaining one used by an uncertain instance order.
				api.after(2 * verdaScriptGrace)
				_, err = client.List(context.Background())
				require.NoError(t, err)
				require.Len(t, api.storedScripts(), wantCreates)
			})
		}
	}
}

// The image a machine boots.

// The live instance-type catalogue still includes retired image names that
// /images no longer publishes. Prefer the current compatible Docker image,
// including the proprietary-driver image required by V100 hardware.
func TestVerdaPrefersCurrentDriverImagesOverRetiredAliases(t *testing.T) {
	for name, tc := range map[string]struct {
		offer   Offer
		retired string
		current string
	}{
		"current GPUs": {verdaSpotL40S, "ubuntu-24.04-cuda-12.8-open-docker", verdaTestCUDAImage},
		"V100":         {verdaSpotV100, "ubuntu-24.04-cuda-12.6-docker", "24.04.cuda12.9-v100.docker"},
	} {
		t.Run(name, func(t *testing.T) {
			api := newVerdaAPI(t)
			api.sells(tc.offer, 1, tc.retired, tc.current)
			_, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: tc.offer, Name: "beam-current-image-test"})
			require.NoError(t, err)
			require.Equal(t, tc.current, api.lastCreate()["image"])
		})
	}
}

func TestVerdaKeepsAnExplicitImagePreference(t *testing.T) {
	const custom = "custom-ubuntu-docker"
	api := newVerdaAPI(t)
	api.sells(verdaSpotL40S, 1, verdaTestCUDAImage, custom)
	client := api.client(testOwner, func(config *types.VerdaProviderConfig) { config.Images = []string{custom} })
	_, err := client.Create(context.Background(), CreateRequest{Offer: verdaSpotL40S, Name: "beam-custom-image-test"})
	require.NoError(t, err)
	require.Equal(t, custom, api.lastCreate()["image"])
}

func TestVerdaCPUBootsPlainUbuntuByDefault(t *testing.T) {
	api := newVerdaAPI(t)
	// The live CPU catalogue also advertises CUDA images. Supporting one must
	// not make it the default for a machine with no GPU.
	api.sells(verdaCPU, 0, verdaTestCUDAImage, verdaTestPlainImage)
	_, err := api.client(testOwner).Create(context.Background(), CreateRequest{Offer: verdaCPU, Name: "beam-cpu-image-test"})
	require.NoError(t, err)
	require.Equal(t, verdaTestPlainImage, api.lastCreate()["image"])
}

func TestVerdaCPUHonorsAnExplicitImagePreference(t *testing.T) {
	const custom = "custom-cpu-image"
	api := newVerdaAPI(t)
	api.sells(verdaCPU, 0, verdaTestPlainImage, custom, verdaTestCUDAImage)
	for name, images := range map[string][]string{
		"custom":        {custom, verdaTestPlainImage},
		"CUDA override": {verdaTestCUDAImage, verdaTestPlainImage},
	} {
		t.Run(name, func(t *testing.T) {
			client := api.client(testOwner, func(config *types.VerdaProviderConfig) { config.Images = images })
			_, err := client.Create(context.Background(), CreateRequest{Offer: verdaCPU, Name: "beam-cpu-override-test"})
			require.NoError(t, err)
			require.Equal(t, images[0], api.lastCreate()["image"])
		})
	}
}

func TestVerdaCPUWithoutPlainImageIsNeitherOfferedNorBoughtByDefault(t *testing.T) {
	ctx := context.Background()
	// It is the image that keeps the machine from being offered: with plain
	// Ubuntu to boot, the same machine is.
	api := newVerdaAPI(t)
	api.sells(verdaCPU, 0, verdaTestCUDAImage, verdaTestPlainImage)
	offers, err := api.client(testOwner).Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.NotEmpty(t, offers)

	api = newVerdaAPI(t)
	api.sells(verdaCPU, 0, verdaTestCUDAImage)
	client := api.client(testOwner)
	offers, err = client.Offers(ctx, OfferFilter{})
	require.NoError(t, err)
	require.Empty(t, offers)
	_, err = client.Create(ctx, CreateRequest{Offer: verdaCPU, Name: "beam-unsupported-cpu-test", StartupScript: "echo agent"})
	require.ErrorContains(t, err, fmt.Sprintf("boots none of the images %v", verdaCPUImages))
	require.Zero(t, api.sent(http.MethodPost, verdaScriptsPath))
	require.Zero(t, api.sent(http.MethodPost, verdaInstancesPath))
}

// The SSH keys a machine is given.

// The real create endpoint rejects an omitted ssh_key_ids with HTTP 400.
// Capture the HTTP-decoded body, including key presence, rather than just
// checking a Go field whose nil value can disappear during marshaling.
func TestVerdaSendsExplicitNullWithoutSSHKeys(t *testing.T) {
	for name, keys := range map[string][]string{"unset": nil, "empty list": {}} {
		t.Run(name, func(t *testing.T) {
			api := newVerdaAPI(t)
			client := api.client(testOwner, func(config *types.VerdaProviderConfig) { config.SSHKeyIDs = keys })
			_, err := client.Create(context.Background(), CreateRequest{Offer: verdaSpotV100, Name: "beam-verda-no-ssh-test"})
			require.NoError(t, err)
			body := api.lastCreate()
			require.Contains(t, body, "ssh_key_ids", "omitting the key reproduces the production validation failure")
			require.Nil(t, body["ssh_key_ids"], "no configured keys must encode JSON null, rather than an empty UUID array")
		})
	}
}

func TestVerdaPreservesConfiguredSSHKeyIDs(t *testing.T) {
	api := newVerdaAPI(t)
	keys := []string{"1d57010e-7d75-488e-9e91-711565030d58", "dd50b622-1c36-48c2-a485-f4d4937a3ea1"}
	client := api.client(testOwner, func(config *types.VerdaProviderConfig) { config.SSHKeyIDs = keys })
	_, err := client.Create(context.Background(), CreateRequest{Offer: verdaSpotV100, Name: "beam-verda-ssh-test"})
	require.NoError(t, err)
	require.Equal(t, []any{keys[0], keys[1]}, api.lastCreate()["ssh_key_ids"])
}
