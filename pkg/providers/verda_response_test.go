package providers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVerdaCreateAcceptsJSONAndPlainUUIDResponses(t *testing.T) {
	for name, plain := range map[string]map[string]bool{
		"JSON":           {},
		"plain script":   {"/scripts": true},
		"plain instance": {"/instances": true},
		"both plain":     {"/scripts": true, "/instances": true},
	} {
		t.Run(name, func(t *testing.T) {
			api := newVerdaAPI(t)
			client := api.client(testOwner)
			verdaCreateResponse(client, func(path string, body []byte) []byte {
				if !plain[path] {
					return body
				}
				var id string
				require.NoError(t, json.Unmarshal(body, &id))
				return []byte(" \n" + id + "\n")
			})

			req := CreateRequest{
				Offer:         Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true},
				Name:          "beam-verda-response-test",
				StartupScript: "echo starting-agent",
				Tags:          map[string]string{"beam-machine": "response-test"},
			}
			created, err := client.Create(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, "0a6842d1-bc59-4e8d-abd7-000000000002", created.ID)
			require.Equal(t, StatusPending, created.Status)
			require.Equal(t, req.Tags, created.Tags)
			create := api.lastCreate()
			require.Equal(t, "4f74ddc3-0293-4f7e-80e6-000000000001", create["startup_script_id"])
			require.Equal(t, map[string]string{client.scriptPrefix() + req.Name: bootScript(req.StartupScript)}, api.storedScripts())
			require.Equal(t, bootScript(req.StartupScript), api.scriptOf(create))
			require.Equal(t, 1, api.sent(http.MethodPost, "/scripts"))
			require.Equal(t, 1, api.sent(http.MethodPost, "/instances"))
		})
	}
}

func TestVerdaCreateRejectsMissingOrInvalidResourceIDsWithoutRetry(t *testing.T) {
	for _, path := range []string{"/scripts", "/instances"} {
		for name, body := range map[string]string{
			"empty":           "",
			"empty JSON":      `""`,
			"null":            "null",
			"object":          `{"id":"4f74ddc3-0293-4f7e-80e6-000000000001"}`,
			"malformed UUID":  "4f74ddc3-0293-4f7e-80e6-00000000000z",
			"unexpected text": "private-response-content",
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
					Offer:         Offer{ID: "1L40S.20V", Region: "FIN-01", Spot: true},
					Name:          "beam-verda-response-test",
					StartupScript: "echo starting-agent",
				})
				require.ErrorContains(t, err, "Verda accepted POST "+path+" without returning a resource ID")
				require.True(t, Unanswered(err), "an accepted mutation without its resource ID remains ambiguous")
				require.NotContains(t, err.Error(), "private-response-content")
				require.Equal(t, 1, api.sent(http.MethodPost, "/scripts"))
				wantCreates := 0
				if path == "/instances" {
					wantCreates = 1
				}
				require.Equal(t, wantCreates, api.sent(http.MethodPost, "/instances"), "an uncertain purchase must never be repeated")
				require.Len(t, api.storedScripts(), 1, "the script must remain available if Verda accepted the order")

				// Existing cleanup still removes an unused script after its grace
				// period, while retaining one used by an uncertain instance order.
				api.after(2 * time.Hour)
				_, err = client.List(context.Background())
				require.NoError(t, err)
				require.Len(t, api.storedScripts(), wantCreates)
			})
		}
	}
}

// Change only successful create response bodies, leaving the fixture's script,
// instance and authentication behavior intact.
func verdaCreateResponse(client *Verda, change func(string, []byte) []byte) {
	client.api.client.Transport = roundTrip(func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil || req.Method != http.MethodPost || (req.URL.Path != "/scripts" && req.URL.Path != "/instances") || resp.StatusCode < 200 || resp.StatusCode >= 300 {
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
