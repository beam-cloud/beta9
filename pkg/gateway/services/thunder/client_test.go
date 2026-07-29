package thunder

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientCreateZone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != thunderZonesPath {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer central-token" {
			t.Fatalf("authorization = %q", got)
		}
		var payload createZoneRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload.DisplayName != "workspace-pool" {
			t.Fatalf("displayName = %q", payload.DisplayName)
		}
		_ = json.NewEncoder(w).Encode(Zone{ZoneID: "zone-1", OrgID: "org-1", DisplayName: payload.DisplayName})
	}))
	defer server.Close()

	client := NewClient(server.URL, "central-token", server.Client())
	zone, err := client.CreateZone(context.Background(), " workspace-pool ")
	if err != nil {
		t.Fatalf("CreateZone() error = %v", err)
	}
	if zone.ZoneID != "zone-1" || zone.DisplayName != "workspace-pool" {
		t.Fatalf("zone = %+v", zone)
	}
}

func TestClientPreservesAPIURLPathPrefix(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/central/api/v1/zones" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(Zone{ZoneID: "zone-1"})
	}))
	defer server.Close()

	client := NewClient(server.URL+"/central", "central-token", server.Client())
	if _, err := client.CreateZone(context.Background(), ""); err != nil {
		t.Fatalf("CreateZone() error = %v", err)
	}
}

func TestClientCreateClientEnrollmentToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != thunderEnrollmentTokenPath {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer central-token" {
			t.Fatalf("authorization = %q", got)
		}
		payload := map[string]any{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["zoneId"] != "zone-1" || payload["role"] != thunderEnrollmentRoleClient || payload["gpuType"] != "h100" || payload["gpuCount"] != float64(2) {
			t.Fatalf("payload = %+v", payload)
		}
		_ = json.NewEncoder(w).Encode(EnrollmentToken{EnrollmentTokenID: "token-id", EnrollmentToken: "tr_secret", ZoneID: "zone-1", Role: thunderEnrollmentRoleClient, GPUType: "h100", GPUCount: 2})
	}))
	defer server.Close()

	client := NewClient(server.URL, "central-token", server.Client())
	token, err := client.CreateClientEnrollmentToken(context.Background(), "zone-1", " h100 ", 2)
	if err != nil {
		t.Fatalf("CreateClientEnrollmentToken() error = %v", err)
	}
	if token.EnrollmentTokenID != "token-id" || token.EnrollmentToken != "tr_secret" {
		t.Fatalf("token = %+v", token)
	}
}

func TestClientCreateServerEnrollmentTokenOmitsGPUFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]any{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["zoneId"] != "zone-1" || payload["role"] != thunderEnrollmentRoleServer {
			t.Fatalf("payload = %+v", payload)
		}
		if _, ok := payload["gpuType"]; ok {
			t.Fatalf("server enrollment payload included gpuType: %+v", payload)
		}
		if _, ok := payload["gpuCount"]; ok {
			t.Fatalf("server enrollment payload included gpuCount: %+v", payload)
		}
		_ = json.NewEncoder(w).Encode(EnrollmentToken{EnrollmentTokenID: "server-token", EnrollmentToken: "tr_server", ZoneID: "zone-1", Role: thunderEnrollmentRoleServer})
	}))
	defer server.Close()

	client := NewClient(server.URL, "central-token", server.Client())
	token, err := client.CreateServerEnrollmentToken(context.Background(), "zone-1")
	if err != nil {
		t.Fatalf("CreateServerEnrollmentToken() error = %v", err)
	}
	if token.Role != thunderEnrollmentRoleServer || token.EnrollmentTokenID != "server-token" {
		t.Fatalf("token = %+v", token)
	}
}

func TestClientDeleteEnrollmentTokenNode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/enrollment-tokens/token-1/node" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer central-token" {
			t.Fatalf("authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(DeleteEnrollmentTokenNodeResponse{EnrollmentTokenID: "token-1", Role: thunderEnrollmentRoleClient, NodeDeleted: true})
	}))
	defer server.Close()

	client := NewClient(server.URL, "central-token", server.Client())
	resp, err := client.DeleteEnrollmentTokenNode(context.Background(), "token-1")
	if err != nil {
		t.Fatalf("DeleteEnrollmentTokenNode() error = %v", err)
	}
	if !resp.NodeDeleted || resp.EnrollmentTokenID != "token-1" {
		t.Fatalf("response = %+v", resp)
	}
}

func TestClientDecodesThunderError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(thunderErrorPayload{Error: "forbidden", Message: "API token is not allowed to create zones", Code: 403})
	}))
	defer server.Close()

	client := NewClient(server.URL, "central-token", server.Client())
	_, err := client.CreateZone(context.Background(), "pool")
	if err == nil {
		t.Fatal("CreateZone() unexpectedly succeeded")
	}
	var thunderErr *ThunderError
	if !errors.As(err, &thunderErr) {
		t.Fatalf("error type = %T, want *ThunderError", err)
	}
	if thunderErr.StatusCode != http.StatusForbidden || thunderErr.ErrorCode != "forbidden" || !strings.Contains(thunderErr.Message, "not allowed") {
		t.Fatalf("ThunderError = %+v", thunderErr)
	}
}

func TestClientValidatesRequiredConfig(t *testing.T) {
	client := NewClient("", "", nil)
	_, err := client.CreateZone(context.Background(), "pool")
	if err == nil || !strings.Contains(err.Error(), thunderAPIURLEnv) {
		t.Fatalf("CreateZone() error = %v", err)
	}

	client = NewClient("https://central.example", "", nil)
	_, err = client.CreateZone(context.Background(), "pool")
	if err == nil || !strings.Contains(err.Error(), thunderAPITokenEnv) {
		t.Fatalf("CreateZone() error = %v", err)
	}
}
