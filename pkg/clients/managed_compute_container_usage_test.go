package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func managedComputeUsageTestRequest(stubName string) *types.ContainerRequest {
	return &types.ContainerRequest{
		ContainerId: "sandbox-1",
		WorkspaceId: "workspace-1",
		Stub: types.StubWithRelated{Stub: types.Stub{
			Name: stubName,
			Type: types.StubType(types.StubTypeSandbox),
		}},
	}
}

func managedComputeUsageTestRecorder(endpoint string) *ManagedComputeContainerUsageRecorder {
	return NewManagedComputeContainerUsageRecorder(types.ManagedComputeConfig{
		Billing: types.ManagedComputeBillingConfig{
			PoolRoutes: []types.ManagedComputeBillingPoolRouteConfig{{PoolNamePrefix: "tama-", Endpoint: endpoint}},
		},
	}, WorkerIdentity{})
}

func TestManagedComputeContainerUsageRecorderReportsTamaCPUUsage(t *testing.T) {
	requests := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/billing/managed-compute/usage/" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer runtime-token" {
			t.Fatalf("authorization = %q", request.Header.Get("Authorization"))
		}
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		requests <- payload
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	recorder := NewManagedComputeContainerUsageRecorder(types.ManagedComputeConfig{
		Billing: types.ManagedComputeBillingConfig{
			Mode: "noop",
			PoolRoutes: []types.ManagedComputeBillingPoolRouteConfig{{
				PoolNamePrefix: "tama-",
				Endpoint:       server.URL + "/v1/billing/managed-compute/",
				AuthToken:      "runtime-token",
			}},
		},
	}, WorkerIdentity{})
	if recorder == nil {
		t.Fatal("expected recorder")
	}
	cost := 0.012345
	start := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	request := managedComputeUsageTestRequest("tama-machine-1")
	request.Cpu = 2_000
	request.Memory = 4_096
	if err := recorder.RecordContainerUsage(context.Background(), request, start, start.Add(time.Minute), &cost); err != nil {
		t.Fatal(err)
	}
	payload := <-requests
	if payload["workspace_id"] != "workspace-1" || payload["reservation_id"] != "sandbox-1" {
		t.Fatalf("attribution = %+v", payload)
	}
	if payload["cost_micros"] != float64(124) || payload["cpu_millicores"] != float64(2_000) || payload["memory_mb"] != float64(4_096) {
		t.Fatalf("usage = %+v", payload)
	}
}

func TestManagedComputeContainerUsageRecorderIgnoresOrdinarySandboxes(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	recorder := managedComputeUsageTestRecorder(server.URL)
	cost := 1.0
	request := managedComputeUsageTestRequest("regular-sandbox")
	if err := recorder.RecordContainerUsage(context.Background(), request, time.Now(), time.Now().Add(time.Second), &cost); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("ordinary sandbox produced %d billing calls", calls.Load())
	}
}

func TestManagedComputeContainerUsageRecorderRetriesTransientFailures(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	recorder := managedComputeUsageTestRecorder(server.URL)
	cost := 1.0
	request := managedComputeUsageTestRequest("tama-machine-1")
	if err := recorder.RecordContainerUsage(context.Background(), request, time.Now(), time.Now().Add(time.Second), &cost); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
}

func TestManagedComputeContainerUsageRecorderChoosesOneLedger(t *testing.T) {
	var tamaCalls atomic.Int32
	tama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		tamaCalls.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer tama.Close()

	var marketplaceCalls atomic.Int32
	marketplace := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		marketplaceCalls.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer marketplace.Close()

	recorder := NewManagedComputeContainerUsageRecorder(types.ManagedComputeConfig{
		Billing: types.ManagedComputeBillingConfig{
			Endpoint: marketplace.URL,
			PoolRoutes: []types.ManagedComputeBillingPoolRouteConfig{{
				PoolNamePrefix: "tama-",
				Endpoint:       tama.URL,
			}},
		},
		MarketplaceListingID: "listing-1",
		SellerWorkspaceID:    "seller-1",
	}, WorkerIdentity{WorkerID: "worker-1", PoolName: "shared", MachineID: "machine-1", Runtime: "runc"})
	if recorder == nil {
		t.Fatal("expected recorder")
	}

	cost := 1.0
	start := time.Now()
	request := managedComputeUsageTestRequest("tama-machine-1")
	if err := recorder.RecordContainerUsage(context.Background(), request, start, start.Add(time.Second), &cost); err != nil {
		t.Fatal(err)
	}
	if tamaCalls.Load() != 1 || marketplaceCalls.Load() != 0 {
		t.Fatalf("Tama/marketplace calls = %d/%d, want 1/0", tamaCalls.Load(), marketplaceCalls.Load())
	}

	request.Stub.Name = "regular-sandbox"
	request.ContainerId = "sandbox-2"
	if err := recorder.RecordContainerUsage(context.Background(), request, start, start.Add(time.Second), &cost); err != nil {
		t.Fatal(err)
	}
	if tamaCalls.Load() != 1 || marketplaceCalls.Load() != 1 {
		t.Fatalf("Tama/marketplace calls = %d/%d, want 1/1", tamaCalls.Load(), marketplaceCalls.Load())
	}
}

func TestManagedComputeContainerUsageRecorderRequiresAcknowledgement(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"ok":false,"message":"not recorded"}`))
	}))
	defer server.Close()

	recorder := managedComputeUsageTestRecorder(server.URL)
	cost := 1.0
	request := managedComputeUsageTestRequest("tama-machine-1")
	err := recorder.RecordContainerUsage(context.Background(), request, time.Now(), time.Now().Add(time.Second), &cost)
	if err == nil || !strings.Contains(err.Error(), "not recorded") {
		t.Fatalf("error = %v, want rejected acknowledgement", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want a non-retryable rejection once", calls.Load())
	}
}

func TestManagedComputeContainerUsageRejectsInt64Overflow(t *testing.T) {
	cost := float64(1<<63) / 10_000
	request := managedComputeUsageTestRequest("tama-machine-1")
	_, err := managedComputeContainerUsageBody(request, time.Now(), time.Now().Add(time.Second), &cost)
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("error = %v, want cost overflow", err)
	}
}

// A cost-hook outage (or a workspace it cannot quote) leaves intervals without
// a cost. Routed usage must still be sent — with resources and duration, no
// cost fields — so the ledger can price it from its own rate card. Dropping it
// instead bricks every machine the route covers: the ledger stops machines
// whose usage goes silent.
func TestManagedComputeContainerUsageSendsUnquotedWindowsWithoutACost(t *testing.T) {
	requests := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		requests <- payload
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	recorder := managedComputeUsageTestRecorder(server.URL)
	start := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	request := managedComputeUsageTestRequest("tama-machine-1")
	request.Cpu = 4_000
	request.Memory = 8_192
	if err := recorder.RecordContainerUsage(context.Background(), request, start, start.Add(time.Minute), nil); err != nil {
		t.Fatal(err)
	}
	payload := <-requests
	if _, quoted := payload["cost_micros"]; quoted {
		t.Fatalf("payload = %+v, want no cost on an unquoted window", payload)
	}
	if payload["cpu_millicores"] != float64(4_000) || payload["memory_mb"] != float64(8_192) {
		t.Fatalf("payload = %+v, want resources for the ledger's rate card", payload)
	}

	// A quoted interval keeps sending the quote.
	cost := 0.012345
	if err := recorder.RecordContainerUsage(context.Background(), request, start, start.Add(time.Minute), &cost); err != nil {
		t.Fatal(err)
	}
	payload = <-requests
	if payload["cost_micros"] != float64(124) {
		t.Fatalf("cost_micros = %v, want 124", payload["cost_micros"])
	}
}
