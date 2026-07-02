package vast

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestHostStartsAndPreemptsPerGPUService(t *testing.T) {
	gpu := GPU{Index: "0", UUID: "GPU-one", Name: "RTX 5090"}
	var availability []availabilityRequest
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/gateway/agent/availability" {
			t.Fatalf("unexpected gateway path %s", r.URL.Path)
		}
		var req availabilityRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		availability = append(availability, req)
		_ = json.NewEncoder(w).Encode(availabilityResponse{Ok: true})
	}))
	defer gateway.Close()

	stateDir := t.TempDir()
	gpuDir := filepath.Join(stateDir, "gpu-0")
	if err := os.MkdirAll(gpuDir, 0700); err != nil {
		t.Fatal(err)
	}
	state := runtimeState{
		GatewayURL: gateway.URL,
		MachineID:  "machine-one",
		AgentToken: "agent-token",
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gpuDir, types.AgentRuntimeStateFileName), data, 0600); err != nil {
		t.Fatal(err)
	}

	services := &fakeServices{}
	cleaner := &fakeCleaner{}
	host, err := newHostServer(context.Background(), HostOptions{
		GatewayURL:      gateway.URL,
		StateDir:        stateDir,
		SentinelToken:   "sentinel-token",
		Services:        services,
		Cleaner:         cleaner,
		DetectGPUs:      func(context.Context) ([]GPU, error) { return []GPU{gpu}, nil },
		ReconcilePeriod: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	host.handleHeartbeat(httptest.NewRecorder(), sentinelRequest(t, "/heartbeat", "sentinel-token", gpu.UUID))
	host.reconcile(context.Background(), time.Now().UTC())
	if got, want := strings.Join(services.starts, ","), "beam-agent-vast-compat-gpu@0.service"; got != want {
		t.Fatalf("starts = %q, want %q", got, want)
	}

	rec := httptest.NewRecorder()
	host.handlePreempt(rec, sentinelRequest(t, "/preempt", "sentinel-token", gpu.UUID))
	if rec.Code != http.StatusOK {
		t.Fatalf("preempt status = %d body=%s", rec.Code, rec.Body.String())
	}
	if got, want := strings.Join(services.stops, ","), "beam-agent-vast-compat-gpu@0.service"; got != want {
		t.Fatalf("stops = %q, want %q", got, want)
	}
	if got, want := strings.Join(cleaner.machines, ","), "machine-one"; got != want {
		t.Fatalf("cleaned machines = %q, want %q", got, want)
	}
	if len(availability) != 1 {
		t.Fatalf("availability calls = %d, want 1", len(availability))
	}
	if availability[0].AgentToken != "agent-token" || availability[0].Schedulable || availability[0].Reason != PreemptReason {
		t.Fatalf("availability request = %#v", availability[0])
	}
}

func TestHostHeartbeatRequiresKnownGPU(t *testing.T) {
	host, err := newHostServer(context.Background(), HostOptions{
		GatewayURL:    "http://gateway.example",
		SentinelToken: "sentinel-token",
		Services:      &fakeServices{},
		Cleaner:       &fakeCleaner{},
		DetectGPUs:    func(context.Context) ([]GPU, error) { return []GPU{{Index: "0", UUID: "GPU-one"}}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	host.handleHeartbeat(rec, sentinelRequest(t, "/heartbeat", "sentinel-token", "GPU-other"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHostStopsUnknownServiceOnceWithoutLease(t *testing.T) {
	gpu := GPU{Index: "0", UUID: "GPU-one"}
	services := &fakeServices{}
	host, err := newHostServer(context.Background(), HostOptions{
		GatewayURL:      "http://gateway.example",
		SentinelToken:   "sentinel-token",
		Services:        services,
		Cleaner:         &fakeCleaner{},
		DetectGPUs:      func(context.Context) ([]GPU, error) { return []GPU{gpu}, nil },
		ReconcilePeriod: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	host.reconcile(context.Background(), time.Now().UTC())
	host.reconcile(context.Background(), time.Now().UTC())

	if got, want := strings.Join(services.stops, ","), "beam-agent-vast-compat-gpu@0.service"; got != want {
		t.Fatalf("stops = %q, want %q", got, want)
	}
}

func TestHostPreemptRejectsStaleSentinelIdentity(t *testing.T) {
	gpu := GPU{Index: "0", UUID: "GPU-one"}
	services := &fakeServices{}
	host, err := newHostServer(context.Background(), HostOptions{
		GatewayURL:      "http://gateway.example",
		StateDir:        t.TempDir(),
		SentinelToken:   "sentinel-token",
		Services:        services,
		Cleaner:         &fakeCleaner{},
		DetectGPUs:      func(context.Context) ([]GPU, error) { return []GPU{gpu}, nil },
		ReconcilePeriod: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	active := sentinelEvent{GPUUUID: gpu.UUID, Hostname: "sentinel-new", PID: 200}
	host.handleHeartbeat(httptest.NewRecorder(), sentinelEventRequest(t, "/heartbeat", "sentinel-token", active))

	stale := sentinelEvent{GPUUUID: gpu.UUID, Hostname: "sentinel-old", PID: 100}
	rec := httptest.NewRecorder()
	host.handlePreempt(rec, sentinelEventRequest(t, "/preempt", "sentinel-token", stale))
	if rec.Code != http.StatusConflict {
		t.Fatalf("stale preempt status = %d, want %d", rec.Code, http.StatusConflict)
	}
	if len(services.stops) != 0 {
		t.Fatalf("stale preempt stopped services: %v", services.stops)
	}
	if !host.leaseActive(gpu.UUID, time.Now().UTC()) {
		t.Fatal("stale preempt removed the active lease")
	}

	// Identity-less events are accepted for compatibility with older sentinels.
	rec = httptest.NewRecorder()
	host.handlePreempt(rec, sentinelEventRequest(t, "/preempt", "sentinel-token", sentinelEvent{GPUUUID: gpu.UUID}))
	if rec.Code != http.StatusOK {
		t.Fatalf("compat preempt status = %d body=%s", rec.Code, rec.Body.String())
	}
	if len(services.stops) != 1 {
		t.Fatalf("compat preempt stops = %v, want one stop", services.stops)
	}
}

func TestHostRetriesStopAfterTransientFailure(t *testing.T) {
	gpu := GPU{Index: "0", UUID: "GPU-one"}
	services := &fakeServices{stopErrs: []error{fmt.Errorf("transient systemctl failure")}}
	host, err := newHostServer(context.Background(), HostOptions{
		GatewayURL:      "http://gateway.example",
		StateDir:        t.TempDir(),
		SentinelToken:   "sentinel-token",
		Services:        services,
		Cleaner:         &fakeCleaner{},
		DetectGPUs:      func(context.Context) ([]GPU, error) { return []GPU{gpu}, nil },
		ReconcilePeriod: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	host.reconcile(context.Background(), time.Now().UTC()) // stop fails
	host.reconcile(context.Background(), time.Now().UTC()) // stop retried, succeeds
	host.reconcile(context.Background(), time.Now().UTC()) // already stopped, no retry

	if got, want := strings.Join(services.stops, ","), "beam-agent-vast-compat-gpu@0.service,beam-agent-vast-compat-gpu@0.service"; got != want {
		t.Fatalf("stops = %q, want %q", got, want)
	}
}

func sentinelEventRequest(t *testing.T, path, token string, event sentinelEvent) *http.Request {
	t.Helper()
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+token)
	return req
}

func sentinelRequest(t *testing.T, path, token, gpuUUID string) *http.Request {
	t.Helper()
	body, err := json.Marshal(sentinelEvent{GPUUUID: gpuUUID})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+token)
	return req
}

type fakeServices struct {
	starts   []string
	stops    []string
	stopErrs []error
}

func (f *fakeServices) Start(_ context.Context, unit string) error {
	f.starts = append(f.starts, unit)
	return nil
}

func (f *fakeServices) Stop(_ context.Context, unit string) error {
	f.stops = append(f.stops, unit)
	if len(f.stopErrs) > 0 {
		err := f.stopErrs[0]
		f.stopErrs = f.stopErrs[1:]
		return err
	}
	return nil
}

type fakeCleaner struct {
	machines []string
}

func (f *fakeCleaner) RemoveManagedWorkerContainersForMachine(machineID string) error {
	f.machines = append(f.machines, machineID)
	return nil
}
