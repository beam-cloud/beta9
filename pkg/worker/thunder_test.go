package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/tj/assert"
)

func TestThunderAssignRegistersClientAndLeavesAssignedEnvUnchanged(t *testing.T) {
	var registerPayload thunderEnrollmentTokenRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != thunderEnrollmentTokenPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, thunderEnrollmentTokenPath)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer central-token" {
			t.Fatalf("authorization header = %q", r.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(r.Body).Decode(&registerPayload); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(thunderEnrollmentTokenResponse{EnrollmentTokenID: "token-id", EnrollmentToken: "client-token", ZoneID: "zone-123", Role: thunderEnrollmentRoleClient, GPUType: "a100", GPUCount: 2})
	}))
	defer server.Close()

	manager := NewContainerThunderManager(server.URL, "central-token", server.Client())
	t.Setenv(thunderZoneIDEnv, "zone-123")
	request := &types.ContainerRequest{
		ContainerId:    "container-123",
		Gpu:            "A100",
		GpuCount:       2,
		GpuVirtualized: true,
	}

	assigned, err := manager.AssignGPUDevices(request)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []int{}, assigned)
	assert.Equal(t, thunderEnrollmentTokenRequest{OrgID: "", ZoneID: "zone-123", Role: thunderEnrollmentRoleClient, GPUType: "a100", GPUCount: 2, ExpiresInSeconds: thunderEnrollmentExpiresSecond}, registerPayload)

	env := manager.InjectAssignedEnvVars([]string{"A=1", "NVIDIA_VISIBLE_DEVICES=void", "WORKER_GPU_DEVICES=0"}, assigned)
	assert.Equal(t, []string{"A=1", "NVIDIA_VISIBLE_DEVICES=void", "WORKER_GPU_DEVICES=0"}, env)
}

func TestThunderUnassignUsesDeleteEnrollmentTokenNode(t *testing.T) {
	var deletePath string
	var sawDelete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case thunderEnrollmentTokenPath:
			_ = json.NewEncoder(w).Encode(thunderEnrollmentTokenResponse{EnrollmentTokenID: "token-id", EnrollmentToken: "client-token", ZoneID: "zone-123", Role: thunderEnrollmentRoleClient, GPUType: "a100", GPUCount: 2})
		case fmt.Sprintf(thunderEnrollmentTokenNodePath, "token-id"):
			sawDelete = true
			deletePath = r.URL.Path
			if r.Method != http.MethodDelete {
				t.Fatalf("method = %s, want DELETE", r.Method)
			}
			if r.Header.Get("Authorization") != "Bearer central-token" {
				t.Fatalf("authorization header = %q", r.Header.Get("Authorization"))
			}
			_ = json.NewEncoder(w).Encode(thunderDeleteEnrollmentTokenNodeResponse{EnrollmentTokenID: "token-id", Role: thunderEnrollmentRoleClient, ClientID: "client-id", NodeDeleted: true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	manager := NewContainerThunderManager(server.URL, "central-token", server.Client())
	t.Setenv(thunderZoneIDEnv, "zone-123")
	request := &types.ContainerRequest{ContainerId: "container-123", Gpu: "H100", GpuCount: 1, GpuVirtualized: true}
	_, err := manager.AssignGPUDevices(request)
	if err != nil {
		t.Fatal(err)
	}
	manager.UnassignGPUDevices(request.ContainerId)

	if !sawDelete {
		t.Fatal("delete enrollment-token node was not called")
	}
	assert.Equal(t, fmt.Sprintf(thunderEnrollmentTokenNodePath, "token-id"), deletePath)
}

func TestInstallThunderClientExecutesInstaller(t *testing.T) {
	rt := &mockRuntime{name: "runc"}
	manager := NewContainerThunderManager("https://worker-default.example", "worker-default-token", nil)
	manager.allocations.Set("container-123", thunderAllocation{
		EnrollmentToken: "enroll-token",
		APIURL:          "https://worker-default.example",
		APIToken:        "worker-default-token",
	})
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set("container-123", &ContainerInstance{
		Runtime: rt,
		Spec:    &specs.Spec{Process: &specs.Process{Cwd: "/workspace", Env: []string{"A=1"}}},
	})
	worker := &Worker{
		containerThunderManager: manager,
		containerInstances:      instances,
	}

	err := worker.installThunderClient(context.Background(), &types.ContainerRequest{ContainerId: "container-123", GpuVirtualized: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rt.execCalls) != 1 {
		t.Fatalf("execCalls = %d, want 1", len(rt.execCalls))
	}
	call := rt.execCalls[0]
	assert.Equal(t, "container-123", call.containerID)
	assert.Equal(t, "/workspace", call.proc.Cwd)
	assert.Equal(t, []string{"sh", "-c", "curl -fsSL https://get.thundercompute.com/install.sh | sudo THUNDER_INSTALL_MODE=client THUNDER_AUTH_TOKEN='enroll-token' sh"}, call.proc.Args)
	assert.Contains(t, call.proc.Env, "A=1")
	assert.Contains(t, call.proc.Env, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
}

func TestThunderAssignReturnsErrorOnUnsuccessfulStatus(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	manager := NewContainerThunderManager(server.URL, "central-token", server.Client())
	t.Setenv(thunderZoneIDEnv, "zone-123")
	_, err := manager.AssignGPUDevices(&types.ContainerRequest{ContainerId: "container-123", GpuRequest: []string{"H100"}, GpuVirtualized: true})
	if err == nil {
		t.Fatal("expected Thunder enrollment error")
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&attempts))
}

func TestThunderAssignRequiresWorkerEnv(t *testing.T) {
	manager := NewContainerThunderManager("", "", nil)
	_, err := manager.AssignGPUDevices(&types.ContainerRequest{ContainerId: "container-123", Gpu: "A100", GpuVirtualized: true})
	if err == nil {
		t.Fatal("expected missing Thunder configuration error")
	}
}

func TestThunderAssignRequiresZoneID(t *testing.T) {
	manager := NewContainerThunderManager("https://thunder.example", "central-token", nil)
	_, err := manager.AssignGPUDevices(&types.ContainerRequest{ContainerId: "container-123", Gpu: "A100", GpuVirtualized: true})
	if err == nil {
		t.Fatal("expected missing Thunder zone id error")
	}
}

func TestThunderAssignUsesRequestScopedCredentials(t *testing.T) {
	var authHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(thunderEnrollmentTokenResponse{EnrollmentTokenID: "token-id", EnrollmentToken: "client-token", ZoneID: "zone-123", Role: thunderEnrollmentRoleClient, GPUType: "a100", GPUCount: 2})
	}))
	defer server.Close()

	manager := NewContainerThunderManager("https://worker-default.example", "worker-default-token", server.Client())
	t.Setenv(thunderZoneIDEnv, "zone-worker")
	request := &types.ContainerRequest{
		ContainerId:    "container-123",
		Gpu:            "A100",
		GpuCount:       1,
		GpuVirtualized: true,
		Env: []string{
			thunderAPIURLEnv + "=" + server.URL,
			thunderAPITokenEnv + "=request-token",
			thunderZoneIDEnv + "=zone-request",
		},
	}

	_, err := manager.AssignGPUDevices(request)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "Bearer request-token", authHeader)

	allocation, ok := manager.allocations.Get(request.ContainerId)
	assert.True(t, ok)
	assert.Equal(t, server.URL, allocation.APIURL)
	assert.Equal(t, "request-token", allocation.APIToken)
	assert.Equal(t, "token-id", allocation.EnrollmentTokenID)
	assert.Equal(t, "client-token", allocation.EnrollmentToken)
	assert.Equal(t, "zone-123", allocation.Response.ZoneID)
}
