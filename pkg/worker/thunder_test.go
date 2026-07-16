package worker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

func TestThunderAssignRegistersClientAndLeavesAssignedEnvUnchanged(t *testing.T) {
	var registerPayload thunderRegisterClientRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != thunderRegisterClientPath {
			t.Fatalf("path = %s, want %s", r.URL.Path, thunderRegisterClientPath)
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
		_ = json.NewEncoder(w).Encode(thunderRegisterClientResponse{Token: "client-token"})
	}))
	defer server.Close()

	manager := NewContainerThunderManager(server.URL, "central-token", server.Client())
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
	assert.Equal(t, thunderRegisterClientRequest{DeviceID: "container-123", GPUType: "A100", GPUCount: 2}, registerPayload)

	env := manager.InjectAssignedEnvVars([]string{"A=1", "NVIDIA_VISIBLE_DEVICES=void", "WORKER_GPU_DEVICES=0"}, assigned)
	assert.Equal(t, []string{"A=1", "NVIDIA_VISIBLE_DEVICES=void", "WORKER_GPU_DEVICES=0"}, env)
}

func TestThunderUnassignUsesPostDeleteClient(t *testing.T) {
	var deletePayload thunderDeleteClientRequest
	var sawDelete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case thunderRegisterClientPath:
			_ = json.NewEncoder(w).Encode(thunderRegisterClientResponse{Token: "client-token"})
		case thunderDeleteClientPath:
			sawDelete = true
			if r.Method != http.MethodPost {
				t.Fatalf("method = %s, want POST", r.Method)
			}
			if r.Header.Get("Authorization") != "Bearer central-token" {
				t.Fatalf("authorization header = %q", r.Header.Get("Authorization"))
			}
			if err := json.NewDecoder(r.Body).Decode(&deletePayload); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(map[string]bool{"success": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	manager := NewContainerThunderManager(server.URL, "central-token", server.Client())
	request := &types.ContainerRequest{ContainerId: "container-123", Gpu: "H100", GpuCount: 1, GpuVirtualized: true}
	_, err := manager.AssignGPUDevices(request)
	if err != nil {
		t.Fatal(err)
	}
	manager.UnassignGPUDevices(request.ContainerId)

	if !sawDelete {
		t.Fatal("delete-client was not called")
	}
	assert.Equal(t, thunderDeleteClientRequest{DeviceID: "container-123", Token: "client-token"}, deletePayload)
}

func TestThunderAssignRetriesUnsuccessfulStatus(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := atomic.AddInt32(&attempts, 1)
		if attempt < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(thunderRegisterClientResponse{Token: "client-token"})
	}))
	defer server.Close()

	manager := NewContainerThunderManager(server.URL, "central-token", server.Client())
	_, err := manager.AssignGPUDevices(&types.ContainerRequest{ContainerId: "container-123", GpuRequest: []string{"H100"}, GpuVirtualized: true})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))
}

func TestThunderAssignRequiresWorkerEnv(t *testing.T) {
	manager := NewContainerThunderManager("", "", nil)
	_, err := manager.AssignGPUDevices(&types.ContainerRequest{ContainerId: "container-123", Gpu: "A100", GpuVirtualized: true})
	if err == nil {
		t.Fatal("expected missing Thunder configuration error")
	}
}

func TestThunderPrepareContainerFilesystemWritesThunderFiles(t *testing.T) {
	const libraryContents = "libthunder-bytes"
	var registerPayload thunderRegisterClientRequest
	var assetRequested bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case thunderRegisterClientPath:
			if r.Method != http.MethodPost {
				t.Fatalf("register method = %s, want POST", r.Method)
			}
			if r.Header.Get("Authorization") != "Bearer central-token" {
				t.Fatalf("authorization header = %q", r.Header.Get("Authorization"))
			}
			if err := json.NewDecoder(r.Body).Decode(&registerPayload); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(thunderRegisterClientResponse{Token: "client-token"})
		case thunderLibraryAssetPath:
			assetRequested = true
			if r.Method != http.MethodGet {
				t.Fatalf("asset method = %s, want GET", r.Method)
			}
			_, _ = w.Write([]byte(libraryContents))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	manager := NewContainerThunderManager(server.URL, "central-token", server.Client())
	request := &types.ContainerRequest{
		ContainerId:    "container-123",
		Gpu:            "A100",
		GpuCount:       2,
		GpuVirtualized: true,
	}
	rootPath := t.TempDir()

	if err := manager.PrepareContainerFilesystem(request, rootPath); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, thunderRegisterClientRequest{DeviceID: "container-123", GPUType: "A100", GPUCount: 2}, registerPayload)
	assert.True(t, assetRequested)

	library, err := os.ReadFile(thunderHostPath(rootPath, thunderLibraryPath))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, libraryContents, string(library))

	token, err := os.ReadFile(thunderHostPath(rootPath, thunderTokenPath))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "client-token", string(token))

	configBytes, err := os.ReadFile(thunderHostPath(rootPath, thunderConfigPath))
	if err != nil {
		t.Fatal(err)
	}
	var config thunderConfigFile
	if err := json.Unmarshal(configBytes, &config); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, thunderConfigFile{
		DeviceID:        "container-123",
		EnableGRPCTLS:   false,
		EnvironmentType: "production",
		GPUCount:        2,
		GPUType:         "A100",
		ManagerAddress:  "",
		OtelCollector:   "",
		UVM:             false,
	}, config)
}
