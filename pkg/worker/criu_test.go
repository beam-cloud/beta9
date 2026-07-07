package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// MockRuntime is a mock implementation of runtime.Runtime for testing
type MockRuntime struct {
	name             string
	checkpointCalled bool
	restoreCalled    bool
	checkpointCalls  int
	checkpointError  error
	checkpointErrors []error
	restoreError     error
	restoreExitCode  int
	restoreOpts      *runtime.RestoreOpts
	killCalled       bool
	killSignal       syscall.Signal
	capabilities     runtime.Capabilities
}

func TestCheckpointRuntimeEnvironmentOverrides(t *testing.T) {
	gpuRequest := &types.ContainerRequest{
		CheckpointEnabled: true,
		Gpu:               "RTX4090",
		Stub:              testServiceStub(t),
	}
	gpuEnv := applyCheckpointRuntimeEnvironmentOverrides([]string{
		"UV_USE_IO_URING=1",
		"MASTER_ADDR=10.0.0.1",
		"VLLM_USAGE_SOURCE=production-docker-image",
	}, gpuRequest, []string{"python", "-m", "vllm.entrypoints.openai.api_server"})

	for _, want := range []string{
		"UV_USE_IO_URING=0",
		"TORCHINDUCTOR_QUIESCE_ASYNC_COMPILE_POOL=1",
		"MASTER_ADDR=127.0.0.1",
		"NCCL_SOCKET_IFNAME=lo",
		"GLOO_SOCKET_IFNAME=lo",
	} {
		if !slices.Contains(gpuEnv, want) {
			t.Fatalf("GPU checkpoint env missing %q in %v", want, gpuEnv)
		}
	}
	if slices.Contains(gpuEnv, "UV_USE_IO_URING=1") || slices.Contains(gpuEnv, "MASTER_ADDR=10.0.0.1") {
		t.Fatalf("GPU checkpoint env did not override conflicting values: %v", gpuEnv)
	}

	genericGPUEnv := applyCheckpointRuntimeEnvironmentOverrides(nil, &types.ContainerRequest{
		CheckpointEnabled: true,
		Gpu:               "RTX4090",
		Stub:              testServiceStub(t),
	}, []string{"python", "app.py"})
	if !slices.Contains(genericGPUEnv, "UV_USE_IO_URING=0") ||
		slices.Contains(genericGPUEnv, "MASTER_ADDR=127.0.0.1") ||
		slices.Contains(genericGPUEnv, "NCCL_SOCKET_IFNAME=lo") ||
		slices.Contains(genericGPUEnv, "GLOO_SOCKET_IFNAME=lo") {
		t.Fatalf("generic GPU checkpoint env should not include pod-service loopback overrides: %v", genericGPUEnv)
	}

	nonServiceEnv := applyCheckpointRuntimeEnvironmentOverrides(nil, &types.ContainerRequest{
		CheckpointEnabled: true,
		Gpu:               "RTX4090",
	}, []string{"vllm", "serve"})
	if slices.Contains(nonServiceEnv, "MASTER_ADDR=127.0.0.1") ||
		slices.Contains(nonServiceEnv, "NCCL_SOCKET_IFNAME=lo") ||
		slices.Contains(nonServiceEnv, "GLOO_SOCKET_IFNAME=lo") {
		t.Fatalf("non-service checkpoint env should not include pod-service loopback overrides: %v", nonServiceEnv)
	}

	disabledEnv := applyCheckpointRuntimeEnvironmentOverrides([]string{"UV_USE_IO_URING=1"}, &types.ContainerRequest{
		CheckpointEnabled: false,
		Gpu:               "RTX4090",
	}, []string{"vllm", "serve"})
	if !slices.Contains(disabledEnv, "UV_USE_IO_URING=1") || slices.Contains(disabledEnv, "UV_USE_IO_URING=0") {
		t.Fatalf("checkpoint-disabled request should leave env unchanged: %v", disabledEnv)
	}
}

func testServiceStub(t *testing.T) types.StubWithRelated {
	t.Helper()
	return testPodStub(t, true)
}

func testPodStub(t *testing.T, isService bool) types.StubWithRelated {
	t.Helper()
	config, err := json.Marshal(&types.StubConfigV1{IsService: isService})
	if err != nil {
		t.Fatalf("failed to marshal stub config: %v", err)
	}
	return types.StubWithRelated{
		Stub: types.Stub{
			Type:   types.StubType(types.StubTypePodDeployment),
			Config: string(config),
		},
	}
}

func TestCheckpointHTTPReadinessRequiresStatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	address := testHTTPServerAddress(t, server)
	worker := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	request := &types.ContainerRequest{ContainerId: "container-1"}
	worker.containerInstances.Set(request.ContainerId, &ContainerInstance{
		ContainerAddressMap: map[int32]string{8000: address},
	})

	err := worker.checkCheckpointHTTPReady(context.Background(), server.Client(), request, 8000, "/ready")
	if err != nil {
		t.Fatalf("checkCheckpointHTTPReady returned error: %v", err)
	}

	err = worker.checkCheckpointHTTPReady(context.Background(), server.Client(), request, 8000, "/not-ready")
	if err == nil {
		t.Fatal("expected non-200 readiness response to fail")
	}
}

func TestCheckpointHTTPReadinessPrefersContainerIP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host, port := testHTTPServerHostPort(t, server)

	worker := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	request := &types.ContainerRequest{ContainerId: "container-direct-ip"}
	worker.containerInstances.Set(request.ContainerId, &ContainerInstance{
		ContainerIp:         host,
		ContainerAddressMap: map[int32]string{port: "127.0.0.1:1"},
	})

	err := worker.checkCheckpointHTTPReady(context.Background(), server.Client(), request, port, "/ready")
	if err != nil {
		t.Fatalf("checkCheckpointHTTPReady returned error: %v", err)
	}
}

func TestCheckpointHTTPReadinessAddresses(t *testing.T) {
	tests := []struct {
		name     string
		instance *ContainerInstance
		want     []string
	}{
		{
			name: "container IPv4 before published route",
			instance: &ContainerInstance{
				ContainerIp:         "192.168.0.65",
				ContainerAddressMap: map[int32]string{8000: "10.42.0.215:43131"},
			},
			want: []string{
				"192.168.0.65:8000",
				"[fd00:abcd::41]:8000",
				"10.42.0.215:43131",
			},
		},
		{
			name: "container IPv6 before published route",
			instance: &ContainerInstance{
				ContainerIp:         "fd00:abcd::41",
				ContainerAddressMap: map[int32]string{8000: "[2600:1f18::1]:43131"},
			},
			want: []string{
				"[fd00:abcd::41]:8000",
				"[2600:1f18::1]:43131",
			},
		},
		{
			name: "published route only",
			instance: &ContainerInstance{
				ContainerAddressMap: map[int32]string{8000: "10.42.0.215:43131"},
			},
			want: []string{"10.42.0.215:43131"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := checkpointHTTPReadinessAddresses(tt.instance, 8000)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("checkpoint readiness addresses = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWaitForCheckpointHTTPReadinessLogsWaitingAndReady(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	address := testHTTPServerAddress(t, server)
	worker := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	request := &types.ContainerRequest{
		ContainerId: "container-ready-log",
		Ports:       []uint32{8000},
	}
	worker.containerInstances.Set(request.ContainerId, &ContainerInstance{
		ContainerAddressMap: map[int32]string{8000: address},
	})
	trigger := &types.CheckpointTrigger{
		Type:           checkpointTriggerHTTP,
		HttpPath:       "v1/models",
		TimeoutSeconds: 1,
	}
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))

	if err := worker.waitForCheckpointHTTPReadiness(context.Background(), request, trigger, logger); err != nil {
		t.Fatalf("waitForCheckpointHTTPReadiness returned error: %v", err)
	}

	logs := output.String()
	if !strings.Contains(logs, "Waiting for container HTTP readiness before checkpoint (port 8000, path /v1/models)") {
		t.Fatalf("expected waiting log, got: %s", logs)
	}
	if !strings.Contains(logs, "Container HTTP readiness reached for checkpoint") {
		t.Fatalf("expected ready log, got: %s", logs)
	}
}

func testHTTPServerAddress(t *testing.T, server *httptest.Server) string {
	t.Helper()
	return strings.TrimPrefix(server.URL, "http://")
}

func testHTTPServerHostPort(t *testing.T, server *httptest.Server) (string, int32) {
	t.Helper()

	host, portText, err := net.SplitHostPort(testHTTPServerAddress(t, server))
	if err != nil {
		t.Fatalf("split test server address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}
	return host, int32(port)
}

func TestCheckpointCreateLockIsPerStub(t *testing.T) {
	worker := &Worker{}
	request := &types.ContainerRequest{WorkspaceId: "workspace", StubId: "stub"}

	if !worker.acquireCheckpointCreateLock(request) {
		t.Fatal("first checkpoint lock acquisition failed")
	}
	if worker.acquireCheckpointCreateLock(request) {
		t.Fatal("second checkpoint lock acquisition succeeded")
	}
	worker.releaseCheckpointCreateLock(request)
	if !worker.acquireCheckpointCreateLock(request) {
		t.Fatal("checkpoint lock was not released")
	}
}

func NewMockRuntime(name string, caps runtime.Capabilities) *MockRuntime {
	return &MockRuntime{
		name:         name,
		capabilities: caps,
	}
}

func (m *MockRuntime) Name() string {
	return m.name
}

func (m *MockRuntime) Capabilities() runtime.Capabilities {
	return m.capabilities
}

func (m *MockRuntime) Prepare(ctx context.Context, spec *specs.Spec) error {
	return nil
}

func (m *MockRuntime) Run(ctx context.Context, containerID, bundlePath string, opts *runtime.RunOpts) (int, error) {
	return 0, nil
}

func (m *MockRuntime) Exec(ctx context.Context, containerID string, proc specs.Process, opts *runtime.ExecOpts) error {
	return nil
}

func (m *MockRuntime) Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *runtime.KillOpts) error {
	m.killCalled = true
	m.killSignal = sig
	return nil
}

func (m *MockRuntime) Delete(ctx context.Context, containerID string, opts *runtime.DeleteOpts) error {
	return nil
}

func (m *MockRuntime) State(ctx context.Context, containerID string) (runtime.State, error) {
	return runtime.State{}, nil
}

func (m *MockRuntime) Events(ctx context.Context, containerID string) (<-chan runtime.Event, error) {
	ch := make(chan runtime.Event)
	close(ch)
	return ch, nil
}

func (m *MockRuntime) Checkpoint(ctx context.Context, containerID string, opts *runtime.CheckpointOpts) error {
	m.checkpointCalled = true
	m.checkpointCalls++
	if len(m.checkpointErrors) > 0 {
		err := m.checkpointErrors[0]
		m.checkpointErrors = m.checkpointErrors[1:]
		if err != nil {
			return err
		}
	}
	if m.checkpointError != nil {
		return m.checkpointError
	}
	// Create checkpoint directory
	if opts != nil && opts.ImagePath != "" {
		return os.MkdirAll(opts.ImagePath, 0755)
	}
	return nil
}

func (m *MockRuntime) Restore(ctx context.Context, containerID string, opts *runtime.RestoreOpts) (int, error) {
	m.restoreCalled = true
	m.restoreOpts = opts
	if m.restoreError != nil {
		return m.restoreExitCode, m.restoreError
	}
	if opts != nil && opts.Started != nil {
		opts.Started <- 12345
	}
	return m.restoreExitCode, nil
}

func (m *MockRuntime) Close() error {
	return nil
}

// Reset clears the mock runtime state flags for reuse in subtests
func (m *MockRuntime) Reset() {
	m.checkpointCalled = false
	m.checkpointCalls = 0
	m.restoreCalled = false
	m.restoreOpts = nil
	m.killCalled = false
	m.killSignal = 0
}

// TestNvidiaCRIUManager tests NVIDIA CRIU manager with different runtimes
func TestNvidiaCRIUManager(t *testing.T) {
	if os.Getenv("SKIP_CRIU_TESTS") == "1" {
		t.Skip("Skipping CRIU tests")
	}

	testCases := []struct {
		name         string
		runtimeName  string
		capabilities runtime.Capabilities
		extraTests   func(t *testing.T, manager CRIUManager, mockRuntime *MockRuntime, tmpDir string)
	}{
		{
			name:        "runc",
			runtimeName: "runc",
			capabilities: runtime.Capabilities{
				CheckpointRestore: true,
				GPU:               true,
			},
		},
		{
			name:        "gvisor",
			runtimeName: "gvisor",
			capabilities: runtime.Capabilities{
				CheckpointRestore: true,
				GPU:               true,
			},
			extraTests: func(t *testing.T, manager CRIUManager, mockRuntime *MockRuntime, tmpDir string) {
				t.Run("checkpoint with CUDA support", func(t *testing.T) {
					// Reset mock state to ensure this test's assertions are independent
					mockRuntime.Reset()

					request := &types.ContainerRequest{
						ContainerId: "cuda-container",
						Gpu:         "nvidia-tesla-v100",
						GpuCount:    1,
					}

					checkpointPath, err := manager.CreateCheckpoint(context.Background(), mockRuntime, "cuda-checkpoint", request)
					if err != nil {
						t.Errorf("CreateCheckpoint with CUDA support failed: %v", err)
					}

					if !mockRuntime.checkpointCalled {
						t.Error("Expected Checkpoint to be called for CUDA container")
					}

					if checkpointPath == "" {
						t.Error("Expected non-empty checkpoint path for CUDA container")
					}
				})
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", fmt.Sprintf("criu-%s-test-*", tc.runtimeName))
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			config := types.CRIUConfig{}

			manager, err := InitializeNvidiaCRIU(context.Background(), config, tmpDir)
			if err != nil {
				t.Fatalf("Failed to initialize NVIDIA CRIU manager: %v", err)
			}

			mockRuntime := NewMockRuntime(tc.runtimeName, tc.capabilities)

			t.Run("CreateCheckpoint", func(t *testing.T) {
				request := &types.ContainerRequest{
					ContainerId: fmt.Sprintf("%s-container-1", tc.runtimeName),
				}

				checkpointPath, err := manager.CreateCheckpoint(context.Background(), mockRuntime, "checkpoint-1", request)
				if err != nil {
					t.Errorf("CreateCheckpoint failed: %v", err)
				}

				if !mockRuntime.checkpointCalled {
					t.Error("Expected Checkpoint to be called on runtime")
				}

				if checkpointPath == "" {
					t.Error("Expected non-empty checkpoint path")
				}
			})

			t.Run("RestoreCheckpoint", func(t *testing.T) {
				request := &types.ContainerRequest{
					ContainerId: fmt.Sprintf("%s-container-2", tc.runtimeName),
				}

				checkpoint := &types.Checkpoint{
					CheckpointId: "checkpoint-1",
				}

				// Create checkpoint directory
				checkpointPath := filepath.Join(tmpDir, checkpoint.CheckpointId)
				os.MkdirAll(checkpointPath, 0755)

				opts := &RestoreOpts{
					request:    request,
					checkpoint: checkpoint,
					configPath: filepath.Join(tmpDir, "config.json"),
					started:    make(chan int, 1),
				}

				exitCode, err := manager.RestoreCheckpoint(context.Background(), mockRuntime, opts)
				if err != nil {
					t.Errorf("RestoreCheckpoint failed: %v", err)
				}

				if !mockRuntime.restoreCalled {
					t.Error("Expected Restore to be called on runtime")
				}

				if mockRuntime.restoreOpts == nil || mockRuntime.restoreOpts.AllowOpenTCP || !mockRuntime.restoreOpts.TCPClose {
					t.Errorf("Expected generic NVIDIA restore to use tcp-close, got %+v", mockRuntime.restoreOpts)
				}

				if exitCode != 0 {
					t.Errorf("Expected exit code 0, got %d", exitCode)
				}
			})

			t.Run("RestoreCheckpoint pod preserves open TCP", func(t *testing.T) {
				mockRuntime.Reset()

				request := &types.ContainerRequest{
					ContainerId: fmt.Sprintf("%s-pod-container", tc.runtimeName),
					Gpu:         "RTX4090",
					Stub:        testPodStub(t, false),
				}

				checkpoint := &types.Checkpoint{
					CheckpointId: "checkpoint-pod",
				}

				checkpointPath := filepath.Join(tmpDir, checkpoint.CheckpointId)
				os.MkdirAll(checkpointPath, 0755)

				opts := &RestoreOpts{
					request:    request,
					checkpoint: checkpoint,
					configPath: filepath.Join(tmpDir, "pod-config.json"),
					started:    make(chan int, 1),
				}

				exitCode, err := manager.RestoreCheckpoint(context.Background(), mockRuntime, opts)
				if err != nil {
					t.Errorf("RestoreCheckpoint failed: %v", err)
				}

				if mockRuntime.restoreOpts == nil || !mockRuntime.restoreOpts.AllowOpenTCP || mockRuntime.restoreOpts.TCPClose {
					t.Errorf("Expected pod NVIDIA restore to preserve open TCP without tcp-close, got %+v", mockRuntime.restoreOpts)
				}

				if exitCode != 0 {
					t.Errorf("Expected exit code 0, got %d", exitCode)
				}
			})

			t.Run("RestoreCheckpoint service preserves open TCP", func(t *testing.T) {
				mockRuntime.Reset()

				request := &types.ContainerRequest{
					ContainerId: fmt.Sprintf("%s-service-container", tc.runtimeName),
					Gpu:         "RTX4090",
					Stub:        testServiceStub(t),
				}

				checkpoint := &types.Checkpoint{
					CheckpointId: "checkpoint-service",
				}

				checkpointPath := filepath.Join(tmpDir, checkpoint.CheckpointId)
				os.MkdirAll(checkpointPath, 0755)

				opts := &RestoreOpts{
					request:    request,
					checkpoint: checkpoint,
					configPath: filepath.Join(tmpDir, "service-config.json"),
					started:    make(chan int, 1),
				}

				exitCode, err := manager.RestoreCheckpoint(context.Background(), mockRuntime, opts)
				if err != nil {
					t.Errorf("RestoreCheckpoint failed: %v", err)
				}

				if mockRuntime.restoreOpts == nil || !mockRuntime.restoreOpts.AllowOpenTCP || mockRuntime.restoreOpts.TCPClose {
					t.Errorf("Expected service NVIDIA restore to preserve open TCP without tcp-close, got %+v", mockRuntime.restoreOpts)
				}

				if exitCode != 0 {
					t.Errorf("Expected exit code 0, got %d", exitCode)
				}
			})

			// Run any extra tests specific to this runtime
			if tc.extraTests != nil {
				tc.extraTests(t, manager, mockRuntime, tmpDir)
			}
		})
	}
}

// TestCheckpointRestoreErrorHandling tests error handling in checkpoint/restore
func TestCheckpointRestoreErrorHandling(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "criu-error-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := types.CRIUConfig{}

	manager, err := InitializeNvidiaCRIU(context.Background(), config, tmpDir)
	if err != nil {
		t.Fatalf("Failed to initialize NVIDIA CRIU manager: %v", err)
	}

	t.Run("checkpoint failure", func(t *testing.T) {
		mockRuntime := NewMockRuntime("runc", runtime.Capabilities{
			CheckpointRestore: true,
		})
		mockRuntime.checkpointError = fmt.Errorf("checkpoint failed")

		request := &types.ContainerRequest{
			ContainerId: "failing-container",
		}

		_, err := manager.CreateCheckpoint(context.Background(), mockRuntime, "failing-checkpoint", request)
		if err == nil {
			t.Error("Expected error when checkpoint fails")
		}
	})

	t.Run("restore failure", func(t *testing.T) {
		mockRuntime := NewMockRuntime("runc", runtime.Capabilities{
			CheckpointRestore: true,
		})
		mockRuntime.restoreError = fmt.Errorf("restore failed")
		mockRuntime.restoreExitCode = -1

		request := &types.ContainerRequest{
			ContainerId: "failing-container",
		}

		checkpoint := &types.Checkpoint{
			CheckpointId: "failing-checkpoint",
		}

		// Create checkpoint directory
		checkpointPath := filepath.Join(tmpDir, checkpoint.CheckpointId)
		os.MkdirAll(checkpointPath, 0755)

		opts := &RestoreOpts{
			request:    request,
			checkpoint: checkpoint,
			configPath: filepath.Join(tmpDir, "config.json"),
		}

		exitCode, err := manager.RestoreCheckpoint(context.Background(), mockRuntime, opts)
		if err == nil {
			t.Error("Expected error when restore fails")
		}
		if exitCode != -1 {
			t.Errorf("Expected exit code -1, got %d", exitCode)
		}
	})

	t.Run("CRIU restore specific error", func(t *testing.T) {
		mockRuntime := NewMockRuntime("runc", runtime.Capabilities{
			CheckpointRestore: true,
		})
		// Simulate CRIU restore failure
		mockRuntime.restoreError = &ErrCRIURestoreFailed{
			Stderr: "criu failed: type RESTORE",
		}
		mockRuntime.restoreExitCode = -1

		request := &types.ContainerRequest{
			ContainerId: "criu-failing-container",
		}

		checkpoint := &types.Checkpoint{
			CheckpointId: "criu-failing-checkpoint",
		}

		// Create checkpoint directory
		checkpointPath := filepath.Join(tmpDir, checkpoint.CheckpointId)
		os.MkdirAll(checkpointPath, 0755)

		opts := &RestoreOpts{
			request:    request,
			checkpoint: checkpoint,
			configPath: filepath.Join(tmpDir, "config.json"),
		}

		_, err := manager.RestoreCheckpoint(context.Background(), mockRuntime, opts)
		if !IsCRIURestoreError(err) {
			t.Error("Expected CRIU restore error")
		}
	})
}

func TestCheckpointMaterializedRequiresRuntimeAndFilesystemPayload(t *testing.T) {
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint-1")

	if checkpointMaterialized(checkpointPath) {
		t.Fatal("missing checkpoint path should not be materialized")
	}

	if err := os.MkdirAll(filepath.Join(checkpointPath, checkpointFsDir), 0755); err != nil {
		t.Fatal(err)
	}
	if checkpointMaterialized(checkpointPath) {
		t.Fatal("filesystem-only checkpoint should not be materialized")
	}

	if err := os.WriteFile(filepath.Join(checkpointPath, "inventory.img"), []byte("runtime payload"), 0644); err != nil {
		t.Fatal(err)
	}
	if !checkpointMaterialized(checkpointPath) {
		t.Fatal("checkpoint with filesystem and runtime payload should be materialized")
	}
}

func TestCreateCheckpointRequiresCRIUManager(t *testing.T) {
	containerID := "container-no-criu"
	worker := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:      containerID,
		Runtime: NewMockRuntime("runc", runtime.Capabilities{CheckpointRestore: true}),
	})

	err := worker.createCheckpoint(context.Background(), &CreateCheckpointOpts{
		Request:      &types.ContainerRequest{ContainerId: containerID},
		CheckpointId: "checkpoint-no-criu",
	})
	if !errors.Is(err, errCRIUManagerUnavailable) {
		t.Fatalf("createCheckpoint error = %v, want %v", err, errCRIUManagerUnavailable)
	}
}

func TestCreateCheckpointFailsWhenRuntimeStartSignalCloses(t *testing.T) {
	containerID := "container-no-runtime-start"
	backendRepoClient := &fakeBackendRepoClient{}
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		backendRepoClient:  backendRepoClient,
		criuManager:        &NvidiaCRIUManager{checkpointRoot: t.TempDir(), available: true},
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:      containerID,
		Runtime: NewMockRuntime("runc", runtime.Capabilities{CheckpointRestore: true}),
	})
	checkpointPIDChan := make(chan int)
	close(checkpointPIDChan)

	err := worker.createCheckpoint(context.Background(), &CreateCheckpointOpts{
		Request:           &types.ContainerRequest{ContainerId: containerID, Stub: types.StubWithRelated{Stub: types.Stub{ExternalId: "stub-no-runtime-start"}}},
		CheckpointId:      "checkpoint-no-runtime-start",
		CheckpointPIDChan: checkpointPIDChan,
	})
	if err == nil || !strings.Contains(err.Error(), "container runtime exited before checkpoint could start") {
		t.Fatalf("createCheckpoint error = %v, want runtime start signal error", err)
	}
	if backendRepoClient.createCalls != 1 {
		t.Fatalf("CreateCheckpoint calls = %d, want 1", backendRepoClient.createCalls)
	}
	if got := backendRepoClient.lastCreate.Status; got != string(types.CheckpointStatusCheckpointFailed) {
		t.Fatalf("checkpoint status = %q, want %q", got, types.CheckpointStatusCheckpointFailed)
	}
}

func TestCreateCheckpointRetriesTransientGvisorTCPStateError(t *testing.T) {
	manager := &NvidiaCRIUManager{checkpointRoot: t.TempDir(), available: true}
	rt := NewMockRuntime(types.ContainerRuntimeGvisor.String(), runtime.Capabilities{CheckpointRestore: true})
	rt.checkpointErrors = []error{
		errors.New("encoding error: invalid memory address or nil pointer dereference for object tcp.Endpoint"),
		nil,
	}

	_, err := manager.CreateCheckpoint(context.Background(), rt, "checkpoint-retry", &types.ContainerRequest{
		ContainerId: "sandbox-retry",
	})

	if err != nil {
		t.Fatalf("CreateCheckpoint error = %v", err)
	}
	if rt.checkpointCalls != 2 {
		t.Fatalf("checkpoint calls = %d, want 2", rt.checkpointCalls)
	}
}

func TestCreateCheckpointDoesNotRetryRuncCheckpointError(t *testing.T) {
	manager := &NvidiaCRIUManager{checkpointRoot: t.TempDir(), available: true}
	rt := NewMockRuntime(types.ContainerRuntimeRunc.String(), runtime.Capabilities{CheckpointRestore: true})
	rt.checkpointError = errors.New("encoding error: invalid memory address or nil pointer dereference for object tcp.Endpoint")

	_, err := manager.CreateCheckpoint(context.Background(), rt, "checkpoint-no-retry", &types.ContainerRequest{
		ContainerId: "sandbox-no-retry",
	})

	if err == nil {
		t.Fatal("expected CreateCheckpoint error")
	}
	if rt.checkpointCalls != 1 {
		t.Fatalf("checkpoint calls = %d, want 1", rt.checkpointCalls)
	}
}

func TestAttemptRestoreCheckpointRequiresCRIUManager(t *testing.T) {
	request := &types.ContainerRequest{
		ContainerId: "container-restore-no-criu",
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-no-criu",
			Status:       string(types.CheckpointStatusAvailable),
		},
	}

	exitCode, restored, started, err := (&Worker{}).attemptRestoreCheckpoint(
		context.Background(),
		request,
		nil,
		nil,
		nil,
		nil,
	)
	if !errors.Is(err, errCRIUManagerUnavailable) {
		t.Fatalf("attemptRestoreCheckpoint error = %v, want %v", err, errCRIUManagerUnavailable)
	}
	if restored {
		t.Fatal("attemptRestoreCheckpoint restored with unavailable CRIU manager")
	}
	if started {
		t.Fatal("attemptRestoreCheckpoint started with unavailable CRIU manager")
	}
	if exitCode != -1 {
		t.Fatalf("attemptRestoreCheckpoint exitCode = %d, want -1", exitCode)
	}
}

func TestSignalRestoredSandboxProcessManager(t *testing.T) {
	rt := NewMockRuntime("gvisor", runtime.Capabilities{CheckpointRestore: true})
	request := &types.ContainerRequest{
		ContainerId: "sandbox-restore",
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type: types.StubType(types.StubTypeSandbox),
		}},
	}

	(&Worker{}).signalRestoredSandboxProcessManager(context.Background(), request, rt)

	if !rt.killCalled {
		t.Fatal("expected restored sandbox process manager to be signaled")
	}
	if rt.killSignal != syscall.SIGWINCH {
		t.Fatalf("signal = %v, want %v", rt.killSignal, syscall.SIGWINCH)
	}
}

func TestSignalRestoredSandboxProcessManagerSkipsNonSandbox(t *testing.T) {
	rt := NewMockRuntime("gvisor", runtime.Capabilities{CheckpointRestore: true})
	request := &types.ContainerRequest{
		ContainerId: "endpoint-restore",
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type: types.StubType(types.StubTypeEndpoint),
		}},
	}

	(&Worker{}).signalRestoredSandboxProcessManager(context.Background(), request, rt)

	if rt.killCalled {
		t.Fatal("did not expect non-sandbox restore to be signaled")
	}
}

func TestMaterializeCheckpointArchiveRejectsFilesystemOnlyPayload(t *testing.T) {
	root := t.TempDir()
	checkpointID := "checkpoint-1"
	sourcePath := filepath.Join(root, "source", checkpointID)
	if err := os.MkdirAll(filepath.Join(sourcePath, checkpointFsDir), 0755); err != nil {
		t.Fatal(err)
	}

	archivePath := filepath.Join(root, checkpointID+checkpointArchiveExtension)
	if err := createTar(sourcePath, archivePath); err != nil {
		t.Fatal(err)
	}

	checkpointPath := filepath.Join(root, "materialized", checkpointID)
	if err := materializeCheckpointArchive(archivePath, checkpointPath, checkpointID); err == nil {
		t.Fatal("expected filesystem-only archive to be rejected")
	}
	if checkpointMaterialized(checkpointPath) {
		t.Fatal("filesystem-only archive should not materialize checkpoint")
	}
}

// TestRuntimeCompatibility tests that the CRIU manager works with different runtimes
func TestRuntimeCompatibility(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "criu-compat-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := types.CRIUConfig{}

	manager, err := InitializeNvidiaCRIU(context.Background(), config, tmpDir)
	if err != nil {
		t.Fatalf("Failed to initialize NVIDIA CRIU manager: %v", err)
	}

	runtimes := []struct {
		name         string
		capabilities runtime.Capabilities
	}{
		{
			name: "runc",
			capabilities: runtime.Capabilities{
				CheckpointRestore: true,
				GPU:               true,
				CDI:               true,
			},
		},
		{
			name: "gvisor",
			capabilities: runtime.Capabilities{
				CheckpointRestore: true,
				GPU:               true,
				CDI:               true,
			},
		},
	}

	for _, rtInfo := range runtimes {
		t.Run(fmt.Sprintf("checkpoint_with_%s", rtInfo.name), func(t *testing.T) {
			mockRuntime := NewMockRuntime(rtInfo.name, rtInfo.capabilities)

			request := &types.ContainerRequest{
				ContainerId: fmt.Sprintf("%s-container", rtInfo.name),
			}

			checkpointID := fmt.Sprintf("%s-checkpoint", rtInfo.name)
			checkpointPath, err := manager.CreateCheckpoint(context.Background(), mockRuntime, checkpointID, request)
			if err != nil {
				t.Errorf("CreateCheckpoint with %s failed: %v", rtInfo.name, err)
			}

			if !mockRuntime.checkpointCalled {
				t.Errorf("Expected Checkpoint to be called for %s", rtInfo.name)
			}

			if checkpointPath == "" {
				t.Errorf("Expected non-empty checkpoint path for %s", rtInfo.name)
			}

			// Verify checkpoint directory was created
			if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
				t.Errorf("Checkpoint directory not created for %s", rtInfo.name)
			}
		})
	}
}
