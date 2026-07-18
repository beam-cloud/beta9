package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockRuntime is a mock implementation of runtime.Runtime for testing
type MockRuntime struct {
	name             string
	checkpointCalled bool
	checkpointOpts   *runtime.CheckpointOpts
	restoreCalled    bool
	restoreCalls     int
	checkpointCalls  int
	checkpointError  error
	checkpointErrors []error
	restoreError     error
	restoreErrors    []error
	restoreExitCode  int
	restoreOpts      *runtime.RestoreOpts
	deleteCalls      int
	killCalled       bool
	killSignal       syscall.Signal
	capabilities     runtime.Capabilities
	state            runtime.State
	stateErr         error
}

type staticCheckpointManager struct {
	path string
}

type restoringCheckpointManager struct{}

type observingCheckpointManager struct {
	create func() error
}

type trackingOOMWatcher struct {
	stopped bool
}

func (*trackingOOMWatcher) Watch(func()) error {
	return nil
}

func (w *trackingOOMWatcher) Stop() {
	w.stopped = true
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type trackingReadCloser struct {
	reader     io.Reader
	reachedEOF bool
	closed     bool
}

func (b *trackingReadCloser) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if err == io.EOF {
		b.reachedEOF = true
	}
	return n, err
}

func (b *trackingReadCloser) Close() error {
	b.closed = true
	return nil
}

type checkpointErrorWriter struct{}

func (checkpointErrorWriter) Write([]byte) (int, error) {
	return 0, errors.New("checkpoint cache write failed")
}

func TestRestoreOutputCaptureStopsBufferingButKeepsForwarding(t *testing.T) {
	var downstream bytes.Buffer
	capture := newRestoreOutputCapture(&downstream)
	if _, err := capture.Write([]byte("restore output")); err != nil {
		t.Fatal(err)
	}
	if got := capture.stop(); got != "restore output" {
		t.Fatalf("captured output = %q", got)
	}
	if _, err := capture.Write([]byte("container output")); err != nil {
		t.Fatal(err)
	}
	if got := capture.stop(); got != "restore output" {
		t.Fatalf("capture continued after stop: %q", got)
	}
	if got := downstream.String(); got != "restore outputcontainer output" {
		t.Fatalf("forwarded output = %q", got)
	}
}

func (m *staticCheckpointManager) Available() bool {
	return true
}

func (m *staticCheckpointManager) CreateCheckpoint(context.Context, runtime.Runtime, string, *types.ContainerRequest, bool) (string, error) {
	return m.path, nil
}

func (m *staticCheckpointManager) RestoreCheckpoint(context.Context, runtime.Runtime, *RestoreOpts) (int, error) {
	return -1, errors.New("not implemented")
}

func (*restoringCheckpointManager) Available() bool {
	return true
}

func (*restoringCheckpointManager) CreateCheckpoint(context.Context, runtime.Runtime, string, *types.ContainerRequest, bool) (string, error) {
	return "", errors.New("not implemented")
}

func (*restoringCheckpointManager) RestoreCheckpoint(_ context.Context, _ runtime.Runtime, opts *RestoreOpts) (int, error) {
	opts.started <- 4242
	return 0, nil
}

func (*observingCheckpointManager) Available() bool {
	return true
}

func (m *observingCheckpointManager) CreateCheckpoint(context.Context, runtime.Runtime, string, *types.ContainerRequest, bool) (string, error) {
	return "", m.create()
}

func (*observingCheckpointManager) RestoreCheckpoint(context.Context, runtime.Runtime, *RestoreOpts) (int, error) {
	return -1, errors.New("not implemented")
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
		"VLLM_HOST_IP=127.0.0.1",
		"VLLM_LOOPBACK_IP=127.0.0.1",
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
		slices.Contains(genericGPUEnv, "GLOO_SOCKET_IFNAME=lo") ||
		slices.Contains(genericGPUEnv, "VLLM_HOST_IP=127.0.0.1") ||
		slices.Contains(genericGPUEnv, "OMP_NUM_THREADS=1") {
		t.Fatalf("generic GPU checkpoint env should not include pod-service loopback overrides: %v", genericGPUEnv)
	}

	nonServiceEnv := applyCheckpointRuntimeEnvironmentOverrides(nil, &types.ContainerRequest{
		CheckpointEnabled: true,
		Gpu:               "RTX4090",
	}, []string{"vllm", "serve"})
	if slices.Contains(nonServiceEnv, "MASTER_ADDR=127.0.0.1") ||
		slices.Contains(nonServiceEnv, "NCCL_SOCKET_IFNAME=lo") ||
		slices.Contains(nonServiceEnv, "GLOO_SOCKET_IFNAME=lo") ||
		slices.Contains(nonServiceEnv, "VLLM_HOST_IP=127.0.0.1") {
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

func TestCheckpointHTTPReadinessClosesConnection(t *testing.T) {
	body := &trackingReadCloser{reader: strings.NewReader("ready")}
	requestClose := false
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestClose = request.Close
		return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
	})}

	if err := checkCheckpointHTTPReadyAt(context.Background(), client, "127.0.0.1:8000", "/ready"); err != nil {
		t.Fatalf("checkCheckpointHTTPReadyAt returned error: %v", err)
	}
	if !requestClose || !body.reachedEOF || !body.closed {
		t.Fatalf("readiness connection not fully closed: request_close=%t reached_eof=%t body_closed=%t", requestClose, body.reachedEOF, body.closed)
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
	worker := &Worker{stopContainerChan: make(chan stopContainerEvent, 1)}
	request := &types.ContainerRequest{ContainerId: "container", WorkspaceId: "workspace", StubId: "stub"}

	state, acquired := worker.acquireCheckpointCreateLock(request)
	if !acquired {
		t.Fatal("first checkpoint lock acquisition failed")
	}
	if _, acquired := worker.acquireCheckpointCreateLock(request); acquired {
		t.Fatal("second checkpoint lock acquisition succeeded")
	}
	worker.finishCheckpointCreate(request, state)
	if _, acquired := worker.acquireCheckpointCreateLock(request); !acquired {
		t.Fatal("checkpoint lock was not released")
	}
}

func checkpointStopInstance(request *types.ContainerRequest, reason types.StopContainerReason) *ContainerInstance {
	return &ContainerInstance{Request: request, StopReason: reason}
}

func TestAutomaticStopIsDeferredDuringCheckpoint(t *testing.T) {
	request := &types.ContainerRequest{ContainerId: "container", WorkspaceId: "workspace", StubId: "stub"}
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		stopContainerChan:  make(chan stopContainerEvent, 1),
	}
	worker.containerInstances.Set(request.ContainerId, &ContainerInstance{
		Id:         request.ContainerId,
		Request:    request,
		StopReason: types.StopContainerReasonScheduler,
	})
	state, acquired := worker.acquireCheckpointCreateLock(request)
	if !acquired {
		t.Fatal("checkpoint lock acquisition failed")
	}
	runcRuntime := NewMockRuntime(types.ContainerRuntimeRunc.String(), runtime.Capabilities{CheckpointRestore: true})
	if worker.awaitTerminalAutoCheckpointStop(context.Background(), request, runcRuntime, 0) {
		t.Fatal("checkpoint was terminal before a stop was deferred")
	}

	if err := worker.stopContainer(request.ContainerId, false); err != nil {
		t.Fatalf("defer scheduler stop: %v", err)
	}
	if err := worker.stopContainer(request.ContainerId, true); err != nil {
		t.Fatalf("coalesce scheduler kill: %v", err)
	}
	if worker.deferStopForCheckpoint(checkpointStopInstance(request, types.StopContainerReasonUser), false) {
		t.Fatal("user stop was deferred")
	}
	otherRequest := &types.ContainerRequest{
		ContainerId: "other-container",
		WorkspaceId: request.WorkspaceId,
		StubId:      request.StubId,
	}
	if worker.deferStopForCheckpoint(checkpointStopInstance(otherRequest, types.StopContainerReasonScheduler), false) {
		t.Fatal("stop for a different container was deferred")
	}
	if !worker.awaitTerminalAutoCheckpointStop(context.Background(), request, NewMockRuntime(types.ContainerRuntimeGvisor.String(), runtime.Capabilities{CheckpointRestore: true}), 0) {
		t.Fatal("terminal gvisor checkpoint was not detected")
	}
	if !worker.awaitTerminalAutoCheckpointStop(context.Background(), request, runcRuntime, 0) {
		t.Fatal("terminal runc checkpoint was not detected")
	}
	waitDone := make(chan bool, 1)
	go func() {
		waitDone <- worker.waitForTerminalAutoCheckpoint(context.Background(), request)
	}()
	select {
	case <-waitDone:
		t.Fatal("terminal checkpoint wait returned before completion")
	default:
	}

	worker.finishCheckpointCreate(request, state)
	if runtimeStopped := <-waitDone; runtimeStopped {
		t.Fatal("non-terminal checkpoint reported a stopped runtime")
	}
	select {
	case event := <-worker.stopContainerChan:
		if event.ContainerId != request.ContainerId || !event.Kill {
			t.Fatalf("unexpected deferred stop: %+v", event)
		}
	default:
		t.Fatal("deferred stop was not replayed")
	}
	if worker.deferStopForCheckpoint(checkpointStopInstance(request, types.StopContainerReasonScheduler), false) {
		t.Fatal("stop remained deferred after checkpoint completion")
	}
	if worker.awaitTerminalAutoCheckpointStop(context.Background(), request, runcRuntime, 0) {
		t.Fatal("checkpoint remained terminal after completion")
	}
}

func TestTerminalCheckpointDoesNotReplaySatisfiedStop(t *testing.T) {
	request := &types.ContainerRequest{ContainerId: "container", WorkspaceId: "workspace", StubId: "stub"}
	worker := &Worker{stopContainerChan: make(chan stopContainerEvent, 1)}
	state, acquired := worker.acquireCheckpointCreateLock(request)
	if !acquired {
		t.Fatal("checkpoint lock acquisition failed")
	}
	if !worker.deferStopForCheckpoint(checkpointStopInstance(request, types.StopContainerReasonScheduler), true) {
		t.Fatal("scheduler stop was not deferred")
	}
	if !worker.awaitTerminalAutoCheckpointStop(
		context.Background(),
		request,
		NewMockRuntime(types.ContainerRuntimeRunc.String(), runtime.Capabilities{CheckpointRestore: true}),
		0,
	) {
		t.Fatal("checkpoint was not marked terminal")
	}

	waitDone := make(chan bool, 1)
	go func() {
		waitDone <- worker.waitForTerminalAutoCheckpoint(context.Background(), request)
	}()
	time.Sleep(20 * time.Millisecond)
	worker.markTerminalCheckpointRuntimeStopped(request)
	worker.finishCheckpointCreate(request, state)
	if runtimeStopped := <-waitDone; !runtimeStopped {
		t.Fatal("terminal checkpoint did not report a stopped runtime")
	}

	select {
	case event := <-worker.stopContainerChan:
		t.Fatalf("terminal checkpoint replayed satisfied stop: %+v", event)
	default:
	}
}

func TestTerminalAutoCheckpointWaitsForScaleToZeroStop(t *testing.T) {
	request := &types.ContainerRequest{
		ContainerId: "container",
		WorkspaceId: "workspace",
		StubId:      "stub",
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type:   types.StubType(types.StubTypePodDeployment),
			Config: `{"keep_warm_seconds":0}`,
		}},
	}
	worker := &Worker{stopContainerChan: make(chan stopContainerEvent, 1)}
	state, acquired := worker.acquireCheckpointCreateLock(request)
	if !acquired {
		t.Fatal("checkpoint lock acquisition failed")
	}
	defer worker.finishCheckpointCreate(request, state)

	result := make(chan bool, 1)
	go func() {
		result <- worker.awaitTerminalAutoCheckpointStop(
			context.Background(),
			request,
			NewMockRuntime(types.ContainerRuntimeRunc.String(), runtime.Capabilities{CheckpointRestore: true}),
			time.Second,
		)
	}()
	select {
	case <-result:
		t.Fatal("terminal checkpoint wait returned before the stop arrived")
	case <-time.After(20 * time.Millisecond):
	}

	if !worker.deferStopForCheckpoint(checkpointStopInstance(request, types.StopContainerReasonScheduler), true) {
		t.Fatal("scheduler stop was not deferred")
	}
	select {
	case terminal := <-result:
		if !terminal {
			t.Fatal("checkpoint did not become terminal after deferred stop")
		}
	case <-time.After(time.Second):
		t.Fatal("terminal checkpoint wait did not observe deferred stop")
	}
}

func TestTerminalAutoCheckpointDoesNotWaitForEndpoint(t *testing.T) {
	request := &types.ContainerRequest{
		ContainerId: "container",
		WorkspaceId: "workspace",
		StubId:      "stub",
		Stub: types.StubWithRelated{Stub: types.Stub{
			Type:   types.StubType(types.StubTypeEndpointDeployment),
			Config: `{"keep_warm_seconds":0}`,
		}},
	}
	worker := &Worker{}
	state, acquired := worker.acquireCheckpointCreateLock(request)
	if !acquired {
		t.Fatal("checkpoint lock acquisition failed")
	}
	defer worker.finishCheckpointCreate(request, state)

	started := time.Now()
	if worker.awaitTerminalAutoCheckpointStop(
		context.Background(),
		request,
		NewMockRuntime(types.ContainerRuntimeRunc.String(), runtime.Capabilities{CheckpointRestore: true}),
		time.Second,
	) {
		t.Fatal("endpoint checkpoint was marked terminal without a deferred stop")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("endpoint checkpoint unexpectedly waited for a stop: %s", elapsed)
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
	m.deleteCalls++
	return nil
}

func (m *MockRuntime) State(ctx context.Context, containerID string) (runtime.State, error) {
	return m.state, m.stateErr
}

func (m *MockRuntime) Events(ctx context.Context, containerID string) (<-chan runtime.Event, error) {
	ch := make(chan runtime.Event)
	close(ch)
	return ch, nil
}

func (m *MockRuntime) Checkpoint(ctx context.Context, containerID string, opts *runtime.CheckpointOpts) error {
	m.checkpointCalled = true
	m.checkpointCalls++
	m.checkpointOpts = opts
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
	m.restoreCalls++
	m.restoreOpts = opts
	if len(m.restoreErrors) > 0 {
		err := m.restoreErrors[0]
		m.restoreErrors = m.restoreErrors[1:]
		if err != nil {
			return m.restoreExitCode, err
		}
	}
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
	m.checkpointOpts = nil
	m.restoreCalled = false
	m.restoreCalls = 0
	m.restoreOpts = nil
	m.deleteCalls = 0
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

					checkpointPath, err := manager.CreateCheckpoint(context.Background(), mockRuntime, "cuda-checkpoint", request, false)
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

				checkpointPath, err := manager.CreateCheckpoint(context.Background(), mockRuntime, "checkpoint-1", request, false)
				if err != nil {
					t.Errorf("CreateCheckpoint failed: %v", err)
				}

				if !mockRuntime.checkpointCalled {
					t.Error("Expected Checkpoint to be called on runtime")
				}

				if checkpointPath == "" {
					t.Error("Expected non-empty checkpoint path")
				}
				if mockRuntime.checkpointOpts == nil || !mockRuntime.checkpointOpts.LeaveRunning {
					t.Fatalf("unexpected hot checkpoint options: %+v", mockRuntime.checkpointOpts)
				}
			})

			t.Run("CreateCheckpoint terminal state", func(t *testing.T) {
				mockRuntime.Reset()
				request := &types.ContainerRequest{
					ContainerId: fmt.Sprintf("%s-container-terminal", tc.runtimeName),
				}

				_, err := manager.CreateCheckpoint(context.Background(), mockRuntime, "checkpoint-terminal-"+tc.runtimeName, request, true)
				if err != nil {
					t.Fatalf("CreateCheckpoint failed: %v", err)
				}
				wantTerminate := tc.runtimeName == types.ContainerRuntimeRunc.String() ||
					tc.runtimeName == types.ContainerRuntimeGvisor.String()
				if mockRuntime.checkpointOpts == nil || mockRuntime.checkpointOpts.LeaveRunning == wantTerminate {
					t.Fatalf("unexpected terminal checkpoint options for %s: %+v", tc.runtimeName, mockRuntime.checkpointOpts)
				}
			})

			t.Run("RestoreCheckpoint endpoint closes TCP", func(t *testing.T) {
				request := &types.ContainerRequest{
					ContainerId: fmt.Sprintf("%s-container-2", tc.runtimeName),
					Stub: types.StubWithRelated{Stub: types.Stub{
						Type: types.StubType(types.StubTypeEndpointDeployment),
					}},
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
				mapPath := filepath.Join(types.AgentTmpPath, checkpoint.CheckpointId, request.ContainerId, checkpointNetworkMapFile)
				if _, err := os.Stat(mapPath); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("Expected endpoint restore not to create a network map, got %v", err)
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
					ContainerIp:  "192.168.0.46",
				}

				checkpointPath := filepath.Join(tmpDir, checkpoint.CheckpointId)
				os.MkdirAll(checkpointPath, 0755)

				opts := &RestoreOpts{
					request:     request,
					checkpoint:  checkpoint,
					containerIP: "192.168.0.73",
					configPath:  filepath.Join(tmpDir, "pod-config.json"),
					started:     make(chan int, 1),
				}

				exitCode, err := manager.RestoreCheckpoint(context.Background(), mockRuntime, opts)
				if err != nil {
					t.Errorf("RestoreCheckpoint failed: %v", err)
				}

				if mockRuntime.restoreOpts == nil || !mockRuntime.restoreOpts.AllowOpenTCP || mockRuntime.restoreOpts.TCPClose {
					t.Errorf("Expected pod NVIDIA restore to preserve open TCP without tcp-close, got %+v", mockRuntime.restoreOpts)
				}
				mapPath := filepath.Join(types.AgentTmpPath, checkpoint.CheckpointId, request.ContainerId, checkpointNetworkMapFile)
				contents, err := os.ReadFile(mapPath)
				if err != nil {
					t.Fatalf("Expected pod restore network map: %v", err)
				}
				if got, want := string(contents), "v1 192.168.0.46 192.168.0.73 fd00:abcd::2e fd00:abcd::49\n"; got != want {
					t.Errorf("network map = %q, want %q", got, want)
				}
				t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(filepath.Dir(mapPath))) })

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
					ContainerIp:  "192.168.0.46",
				}

				checkpointPath := filepath.Join(tmpDir, checkpoint.CheckpointId)
				os.MkdirAll(checkpointPath, 0755)

				opts := &RestoreOpts{
					request:     request,
					checkpoint:  checkpoint,
					containerIP: "192.168.0.74",
					configPath:  filepath.Join(tmpDir, "service-config.json"),
					started:     make(chan int, 1),
				}

				exitCode, err := manager.RestoreCheckpoint(context.Background(), mockRuntime, opts)
				if err != nil {
					t.Errorf("RestoreCheckpoint failed: %v", err)
				}

				if mockRuntime.restoreOpts == nil || !mockRuntime.restoreOpts.AllowOpenTCP || mockRuntime.restoreOpts.TCPClose {
					t.Errorf("Expected service NVIDIA restore to preserve open TCP without tcp-close, got %+v", mockRuntime.restoreOpts)
				}
				mapPath := filepath.Join(types.AgentTmpPath, checkpoint.CheckpointId, request.ContainerId, checkpointNetworkMapFile)
				if _, err := os.Stat(mapPath); err != nil {
					t.Errorf("Expected service restore network map: %v", err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(filepath.Dir(mapPath))) })

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

func TestWriteCheckpointNetworkMapRejectsInvalidAddresses(t *testing.T) {
	err := writeCheckpointNetworkMap(t.TempDir(), "", "192.168.0.2")
	if err == nil {
		t.Fatal("expected invalid checkpoint address to fail")
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

		_, err := manager.CreateCheckpoint(context.Background(), mockRuntime, "failing-checkpoint", request, false)
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
		if mockRuntime.restoreCalls != 1 || mockRuntime.deleteCalls != 0 {
			t.Errorf("generic restore failure retried: restores=%d cleanups=%d", mockRuntime.restoreCalls, mockRuntime.deleteCalls)
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
		if mockRuntime.restoreCalls != nvidiaRestoreAttempts || mockRuntime.deleteCalls != nvidiaRestoreAttempts-1 {
			t.Errorf("CRIU restore attempts=%d cleanups=%d", mockRuntime.restoreCalls, mockRuntime.deleteCalls)
		}
	})
}

func TestNvidiaRestoreRetriesCRIUFailure(t *testing.T) {
	tmpDir := t.TempDir()
	checkpoint := &types.Checkpoint{CheckpointId: "checkpoint-retry"}
	if err := os.MkdirAll(filepath.Join(tmpDir, checkpoint.CheckpointId), 0755); err != nil {
		t.Fatal(err)
	}
	rt := NewMockRuntime(types.ContainerRuntimeRunc.String(), runtime.Capabilities{CheckpointRestore: true})
	rt.restoreErrors = []error{
		&ErrCRIURestoreFailed{Stderr: "criu failed: type RESTORE"},
		nil,
	}
	manager := &NvidiaCRIUManager{checkpointRoot: tmpDir}

	exitCode, err := manager.RestoreCheckpoint(context.Background(), rt, &RestoreOpts{
		request:      &types.ContainerRequest{ContainerId: "container-retry"},
		checkpoint:   checkpoint,
		configPath:   filepath.Join(tmpDir, "config.json"),
		started:      make(chan int, 1),
		outputWriter: io.Discard,
	})
	if err != nil {
		t.Fatalf("RestoreCheckpoint failed: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
	if rt.restoreCalls != 2 {
		t.Fatalf("restore calls = %d, want 2", rt.restoreCalls)
	}
	if rt.deleteCalls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", rt.deleteCalls)
	}
}

func TestNvidiaRestorePublishesStartedAfterValidation(t *testing.T) {
	tmpDir := t.TempDir()
	checkpoint := &types.Checkpoint{CheckpointId: "checkpoint-validate"}
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, checkpoint.CheckpointId), 0755))
	rt := NewMockRuntime(types.ContainerRuntimeRunc.String(), runtime.Capabilities{CheckpointRestore: true})
	manager := &NvidiaCRIUManager{checkpointRoot: tmpDir}
	started := make(chan int, 1)
	validationStarted := make(chan struct{})
	releaseValidation := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		_, err := manager.RestoreCheckpoint(context.Background(), rt, &RestoreOpts{
			request:      &types.ContainerRequest{ContainerId: "container-validate"},
			checkpoint:   checkpoint,
			configPath:   filepath.Join(tmpDir, "config.json"),
			started:      started,
			outputWriter: io.Discard,
			validate: func(context.Context, runtime.Runtime) error {
				close(validationStarted)
				<-releaseValidation
				return nil
			},
		})
		done <- err
	}()

	<-validationStarted
	select {
	case pid := <-started:
		t.Fatalf("restore published pid %d before validation", pid)
	default:
	}
	close(releaseValidation)
	require.Equal(t, 12345, <-started)
	require.NoError(t, <-done)
}

func TestRestoredCheckpointHTTPReadinessRequiresStableRuntime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	address := strings.TrimPrefix(server.URL, "http://")
	containerID := "container-restore-ready"
	worker := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:                  containerID,
		ContainerAddressMap: map[int32]string{8000: address},
	})
	request := &types.ContainerRequest{
		ContainerId: containerID,
		CheckpointTrigger: &types.CheckpointTrigger{
			Type:           checkpointTriggerHTTP,
			HttpPath:       "/ready",
			HttpPort:       8000,
			TimeoutSeconds: 2,
		},
	}
	rt := NewMockRuntime(types.ContainerRuntimeRunc.String(), runtime.Capabilities{})
	rt.state = runtime.State{Status: types.RuncContainerStatusRunning}

	started := time.Now()
	require.NoError(t, worker.waitForRestoredCheckpointHTTPReadiness(context.Background(), request, request.CheckpointTrigger, rt))
	require.GreaterOrEqual(t, time.Since(started), restoreReadinessStableFor)
}

func TestClassifyRestoreErrorDetectsHostIncompatibility(t *testing.T) {
	err := classifyRestoreError(
		types.ContainerRuntimeRunc.String(),
		assert.AnError,
		"criu failed: type RESTORE: CPU instruction capabilities do not match run time",
	)

	require.True(t, IsCheckpointHostIncompatible(err))
	require.False(t, IsCRIURestoreError(err))
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

func TestStopOOMWatcherForTerminalCheckpoint(t *testing.T) {
	containerID := "container-terminal-checkpoint"
	watcher := &trackingOOMWatcher{}
	worker := &Worker{containerInstances: common.NewSafeMap[*ContainerInstance]()}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:         containerID,
		OOMWatcher: watcher,
	})

	worker.stopOOMWatcherForTerminalCheckpoint(containerID)

	require.True(t, watcher.stopped)
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

func TestCreateCheckpointMarksFilesystemCopyFailure(t *testing.T) {
	root := t.TempDir()
	containerID := "container-copy-failure"
	request := &types.ContainerRequest{
		ContainerId: containerID,
		Stub: types.StubWithRelated{Stub: types.Stub{
			ExternalId: "stub-copy-failure",
		}},
	}
	checkpointPath := filepath.Join(root, "checkpoints", "checkpoint-copy-failure")
	backendRepoClient := &fakeBackendRepoClient{}
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		backendRepoClient:  backendRepoClient,
		criuManager:        &staticCheckpointManager{path: checkpointPath},
	}
	worker.containerInstances.Set(containerID, &ContainerInstance{
		Id:      containerID,
		Runtime: NewMockRuntime("runc", runtime.Capabilities{CheckpointRestore: true}),
		Overlay: common.NewContainerOverlay(
			request,
			filepath.Join(root, "overlay", "merged"),
			filepath.Join(root, "overlay-storage"),
		),
	})

	err := worker.createCheckpoint(context.Background(), &CreateCheckpointOpts{
		Request:      request,
		CheckpointId: "checkpoint-copy-failure",
	})
	if err == nil || !strings.Contains(err.Error(), "read source directory") {
		t.Fatalf("createCheckpoint error = %v, want filesystem copy error", err)
	}
	if backendRepoClient.createCalls != 1 {
		t.Fatalf("CreateCheckpoint calls = %d, want 1", backendRepoClient.createCalls)
	}
	if got := backendRepoClient.lastCreate.Status; got != string(types.CheckpointStatusCheckpointFailed) {
		t.Fatalf("checkpoint status = %q, want %q", got, types.CheckpointStatusCheckpointFailed)
	}
}

func TestMarkCheckpointFailedRetainsPersistedMetadata(t *testing.T) {
	backendRepoClient := &fakeBackendRepoClient{}
	worker := &Worker{backendRepoClient: backendRepoClient}
	metadata := &checkpointCacheMetadata{
		hash:        "checkpoint-hash",
		sizeBytes:   42,
		originKey:   "checkpoints/checkpoint-a.tar",
		locality:    "locality-a",
		accelerator: "A10G",
	}

	worker.markCheckpointFailed(&CreateCheckpointOpts{
		Request: &types.ContainerRequest{
			ContainerId: "container-a",
			Stub:        types.StubWithRelated{Stub: types.Stub{ExternalId: "stub-a"}},
		},
		CheckpointId: "checkpoint-a",
	}, metadata)

	got := backendRepoClient.lastCreate
	if got == nil || got.Status != string(types.CheckpointStatusCheckpointFailed) {
		t.Fatalf("checkpoint state = %+v, want failed", got)
	}
	if got.CacheHash != metadata.hash || got.CacheSizeBytes != metadata.sizeBytes || got.OriginKey != metadata.originKey || got.Locality != metadata.locality || got.Accelerator != metadata.accelerator {
		t.Fatalf("checkpoint metadata = %+v, want %+v", got, metadata)
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
	}, false)

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
	}, false)

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

func TestMaterializeCheckpointReaderVerifiesAndPublishes(t *testing.T) {
	root := t.TempDir()
	checkpointID := "checkpoint-stream"
	sourcePath := filepath.Join(root, "source", checkpointID)
	if err := os.MkdirAll(filepath.Join(sourcePath, checkpointFsDir), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, checkpointFsDir, "state.txt"), []byte("filesystem"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, "inventory.img"), []byte("runtime"), 0644); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(root, checkpointID+checkpointArchiveExtension)
	if err := createTar(sourcePath, archivePath); err != nil {
		t.Fatal(err)
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	hash, size, err := fileSHA256(archivePath)
	if err != nil {
		t.Fatal(err)
	}

	checkpointPath := filepath.Join(root, "materialized", checkpointID)
	var archiveCopy bytes.Buffer
	if err := materializeCheckpointReader(context.Background(), bytes.NewReader(archive), hash, size, checkpointPath, checkpointID, &archiveCopy); err != nil {
		t.Fatal(err)
	}
	if !checkpointMaterialized(checkpointPath) {
		t.Fatal("expected verified checkpoint to be published")
	}
	if !bytes.Equal(archive, archiveCopy.Bytes()) {
		t.Fatal("archive copy does not match streamed checkpoint")
	}

	bestEffortWriter := &bestEffortCheckpointCacheWriter{writer: checkpointErrorWriter{}}
	checkpointPath = filepath.Join(root, "materialized-best-effort", checkpointID)
	if err := materializeCheckpointReader(context.Background(), bytes.NewReader(archive), hash, size, checkpointPath, checkpointID, bestEffortWriter); err != nil {
		t.Fatalf("best-effort cache write blocked materialization: %v", err)
	}
	if bestEffortWriter.err == nil {
		t.Fatal("expected best-effort cache writer to retain its error")
	}
	if !checkpointMaterialized(checkpointPath) {
		t.Fatal("expected checkpoint to publish despite cache staging failure")
	}
}

func TestMaterializeCheckpointReaderRejectsHashMismatch(t *testing.T) {
	root := t.TempDir()
	checkpointID := "checkpoint-corrupt"
	sourcePath := filepath.Join(root, "source", checkpointID)
	if err := os.MkdirAll(filepath.Join(sourcePath, checkpointFsDir), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, checkpointFsDir, "state.txt"), []byte("filesystem"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, "inventory.img"), []byte("runtime"), 0644); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(root, checkpointID+checkpointArchiveExtension)
	if err := createTar(sourcePath, archivePath); err != nil {
		t.Fatal(err)
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	checkpointPath := filepath.Join(root, "materialized", checkpointID)
	err = materializeCheckpointReader(context.Background(), bytes.NewReader(archive), strings.Repeat("0", 64), int64(len(archive)), checkpointPath, checkpointID, nil)
	if err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("expected hash mismatch, got %v", err)
	}
	if _, err := os.Stat(checkpointPath); !os.IsNotExist(err) {
		t.Fatalf("unverified checkpoint was published: %v", err)
	}
}

func TestCheckpointCacheReaderBuffersReadAtCalls(t *testing.T) {
	content := make([]byte, 2*checkpointCacheReadBufferSize+37)
	for i := range content {
		content[i] = byte(i % 251)
	}
	var offsets []int64
	reader := newCheckpointCacheReader(context.Background(), "hash", int64(len(content)), func(_ context.Context, _ string, offset int64, dst []byte) (int64, error) {
		offsets = append(offsets, offset)
		return int64(copy(dst, content[offset:])), nil
	})
	actual, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, content) {
		t.Fatal("buffered cache reader returned different content")
	}
	wantOffsets := []int64{0, checkpointCacheReadBufferSize, 2 * checkpointCacheReadBufferSize}
	if !slices.Equal(offsets, wantOffsets) {
		t.Fatalf("read offsets = %v, want %v", offsets, wantOffsets)
	}
}

func TestCheckpointCacheReaderRejectsShortRead(t *testing.T) {
	reader := newCheckpointCacheReader(context.Background(), "hash", 128, func(_ context.Context, _ string, _ int64, dst []byte) (int64, error) {
		return int64(len(dst) - 1), nil
	})
	_, err := reader.Read(make([]byte, 1))
	if err == nil || !strings.Contains(err.Error(), "short checkpoint cache read") {
		t.Fatalf("expected short read error, got %v", err)
	}
}

func TestCheckpointUploadReaderPreservesSeekableProgress(t *testing.T) {
	content := []byte("checkpoint upload payload")
	filePath := filepath.Join(t.TempDir(), "checkpoint.tar")
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var progress int64
	reader := &checkpointUploadReader{
		file:     file,
		total:    int64(len(content)),
		progress: func(completed int64) { progress = completed },
	}

	section, err := io.ReadAll(io.NewSectionReader(reader, 4, 10))
	if err != nil {
		t.Fatal(err)
	}
	if string(section) != string(content[4:14]) {
		t.Fatalf("section = %q, want %q", section, content[4:14])
	}
	if progress != 10 {
		t.Fatalf("progress = %d, want 10", progress)
	}

	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(reader, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != string(content[:4]) {
		t.Fatalf("prefix = %q, want %q", buf, content[:4])
	}
	if progress != 14 {
		t.Fatalf("progress = %d, want 14", progress)
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
			checkpointPath, err := manager.CreateCheckpoint(context.Background(), mockRuntime, checkpointID, request, false)
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
