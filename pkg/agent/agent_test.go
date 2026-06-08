package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
)

func TestRouteProxySingleListenerRoutesByPreface(t *testing.T) {
	backend := startEchoListener(t, "127.0.0.1:0")
	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxyListener.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	proxy := &routeProxy{
		listener: proxyListener,
		routes: map[string]string{
			"route-one": backend.Addr().String(),
		},
	}

	go func() {
		_ = proxy.accept(ctx)
	}()

	conn, err := net.DialTimeout("tcp", proxyListener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := fmt.Fprintf(conn, "%sroute-one\n", network.BackendRoutePreface); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("expected ping echo, got %q", string(buf))
	}
}

func TestRouteProxyMarksRouteReadyForReachableLocalTarget(t *testing.T) {
	backend := startEchoListener(t, "127.0.0.1:0")
	target := backend.Addr().String()
	updates := make(chan *pb.UpdateAgentRouteStatusRequest, 1)
	client := &routeStatusClient{updates: updates}
	proxy := newRouteProxy(client, "agent-token", nil, "agent.tailnet:29443", nil, io.Discard, io.Discard)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	proxy.setRoute("route-one", target)
	proxy.ensureRouteReady(ctx, "route-one", target)

	select {
	case update := <-updates:
		if update.RouteId != "route-one" {
			t.Fatalf("route id = %q, want route-one", update.RouteId)
		}
		if update.State != types.BackendRouteStateReady {
			t.Fatalf("route state = %q, want ready", update.State)
		}
		if update.ProxyTarget != "agent.tailnet:29443" {
			t.Fatalf("proxy target = %q, want agent.tailnet:29443", update.ProxyTarget)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("route was not marked ready for reachable local target")
	}
}

func TestDialLocalTargetFallsBackToLoopback(t *testing.T) {
	backend := startEchoListener(t, "127.0.0.1:0")
	_, port, err := net.SplitHostPort(backend.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	conn, err := dialLocalTargetWithTimeout(net.JoinHostPort("127.0.0.2", port), 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
}

type routeStatusClient struct {
	pb.GatewayServiceClient
	updates chan *pb.UpdateAgentRouteStatusRequest
}

func (c *routeStatusClient) UpdateAgentRouteStatus(ctx context.Context, in *pb.UpdateAgentRouteStatusRequest, _ ...grpc.CallOption) (*pb.UpdateAgentRouteStatusResponse, error) {
	select {
	case c.updates <- in:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &pb.UpdateAgentRouteStatusResponse{Ok: true}, nil
}

func TestNormalizeBootstrapForAgentRuntimeUsesReachableGatewayHost(t *testing.T) {
	t.Setenv(types.AgentInContainerEnv, "1")

	got := normalizeBootstrapForAgentRuntime("http://host.docker.internal:1994", bootstrapConfig{
		GatewayHTTPURL:  "http://localhost:1994",
		GatewayGRPCHost: "beta9-gateway",
		GatewayGRPCPort: 1993,
	})

	if got.GatewayHTTPURL != "http://host.docker.internal:1994" {
		t.Fatalf("expected docker-reachable http url, got %q", got.GatewayHTTPURL)
	}
	if got.GatewayGRPCHost != "host.docker.internal" {
		t.Fatalf("expected docker-reachable grpc host, got %q", got.GatewayGRPCHost)
	}
}

func TestNormalizeBootstrapForAgentRuntimePreservesPublicGRPCHost(t *testing.T) {
	t.Setenv(types.AgentInContainerEnv, "1")

	got := normalizeBootstrapForAgentRuntime("https://app.stage.beam.cloud", bootstrapConfig{
		GatewayHTTPURL:  "https://app.stage.beam.cloud",
		GatewayGRPCHost: "gateway.stage.beam.cloud",
		GatewayGRPCPort: 443,
		GatewayGRPCTLS:  true,
	})

	if got.GatewayHTTPURL != "https://app.stage.beam.cloud" {
		t.Fatalf("expected public http url to be preserved, got %q", got.GatewayHTTPURL)
	}
	if got.GatewayGRPCHost != "gateway.stage.beam.cloud" {
		t.Fatalf("expected public grpc host to be preserved, got %q", got.GatewayGRPCHost)
	}
}

func TestNormalizeBootstrapForAgentRuntimeKeepsEmptyPublicGRPCHost(t *testing.T) {
	t.Setenv(types.AgentInContainerEnv, "1")

	got := normalizeBootstrapForAgentRuntime("https://app.stage.beam.cloud", bootstrapConfig{
		GatewayHTTPURL:  "https://app.stage.beam.cloud",
		GatewayGRPCPort: 443,
		GatewayGRPCTLS:  true,
	})

	if got.GatewayGRPCHost != "" {
		t.Fatalf("expected empty public grpc host to remain unset, got %q", got.GatewayGRPCHost)
	}
}

func TestGatewayGRPCAddrDoesNotInferPublicGRPCHost(t *testing.T) {
	_, err := gatewayGRPCAddr("https://app.stage.beam.cloud", "", 443)
	if err == nil {
		t.Fatal("expected missing public grpc host to fail")
	}
	if !strings.Contains(err.Error(), "gateway.grpc.externalHost") {
		t.Fatalf("expected config-specific error, got %v", err)
	}
}

func TestGatewayGRPCAddrInfersLoopbackGRPCHost(t *testing.T) {
	addr, err := gatewayGRPCAddr("http://localhost:1994", "", 1993)
	if err != nil {
		t.Fatal(err)
	}
	if addr != "localhost:1993" {
		t.Fatalf("addr = %q, want localhost:1993", addr)
	}
}

func TestDockerRunArgsUsesConfigurableRouteTargetHost(t *testing.T) {
	t.Setenv(types.AgentTargetHostEnv, "host.docker.internal")
	t.Setenv(types.AgentDockerHostsEnv, "registry.localhost:127.0.0.1,localstack:host-gateway")
	t.Setenv(types.AgentWorkerPlatformEnv, "linux/amd64")

	args := dockerRunArgs("slot-one", "worker:dev", "sha256:worker-image", "/tmp/config.json", bootstrapConfig{
		GatewayHTTPURL:  "http://host.docker.internal:1994",
		GatewayGRPCHost: "host.docker.internal",
		GatewayGRPCPort: 1993,
	}, &pb.AgentWorkerSlot{
		WorkerId:                  "worker-one",
		WorkerToken:               "token",
		PoolName:                  "private",
		MachineId:                 "machine",
		Memory:                    512,
		GpuCount:                  2,
		GpuAssignment:             "0,1",
		NetworkPrefix:             "10.0.0.0/24",
		NetworkSlotPoolSize:       64,
		ContainerStartConcurrency: 12,
	}, agentWorkerDirs("/tmp/agent-state", "worker-one"))

	if !containsArg(args, "-e", types.WorkerRouteTargetEnv+"=host.docker.internal") {
		t.Fatalf("expected route target host env in docker args: %#v", args)
	}
	if !containsArg(args, "--platform", "linux/amd64") {
		t.Fatalf("expected worker platform in docker args: %#v", args)
	}
	for _, want := range []string{
		types.CacheLocalityEnv + "=private",
		types.CacheNodeEnv + "=machine",
		types.CacheHostNetworkEnv + "=true",
		types.AgentGatewayURLEnv + "=http://host.docker.internal:1994",
		types.ContainerGatewayGRPCHostEnv + "=host.docker.internal",
		types.ContainerGatewayGRPCPortEnv + "=1993",
		types.ContainerGatewayHTTPHostEnv + "=host.docker.internal",
		types.ContainerGatewayHTTPPortEnv + "=1994",
		types.NvidiaVisibleDevicesEnv + "=0,1",
		types.WorkerStartConcurrencyEnv + "=12",
		types.WorkerNetworkSlotsEnv + "=64",
	} {
		if !containsArg(args, "-e", want) {
			t.Fatalf("expected %s env in docker args: %#v", want, args)
		}
	}
	for _, want := range []string{
		"/tmp/agent-state/images:" + types.AgentImagesPath,
		"/tmp/agent-state/data:" + types.AgentDataPath,
		"/tmp/agent-state/workspace-data:" + types.AgentWorkspacePath,
		"/tmp/agent-state/cache:" + types.AgentCachePath,
		"/tmp/agent-state/checkpoints:" + types.AgentCheckpointPath,
	} {
		if !containsArg(args, "-v", want) {
			t.Fatalf("expected %s volume in docker args: %#v", want, args)
		}
	}
	if !containsArg(args, "--shm-size", "256m") {
		t.Fatalf("expected shm size to track worker memory: %#v", args)
	}
	if !containsArg(args, "--gpus", "device=0,1") {
		t.Fatalf("expected GPU device assignment: %#v", args)
	}
	for _, want := range []string{
		types.AgentDockerLabelManaged + "=true",
		types.AgentDockerLabelWorkerID + "=worker-one",
		types.AgentDockerLabelMachineID + "=machine",
		types.AgentDockerLabelPoolName + "=private",
		types.AgentDockerLabelWorkerImageID + "=sha256:worker-image",
	} {
		if !containsArg(args, "--label", want) {
			t.Fatalf("expected %s docker label in args: %#v", want, args)
		}
	}
	for _, want := range []string{
		"registry.localhost:127.0.0.1",
		"localstack:host-gateway",
	} {
		if !containsArg(args, "--add-host", want) {
			t.Fatalf("expected %s host alias in docker args: %#v", want, args)
		}
	}
}

func TestDockerContainerInspectOwnedByAgentAcceptsLabelsAndLegacyEnv(t *testing.T) {
	slot := &pb.AgentWorkerSlot{
		WorkerId:  "worker-one",
		MachineId: "machine-one",
		PoolName:  "private-dev",
	}

	tests := []struct {
		name string
		data string
		want bool
	}{
		{
			name: "current labels",
			data: `{"Config":{"Labels":{"dev.beam.agent.worker":"true","dev.beam.agent.worker_id":"worker-one","dev.beam.agent.machine_id":"machine-one","dev.beam.agent.pool_name":"private-dev"}}}`,
			want: true,
		},
		{
			name: "legacy env",
			data: `{"Config":{"Labels":{},"Env":["WORKER_ID=worker-one","WORKER_MACHINE_ID=machine-one","WORKER_POOL_NAME=private-dev"]}}`,
			want: true,
		},
		{
			name: "wrong worker",
			data: `{"Config":{"Labels":{"dev.beam.agent.worker":"true","dev.beam.agent.worker_id":"worker-two","dev.beam.agent.machine_id":"machine-one","dev.beam.agent.pool_name":"private-dev"}}}`,
			want: false,
		},
		{
			name: "unrelated container",
			data: `{"Config":{"Labels":{},"Env":["WORKER_ID=worker-one","WORKER_MACHINE_ID=other","WORKER_POOL_NAME=private-dev"]}}`,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dockerContainerInspectOwnedByAgent([]byte(tt.data), slot)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("owned = %v, want %v", got, tt.want)
			}
		})
	}

	got, err := dockerContainerInspectOwnedByAgent([]byte(`{"Config":{"Labels":{"dev.beam.agent.worker":"true","dev.beam.agent.worker_id":"worker-one","dev.beam.agent.machine_id":"machine-one","dev.beam.agent.pool_name":"private-dev"}}}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Fatal("nil slot must not own a managed container")
	}
}

func TestShouldRemoveManagedWorkerContainer(t *testing.T) {
	slot := &pb.AgentWorkerSlot{
		WorkerId:  "worker-one",
		MachineId: "machine-one",
		PoolName:  "private-dev",
	}

	tests := []struct {
		name       string
		inspect    *dockerContainerInspect
		desired    string
		wantRemove bool
	}{
		{
			name: "keep desired worker",
			inspect: dockerInspectWithLabels("/beam-agent-worker-one", map[string]string{
				types.AgentDockerLabelManaged:   "true",
				types.AgentDockerLabelWorkerID:  "worker-one",
				types.AgentDockerLabelMachineID: "machine-one",
				types.AgentDockerLabelPoolName:  "private-dev",
			}),
			desired: "beam-agent-worker-one",
		},
		{
			name: "remove old worker",
			inspect: dockerInspectWithLabels("/beam-agent-worker-two", map[string]string{
				types.AgentDockerLabelManaged:   "true",
				types.AgentDockerLabelWorkerID:  "worker-two",
				types.AgentDockerLabelMachineID: "machine-old",
				types.AgentDockerLabelPoolName:  "old-pool",
			}),
			desired:    "beam-agent-worker-one",
			wantRemove: true,
		},
		{
			name: "remove duplicate with unexpected name",
			inspect: dockerInspectWithLabels("/duplicate-worker", map[string]string{
				types.AgentDockerLabelManaged:   "true",
				types.AgentDockerLabelWorkerID:  "worker-one",
				types.AgentDockerLabelMachineID: "machine-one",
				types.AgentDockerLabelPoolName:  "private-dev",
			}),
			desired:    "beam-agent-worker-one",
			wantRemove: true,
		},
		{
			name:    "ignore unrelated container",
			inspect: dockerInspectWithLabels("/postgres", nil),
			desired: "beam-agent-worker-one",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldRemoveManagedWorkerContainer(tt.inspect, tt.desired, slot)
			if got != tt.wantRemove {
				t.Fatalf("remove = %v, want %v", got, tt.wantRemove)
			}
		})
	}
}

func dockerInspectWithLabels(name string, labels map[string]string) *dockerContainerInspect {
	if labels == nil {
		labels = map[string]string{}
	}
	inspect := &dockerContainerInspect{Name: name}
	inspect.Config.Labels = labels
	return inspect
}

func TestWorkerImagePullKeyIncludesPlatform(t *testing.T) {
	t.Setenv(types.AgentWorkerPlatformEnv, "linux/amd64")
	if got := workerImagePullKey("registry.example.com/worker:latest"); got != "linux/amd64 registry.example.com/worker:latest" {
		t.Fatalf("pull key = %q", got)
	}
}

func TestDetailLogWriterReportsUnconsumedBytesOnFlushError(t *testing.T) {
	writer := newDetailLogWriter(errorWriter{})
	n, err := writer.Write([]byte("hello\nworld"))
	if err == nil {
		t.Fatal("expected flush error")
	}
	if n != 0 {
		t.Fatalf("written = %d, want 0", n)
	}
}

func TestResolveAgentIdentityDoesNotFallbackWhenJoinRejected(t *testing.T) {
	t.Setenv(types.AgentStateDirEnv, t.TempDir())
	if err := saveRuntimeState("http://gateway.local", &joinResponse{
		Ok:          true,
		WorkspaceID: "workspace-one",
		PoolName:    "pool-one",
		MachineID:   "machine-one",
		AgentToken:  "saved-token",
	}); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(joinResponse{Ok: false, ErrMsg: "join token revoked"})
	}))
	t.Cleanup(server.Close)

	_, err := resolveAgentIdentity(context.Background(), NewClient(server.URL), types.AgentJoinOptions{
		GatewayURL: server.URL,
		JoinToken:  "bad-token",
		DevMode:    true,
		Stdout:     io.Discard,
		Stderr:     io.Discard,
	})
	if err == nil {
		t.Fatal("expected rejected join token error")
	}
	if !strings.Contains(err.Error(), "join token revoked") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTelemetryTeeCloseFlushesBufferedLogLine(t *testing.T) {
	telemetry := &agentTelemetry{
		ch: make(chan *pb.AgentTelemetryRequest, 1),
	}
	out := &bytes.Buffer{}
	writer := telemetry.teeLogWriter(out, types.AgentTelemetrySourceAgent, "", types.EventLogStreamStderr)
	if _, err := writer.Write([]byte("partial log")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if out.String() != "partial log" {
		t.Fatalf("tee output = %q, want partial log", out.String())
	}

	select {
	case req := <-telemetry.ch:
		if len(req.Logs) != 1 || req.Logs[0].Line != "partial log" {
			t.Fatalf("unexpected telemetry request: %#v", req)
		}
	default:
		t.Fatal("expected buffered log line to flush on close")
	}
}

func TestTelemetryTeeCloseDoesNotCloseOutputWriter(t *testing.T) {
	telemetry := &agentTelemetry{
		ch: make(chan *pb.AgentTelemetryRequest, 1),
	}
	out := &closeTrackingWriter{}
	writer := telemetry.teeLogWriter(out, types.AgentTelemetrySourceAgent, "", types.EventLogStreamStderr)
	if _, err := writer.Write([]byte("partial log")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if out.closed {
		t.Fatal("tee close closed caller-owned output writer")
	}
}

func TestTelemetryEnqueueReportsDroppedRecords(t *testing.T) {
	t.Setenv(types.AgentVerboseEnv, "1")
	stderr := &bytes.Buffer{}
	telemetry := &agentTelemetry{
		stderr: stderr,
		ch:     make(chan *pb.AgentTelemetryRequest, 1),
	}
	telemetry.enqueue(&pb.AgentTelemetryRequest{Logs: []*pb.AgentLogRecord{{Line: "queued"}}})
	telemetry.enqueue(&pb.AgentTelemetryRequest{Logs: []*pb.AgentLogRecord{{Line: "dropped"}}})

	if telemetry.dropped.Load() != 1 {
		t.Fatalf("dropped = %d, want 1", telemetry.dropped.Load())
	}
	if !strings.Contains(stderr.String(), "dropped 1 records") {
		t.Fatalf("missing drop warning: %q", stderr.String())
	}
}

type closeTrackingWriter struct {
	bytes.Buffer
	closed bool
}

func (w *closeTrackingWriter) Close() error {
	w.closed = true
	return nil
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestAgentLockRejectsSecondAgentForSameStateDir(t *testing.T) {
	t.Setenv(types.AgentStateDirEnv, t.TempDir())

	first, err := acquireAgentLock()
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()

	second, err := acquireAgentLock()
	if err == nil {
		second.release()
		t.Fatal("expected second agent lock acquisition to fail")
	}
	if !strings.Contains(err.Error(), "already running") {
		t.Fatalf("expected already running error, got %v", err)
	}
}

func TestWriteWorkerConfigUsesGatewayBootstrapParts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	slot := &pb.AgentWorkerSlot{
		PoolName:                  "private-dev",
		MachineId:                 "machine-a",
		Cpu:                       500,
		Memory:                    256,
		NetworkPrefix:             "10.0.0.0/24",
		NetworkSlotPoolSize:       64,
		ContainerStartConcurrency: 12,
	}

	if err := writeWorkerConfig(path, bootstrapConfig{
		GatewayHTTPURL:  "http://host.docker.internal:1994",
		GatewayGRPCHost: "host.docker.internal",
		GatewayGRPCPort: 1993,
		GatewayGRPCTLS:  false,
	}, slot); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}

	gatewayConfig := config["gateway"].(map[string]any)
	grpcConfig := gatewayConfig["grpc"].(map[string]any)
	httpConfig := gatewayConfig["http"].(map[string]any)
	if got := grpcConfig["externalHost"]; got != "host.docker.internal" {
		t.Fatalf("grpc external host = %v, want host.docker.internal", got)
	}
	if got := grpcConfig["externalPort"]; got != float64(1993) {
		t.Fatalf("grpc external port = %v, want 1993", got)
	}
	if got := httpConfig["externalHost"]; got != "host.docker.internal" {
		t.Fatalf("http external host = %v, want host.docker.internal", got)
	}
	if got := httpConfig["externalPort"]; got != float64(1994) {
		t.Fatalf("http external port = %v, want 1994", got)
	}
}

func TestAgentLocalRegistryForwardTargetUsesLocalK3DPort(t *testing.T) {
	t.Setenv(types.AgentRegistryForwardEnv, "host.docker.internal:5001")

	got := agentLocalRegistryForwardTarget()
	if got != "host.docker.internal:5001" {
		t.Fatalf("registry forward target = %q, want host-published k3d registry", got)
	}
}

func TestAgentLocalRegistryForwardTargetDisabledByDefault(t *testing.T) {
	if got := agentLocalRegistryForwardTarget(); got != "" {
		t.Fatalf("registry forward target = %q, want disabled", got)
	}
}

func TestAgentDockerHostAliasesAreEnvironmentDriven(t *testing.T) {
	t.Setenv(types.AgentDockerHostsEnv, "registry.localhost:127.0.0.1, localstack:host-gateway")

	got := agentDockerHostAliases()
	want := []string{"registry.localhost:127.0.0.1", "localstack:host-gateway"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("aliases = %#v, want %#v", got, want)
	}
}

func TestMachineFingerprintCanBeProvidedByHost(t *testing.T) {
	t.Setenv("BEAM_AGENT_MACHINE_FINGERPRINT", "mac-hardware-id")

	if got := machineFingerprint("container-hostname"); got != "mac-hardware-id" {
		t.Fatalf("expected host-provided fingerprint, got %q", got)
	}
}

func TestResolveAgentCapacityAppliesHardCaps(t *testing.T) {
	capacity, checks, schedulable := resolveAgentCapacity(types.AgentJoinOptions{
		MaxCPU:    "999999",
		MaxMemory: "999999Ti",
	}, preflightResult{
		schedulable: true,
	})

	if schedulable {
		t.Fatal("expected over-inventory caps to make machine unschedulable")
	}
	if capacity.CPUMillicores <= 0 || capacity.MemoryMB == 0 {
		t.Fatalf("expected detected fallback capacity, got %#v", capacity)
	}
	if len(checks) < 2 {
		t.Fatalf("expected cap checks, got %#v", checks)
	}
}

func TestResolveAgentCapacitySelectsGPUIDs(t *testing.T) {
	capacity, _, schedulable := resolveAgentCapacity(types.AgentJoinOptions{
		GPUIDs: "GPU-a,1",
	}, preflightResult{
		gpus: []string{"A10G", "A10G"},
		gpuDevices: []gpuDevice{
			{ID: "0", UUID: "GPU-a", Name: "A10G"},
			{ID: "1", UUID: "GPU-b", Name: "A10G"},
		},
		schedulable: true,
	})

	if !schedulable {
		t.Fatal("expected selected GPU IDs to be schedulable")
	}
	if capacity.GPUCount != 2 {
		t.Fatalf("gpu count = %d, want 2", capacity.GPUCount)
	}
	if got := strings.Join(capacity.GPUIDs, ","); got != "GPU-a,1" {
		t.Fatalf("gpu ids = %q, want GPU-a,1", got)
	}
}

func TestWriteWorkerConfigUsesGeeseForWorkspaceStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	slot := &pb.AgentWorkerSlot{
		PoolName:                  "private-dev",
		MachineId:                 "machine-a",
		Cpu:                       500,
		Memory:                    256,
		NetworkPrefix:             "10.0.0.0/24",
		NetworkSlotPoolSize:       64,
		ContainerStartConcurrency: 12,
	}

	if err := writeWorkerConfig(path, bootstrapConfig{
		ImageRegistryStore:     reg.S3ImageRegistryStore,
		ImageClipVersion:       1,
		ImageLocalCacheEnabled: true,
	}, slot); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}

	storageConfig := config["storage"].(map[string]any)
	if got := storageConfig["fsPath"]; got != types.AgentDataPath {
		t.Fatalf("storage fsPath = %v, want %q", got, types.AgentDataPath)
	}
	if got := storageConfig["objectPath"]; got != filepath.Join(types.AgentDataPath, "objects") {
		t.Fatalf("storage objectPath = %v, want %q", got, filepath.Join(types.AgentDataPath, "objects"))
	}
	workspaceStorage := storageConfig["workspaceStorage"].(map[string]any)
	if got := workspaceStorage["baseMountPath"]; got != types.AgentWorkspacePath {
		t.Fatalf("workspace base path = %v, want %q", got, types.AgentWorkspacePath)
	}
	if got := workspaceStorage["defaultStorageMode"]; got != types.StorageModeGeese {
		t.Fatalf("workspace storage mode = %v, want %q", got, types.StorageModeGeese)
	}

	imageConfig := config["imageService"].(map[string]any)
	if got := imageConfig["registryStore"]; got != reg.S3ImageRegistryStore {
		t.Fatalf("image registry store = %v, want %q", got, reg.S3ImageRegistryStore)
	}
	if got := imageConfig["clipVersion"]; got != float64(1) {
		t.Fatalf("image clip version = %v, want 1", got)
	}
	if got := imageConfig["localCacheEnabled"]; got != false {
		t.Fatalf("image local cache enabled = %v, want false", got)
	}

	monitoringConfig := config["monitoring"].(map[string]any)
	if got := monitoringConfig["metricsCollector"]; got != string(types.MetricsCollectorNone) {
		t.Fatalf("metrics collector = %v, want disabled collector", got)
	}
	prometheusConfig := monitoringConfig["prometheus"].(map[string]any)
	if got := prometheusConfig["scrapeWorkers"]; got != false {
		t.Fatalf("prometheus scrapeWorkers = %v, want false", got)
	}
	if got := prometheusConfig["port"]; got != float64(0) {
		t.Fatalf("prometheus port = %v, want 0", got)
	}

	workerConfig := config["worker"].(map[string]any)
	if got := workerConfig["cacheEnabled"]; got != true {
		t.Fatalf("worker cacheEnabled = %v, want true", got)
	}
	pools := workerConfig["pools"].(map[string]any)
	pool := pools["private-dev"].(map[string]any)
	if got := pool["storageMode"]; got != types.StorageModeGeese {
		t.Fatalf("pool storage mode = %v, want %q", got, types.StorageModeGeese)
	}
	if got := pool["networkPreallocation"]; got != true {
		t.Fatalf("pool networkPreallocation = %v, want true", got)
	}
	if got := pool["networkSlotPoolSize"]; got != float64(64) {
		t.Fatalf("pool networkSlotPoolSize = %v, want 64", got)
	}
	if got := pool["containerStartConcurrency"]; got != float64(12) {
		t.Fatalf("pool containerStartConcurrency = %v, want 12", got)
	}
	poolCache := pool["cache"].(map[string]any)
	poolDisk := poolCache["disk"].(map[string]any)
	if got := poolCache["enabled"]; got != true {
		t.Fatalf("pool cache enabled = %v, want true", got)
	}
	if got := poolDisk["mountPath"]; got != types.AgentCachePath {
		t.Fatalf("pool cache mount path = %v, want %q", got, types.AgentCachePath)
	}

	cacheConfig := config["cache"].(map[string]any)
	cacheDisk := cacheConfig["disk"].(map[string]any)
	cacheServer := cacheConfig["server"].(map[string]any)
	cacheClient := cacheConfig["client"].(map[string]any)
	cacheFS := cacheClient["cachefs"].(map[string]any)
	if got := cacheConfig["enabled"]; got != true {
		t.Fatalf("cache enabled = %v, want true", got)
	}
	if got := cacheDisk["mountPath"]; got != types.AgentCachePath {
		t.Fatalf("cache disk mount path = %v, want %q", got, types.AgentCachePath)
	}
	if got := cacheServer["diskCacheDir"]; got != filepath.Join(types.AgentCachePath, "private-dev", "machine-a") {
		t.Fatalf("cache disk dir = %v, want machine-scoped persistent path", got)
	}
	if got := cacheFS["mountPoint"]; got != types.AgentCacheFSMountPath {
		t.Fatalf("cachefs mount point = %v, want %q", got, types.AgentCacheFSMountPath)
	}
	if got := cacheFS["enabled"]; got != true {
		t.Fatalf("cachefs enabled = %v, want true", got)
	}
}

func TestWriteWorkerConfigDisablesCacheFSInAgentContainer(t *testing.T) {
	t.Setenv(types.AgentInContainerEnv, "1")
	path := filepath.Join(t.TempDir(), "config.json")
	slot := &pb.AgentWorkerSlot{
		PoolName:  "private-dev",
		MachineId: "machine-a",
	}

	if err := writeWorkerConfig(path, bootstrapConfig{}, slot); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}

	storageConfig := config["storage"].(map[string]any)
	workspaceStorage := storageConfig["workspaceStorage"].(map[string]any)
	if got := workspaceStorage["defaultStorageMode"]; got != types.StorageModeGeese {
		t.Fatalf("workspace storage mode = %v, want %q", got, types.StorageModeGeese)
	}
	workerConfig := config["worker"].(map[string]any)
	pools := workerConfig["pools"].(map[string]any)
	pool := pools["private-dev"].(map[string]any)
	if got := pool["storageMode"]; got != types.StorageModeGeese {
		t.Fatalf("pool storage mode = %v, want %q", got, types.StorageModeGeese)
	}

	cacheConfig := config["cache"].(map[string]any)
	cacheClient := cacheConfig["client"].(map[string]any)
	cacheFS := cacheClient["cachefs"].(map[string]any)
	if got := cacheFS["enabled"]; got != false {
		t.Fatalf("cachefs enabled = %v, want false", got)
	}
}

func containsArg(args []string, prefix, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == prefix && args[i+1] == value {
			return true
		}
	}
	return false
}

func startEchoListener(t *testing.T, addr string) net.Listener {
	t.Helper()

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	t.Cleanup(func() {
		_ = listener.Close()
		wg.Wait()
	})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return listener
}
