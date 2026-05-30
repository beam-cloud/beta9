package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
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

func TestNormalizeBootstrapForAgentContainerUsesReachableGatewayHost(t *testing.T) {
	t.Setenv("BEAM_AGENT_CONTAINER", "1")

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

func TestDockerRunArgsUsesConfigurableRouteTargetHost(t *testing.T) {
	t.Setenv("BEAM_AGENT_LOCAL_TARGET_HOST", "host.docker.internal")
	t.Setenv(agentDockerHostAliasesEnv, "registry.localhost:127.0.0.1,localstack:host-gateway")

	args := dockerRunArgs("slot-one", "worker:dev", "/tmp/config.json", bootstrapConfig{
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

	if !containsArg(args, "-e", types.WorkerEnvRouteLocalTargetHost+"=host.docker.internal") {
		t.Fatalf("expected route target host env in docker args: %#v", args)
	}
	for _, want := range []string{
		"CACHE_LOCALITY=private",
		"CACHE_NODE_ID=machine",
		"CACHE_HOST_NETWORK=true",
		"BEAM_GATEWAY_HTTP_URL=http://host.docker.internal:1994",
		"BETA9_GATEWAY_HOST=host.docker.internal",
		"BETA9_GATEWAY_PORT=1993",
		"BETA9_GATEWAY_HOST_HTTP=host.docker.internal",
		"BETA9_GATEWAY_PORT_HTTP=1994",
		"NVIDIA_VISIBLE_DEVICES=0,1",
		"WORKER_CONTAINER_START_CONCURRENCY=12",
		"CONTAINER_NETWORK_SLOT_POOL_SIZE=64",
	} {
		if !containsArg(args, "-e", want) {
			t.Fatalf("expected %s env in docker args: %#v", want, args)
		}
	}
	for _, want := range []string{
		"/tmp/agent-state/images:/images",
		"/tmp/agent-state/data:/data",
		"/tmp/agent-state/workspace-data:/workspace/data",
		"/tmp/agent-state/cache:/var/lib/beta9/cache",
		"/tmp/agent-state/checkpoints:/checkpoints",
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
		"registry.localhost:127.0.0.1",
		"localstack:host-gateway",
	} {
		if !containsArg(args, "--add-host", want) {
			t.Fatalf("expected %s host alias in docker args: %#v", want, args)
		}
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
	}, slot, agentWorkerDirs("/tmp/agent-state", "worker-one")); err != nil {
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
	t.Setenv(agentLocalRegistryForwardEnv, "host.docker.internal:5001")

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
	t.Setenv(agentDockerHostAliasesEnv, "registry.localhost:127.0.0.1, localstack:host-gateway")

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
	capacity, checks, schedulable := resolveAgentCapacity(JoinOptions{
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
	capacity, _, schedulable := resolveAgentCapacity(JoinOptions{
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

	if err := writeWorkerConfig(path, bootstrapConfig{}, slot, agentWorkerDirs("/tmp/agent-state", "worker-one")); err != nil {
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
	if got := storageConfig["fsPath"]; got != agentContainerDataPath {
		t.Fatalf("storage fsPath = %v, want %q", got, agentContainerDataPath)
	}
	if got := storageConfig["objectPath"]; got != "/data/objects" {
		t.Fatalf("storage objectPath = %v, want /data/objects", got)
	}
	workspaceStorage := storageConfig["workspaceStorage"].(map[string]any)
	if got := workspaceStorage["baseMountPath"]; got != agentContainerWorkspaceStoragePath {
		t.Fatalf("workspace base path = %v, want %q", got, agentContainerWorkspaceStoragePath)
	}
	if got := workspaceStorage["defaultStorageMode"]; got != storage.StorageModeGeese {
		t.Fatalf("workspace storage mode = %v, want %q", got, storage.StorageModeGeese)
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
	if got := pool["storageMode"]; got != storage.StorageModeGeese {
		t.Fatalf("pool storage mode = %v, want %q", got, storage.StorageModeGeese)
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
	if got := poolDisk["mountPath"]; got != agentContainerCachePath {
		t.Fatalf("pool cache mount path = %v, want %q", got, agentContainerCachePath)
	}

	cacheConfig := config["cache"].(map[string]any)
	cacheDisk := cacheConfig["disk"].(map[string]any)
	cacheServer := cacheConfig["server"].(map[string]any)
	cacheClient := cacheConfig["client"].(map[string]any)
	cacheFS := cacheClient["cachefs"].(map[string]any)
	if got := cacheConfig["enabled"]; got != true {
		t.Fatalf("cache enabled = %v, want true", got)
	}
	if got := cacheDisk["mountPath"]; got != agentContainerCachePath {
		t.Fatalf("cache disk mount path = %v, want %q", got, agentContainerCachePath)
	}
	if got := cacheServer["diskCacheDir"]; got != "/var/lib/beta9/cache/private-dev/machine-a" {
		t.Fatalf("cache disk dir = %v, want machine-scoped persistent path", got)
	}
	if got := cacheFS["mountPoint"]; got != agentContainerCacheFSMountPath {
		t.Fatalf("cachefs mount point = %v, want %q", got, agentContainerCacheFSMountPath)
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
