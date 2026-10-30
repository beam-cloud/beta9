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
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
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
	proxy := newRouteProxy(client, "agent-token", "machine-one", nil, "agent.tailnet:29443", nil, io.Discard, io.Discard)

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

func TestRouteProxyPollsOpeningRouteWithoutBackoff(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := reserved.Addr().String()
	_ = reserved.Close()

	backend := make(chan net.Listener, 1)
	backendErr := make(chan error, 1)
	go func() {
		time.Sleep(350 * time.Millisecond)
		listener, err := net.Listen("tcp", target)
		if err != nil {
			backendErr <- err
			return
		}
		backend <- listener
	}()

	updates := make(chan *pb.UpdateAgentRouteStatusRequest, 1)
	proxy := newRouteProxy(&routeStatusClient{updates: updates}, "agent-token", "machine-one", nil, "agent.tailnet:29443", nil, io.Discard, io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	proxy.setRoute("route-one", target)
	proxy.ensureRouteReady(ctx, "route-one", target)

	select {
	case listener := <-backend:
		t.Cleanup(func() { _ = listener.Close() })
	case err := <-backendErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("backend did not start")
	}

	select {
	case <-updates:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("route readiness polling backed off after the backend started")
	}
}

func TestRouteProxyRechecksOpeningRouteThatRacesReadyUpdate(t *testing.T) {
	backend := startEchoListener(t, "127.0.0.1:0")
	target := backend.Addr().String()
	client := &blockingRouteStatusClient{
		updates:      make(chan *pb.UpdateAgentRouteStatusRequest, 2),
		releaseFirst: make(chan struct{}),
	}
	proxy := newRouteProxy(client, "agent-token", "machine-one", nil, "agent.tailnet:29443", nil, io.Discard, io.Discard)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	proxy.setRoute("route-one", target)
	proxy.ensureRouteReady(ctx, "route-one", target)

	select {
	case <-client.updates:
	case <-time.After(time.Second):
		t.Fatal("first route readiness update did not start")
	}

	// This models an opening snapshot arriving while the previous ready update
	// is still in flight. The second check must not be lost when the first exits.
	proxy.ensureRouteReady(ctx, "route-one", target)
	close(client.releaseFirst)

	select {
	case <-client.updates:
	case <-time.After(time.Second):
		t.Fatal("opening route was not rechecked after the active ready update")
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

func TestRouteProxyHoldsConnectionWhileLocalTargetRestarts(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := reserved.Addr().String()
	_ = reserved.Close()

	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxyListener.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	proxy := &routeProxy{listener: proxyListener, routes: map[string]string{"route-one": target}}
	go func() { _ = proxy.accept(ctx) }()

	conn, err := net.DialTimeout("tcp", proxyListener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "%sroute-one\nping", network.BackendRoutePreface); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	startEchoListener(t, target)

	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("expected ping echo, got %q", string(buf))
	}
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

type blockingRouteStatusClient struct {
	pb.GatewayServiceClient
	mu           sync.Mutex
	calls        int
	updates      chan *pb.UpdateAgentRouteStatusRequest
	releaseFirst chan struct{}
}

func (c *blockingRouteStatusClient) UpdateAgentRouteStatus(ctx context.Context, in *pb.UpdateAgentRouteStatusRequest, _ ...grpc.CallOption) (*pb.UpdateAgentRouteStatusResponse, error) {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.mu.Unlock()

	select {
	case c.updates <- in:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if call == 1 {
		select {
		case <-c.releaseFirst:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
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

	args, err := dockerRunArgs("slot-one", "worker:dev", "sha256:worker-image", "/tmp/config.json", bootstrapConfig{
		GatewayHTTPURL:  "http://host.docker.internal:1994",
		GatewayGRPCHost: "host.docker.internal",
		GatewayGRPCPort: 1993,
		WorkspaceID:     "workspace-one",
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
		PoolConfig:                &pb.AgentPoolRuntimeConfig{GpuVirtualized: true},
	}, agentWorkerDirs("/tmp/agent-state", "", "worker-one"), workerContainerResourceLimits{})
	if err != nil {
		t.Fatal(err)
	}

	if !containsArg(args, "-e", types.WorkerRouteTargetEnv+"=host.docker.internal") {
		t.Fatalf("expected route target host env in docker args: %#v", args)
	}
	if !containsArg(args, "--platform", "linux/amd64") {
		t.Fatalf("expected worker platform in docker args: %#v", args)
	}
	if !slices.Contains(args, "--init") {
		t.Fatal("worker must reap daemonized disk processes")
	}
	for _, want := range []string{
		types.WorkerPoolEnv + "=private",
		types.CacheLocalityEnv + "=workspace-one/private",
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
		types.WorkerGPUVirtualizedEnv + "=true",
	} {
		if !containsArg(args, "-e", want) {
			t.Fatalf("expected %s env in docker args: %#v", want, args)
		}
	}
	for _, arg := range args {
		if strings.HasSuffix(arg, ":/data") {
			t.Fatalf("obsolete global /data mount in Docker args: %s", arg)
		}
	}

	for _, want := range []string{
		"/tmp/agent-state/images:" + types.AgentImagesPath,
		"/tmp/agent-state/workspace-data:" + types.AgentWorkspacePath,
		"/tmp/agent-state/cache:" + types.AgentCachePath,
		"/tmp/agent-state/checkpoints:" + types.AgentCheckpointPath,
		"/tmp/agent-state/durable-disks:" + types.DefaultDurableDisksPath,
	} {
		if !containsArg(args, "-v", want) {
			t.Fatalf("expected %s volume in docker args: %#v", want, args)
		}
	}
	if !containsArg(args, "--shm-size", "256m") {
		t.Fatalf("expected shm size to track worker memory: %#v", args)
	}
	if slices.Contains(args, "--cpus") || slices.Contains(args, "--memory") {
		t.Fatalf("default agent worker should not have Docker CPU or memory limits: %#v", args)
	}
	if !containsArg(args, "--gpus", `"device=0,1"`) {
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

func TestManagedPoolPathsOverrideAgentInstallerDirectories(t *testing.T) {
	slot := &pb.AgentWorkerSlot{
		WorkerId:  "worker-one",
		PoolName:  "serverless",
		MachineId: "machine-one",
		PoolConfig: &pb.AgentPoolRuntimeConfig{
			StoragePath:      "/mnt/raid/storage",
			ImagesPath:       "/mnt/raid/images",
			DurableDisksPath: "/mnt/raid/durable-disks",
			Cache: &pb.AgentPoolCacheConfig{
				Enabled: true,
				Disk: &pb.AgentPoolCacheDiskConfig{
					Enabled:   true,
					HostPath:  "/mnt/raid/cache",
					MountPath: "/var/lib/beta9/cache",
				},
			},
		},
	}
	dirs := agentWorkerDirsForSlot("/var/lib/beam/agent", "/installer/cache", slot)
	for label, gotWant := range map[string][2]string{
		"images":        {dirs.Images, "/mnt/raid/images"},
		"workspace":     {dirs.Workspace, "/mnt/raid/storage/workspace-data"},
		"checkpoints":   {dirs.Checkpoints, "/mnt/raid/storage/checkpoints"},
		"durable disks": {dirs.DurableDisk, "/mnt/raid/durable-disks"},
		"cache":         {dirs.Cache, "/mnt/raid/cache"},
		"cache mount":   {dirs.CacheMount, "/var/lib/beta9/cache"},
	} {
		if gotWant[0] != gotWant[1] {
			t.Fatalf("%s path = %q, want %q", label, gotWant[0], gotWant[1])
		}
	}

	args, err := dockerRunArgs(
		"slot-one",
		"worker:dev",
		"",
		"/tmp/config.json",
		bootstrapConfig{},
		slot,
		dirs,
		workerContainerResourceLimits{},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, volume := range []string{
		"/mnt/raid/images:" + types.AgentImagesPath,
		"/mnt/raid/storage/workspace-data:" + types.AgentWorkspacePath,
		"/mnt/raid/storage/checkpoints:" + types.AgentCheckpointPath,
		"/mnt/raid/durable-disks:" + types.DefaultDurableDisksPath,
		"/mnt/raid/cache:" + types.AgentCachePath,
	} {
		if !containsArg(args, "-v", volume) {
			t.Fatalf("expected %s volume in Docker args: %#v", volume, args)
		}
	}
}

func TestManagedPoolCanDisableAgentDiskCacheMount(t *testing.T) {
	slot := &pb.AgentWorkerSlot{
		WorkerId: "worker-one",
		PoolConfig: &pb.AgentPoolRuntimeConfig{
			Cache: &pb.AgentPoolCacheConfig{
				Enabled: false,
				Disk: &pb.AgentPoolCacheDiskConfig{
					Enabled:  true,
					HostPath: "/mnt/raid/cache",
				},
			},
		},
	}
	dirs := agentWorkerDirsForSlot("/var/lib/beam/agent", "", slot)
	if dirs.CacheEnabled {
		t.Fatal("cache mount remained enabled")
	}
	for _, dir := range dirs.All() {
		if dir == "/mnt/raid/cache" {
			t.Fatal("disabled cache directory should not be created")
		}
	}
	args, err := dockerRunArgs("slot-one", "worker:dev", "", "/tmp/config.json", bootstrapConfig{}, slot, dirs, workerContainerResourceLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if containsArg(args, "-v", "/mnt/raid/cache:"+types.AgentCachePath) {
		t.Fatalf("disabled cache was mounted: %#v", args)
	}
}

func TestDockerRunArgsAppliesExplicitResourceLimits(t *testing.T) {
	args, err := dockerRunArgs(
		"slot-one",
		"worker:dev",
		"",
		"/tmp/config.json",
		bootstrapConfig{},
		&pb.AgentWorkerSlot{Cpu: 4000, Memory: 8192},
		agentWorkerDirs("/tmp/agent-state", "", "worker-one"),
		workerContainerResourceLimits{cpu: true, memory: true},
	)
	if err != nil {
		t.Fatal(err)
	}

	if !containsArg(args, "--cpus", "4.000") {
		t.Fatalf("expected explicit CPU limit in Docker args: %#v", args)
	}
	if !containsArg(args, "--memory", "8192m") {
		t.Fatalf("expected explicit memory limit in Docker args: %#v", args)
	}
}

func TestAgentWorkerConfigDefaultsToPrivateRunc(t *testing.T) {
	t.Setenv(types.AgentCPUAffinityEnforcedEnv, "")
	slot := &pb.AgentWorkerSlot{PoolName: "private-dev", Cpu: 4000, Memory: 8192}
	config := newAgentWorkerConfig(bootstrapConfig{}, slot).sanitizedForAgent()

	pool, ok := config.Worker.Pools["private-dev"]
	if !ok {
		t.Fatal("pool missing from worker config")
	}
	if pool.Mode != string(types.PoolModePrivate) {
		t.Fatalf("pool mode = %q, want private", pool.Mode)
	}
	if pool.ContainerRuntime != types.ContainerRuntimeRunc.String() {
		t.Fatalf("pool runtime = %q, want runc", pool.ContainerRuntime)
	}
	if !pool.RequiresPoolSelector {
		t.Fatal("private pool should require pool selector")
	}
	if pool.Priority != 1000 {
		t.Fatalf("legacy slot priority = %d, want private default 1000", pool.Priority)
	}
	if !config.Worker.ContainerResourceLimits.CPUAffinityEnforced {
		t.Fatal("agent CPU affinity must default to enabled")
	}
}

func TestAgentWorkerConfigCanDisableCPUAffinity(t *testing.T) {
	t.Setenv(types.AgentCPUAffinityEnforcedEnv, "false")
	path := filepath.Join(t.TempDir(), "config.json")
	if err := writeWorkerConfig(path, bootstrapConfig{}, &pb.AgentWorkerSlot{PoolName: "private-dev"}); err != nil {
		t.Fatal(err)
	}
	t.Setenv(types.WorkerConfigPathEnv, path)
	t.Setenv(types.WorkerMinimalConfigEnv, "true")
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		t.Fatal(err)
	}
	config := configManager.GetConfig()

	if config.Worker.ContainerResourceLimits.CPUAffinityEnforced {
		t.Fatal("agent CPU affinity opt-out was not propagated to worker config")
	}
}

func TestManagedAgentWorkerConfigDefaultsCPUAffinityOff(t *testing.T) {
	t.Setenv(types.AgentCPUAffinityEnforcedEnv, "true")
	slot := &pb.AgentWorkerSlot{PoolName: "serverless", Mode: string(types.PoolModeExternal)}

	config := newAgentWorkerConfig(bootstrapConfig{}, slot).sanitizedForAgent()
	if config.Worker.ContainerResourceLimits.CPUAffinityEnforced {
		t.Fatal("managed pool CPU affinity must default to disabled")
	}

	slot.CpuAffinityEnforced = true
	config = newAgentWorkerConfig(bootstrapConfig{}, slot).sanitizedForAgent()
	if !config.Worker.ContainerResourceLimits.CPUAffinityEnforced {
		t.Fatal("managed pool CPU affinity setting was not propagated")
	}
}

func TestGVisorAgentWorkerConfigEnforcesMemory(t *testing.T) {
	slot := &pb.AgentWorkerSlot{
		PoolName:         "serverless",
		Mode:             string(types.PoolModeExternal),
		ContainerRuntime: types.ContainerRuntimeGvisor.String(),
	}

	config := newAgentWorkerConfig(bootstrapConfig{}, slot).sanitizedForAgent()
	if !config.Worker.ContainerResourceLimits.MemoryEnforced {
		t.Fatal("gVisor agent workloads must receive a bounded memory view")
	}
	if !config.Worker.ContainerResourceLimits.CPUAffinityEnforced {
		t.Fatal("gVisor agent workloads must receive a bounded CPU view")
	}

	slot.ContainerRuntime = types.ContainerRuntimeRunc.String()
	config = newAgentWorkerConfig(bootstrapConfig{}, slot).sanitizedForAgent()
	if config.Worker.ContainerResourceLimits.MemoryEnforced {
		t.Fatal("runc agent memory enforcement must remain unchanged")
	}
}

// Provider pools are workspace machines that serve only platform-managed
// endpoints. Revenue share is computed gateway-side from inference usage, so
// the worker config carries no billing identity; otherwise the slot behaves
// like a private pool that is addressed by selector.
func TestAgentWorkerConfigProviderSlotUsesGatewayRuntime(t *testing.T) {
	slot := &pb.AgentWorkerSlot{
		PoolName:         "provider-pool-1",
		Mode:             string(types.PoolModeProvider),
		ContainerRuntime: types.ContainerRuntimeGvisor.String(),
		Cpu:              4000,
		Memory:           8192,
	}
	config := newAgentWorkerConfig(bootstrapConfig{WorkspaceID: "workspace-a"}, slot).sanitizedForAgent()

	pool, ok := config.Worker.Pools["provider-pool-1"]
	if !ok {
		t.Fatal("pool missing from worker config")
	}
	if pool.Mode != string(types.PoolModeProvider) {
		t.Fatalf("pool mode = %q, want provider (must survive sanitize)", pool.Mode)
	}
	if pool.ContainerRuntime != types.ContainerRuntimeGvisor.String() {
		t.Fatalf("pool runtime = %q, want gateway-provided runtime", pool.ContainerRuntime)
	}
	if config.Worker.ContainerRuntime != types.ContainerRuntimeGvisor.String() {
		t.Fatalf("worker runtime = %q, want gateway-provided runtime", config.Worker.ContainerRuntime)
	}
	if !pool.RequiresPoolSelector {
		t.Fatal("provider pool must require pool selector")
	}
	if pool.Priority != 1000 {
		t.Fatalf("provider slot priority = %d, want default 1000", pool.Priority)
	}
	if config.Cache.Global == nil || config.Cache.Global.DefaultLocality != "workspace-a/provider-pool-1" {
		t.Fatalf("cache locality = %#v, want workspace-scoped pool", config.Cache.Global)
	}

	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"managedCompute", "containerCostHook", "billing"} {
		if strings.Contains(string(data), `"`+key+`"`) {
			t.Fatalf("worker config carries %q; provider workers must not receive billing config: %s", key, data)
		}
	}
}

func TestAgentWorkerConfigManagedExternalPreservesPoolSemantics(t *testing.T) {
	slot := &pb.AgentWorkerSlot{
		PoolName:                  "public-h100",
		Mode:                      string(types.PoolModeExternal),
		ContainerRuntime:          types.ContainerRuntimeRunc.String(),
		RequiresPoolSelector:      false,
		Priority:                  250,
		Preemptable:               true,
		NetworkSlotPoolSize:       64,
		ContainerStartConcurrency: 8,
	}
	config := newAgentWorkerConfig(bootstrapConfig{}, slot).sanitizedForAgent()
	pool := config.Worker.Pools[slot.PoolName]

	if pool.Mode != string(types.PoolModeExternal) {
		t.Fatalf("pool mode = %q, want external", pool.Mode)
	}
	if pool.RequiresPoolSelector {
		t.Fatal("public external pool unexpectedly requires a selector")
	}
	if pool.Priority != 250 || !pool.Preemptable {
		t.Fatalf("scheduling config = priority %d preemptable %v", pool.Priority, pool.Preemptable)
	}
	if pool.NetworkSlotPoolSize != 64 || pool.ContainerStartConcurrency != 8 {
		t.Fatalf("agent capacity config = %+v", pool)
	}
}

func TestAgentWorkerConfigUsesManagedPoolRuntimeAndCacheSettings(t *testing.T) {
	slot := &pb.AgentWorkerSlot{
		PoolName:  "public-h100",
		MachineId: "machine-one",
		Mode:      string(types.PoolModeExternal),
		PoolConfig: &pb.AgentPoolRuntimeConfig{
			NetworkPreallocation: false,
			GpuVirtualized:       true,
			CriuEnabled:          false,
			TmpSizeLimit:         "50Gi",
			StorageMode:          types.StorageModeMountPoint,
			ConfigGroup:          "raid-cache",
			Cache: &pb.AgentPoolCacheConfig{
				Enabled: true,
				Disk: &pb.AgentPoolCacheDiskConfig{
					Enabled:      true,
					HostPath:     "/mnt/raid/cache",
					MountPath:    "/var/lib/beta9/cache",
					MaxUsagePct:  0.9,
					MinFreeBytes: 1 << 30,
				},
			},
		},
	}
	config := newAgentWorkerConfig(bootstrapConfig{WorkspaceID: "admin-workspace"}, slot).sanitizedForAgent()
	pool := config.Worker.Pools[slot.PoolName]

	if pool.NetworkPreallocation || pool.CRIUEnabled || !pool.GPUVirtualized || pool.TmpSizeLimit != "50Gi" || pool.StorageMode != types.StorageModeMountPoint {
		t.Fatalf("managed pool runtime settings were not preserved: %#v", pool)
	}
	if !config.Cache.Enabled || !config.Worker.CacheEnabled ||
		config.Cache.Disk.HostPath != "/mnt/raid/cache" ||
		config.Cache.Disk.MountPath != "/var/lib/beta9/cache" ||
		config.Cache.Disk.MaxUsagePct != 0.9 ||
		config.Cache.Disk.MinFreeBytes != 1<<30 {
		t.Fatalf("managed pool cache settings were not preserved: %#v", config.Cache)
	}
	if config.Cache.Global == nil || config.Cache.Global.DefaultLocality != "raid-cache" {
		t.Fatalf("cache locality = %#v, want config group", config.Cache.Global)
	}
}

func TestAgentWorkerConfigPreservesExplicitZeroPriority(t *testing.T) {
	slot := &pb.AgentWorkerSlot{
		PoolName:    "lowest-priority",
		Mode:        string(types.PoolModeExternal),
		Priority:    0,
		PrioritySet: true,
	}

	config := newAgentWorkerConfig(bootstrapConfig{}, slot).sanitizedForAgent()
	if priority := config.Worker.Pools[slot.PoolName].Priority; priority != 0 {
		t.Fatalf("priority = %d, want explicit zero", priority)
	}
}

func TestAgentWorkerConfigProviderRuntimeDefaultsAndOverrides(t *testing.T) {
	for name, slot := range map[string]*pb.AgentWorkerSlot{
		"missing runtime defaults to runc": {
			PoolName: "provider-pool-1",
			Mode:     string(types.PoolModeProvider),
		},
		"gvisor sent by gateway is respected": {
			PoolName:         "provider-pool-1",
			Mode:             string(types.PoolModeProvider),
			ContainerRuntime: types.ContainerRuntimeGvisor.String(),
		},
	} {
		config := newAgentWorkerConfig(bootstrapConfig{}, slot).sanitizedForAgent()
		want := firstNonEmpty(slot.ContainerRuntime, types.ContainerRuntimeRunc.String())
		if got := config.Worker.Pools["provider-pool-1"].ContainerRuntime; got != want {
			t.Fatalf("%s: pool runtime = %q, want %q", name, got, want)
		}
	}
}

func TestSlotPoolModeAcceptsOnlyExplicitAgentModes(t *testing.T) {
	for mode, want := range map[string]string{
		"":                             string(types.PoolModePrivate),
		string(types.PoolModePrivate):  string(types.PoolModePrivate),
		string(types.PoolModeProvider): string(types.PoolModeProvider),
		string(types.PoolModeExternal): string(types.PoolModeExternal),
		string(types.PoolModeLocal):    string(types.PoolModePrivate),
		"unknown-mode":                 string(types.PoolModePrivate),
	} {
		if got := slotPoolMode(&pb.AgentWorkerSlot{Mode: mode}); got != want {
			t.Fatalf("slotPoolMode(%q) = %q, want %q", mode, got, want)
		}
	}
	if got := slotPoolMode(nil); got != string(types.PoolModePrivate) {
		t.Fatalf("slotPoolMode(nil) = %q, want private", got)
	}
}

func TestAgentCacheLocalityScopesPoolByWorkspace(t *testing.T) {
	slot := &pb.AgentWorkerSlot{PoolName: "aws-cpu-pool"}

	if got := agentCacheLocality(bootstrapConfig{WorkspaceID: "workspace-a"}, slot); got != "workspace-a/aws-cpu-pool" {
		t.Fatalf("cache locality = %q, want workspace-scoped pool", got)
	}
	if got := agentCacheLocality(bootstrapConfig{WorkspaceID: "workspace-b"}, slot); got != "workspace-b/aws-cpu-pool" {
		t.Fatalf("cache locality = %q, want workspace-scoped pool", got)
	}
	if got := agentCacheLocality(bootstrapConfig{}, slot); got != "aws-cpu-pool" {
		t.Fatalf("cache locality = %q, want raw pool fallback without workspace", got)
	}
	if got := agentCacheLocality(bootstrapConfig{WorkspaceID: "workspace-a", PoolName: "bootstrap-pool"}, nil); got != "workspace-a/bootstrap-pool" {
		t.Fatalf("cache locality = %q, want bootstrap pool fallback", got)
	}
}

func dockerContainerInspectOwnedByAgent(data []byte, slot *pb.AgentWorkerSlot) (bool, error) {
	var inspect dockerContainerInspect
	if err := json.Unmarshal(data, &inspect); err != nil {
		return false, err
	}
	return dockerContainerInspectMatchesSlot(&inspect, slot)
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

func TestDockerContainerManagedByMachine(t *testing.T) {
	tests := []struct {
		name      string
		inspect   *dockerContainerInspect
		machineID string
		want      bool
	}{
		{
			name: "matches managed machine",
			inspect: dockerInspectWithLabels("/beam-agent-worker", map[string]string{
				types.AgentDockerLabelManaged:   "true",
				types.AgentDockerLabelMachineID: "machine-one",
			}),
			machineID: "machine-one",
			want:      true,
		},
		{
			name: "ignores unmanaged same machine label",
			inspect: dockerInspectWithLabels("/other", map[string]string{
				types.AgentDockerLabelMachineID: "machine-one",
			}),
			machineID: "machine-one",
		},
		{
			name: "ignores different machine",
			inspect: dockerInspectWithLabels("/beam-agent-worker", map[string]string{
				types.AgentDockerLabelManaged:   "true",
				types.AgentDockerLabelMachineID: "machine-two",
			}),
			machineID: "machine-one",
		},
		{
			name:      "ignores nil inspect",
			machineID: "machine-one",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dockerContainerManagedByMachine(tt.inspect, tt.machineID)
			if got != tt.want {
				t.Fatalf("managed = %v, want %v", got, tt.want)
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

func TestAgentWorkerDirsUsesOptionalCacheDir(t *testing.T) {
	t.Setenv(types.AgentCacheDirEnv, "")
	stateDir := filepath.Join(t.TempDir(), "state")
	if got := agentWorkerDirs(stateDir, "", "worker-one").Cache; got != filepath.Join(stateDir, "cache") {
		t.Fatalf("default cache dir = %q, want state cache dir", got)
	}

	cacheDir := filepath.Join(t.TempDir(), "cache")
	if got := agentWorkerDirs(stateDir, cacheDir, "worker-one").Cache; got != cacheDir {
		t.Fatalf("cache dir = %q, want override %q", got, cacheDir)
	}
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

func TestResolveAgentIdentityFallsBackWhenBootstrapTokenExpires(t *testing.T) {
	t.Setenv(types.AgentStateDirEnv, t.TempDir())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(joinResponse{Ok: false, ErrMsg: "join token revoked"})
	}))
	t.Cleanup(server.Close)

	if err := saveRuntimeState(server.URL, &joinResponse{
		Ok:          true,
		WorkspaceID: "workspace-one",
		PoolName:    "pool-one",
		MachineID:   "machine-one",
		AgentToken:  "saved-token",
	}); err != nil {
		t.Fatal(err)
	}

	resolved, err := resolveAgentIdentity(context.Background(), NewClient(server.URL), types.AgentJoinOptions{
		GatewayURL: server.URL,
		JoinToken:  "bad-token",
		DevMode:    true,
		Stdout:     io.Discard,
		Stderr:     io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.AgentToken != "saved-token" {
		t.Fatalf("agent token = %q, want saved identity", resolved.AgentToken)
	}
}

func TestNormalizeJoinOptionsAllowsConsumedTokenFile(t *testing.T) {
	opts, err := normalizeJoinOptions(types.AgentJoinOptions{
		GatewayURL:    "https://gateway.beam.cloud",
		JoinTokenFile: filepath.Join(t.TempDir(), "consumed-token"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if opts.JoinToken != "" {
		t.Fatalf("join token = %q, want empty", opts.JoinToken)
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

func TestCollectPathMetricsSkipsStateChildrenWhenStateDirMissing(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmp, "images", "cache"), 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cache", "checkpoints"} {
		if err := os.MkdirAll(filepath.Join(tmp, name), 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chdir(tmp); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	metrics := (&agentTelemetry{}).collectPathMetrics()
	labels := map[string]struct{}{}
	for _, metric := range metrics {
		labels[metric.Label] = struct{}{}
	}
	for _, label := range []string{"state", "images", "images_cache", "cache", "checkpoints"} {
		if _, ok := labels[label]; ok {
			t.Fatalf("unexpected %q path metric with empty state dir", label)
		}
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

func testWorkerSlot() *pb.AgentWorkerSlot {
	return &pb.AgentWorkerSlot{
		PoolName:                  "private-dev",
		MachineId:                 "machine-a",
		ContainerRuntime:          types.ContainerRuntimeGvisor.String(),
		GvisorPlatform:            "kvm",
		GvisorRoot:                "/run/gvisor-test",
		GvisorExtraArgs:           []string{"--overlay2=none", "--file-access=exclusive"},
		Cpu:                       500,
		Memory:                    256,
		NetworkPrefix:             "10.0.0.0/24",
		NetworkSlotPoolSize:       64,
		ContainerStartConcurrency: 12,
	}
}

func TestAgentS2ConfigUsesClusterCredentialsOnlyForPlatformPools(t *testing.T) {
	telemetry := telemetryConfig{
		StreamPrefix: "events",
		Logs: telemetrySinkConfig{
			Destination:  "basin",
			Credential:   "cluster-key",
			StreamPrefix: "events/logs/workspaces",
		},
		Events: telemetrySinkConfig{
			Destination:  "basin",
			Credential:   "cluster-key",
			StreamPrefix: "events/workspaces",
		},
	}

	platform := agentS2Config(telemetry, string(types.PoolModeExternal))
	if platform.ApiKey != "cluster-key" || platform.LogApiKey != "" || platform.EventApiKey != "" {
		t.Fatalf("unexpected platform S2 config: %#v", platform)
	}

	private := agentS2Config(telemetry, string(types.PoolModePrivate))
	if private.ApiKey != "" || private.LogApiKey != "cluster-key" || private.EventApiKey != "cluster-key" {
		t.Fatalf("unexpected private S2 config: %#v", private)
	}
}

func assertGVisorRuntimeConfig(t *testing.T, config map[string]any, slot *pb.AgentWorkerSlot) {
	t.Helper()
	if got := config["gvisorPlatform"]; got != slot.GvisorPlatform {
		t.Fatalf("gVisor platform = %v, want %q", got, slot.GvisorPlatform)
	}
	if got := config["gvisorRoot"]; got != slot.GvisorRoot {
		t.Fatalf("gVisor root = %v, want %q", got, slot.GvisorRoot)
	}
	wantExtraArgs := make([]any, len(slot.GvisorExtraArgs))
	for i, arg := range slot.GvisorExtraArgs {
		wantExtraArgs[i] = arg
	}
	if got := config["gvisorExtraArgs"]; !reflect.DeepEqual(got, wantExtraArgs) {
		t.Fatalf("gVisor extra args = %#v, want %#v", got, wantExtraArgs)
	}
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
	slot := testWorkerSlot()

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
	workerConfig := config["worker"].(map[string]any)
	if got := workerConfig["useHostResolvConf"]; got != true {
		t.Fatalf("useHostResolvConf = %v, want true", got)
	}
	poolsConfig := workerConfig["pools"].(map[string]any)
	poolConfig := poolsConfig["private-dev"].(map[string]any)
	if got := poolConfig["criuEnabled"]; got != true {
		t.Fatalf("private pool criuEnabled = %v, want true", got)
	}
	assertGVisorRuntimeConfig(t, poolConfig["containerRuntimeConfig"].(map[string]any), slot)
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
		gpus: []string{"NVIDIA RTX A4000", "NVIDIA RTX A4000"},
		gpuDevices: []gpuDevice{
			{ID: "0", UUID: "GPU-a", Name: "NVIDIA RTX A4000"},
			{ID: "1", UUID: "GPU-b", Name: "NVIDIA RTX A4000"},
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
	if got := strings.Join(capacity.GPUs, ","); got != "A4000,A4000" {
		t.Fatalf("gpus = %q, want A4000,A4000", got)
	}
}

func TestResolveAgentCapacityDefaultsToDetectedGPUIDs(t *testing.T) {
	capacity, _, schedulable := resolveAgentCapacity(types.AgentJoinOptions{}, preflightResult{
		gpus: []string{"NVIDIA RTX A4000", "NVIDIA RTX A4000"},
		gpuDevices: []gpuDevice{
			{ID: "0", UUID: "GPU-a", Name: "NVIDIA RTX A4000"},
			{ID: "1", UUID: "GPU-b", Name: "NVIDIA RTX A4000"},
		},
		schedulable: true,
	})

	if !schedulable {
		t.Fatal("expected detected GPU IDs to be schedulable")
	}
	if got := strings.Join(capacity.GPUIDs, ","); got != "0,1" {
		t.Fatalf("gpu ids = %q, want 0,1", got)
	}
}

func TestDetectNvidiaGPUDevicesSkipsExcludedProcAdapter(t *testing.T) {
	previousQuery := queryNvidiaGPUDevices
	queryNvidiaGPUDevices = func() ([]byte, error) {
		return []byte(`0, GPU-bad, NVIDIA GeForce RTX 4090, 0x0000, 00000000:01:00.0
1, GPU-good, NVIDIA GeForce RTX 4090, 0x0000, 00000000:24:00.0`), nil
	}
	t.Cleanup(func() { queryNvidiaGPUDevices = previousQuery })

	root := t.TempDir()
	writeNvidiaProcGPUInfo(t, root, "0000:01:00.0", "GPU-bad", "N/A", "??.??.??.??.?", true)
	writeNvidiaProcGPUInfo(t, root, "0000:24:00.0", "GPU-good", "580.126.18", "95.02.3c.40.b8", false)

	previousRoot := nvidiaProcGPUInfoRoot
	nvidiaProcGPUInfoRoot = root
	t.Cleanup(func() { nvidiaProcGPUInfoRoot = previousRoot })

	devices := detectNvidiaGPUDevices()
	if len(devices) != 1 {
		t.Fatalf("devices = %#v, want one healthy GPU", devices)
	}
	if devices[0].ID != "1" || devices[0].UUID != "GPU-good" {
		t.Fatalf("device = %#v, want healthy GPU", devices[0])
	}
}

func TestDetectNvidiaGPUDevicesAllowsUnknownVideoBIOS(t *testing.T) {
	previousQuery := queryNvidiaGPUDevices
	queryNvidiaGPUDevices = func() ([]byte, error) {
		return []byte(`0, GPU-a, NVIDIA GeForce RTX 4090, 0x0000, 00000000:01:00.0`), nil
	}
	t.Cleanup(func() { queryNvidiaGPUDevices = previousQuery })

	root := t.TempDir()
	writeNvidiaProcGPUInfo(t, root, "0000:01:00.0", "GPU-a", "N/A", "??.??.??.??.?", false)

	previousRoot := nvidiaProcGPUInfoRoot
	nvidiaProcGPUInfoRoot = root
	t.Cleanup(func() { nvidiaProcGPUInfoRoot = previousRoot })

	devices := detectNvidiaGPUDevices()
	if len(devices) != 1 {
		t.Fatalf("devices = %#v, want GPU with working nvidia-smi despite unknown Video BIOS metadata", devices)
	}
}

func TestNvidiaProcDriverStateReportsFailedAdapterWithoutHardFail(t *testing.T) {
	root := t.TempDir()
	writeNvidiaProcGPUInfo(t, root, "0000:01:00.0", "GPU-bad", "N/A", "??.??.??.??.?", true)
	writeNvidiaProcGPUInfo(t, root, "0000:24:00.0", "GPU-good", "580.126.18", "95.02.3c.40.b8", false)

	previous := nvidiaProcGPUInfoRoot
	nvidiaProcGPUInfoRoot = root
	t.Cleanup(func() { nvidiaProcGPUInfoRoot = previous })

	ok, message := nvidiaProcDriverStateOK([]gpuDevice{{ID: "0", UUID: "GPU-good", Name: "RTX4090"}})
	if !ok {
		t.Fatal("expected failed adapter state to be reported without failing the whole node")
	}
	if !strings.Contains(message, "0000:01:00.0") {
		t.Fatalf("message = %q, want failed bus id", message)
	}
	if !strings.Contains(message, "excluded") {
		t.Fatalf("message = %q, want exclusion message", message)
	}
}

func TestNvidiaProcDriverStateReportsProcSmiMismatchWithoutHardFail(t *testing.T) {
	root := t.TempDir()
	writeNvidiaProcGPUInfo(t, root, "0000:24:00.0", "GPU-a", "580.126.18", "95.02.3c.40.b8", false)
	writeNvidiaProcGPUInfo(t, root, "0000:41:00.0", "GPU-b", "580.126.18", "95.02.3c.40.b8", false)

	previous := nvidiaProcGPUInfoRoot
	nvidiaProcGPUInfoRoot = root
	t.Cleanup(func() { nvidiaProcGPUInfoRoot = previous })

	ok, message := nvidiaProcDriverStateOK([]gpuDevice{{ID: "0", UUID: "GPU-a", Name: "RTX4090"}})
	if !ok {
		t.Fatal("expected procfs/nvidia-smi mismatch to be reported without failing the whole node")
	}
	if !strings.Contains(message, "2 GPUs") {
		t.Fatalf("message = %q, want count mismatch", message)
	}
}

func TestNvidiaProcDriverStateAllowsHealthyDetectedGPUs(t *testing.T) {
	root := t.TempDir()
	writeNvidiaProcGPUInfo(t, root, "0000:24:00.0", "GPU-a", "580.126.18", "95.02.3c.40.b8", false)

	previous := nvidiaProcGPUInfoRoot
	nvidiaProcGPUInfoRoot = root
	t.Cleanup(func() { nvidiaProcGPUInfoRoot = previous })

	ok, message := nvidiaProcDriverStateOK([]gpuDevice{{ID: "0", UUID: "GPU-a", Name: "RTX4090"}})
	if !ok {
		t.Fatalf("expected healthy driver state, got %q", message)
	}
}

func TestWorkerConfigPreservesPublishingDefaultsFromOlderGateways(t *testing.T) {
	data, err := json.Marshal(newAgentWorkerConfig(bootstrapConfig{}, testWorkerSlot()))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"buildRegistry", "buildRepositoryName", "buildRegistryInsecure"} {
		if strings.Contains(string(data), `"`+key+`"`) {
			t.Fatalf("unavailable bootstrap setting %q overrides embedded defaults", key)
		}
	}
}

func TestWriteWorkerConfigUsesGeeseForWorkspaceStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	slot := testWorkerSlot()

	if err := writeWorkerConfig(path, bootstrapConfig{
		ImageRegistryStore:         reg.S3ImageRegistryStore,
		ImageClipVersion:           1,
		ImageLocalCacheEnabled:     true,
		ImageBuildRegistry:         "build.example.com",
		ImageBuildRepositoryName:   "stage/vm-images",
		ImageBuildRegistryInsecure: false,
		WorkspaceID:                "workspace-one",
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
	for _, key := range []string{"mode", "fsPath", "objectPath"} {
		if _, exists := storageConfig[key]; exists {
			t.Fatalf("obsolete global storage setting %q in worker config", key)
		}
	}
	workspaceStorage := storageConfig["workspaceStorage"].(map[string]any)
	if got := workspaceStorage["baseMountPath"]; got != types.AgentWorkspacePath {
		t.Fatalf("workspace base path = %v, want %q", got, types.AgentWorkspacePath)
	}
	if got := workspaceStorage["defaultStorageMode"]; got != types.StorageModeGeese {
		t.Fatalf("workspace storage mode = %v, want %q", got, types.StorageModeGeese)
	}
	poolConfig := config["worker"].(map[string]any)["pools"].(map[string]any)[slot.PoolName].(map[string]any)
	assertGVisorRuntimeConfig(t, poolConfig["containerRuntimeConfig"].(map[string]any), slot)

	imageConfig := config["imageService"].(map[string]any)
	if got := imageConfig["registryStore"]; got != reg.S3ImageRegistryStore {
		t.Fatalf("image registry store = %v, want %q", got, reg.S3ImageRegistryStore)
	}
	if got := imageConfig["buildRegistry"]; got != "build.example.com" {
		t.Fatalf("image build registry = %v", got)
	}
	if got := imageConfig["buildRepositoryName"]; got != "stage/vm-images" {
		t.Fatalf("image build repository = %v", got)
	}
	if got := imageConfig["buildRegistryInsecure"]; got != false {
		t.Fatalf("image build registry insecure = %v", got)
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
	cacheGlobal := cacheConfig["global"].(map[string]any)
	cacheServer := cacheConfig["server"].(map[string]any)
	cacheClient := cacheConfig["client"].(map[string]any)
	cacheFS := cacheClient["cachefs"].(map[string]any)
	if got := cacheConfig["enabled"]; got != true {
		t.Fatalf("cache enabled = %v, want true", got)
	}
	if got := cacheDisk["mountPath"]; got != types.AgentCachePath {
		t.Fatalf("cache disk mount path = %v, want %q", got, types.AgentCachePath)
	}
	if got := cacheGlobal["defaultLocality"]; got != "workspace-one/private-dev" {
		t.Fatalf("cache default locality = %v, want workspace-scoped private pool", got)
	}
	if got := cacheServer["diskCacheDir"]; got != filepath.Join(types.AgentCachePath, "workspace-one-private-dev", "machine-a") {
		t.Fatalf("cache disk dir = %v, want machine-scoped persistent path", got)
	}
	if got := cacheFS["mountPoint"]; got != types.AgentCacheFSMountPath {
		t.Fatalf("cachefs mount point = %v, want %q", got, types.AgentCacheFSMountPath)
	}
	if got := cacheFS["enabled"]; got != true {
		t.Fatalf("cachefs enabled = %v, want true", got)
	}
}

func writeNvidiaProcGPUInfo(t *testing.T, root, busID, uuid, firmware, videoBIOS string, excluded bool) {
	t.Helper()
	dir := filepath.Join(root, busID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	info := strings.Join([]string{
		"Model: \t\t NVIDIA GeForce RTX 4090",
		"GPU UUID: \t " + uuid,
		"Video BIOS: \t " + videoBIOS,
		"Bus Location: \t " + busID,
		"Device Minor: \t 0",
		"GPU Firmware: \t " + firmware,
		fmt.Sprintf("GPU Excluded:\t %v", map[bool]string{true: "Yes", false: "No"}[excluded]),
	}, "\n")
	if err := os.WriteFile(filepath.Join(dir, "information"), []byte(info), 0644); err != nil {
		t.Fatal(err)
	}
}

// The worker image embeds config.default.yaml, which contains placeholder
// registry credentials (e.g. accessKey "test"). The agent-generated config must
// explicitly clear them so private workers never send placeholder credentials
// to real object storage.
func TestWriteWorkerConfigClearsDefaultRegistryCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	slot := &pb.AgentWorkerSlot{
		PoolName:  "private-dev",
		MachineId: "machine-a",
	}
	if err := writeWorkerConfig(path, bootstrapConfig{}, slot); err != nil {
		t.Fatal(err)
	}

	// Load the config exactly like the worker process does: embedded defaults
	// overlaid with the agent-written CONFIG_PATH file.
	t.Setenv("CONFIG_PATH", path)
	t.Setenv(types.WorkerMinimalConfigEnv, "true")
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		t.Fatal(err)
	}
	config := configManager.GetConfig()
	pool, ok := config.Worker.Pools["private-dev"]
	if !ok {
		t.Fatal("private-dev pool missing from effective worker config")
	}
	if !pool.CRIUEnabled {
		t.Fatal("private-dev pool should enable CRIU")
	}
	if config.Worker.CRIU.Mode != types.CRIUConfigModeNvidia {
		t.Fatalf("worker CRIU mode = %q, want %q", config.Worker.CRIU.Mode, types.CRIUConfigModeNvidia)
	}
	wantEgress := types.EgressConfig{Enabled: true, MaxConnectionsPerContainer: 256}
	if config.Worker.Egress != wantEgress {
		t.Fatalf("agent worker egress = %+v, want %+v", config.Worker.Egress, wantEgress)
	}

	s3 := config.ImageService.Registries.S3
	if s3.BucketName != "" || s3.AccessKey != "" || s3.SecretKey != "" || s3.Endpoint != "" || s3.Region != "" {
		t.Fatalf("placeholder S3 registry credentials leaked into agent worker config: %+v", s3)
	}
	docker := config.ImageService.Registries.Docker
	if docker.Username != "" || docker.Password != "" {
		t.Fatalf("placeholder docker registry credentials leaked into agent worker config: %+v", docker)
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

func TestNvidiaSMISystemBusIDAcceptsUppercaseDomainPrefix(t *testing.T) {
	got, ok := nvidiaSMISystemBusID("0X0000", "00000000:01:00.0")
	if !ok {
		t.Fatal("expected uppercase 0X domain prefix to parse")
	}
	if got != "0000:01:00.0" {
		t.Fatalf("bus id = %q, want %q", got, "0000:01:00.0")
	}
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

func agentVFIOFixture(t *testing.T, count int) []string {
	t.Helper()
	root := t.TempDir()
	previous := agentVFIOPCIRoot
	agentVFIOPCIRoot = filepath.Join(root, "pci")
	t.Cleanup(func() { agentVFIOPCIRoot = previous })
	driver := filepath.Join(root, "drivers", "vfio-pci")
	if err := os.MkdirAll(driver, 0755); err != nil {
		t.Fatal(err)
	}
	var gpus, functions []string
	for i := range count {
		group := filepath.Join(root, "iommu_groups", fmt.Sprint(i))
		if err := os.MkdirAll(filepath.Join(group, "devices"), 0755); err != nil {
			t.Fatal(err)
		}
		for function, class := range []string{"0x030000", "0x040300"} {
			device := fmt.Sprintf("0000:%02x:00.%d", i+1, function)
			path := filepath.Join(agentVFIOPCIRoot, device)
			if err := os.MkdirAll(path, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(path, "class"), []byte(class), 0644); err != nil {
				t.Fatal(err)
			}
			for link, target := range map[string]string{
				filepath.Join(path, "driver"):           driver,
				filepath.Join(path, "iommu_group"):      group,
				filepath.Join(group, "devices", device): path,
			} {
				if err := os.Symlink(target, link); err != nil {
					t.Fatal(err)
				}
			}
			functions = append(functions, device)
			if function == 0 {
				gpus = append(gpus, device)
			}
		}
	}
	t.Setenv(types.MicroVMGPUTypeEnv, "NVIDIA GeForce RTX 4090")
	t.Setenv(types.MicroVMVFIODevicesEnv, strings.Join(functions, ","))
	return gpus
}

func TestVFIOPreflightAccountsForPhysicalGroupsWithoutNvidia(t *testing.T) {
	gpus := agentVFIOFixture(t, 8)
	previous := queryNvidiaGPUDevices
	queryNvidiaGPUDevices = func() ([]byte, error) { panic("VFIO preflight must not query nvidia-smi") }
	t.Cleanup(func() { queryNvidiaGPUDevices = previous })
	preflight := runPreflight(true, "native")
	capacity, checks, schedulable := resolveAgentCapacity(types.AgentJoinOptions{}, preflight)
	if !schedulable || capacity.GPUCount != 8 || !reflect.DeepEqual(capacity.GPUIDs, gpus) {
		t.Fatalf("VFIO capacity = %#v, checks = %#v", capacity, checks)
	}
	for _, gpu := range capacity.GPUs {
		if gpu != types.GPU_RTX4090.String() {
			t.Fatalf("GPU model = %q", gpu)
		}
	}
	for _, check := range checks {
		if strings.HasPrefix(check.Name, "nvidia-") || check.Name == "cuda-driver-init" {
			t.Fatalf("host NVIDIA check in VFIO preflight: %#v", check)
		}
	}
	limited, _, ok := resolveAgentCapacity(types.AgentJoinOptions{GPUIDs: gpus[3]}, preflight)
	if !ok || limited.GPUCount != 1 || !reflect.DeepEqual(limited.GPUIDs, []string{gpus[3]}) {
		t.Fatalf("selected VFIO capacity = %#v", limited)
	}
}

func TestVFIOPreflightFailsClosed(t *testing.T) {
	for _, name := range []string{"missing model", "unknown model", "any model", "no GPU model", "empty inventory", "invalid address", "duplicate address", "missing driver", "missing group", "invalid class", "missing GPU", "two GPUs", "incomplete group"} {
		t.Run(name, func(t *testing.T) {
			gpus := agentVFIOFixture(t, 1)
			gpuPath := filepath.Join(agentVFIOPCIRoot, gpus[0])
			class := filepath.Join(gpuPath, "class")
			switch name {
			case "missing model":
				t.Setenv(types.MicroVMGPUTypeEnv, "")
			case "unknown model":
				t.Setenv(types.MicroVMGPUTypeEnv, "unsupported")
			case "any model":
				t.Setenv(types.MicroVMGPUTypeEnv, "any")
			case "no GPU model":
				t.Setenv(types.MicroVMGPUTypeEnv, "none")
			case "empty inventory":
				t.Setenv(types.MicroVMVFIODevicesEnv, "")
			case "invalid address":
				t.Setenv(types.MicroVMVFIODevicesEnv, "../../elsewhere")
			case "duplicate address":
				t.Setenv(types.MicroVMVFIODevicesEnv, os.Getenv(types.MicroVMVFIODevicesEnv)+","+gpus[0])
			case "missing driver":
				if err := os.Remove(filepath.Join(gpuPath, "driver")); err != nil {
					t.Fatal(err)
				}
			case "missing group":
				if err := os.Remove(filepath.Join(gpuPath, "iommu_group")); err != nil {
					t.Fatal(err)
				}
			case "invalid class":
				if err := os.WriteFile(class, []byte("invalid"), 0644); err != nil {
					t.Fatal(err)
				}
			case "missing GPU":
				if err := os.WriteFile(class, []byte("0x040300"), 0644); err != nil {
					t.Fatal(err)
				}
			case "two GPUs":
				if err := os.WriteFile(filepath.Join(agentVFIOPCIRoot, "0000:01:00.1", "class"), []byte("0x030000"), 0644); err != nil {
					t.Fatal(err)
				}
			case "incomplete group":
				t.Setenv(types.MicroVMVFIODevicesEnv, gpus[0])
			}
			previous := queryNvidiaGPUDevices
			queryNvidiaGPUDevices = func() ([]byte, error) { panic("invalid VFIO inventory must not fall back to nvidia-smi") }
			t.Cleanup(func() { queryNvidiaGPUDevices = previous })
			preflight := runPreflight(true, "native")
			capacity, _, ok := resolveAgentCapacity(types.AgentJoinOptions{}, preflight)
			if ok || capacity.GPUCount != 0 {
				t.Fatalf("invalid VFIO inventory was schedulable: %#v", preflight)
			}
		})
	}
}

func TestDockerMicroVMGPUAssignmentUsesOnlyAuthorizedFunctions(t *testing.T) {
	gpus := agentVFIOFixture(t, 2)
	slot := &pb.AgentWorkerSlot{ContainerRuntime: types.ContainerRuntimeMicroVM.String(), Gpu: "RTX4090", GpuCount: 1, GpuAssignment: gpus[1]}
	args, err := dockerRunArgs("worker", "worker:dev", "", "/tmp/config.json", bootstrapConfig{}, slot, workerDirs{}, workerContainerResourceLimits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, env := range []string{
		types.MicroVMVFIODevicesEnv + "=0000:02:00.0,0000:02:00.1",
		types.MicroVMGPUTypeEnv + "=RTX4090",
		types.WorkerGPUDevicesEnv + "=" + gpus[1],
		types.NvidiaVisibleDevicesEnv + "=void",
	} {
		if !containsArg(args, "-e", env) {
			t.Fatalf("missing %q: %#v", env, args)
		}
	}
	if slices.Contains(args, "--gpus") || strings.Contains(strings.Join(args, " "), "0000:01:00") {
		t.Fatalf("worker received host NVIDIA injection or unassigned functions: %#v", args)
	}
	slot.GpuCount = 0
	slot.GpuAssignment = ""
	args, err = dockerRunArgs("cpu", "worker:dev", "", "/tmp/config.json", bootstrapConfig{}, slot, workerDirs{}, workerContainerResourceLimits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{types.MicroVMVFIODevicesEnv, types.MicroVMGPUTypeEnv, types.WorkerGPUDevicesEnv} {
		if !containsArg(args, "-e", key+"=") {
			t.Fatalf("CPU worker inherited %s", key)
		}
	}
}

func TestDockerVFIOAssignmentRejectsInvalidSlots(t *testing.T) {
	gpus := agentVFIOFixture(t, 2)
	for _, slot := range []*pb.AgentWorkerSlot{
		{ContainerRuntime: "microvm", Gpu: "RTX4090", GpuCount: 1},
		{ContainerRuntime: "microvm", Gpu: "RTX4090", GpuCount: 1, GpuAssignment: "0000:03:00.0"},
		{ContainerRuntime: "microvm", Gpu: "RTX4090", GpuCount: 2, GpuAssignment: gpus[0] + "," + gpus[0]},
		{ContainerRuntime: "microvm", Gpu: "RTX4090", GpuCount: 2, GpuAssignment: gpus[0]},
		{ContainerRuntime: "microvm", Gpu: "H100", GpuCount: 1, GpuAssignment: gpus[0]},
		{ContainerRuntime: "microvm", Gpu: "RTX4090", GpuCount: 1, GpuAssignment: gpus[0], PoolConfig: &pb.AgentPoolRuntimeConfig{GpuVirtualized: true}},
		{ContainerRuntime: "runc", Gpu: "RTX4090", GpuCount: 1, GpuAssignment: gpus[0]},
	} {
		if args, err := dockerRunArgs("worker", "worker:dev", "", "/tmp/config.json", bootstrapConfig{}, slot, workerDirs{}, workerContainerResourceLimits{}); err == nil {
			t.Fatalf("invalid slot %#v produced arguments %#v", slot, args)
		}
	}
}

func TestGPUSelectionRejectsAliasesOfTheSameDevice(t *testing.T) {
	_, _, ok := resolveAgentCapacity(types.AgentJoinOptions{GPUIDs: "GPU-a,0"}, preflightResult{
		gpus: []string{"RTX4090"}, gpuDevices: []gpuDevice{{ID: "0", UUID: "GPU-a", Name: "RTX4090"}}, schedulable: true,
	})
	if ok {
		t.Fatal("duplicate GPU aliases inflated advertised capacity")
	}
}
