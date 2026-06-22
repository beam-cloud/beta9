package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestNormalizeCacheConfigAppliesPoolDiskOverrides(t *testing.T) {
	config := types.AppConfig{
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:     true,
				HostPath:    types.AgentCachePath,
				MountPath:   types.AgentCachePath,
				MaxUsagePct: 0.95,
			},
		},
	}
	poolConfig := types.WorkerPoolConfig{
		Cache: types.WorkerPoolCacheConfig{
			Disk: types.WorkerPoolCacheDiskConfig{
				HostPath:    "/mnt/a100-cache",
				MountPath:   "/cache-disk",
				MaxUsagePct: 0.85,
			},
		},
	}

	got := normalizeCacheConfig(config, poolConfig, "node-a", "gpu")

	require.Equal(t, "/mnt/a100-cache", got.Disk.HostPath)
	require.Equal(t, "/cache-disk", got.Disk.MountPath)
	require.Equal(t, 0.85, got.Disk.MaxUsagePct)
	require.Equal(t, filepath.Join("/cache-disk", "gpu", "node-a"), got.Server.DiskCacheDir)
	require.Equal(t, 0.85, got.Server.DiskCacheMaxUsagePct)
}

func TestNormalizeCacheConfigDefaultsTopHostsToThree(t *testing.T) {
	config := types.AppConfig{
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:   true,
				MountPath: types.AgentCachePath,
				HostPath:  types.AgentCachePath,
			},
		},
	}

	got := normalizeCacheConfig(config, types.WorkerPoolConfig{}, "node-a", "default")

	require.Equal(t, 3, got.Client.NTopHosts)
}

func TestWorkerCacheManagerDisabledWhenPoolDiskCacheDisabled(t *testing.T) {
	disabled := false
	manager := &WorkerCacheManager{
		config: types.AppConfig{
			Worker: types.WorkerConfig{CacheEnabled: true},
			Cache: cache.Config{
				Enabled: true,
				Disk:    cache.DiskConfig{Enabled: true},
			},
		},
		poolConfig: types.WorkerPoolConfig{
			Cache: types.WorkerPoolCacheConfig{
				Disk: types.WorkerPoolCacheDiskConfig{Enabled: &disabled},
			},
		},
	}

	require.False(t, manager.enabled())
}

func TestWorkerCacheManagerDrainStopsRegistrationLoopOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancelCount := 0
	manager := &WorkerCacheManager{
		registrationCancel: func() {
			cancelCount++
			cancel()
		},
	}

	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		<-ctx.Done()
	}()

	require.NoError(t, manager.Drain())
	require.NoError(t, manager.Drain())
	require.Equal(t, 1, cancelCount)
}

func TestCacheServerLockAllowsSingleNodeLocalOwner(t *testing.T) {
	cacheDir := t.TempDir()

	first, acquired, err := acquireCacheServerLock(cacheDir)
	require.NoError(t, err)
	require.True(t, acquired)

	second, acquired, err := acquireCacheServerLock(cacheDir)
	require.NoError(t, err)
	require.False(t, acquired)
	require.Nil(t, second)

	require.NoError(t, releaseCacheServerLock(first))

	second, acquired, err = acquireCacheServerLock(cacheDir)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, releaseCacheServerLock(second))
}

func TestEmbeddedWorkerSkipsCacheServerWhenDaemonSetMarkerFresh(t *testing.T) {
	t.Setenv(types.CacheServerOnlyEnv, "false")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := t.TempDir()
	require.NoError(t, writeCacheServerDaemonSetMarker(cacheDir, "cache-server-node", "pod-a"))

	config := testCacheManagerConfig(cacheDir)
	manager := NewWorkerCacheManager(ctx, config, types.WorkerPoolConfig{}, nil, nil, nil, "worker-a", "default", "127.0.0.1")
	cacheConfig := normalizeCacheConfig(config, types.WorkerPoolConfig{}, "single-node", "test")
	started, err := manager.nodeCacheServer(cacheConfig, "cache-host").Start()

	require.NoError(t, err)
	require.False(t, started)
	require.False(t, manager.runningCacheServer())
}

func TestEmbeddedWorkerYieldsCacheServerToDaemonSetMarker(t *testing.T) {
	t.Setenv(types.CacheServerOnlyEnv, "false")
	t.Setenv(types.CacheNodeEnv, "single-node")
	t.Setenv(types.CacheLocalityEnv, "test")
	t.Setenv(types.WorkerCacheAdvertiseAddrEnv, "127.0.0.1")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	repoClient, repo, cleanup := startTestCacheCoordinator(t)
	defer cleanup()

	cacheDir := t.TempDir()
	config := testCacheManagerConfig(cacheDir)
	manager := NewWorkerCacheManager(ctx, config, types.WorkerPoolConfig{}, repoClient, nil, nil, "worker-a", "default", "127.0.0.1")
	client, err := manager.Start()
	require.NoError(t, err)
	require.NotNil(t, client)
	defer func() { _ = manager.Close() }()
	require.DirExists(t, filepath.Join(cacheDir, "checkpoints"))

	require.Eventually(t, func() bool {
		return manager.runningCacheServer() && len(repo.activeHosts()) == 1
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, writeCacheServerDaemonSetMarker(cacheDir, "cache-server-single-node", "pod-a"))

	require.Eventually(t, func() bool {
		return !manager.runningCacheServer() && repo.unregisterCalled("worker-a")
	}, 5*time.Second, 50*time.Millisecond)
}

func TestNodeCacheServerWatchStartsImmediately(t *testing.T) {
	t.Setenv(types.CacheServerOnlyEnv, "false")
	t.Setenv(types.CacheNodeEnv, "single-node")
	t.Setenv(types.CacheLocalityEnv, "test")
	t.Setenv(types.WorkerCacheAdvertiseAddrEnv, "127.0.0.1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	repoClient, repo, cleanup := startTestCacheCoordinator(t)
	defer cleanup()

	cacheDir := t.TempDir()
	config := testCacheManagerConfig(cacheDir)
	config.Cache.Coordinator.HostWatchIntervalSeconds = 3600
	manager := NewWorkerCacheManager(ctx, config, types.WorkerPoolConfig{}, repoClient, nil, nil, "worker-a", "default", "127.0.0.1")
	manager.metadataStore = newGatewayCacheMetadataStore(repoClient)
	defer func() { _ = manager.Close() }()

	cacheConfig := normalizeCacheConfig(config, types.WorkerPoolConfig{}, "single-node", "test")
	cacheConfig.Global.ServerPort = uint(freeTCPPort(t))
	manager.nodeCacheServer(cacheConfig, "cache-host").Watch()

	require.Eventually(t, func() bool {
		return manager.runningCacheServer() && len(repo.activeHosts()) == 1
	}, time.Second, 10*time.Millisecond)
}

func TestCacheServerDaemonSetMarkerFreshUsesTTL(t *testing.T) {
	cacheDir := t.TempDir()
	require.False(t, cacheServerDaemonSetMarkerFresh(cacheDir, time.Second))

	require.NoError(t, writeCacheServerDaemonSetMarker(cacheDir, "cache-server-node", "pod-a"))
	require.True(t, cacheServerDaemonSetMarkerFresh(cacheDir, time.Second))

	stale := time.Now().Add(-2 * time.Second)
	require.NoError(t, os.Chtimes(cacheServerDaemonSetMarkerPath(cacheDir), stale, stale))
	require.False(t, cacheServerDaemonSetMarkerFresh(cacheDir, time.Second))
}

func TestCacheServerDaemonSetMarkerLoopStopsOnClose(t *testing.T) {
	t.Setenv(types.CacheServerOnlyEnv, "true")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cacheDir := t.TempDir()
	config := testCacheManagerConfig(cacheDir)
	manager := NewWorkerCacheManager(ctx, config, types.WorkerPoolConfig{}, nil, nil, nil, "cache-server-single-node", "default", "127.0.0.1")
	cacheConfig := normalizeCacheConfig(config, types.WorkerPoolConfig{}, "single-node", "test")
	manager.nodeCacheServer(cacheConfig, "cache-host").StartDaemonSetPresence()

	require.Eventually(t, func() bool {
		return cacheServerDaemonSetMarkerFresh(cacheDir, time.Second)
	}, time.Second, 10*time.Millisecond)

	done := make(chan error, 1)
	go func() {
		done <- manager.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("cache server daemonset marker loop did not stop on close")
	}
}

func TestWorkerCacheManagersUseSingleNodeLocalServerAndStandbyTakeover(t *testing.T) {
	t.Setenv(types.CacheNodeEnv, "single-node")
	t.Setenv(types.CacheLocalityEnv, "test")
	t.Setenv(types.WorkerCacheAdvertiseAddrEnv, "127.0.0.1")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	repoClient, repo, cleanup := startTestCacheCoordinator(t)
	defer cleanup()

	cacheDir := t.TempDir()
	config := testCacheManagerConfig(cacheDir)
	managers := []*WorkerCacheManager{
		NewWorkerCacheManager(ctx, config, types.WorkerPoolConfig{}, repoClient, nil, nil, "worker-a", "default", "127.0.0.1"),
		NewWorkerCacheManager(ctx, config, types.WorkerPoolConfig{}, repoClient, nil, nil, "worker-b", "default", "127.0.0.1"),
		NewWorkerCacheManager(ctx, config, types.WorkerPoolConfig{}, repoClient, nil, nil, "worker-c", "default", "127.0.0.1"),
	}

	for _, manager := range managers {
		client, err := manager.Start()
		require.NoError(t, err)
		require.NotNil(t, client)
	}
	defer func() {
		for _, manager := range managers {
			_ = manager.Close()
		}
	}()

	require.Eventually(t, func() bool {
		return countRunningCacheServers(managers) == 1 && len(repo.activeHosts()) == 1
	}, 5*time.Second, 50*time.Millisecond)

	active := runningCacheServers(managers)[0]
	require.Equal(t, "worker-a", active.workerID)
	logicalHostID := active.registration.logicalHostID
	activeRegistrationID := active.registration.registrationID

	content := []byte("node-local-cache-content-survives-worker-churn")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	chunks := make(chan []byte, 1)
	chunks <- content
	close(chunks)
	storedHash, err := active.client.StoreContent(chunks, expectedHash, struct{ RoutingKey string }{RoutingKey: expectedHash})
	require.NoError(t, err)
	require.Equal(t, expectedHash, storedHash)

	require.NoError(t, active.Close())

	standbys := managers[1:]
	require.Eventually(t, func() bool {
		activeHosts := repo.activeHosts()
		if countRunningCacheServers(standbys) != 1 || len(activeHosts) != 1 {
			return false
		}
		for _, host := range activeHosts {
			return host.GetLogicalHostId() == logicalHostID && host.GetRegistrationId() != activeRegistrationID
		}
		return false
	}, 5*time.Second, 50*time.Millisecond)

	newActive := runningCacheServers(standbys)[0]
	require.Equal(t, logicalHostID, newActive.registration.logicalHostID)

	readBack := make([]byte, len(content))
	n, err := newActive.client.ReadContentInto(ctx, expectedHash, 0, readBack, cache.ClientOptions{RoutingKey: expectedHash})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, readBack)
}

func TestCacheLogicalHostIDDeduplicatesSharedNodeCachePath(t *testing.T) {
	first := cacheLogicalHostID("default", "node-a", filepath.Join(types.AgentCachePath, "default", "node-a"))
	second := cacheLogicalHostID("default", "node-a", filepath.Join(types.AgentCachePath, "default", "node-a"))
	otherPath := cacheLogicalHostID("default", "node-a", "/mnt/cache/default/node-a")

	require.Equal(t, first, second)
	require.NotEqual(t, first, otherPath)
}

func TestCacheLogicalHostIDIgnoresWorkerPool(t *testing.T) {
	defaultPool := cacheLogicalHostID("default", "node-a", filepath.Join(types.AgentCachePath, "default", "node-a"))
	buildPool := cacheLogicalHostID("default", "node-a", filepath.Join(types.AgentCachePath, "default", "node-a"))

	require.Equal(t, defaultPool, buildPool)
}

func TestCachePlacementIdentityUsesHostPathForDefaultDiskDir(t *testing.T) {
	base := types.AppConfig{
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:     true,
				HostPath:    "/mnt/cache",
				MountPath:   "/cache-a",
				MaxUsagePct: 0.95,
			},
		},
	}
	alternateMount := base
	alternateMount.Cache.Disk.MountPath = "/cache-b"

	first := normalizeCacheConfig(base, types.WorkerPoolConfig{}, "node-a", "default")
	second := normalizeCacheConfig(alternateMount, types.WorkerPoolConfig{}, "node-a", "default")
	firstIdentity := cachePlacementIdentityPath(base, first)
	secondIdentity := cachePlacementIdentityPath(alternateMount, second)

	require.Equal(t, filepath.Join("/cache-a", "default", "node-a"), first.Server.DiskCacheDir)
	require.Equal(t, filepath.Join("/cache-b", "default", "node-a"), second.Server.DiskCacheDir)
	require.Equal(t, filepath.Join("/mnt/cache", "default", "node-a"), firstIdentity)
	require.Equal(t, firstIdentity, secondIdentity)
	require.Equal(t,
		cacheLogicalHostID("default", "node-a", firstIdentity),
		cacheLogicalHostID("default", "node-a", secondIdentity),
	)
}

func TestCachePlacementIdentityChangesWithHostPath(t *testing.T) {
	base := types.AppConfig{
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:     true,
				HostPath:    "/mnt/cache-a",
				MountPath:   "/cache",
				MaxUsagePct: 0.95,
			},
		},
	}
	otherHostPath := base
	otherHostPath.Cache.Disk.HostPath = "/mnt/cache-b"

	firstIdentity := cachePlacementIdentityPath(base, normalizeCacheConfig(base, types.WorkerPoolConfig{}, "node-a", "default"))
	secondIdentity := cachePlacementIdentityPath(otherHostPath, normalizeCacheConfig(otherHostPath, types.WorkerPoolConfig{}, "node-a", "default"))

	require.NotEqual(t, firstIdentity, secondIdentity)
	require.NotEqual(t,
		cacheLogicalHostID("default", "node-a", firstIdentity),
		cacheLogicalHostID("default", "node-a", secondIdentity),
	)
}

func TestCachePlacementIdentityKeepsExplicitDiskCacheDir(t *testing.T) {
	config := types.AppConfig{
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:     true,
				HostPath:    "/mnt/cache",
				MountPath:   "/cache",
				MaxUsagePct: 0.95,
			},
			Server: cache.ServerConfig{
				DiskCacheDir: "/cache/custom/../explicit",
			},
		},
	}

	normalized := normalizeCacheConfig(config, types.WorkerPoolConfig{}, "node-a", "default")
	identity := cachePlacementIdentityPath(config, normalized)

	require.Equal(t, filepath.Clean("/cache/explicit"), normalized.Server.DiskCacheDir)
	require.Equal(t, filepath.Clean("/cache/explicit"), identity)
	require.NotEqual(t, filepath.Clean("/mnt/cache/explicit"), identity)
}

func testCacheManagerConfig(cacheDir string) types.AppConfig {
	return types.AppConfig{
		Worker: types.WorkerConfig{
			CacheEnabled: true,
			HostNetwork:  true,
		},
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:     true,
				MountPath:   cacheDir,
				HostPath:    cacheDir,
				MaxUsagePct: 0.95,
			},
			Coordinator: cache.CoordinatorConfig{
				RegistrationTTLSeconds:   30,
				HeartbeatIntervalSeconds: 1,
				HostWatchIntervalSeconds: 1,
			},
			Server: cache.ServerConfig{
				DiskCacheDir:         cacheDir,
				DiskCacheMaxUsagePct: 0.95,
				PageSizeBytes:        4,
				ObjectTtlS:           300,
			},
			Client: cache.ClientConfig{
				NTopHosts:             3,
				MaxGetContentAttempts: 1,
			},
			Global: cache.GlobalConfig{
				DefaultLocality:         "test",
				DiscoveryIntervalS:      1,
				MaxDiscoveryConcurrency: 4,
				HostMonitorIntervalS:    1,
				GRPCDialTimeoutS:        1,
				GRPCMessageSizeBytes:    4 * 1024 * 1024,
			},
		},
	}
}

func countRunningCacheServers(managers []*WorkerCacheManager) int {
	return len(runningCacheServers(managers))
}

func runningCacheServers(managers []*WorkerCacheManager) []*WorkerCacheManager {
	running := make([]*WorkerCacheManager, 0, len(managers))
	for _, manager := range managers {
		if manager.runningCacheServer() {
			running = append(running, manager)
		}
	}
	return running
}

type testCacheCoordinator struct {
	pb.UnimplementedWorkerRepositoryServiceServer
	mu          sync.Mutex
	hosts       map[string]*pb.CacheCoordinatorHost
	unregisters map[string]bool
}

func startTestCacheCoordinator(t *testing.T) (pb.WorkerRepositoryServiceClient, *testCacheCoordinator, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	coordinator := &testCacheCoordinator{
		hosts:       make(map[string]*pb.CacheCoordinatorHost),
		unregisters: make(map[string]bool),
	}
	pb.RegisterWorkerRepositoryServiceServer(server, coordinator)
	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	}
	return pb.NewWorkerRepositoryServiceClient(conn), coordinator, cleanup
}

func freeTCPPort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	return addr.Port
}

func (s *testCacheCoordinator) RegisterCacheHost(ctx context.Context, req *pb.RegisterCacheHostRequest) (*pb.RegisterCacheHostResponse, error) {
	if req == nil || req.Host == nil {
		return &pb.RegisterCacheHostResponse{Ok: false, ErrorMsg: "host is required"}, nil
	}

	host := proto.Clone(req.Host).(*pb.CacheCoordinatorHost)
	s.mu.Lock()
	s.hosts[host.GetLogicalHostId()] = host
	s.mu.Unlock()
	return &pb.RegisterCacheHostResponse{Ok: true}, nil
}

func (s *testCacheCoordinator) UnregisterCacheHost(ctx context.Context, req *pb.UnregisterCacheHostRequest) (*pb.UnregisterCacheHostResponse, error) {
	if req == nil {
		return &pb.UnregisterCacheHostResponse{Ok: false, ErrorMsg: "request is required"}, nil
	}

	s.mu.Lock()
	s.unregisters[req.GetRegistrationId()] = true
	if host := s.hosts[req.GetLogicalHostId()]; host != nil && host.GetRegistrationId() == req.GetRegistrationId() {
		delete(s.hosts, req.GetLogicalHostId())
	}
	s.mu.Unlock()
	return &pb.UnregisterCacheHostResponse{Ok: true}, nil
}

func (s *testCacheCoordinator) ListCacheHosts(ctx context.Context, req *pb.ListCacheHostsRequest) (*pb.ListCacheHostsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hosts := make([]*pb.CacheCoordinatorHost, 0, len(s.hosts))
	for _, host := range s.hosts {
		if req != nil && req.GetLocality() != "" && host.GetLocality() != req.GetLocality() {
			continue
		}
		hosts = append(hosts, proto.Clone(host).(*pb.CacheCoordinatorHost))
	}
	return &pb.ListCacheHostsResponse{Ok: true, Hosts: hosts}, nil
}

func (s *testCacheCoordinator) activeHosts() map[string]*pb.CacheCoordinatorHost {
	s.mu.Lock()
	defer s.mu.Unlock()

	hosts := make(map[string]*pb.CacheCoordinatorHost, len(s.hosts))
	for id, host := range s.hosts {
		hosts[id] = proto.Clone(host).(*pb.CacheCoordinatorHost)
	}
	return hosts
}

func (s *testCacheCoordinator) unregisterCalled(registrationID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.unregisters[registrationID]
}

func TestCacheAdvertiseHost(t *testing.T) {
	tests := []struct {
		name     string
		podAddr  string
		podIP    string
		expected string
	}{
		{
			name:     "pod addr is an ip",
			podAddr:  "10.0.0.5",
			expected: "10.0.0.5",
		},
		{
			name:     "loopback pod addr falls back to server discovery",
			podAddr:  "127.0.0.1",
			expected: "",
		},
		{
			name:     "loopback pod ip falls back to server discovery",
			podIP:    "127.0.0.1",
			expected: "",
		},
		{
			name:     "tailscale hostname falls back to pod ip",
			podAddr:  "machine-6a392694.tailc480d.ts.net",
			podIP:    "10.0.0.6",
			expected: "10.0.0.6",
		},
		{
			name:     "hostname without pod ip yields empty (server discovers private ip)",
			podAddr:  "machine-6a392694.tailc480d.ts.net",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(types.WorkerPodIPEnv, tc.podIP)
			m := &WorkerCacheManager{podAddr: tc.podAddr}
			require.Equal(t, tc.expected, m.cacheAdvertiseHost())
		})
	}
}

func TestCacheWorkerInstanceIDPrefersMachineAndSkipsLoopback(t *testing.T) {
	t.Setenv(types.WorkerPodHostEnv, "127.0.0.1")
	t.Setenv(types.WorkerPodIPEnv, "127.0.0.1")
	t.Setenv(types.WorkerMachineEnv, "machine-a")
	t.Setenv(types.WorkerHostnameEnv, "container-host")

	require.Equal(t, "machine-a", cacheWorkerInstanceID("worker-a"))
}

func TestCacheWorkerInstanceIDUsesReachablePodIP(t *testing.T) {
	t.Setenv(types.WorkerPodIPEnv, "10.0.0.8")
	t.Setenv(types.WorkerPodHostEnv, "127.0.0.1")

	require.Equal(t, "10.0.0.8", cacheWorkerInstanceID("worker-a"))
}

func TestBindAddr(t *testing.T) {
	tests := []struct {
		name string
		// env
		hostNetwork    string // CACHE_HOST_NETWORK
		cacheServer    string // CACHE_SERVER_ONLY
		envPort        string // CACHE_SERVER_PORT
		configPort     uint   // raw config (m.config.Cache.Global.ServerPort)
		normalizedPort uint   // resolved port passed to bindAddr
		expected       string
	}{
		{
			name:           "daemonset host-network binds fixed port",
			hostNetwork:    "true",
			cacheServer:    "true",
			normalizedPort: 2050,
			expected:       ":2050",
		},
		{
			name:           "embedded host-network binds default fixed port",
			hostNetwork:    "true",
			cacheServer:    "false",
			normalizedPort: 2050,
			expected:       ":2050",
		},
		{
			name:           "embedded host-network honors config port",
			hostNetwork:    "true",
			cacheServer:    "false",
			configPort:     9000,
			normalizedPort: 9000,
			expected:       ":9000",
		},
		{
			name:           "embedded host-network honors CACHE_SERVER_PORT env",
			hostNetwork:    "true",
			cacheServer:    "false",
			envPort:        "9001",
			normalizedPort: 9001,
			expected:       ":9001",
		},
		{
			name:           "non-host-network binds fixed port",
			hostNetwork:    "false",
			cacheServer:    "false",
			normalizedPort: 2050,
			expected:       ":2050",
		},
		{
			name:           "zero port falls back to default",
			hostNetwork:    "true",
			cacheServer:    "true",
			normalizedPort: 0,
			expected:       ":2050",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(types.CacheHostNetworkEnv, tc.hostNetwork)
			t.Setenv(types.CacheServerOnlyEnv, tc.cacheServer)
			t.Setenv(types.CacheServerPortEnv, tc.envPort)

			m := &WorkerCacheManager{
				config: types.AppConfig{
					Cache: cache.Config{
						Global: cache.GlobalConfig{ServerPort: tc.configPort},
					},
				},
			}
			cacheConfig := cache.Config{Global: cache.GlobalConfig{ServerPort: tc.normalizedPort}}

			require.Equal(t, tc.expected, m.bindAddr(cacheConfig))
		})
	}
}

func TestCacheBindAddrInUse(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)

	second, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", port))
	if err == nil {
		_ = second.Close()
		t.Fatal("expected duplicate listener bind to fail")
	}
	require.True(t, cacheBindAddrInUse(err))

	t.Setenv(types.CacheHostNetworkEnv, "true")
	t.Setenv(types.CacheServerOnlyEnv, "false")
	require.True(t, (&WorkerCacheManager{}).allowEphemeralCachePortFallback(cache.Config{
		Global: cache.GlobalConfig{ServerPort: cacheDefaultServerPort},
	}, err))

	require.False(t, (&WorkerCacheManager{}).allowEphemeralCachePortFallback(cache.Config{
		Global: cache.GlobalConfig{ServerPort: 9000},
	}, err))

	t.Setenv(types.CacheServerPortEnv, "2050")
	require.False(t, (&WorkerCacheManager{}).allowEphemeralCachePortFallback(cache.Config{
		Global: cache.GlobalConfig{ServerPort: cacheDefaultServerPort},
	}, err))
	t.Setenv(types.CacheServerPortEnv, "")

	t.Setenv(types.CacheServerOnlyEnv, "true")
	require.False(t, (&WorkerCacheManager{}).allowEphemeralCachePortFallback(cache.Config{
		Global: cache.GlobalConfig{ServerPort: cacheDefaultServerPort},
	}, err))
}
