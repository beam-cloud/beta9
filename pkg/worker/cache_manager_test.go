package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
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
)

func TestNormalizeCacheConfigAppliesPoolDiskOverrides(t *testing.T) {
	config := types.AppConfig{
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:     true,
				HostPath:    "/var/lib/beta9/cache",
				MountPath:   "/var/lib/beta9/cache",
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
				MountPath: "/var/lib/beta9/cache",
				HostPath:  "/var/lib/beta9/cache",
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

func TestWorkerCacheManagersUseSingleNodeLocalServerAndStandbyTakeover(t *testing.T) {
	t.Setenv("CACHE_NODE_ID", "single-node")
	t.Setenv("CACHE_LOCALITY", "test")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	repoClient, repo, cleanup := startTestCacheCoordinator(t)
	defer cleanup()

	cacheDir := t.TempDir()
	config := testCacheManagerConfig(cacheDir)
	managers := []*WorkerCacheManager{
		NewWorkerCacheManager(ctx, config, types.WorkerPoolConfig{}, repoClient, "worker-a", "default", "127.0.0.1"),
		NewWorkerCacheManager(ctx, config, types.WorkerPoolConfig{}, repoClient, "worker-b", "default", "127.0.0.1"),
		NewWorkerCacheManager(ctx, config, types.WorkerPoolConfig{}, repoClient, "worker-c", "default", "127.0.0.1"),
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
			return host.GetLogicalHostId() == logicalHostID && host.GetRegistrationId() != active.registration.registrationID
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
	first := cacheLogicalHostID("default", "node-a", "/var/lib/beta9/cache/default/node-a")
	second := cacheLogicalHostID("default", "node-a", "/var/lib/beta9/cache/default/node-a")
	otherPath := cacheLogicalHostID("default", "node-a", "/mnt/cache/default/node-a")

	require.Equal(t, first, second)
	require.NotEqual(t, first, otherPath)
}

func TestCacheLogicalHostIDIgnoresWorkerPool(t *testing.T) {
	defaultPool := cacheLogicalHostID("default", "node-a", "/var/lib/beta9/cache/default/node-a")
	buildPool := cacheLogicalHostID("default", "node-a", "/var/lib/beta9/cache/default/node-a")

	require.Equal(t, defaultPool, buildPool)
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
	mu    sync.Mutex
	hosts map[string]*pb.CacheCoordinatorHost
}

func startTestCacheCoordinator(t *testing.T) (pb.WorkerRepositoryServiceClient, *testCacheCoordinator, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	coordinator := &testCacheCoordinator{hosts: make(map[string]*pb.CacheCoordinatorHost)}
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

func (s *testCacheCoordinator) RegisterCacheHost(ctx context.Context, req *pb.RegisterCacheHostRequest) (*pb.RegisterCacheHostResponse, error) {
	if req == nil || req.Host == nil {
		return &pb.RegisterCacheHostResponse{Ok: false, ErrorMsg: "host is required"}, nil
	}

	host := *req.Host
	s.mu.Lock()
	s.hosts[host.GetLogicalHostId()] = &host
	s.mu.Unlock()
	return &pb.RegisterCacheHostResponse{Ok: true}, nil
}

func (s *testCacheCoordinator) UnregisterCacheHost(ctx context.Context, req *pb.UnregisterCacheHostRequest) (*pb.UnregisterCacheHostResponse, error) {
	if req == nil {
		return &pb.UnregisterCacheHostResponse{Ok: false, ErrorMsg: "request is required"}, nil
	}

	s.mu.Lock()
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
		copyHost := *host
		hosts = append(hosts, &copyHost)
	}
	return &pb.ListCacheHostsResponse{Ok: true, Hosts: hosts}, nil
}

func (s *testCacheCoordinator) activeHosts() map[string]*pb.CacheCoordinatorHost {
	s.mu.Lock()
	defer s.mu.Unlock()

	hosts := make(map[string]*pb.CacheCoordinatorHost, len(s.hosts))
	for id, host := range s.hosts {
		copyHost := *host
		hosts[id] = &copyHost
	}
	return hosts
}
