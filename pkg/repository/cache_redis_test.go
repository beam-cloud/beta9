package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCacheRedisRepositoryUsesReadableCoordinatorHostIndexKeys(t *testing.T) {
	ctx := context.Background()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	repo := NewCacheRedisRepository(rdb)
	host := cache.CoordinatorHost{
		LogicalHostID:    "cache-host-default-node-a-path-0",
		RegistrationID:   "worker-a",
		PoolName:         "default",
		Locality:         "default",
		NodeID:           "node-a",
		CachePathID:      "path",
		Addr:             "10.0.0.1:2049",
		PrivateAddr:      "10.0.0.1:2049",
		CapacityUsagePct: 0,
	}

	require.NoError(t, repo.SetCacheRegistration(ctx, host, 30*time.Second))
	require.NoError(t, repo.SetActiveCacheRegistration(ctx, host.LogicalHostID, host.RegistrationID, 30*time.Second))

	keys := server.Keys()
	require.Contains(t, keys, "cache:coordinator:host_index:default:default")
	require.Contains(t, keys, "cache:coordinator:host_index_by_locality:default")
	require.Contains(t, keys, "cache:coordinator:host:cache-host-default-node-a-path-0:registrations")
	require.Contains(t, keys, "cache:coordinator:host:cache-host-default-node-a-path-0:logical")
	require.Contains(t, keys, "cache:coordinator:host:cache-host-default-node-a-path-0:registration:worker-a")
	require.Contains(t, keys, "cache:coordinator:host:cache-host-default-node-a-path-0:active_registration")
	for _, key := range keys {
		require.NotContains(t, key, "cache:coordinator:index:")
	}
	for _, key := range []string{
		cacheCoordinatorIndexKey(host.PoolName, host.Locality),
		cacheCoordinatorLocalityIndexKey(host.Locality),
		cacheCoordinatorRegistrationSetKey(host.LogicalHostID),
	} {
		require.Positive(t, server.TTL(key), "coordinator set must expire after its last heartbeat: %s", key)
	}

	logicalHost, ok, err := repo.GetCacheLogicalHost(ctx, host.LogicalHostID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, host.LogicalHostID, logicalHost.LogicalHostID)
	require.Empty(t, logicalHost.RegistrationID)
	require.Empty(t, logicalHost.PrivateAddr)
}

func TestCacheRedisRepositoryListsLocalityHostsWithoutScanningPoolIndexes(t *testing.T) {
	ctx := context.Background()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	repo := NewCacheRedisRepository(rdb)
	host := cache.CoordinatorHost{
		LogicalHostID:  "cache-host-default-node-a-path-0",
		RegistrationID: "worker-a",
		PoolName:       "default",
		Locality:       "default",
		NodeID:         "node-a",
		CachePathID:    "path",
		PrivateAddr:    "10.0.0.1:2049",
	}

	require.NoError(t, repo.SetCacheRegistration(ctx, host, 30*time.Second))
	require.NoError(t, rdb.SAdd(ctx, cacheCoordinatorIndexKey("stale-pool", "default"), "stale-logical-host").Err())

	hosts, err := repo.ListCacheLogicalHosts(ctx, "", "default")

	require.NoError(t, err)
	require.Equal(t, []string{host.LogicalHostID}, hosts)
}

func TestCacheRedisRepositoryRemoveNodeDeletesPersistentCoordinatorState(t *testing.T) {
	ctx := context.Background()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	coordinator := cache.NewCoordinator(NewCacheRedisRepository(rdb))
	target := cache.CoordinatorHost{
		LogicalHostID:  "cache-host-workspace-pool-machine-a-path",
		RegistrationID: "worker-a",
		PoolName:       "pool",
		Locality:       "workspace/pool",
		NodeID:         "machine-a",
		CachePathID:    "path",
		PrivateAddr:    "10.0.0.1:2049",
	}
	other := cache.CoordinatorHost{
		LogicalHostID:  "cache-host-workspace-pool-machine-b-path",
		RegistrationID: "worker-b",
		PoolName:       "pool",
		Locality:       "workspace/pool",
		NodeID:         "machine-b",
		CachePathID:    "path",
		PrivateAddr:    "10.0.0.2:2049",
	}
	require.NoError(t, coordinator.RegisterHost(ctx, target, 30*time.Second))
	require.NoError(t, coordinator.RegisterHost(ctx, other, 30*time.Second))

	require.NoError(t, coordinator.RemoveNode(ctx, target.PoolName, target.Locality, target.NodeID))

	for _, key := range []string{
		cacheCoordinatorRegistrationSetKey(target.LogicalHostID),
		cacheCoordinatorLogicalHostKey(target.LogicalHostID),
		cacheCoordinatorRegistrationKey(target.LogicalHostID, target.RegistrationID),
		cacheCoordinatorActiveRegistrationKey(target.LogicalHostID),
	} {
		require.False(t, server.Exists(key), "released node key survived: %s", key)
	}
	poolHosts, err := rdb.SMembers(ctx, cacheCoordinatorIndexKey(target.PoolName, target.Locality)).Result()
	require.NoError(t, err)
	require.Equal(t, []string{other.LogicalHostID}, poolHosts)
	localityHosts, err := rdb.SMembers(ctx, cacheCoordinatorLocalityIndexKey(target.Locality)).Result()
	require.NoError(t, err)
	require.Equal(t, []string{other.LogicalHostID}, localityHosts)
}
