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

	server, repo := newCacheRedisRepositoryForTest(t)
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
	require.Contains(t, keys, "cache:coordinator:host:cache-host-default-node-a-path-0:registrations")
	require.Contains(t, keys, "cache:coordinator:host:cache-host-default-node-a-path-0:logical")
	require.Contains(t, keys, "cache:coordinator:host:cache-host-default-node-a-path-0:registration:worker-a")
	require.Contains(t, keys, "cache:coordinator:host:cache-host-default-node-a-path-0:active_registration")
	for _, key := range keys {
		require.NotContains(t, key, "cache:coordinator:index:")
	}

	logicalHost, ok, err := repo.GetCacheLogicalHost(ctx, host.LogicalHostID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, host.LogicalHostID, logicalHost.LogicalHostID)
	require.Empty(t, logicalHost.RegistrationID)
	require.Empty(t, logicalHost.PrivateAddr)
}

func TestCacheRedisRequiredContentIndexStatusAndLocks(t *testing.T) {
	ctx := context.Background()
	server, repo := newCacheRedisRepositoryForTest(t)
	ttl := 2 * time.Second

	require.NoError(t, repo.MarkStubLocalityAccessed(ctx, "default", "workspace-1", "stub-1", ttl))

	stubs, err := repo.ListRecentStubLocalities(ctx, "default", time.Now().Add(-time.Minute), 10)
	require.NoError(t, err)
	require.Len(t, stubs, 1)
	require.Equal(t, "stub-1", stubs[0].StubID)

	require.NoError(t, repo.SetRequiredContentReconciliationStatus(ctx, "default", "workspace-1", "stub-1", "hash-a", "route-a", cache.RequiredContentStatusPresent, "", ttl))
	status, _, _, found, err := repo.GetRequiredContentReconciliationStatus(ctx, "default", "workspace-1", "stub-1", "hash-a", "route-a")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, cache.RequiredContentStatusPresent, status)

	lock, ok, err := repo.AcquireRequiredContentReconciliationLock(ctx, "default", "host-a", "hash-a", ttl)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = repo.AcquireRequiredContentReconciliationLock(ctx, "default", "host-a", "hash-a", ttl)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, lock.Release(ctx))
	_, ok, err = repo.AcquireRequiredContentReconciliationLock(ctx, "default", "host-a", "hash-a", ttl)
	require.NoError(t, err)
	require.True(t, ok)

	server.FastForward(ttl + time.Second)
	_, _, _, found, err = repo.GetRequiredContentReconciliationStatus(ctx, "default", "workspace-1", "stub-1", "hash-a", "route-a")
	require.NoError(t, err)
	require.False(t, found)
	stubs, err = repo.ListRecentStubLocalities(ctx, "default", time.Now().Add(-time.Minute), 10)
	require.NoError(t, err)
	require.Empty(t, stubs)
}

func newCacheRedisRepositoryForTest(t *testing.T) (*miniredis.Miniredis, *CacheRedisRepository) {
	t.Helper()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	return server, NewCacheRedisRepository(rdb)
}
