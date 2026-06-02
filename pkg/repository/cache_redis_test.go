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

func TestCacheRedisRequiredContentDedupeTTLStatusAndLocks(t *testing.T) {
	ctx := context.Background()
	server, repo := newCacheRedisRepositoryForTest(t)
	ttl := 2 * time.Second

	require.NoError(t, repo.MarkStubLocalityAccessed(ctx, "default", "workspace-1", "stub-1", ttl))
	base := cache.RequiredContentItem{
		Locality:     "default",
		WorkspaceID:  "workspace-1",
		StubID:       "stub-1",
		Kind:         cache.RequiredContentKindVolume,
		Hash:         "hash-a",
		ExpectedHash: "hash-a",
		SizeBytes:    64 * 1024 * 1024,
		Source: cache.RequiredContentSource{
			Type:       cache.RequiredContentSourceS3,
			Descriptor: "volume object",
		},
	}
	first := base
	first.RoutingKey = "route-a"
	second := base
	second.RoutingKey = "route-b"

	require.NoError(t, repo.UpsertRequiredContent(ctx, first, ttl))
	require.NoError(t, repo.UpsertRequiredContent(ctx, first, ttl))
	require.NoError(t, repo.UpsertRequiredContent(ctx, second, ttl))

	stubs, err := repo.ListRecentStubLocalities(ctx, "default", time.Now().Add(-time.Minute), 10)
	require.NoError(t, err)
	require.Len(t, stubs, 1)
	require.Equal(t, "stub-1", stubs[0].StubID)

	items, err := repo.ListRequiredContentForStub(ctx, "default", "workspace-1", "stub-1", 10)
	require.NoError(t, err)
	require.Len(t, items, 2)
	require.Equal(t, int64(2), requiredContentItemByRoutingKey(t, items, "route-a").AccessCount)
	require.Equal(t, int64(1), requiredContentItemByRoutingKey(t, items, "route-b").AccessCount)

	require.NoError(t, repo.SetRequiredContentReconciliationStatus(ctx, "default", "workspace-1", "stub-1", "hash-a", "route-a", cache.RequiredContentStatusPresent, "", ttl))
	items, err = repo.ListRequiredContentForStub(ctx, "default", "workspace-1", "stub-1", 10)
	require.NoError(t, err)
	require.Equal(t, cache.RequiredContentStatusPresent, requiredContentItemByRoutingKey(t, items, "route-a").Status)

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
	items, err = repo.ListRequiredContentForStub(ctx, "default", "workspace-1", "stub-1", 10)
	require.NoError(t, err)
	require.Empty(t, items)
	stubs, err = repo.ListRecentStubLocalities(ctx, "default", time.Now().Add(-time.Minute), 10)
	require.NoError(t, err)
	require.Empty(t, stubs)
}

func requiredContentItemByRoutingKey(t *testing.T, items []cache.RequiredContentItem, routingKey string) cache.RequiredContentItem {
	t.Helper()
	for _, item := range items {
		if item.RoutingKey == routingKey {
			return item
		}
	}
	t.Fatalf("required content item with routing key %q not found in %#v", routingKey, items)
	return cache.RequiredContentItem{}
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
