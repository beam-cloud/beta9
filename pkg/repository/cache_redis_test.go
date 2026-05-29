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
	require.Contains(t, keys, "cache:coordinator:host:cache-host-default-node-a-path-0:registrations")
	require.Contains(t, keys, "cache:coordinator:host:cache-host-default-node-a-path-0:registration:worker-a")
	require.Contains(t, keys, "cache:coordinator:host:cache-host-default-node-a-path-0:active_registration")
	for _, key := range keys {
		require.NotContains(t, key, "cache:coordinator:index:")
	}
}
