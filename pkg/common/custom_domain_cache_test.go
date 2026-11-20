package common

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCustomDomainCacheWriteIsRejectedAfterBindingChange(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	rdb, err := NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	domain := "race.customer.io"
	key := CustomDomainHandlerCacheKey(domain)

	// No change in between: the write lands.
	rev := CustomDomainRevision(ctx, rdb, domain)
	require.True(t, SetCustomDomainCacheIfCurrent(ctx, rdb, domain, rev, key, "ws|/endpoint/a/latest", time.Minute))
	require.Equal(t, "ws|/endpoint/a/latest", rdb.Get(ctx, key).Val())

	// A slow request read the binding, then the domain was removed or
	// transferred (which invalidates the cache). Its late write must not bring
	// the old binding back.
	rev = CustomDomainRevision(ctx, rdb, domain)
	InvalidateCustomDomainCache(ctx, rdb, domain)
	require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
	require.False(t, SetCustomDomainCacheIfCurrent(ctx, rdb, domain, rev, key, "oldws|/endpoint/old/latest", time.Minute))
	require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())

	// A request that starts after the change sees the new revision and caches normally.
	rev = CustomDomainRevision(ctx, rdb, domain)
	require.True(t, SetCustomDomainCacheIfCurrent(ctx, rdb, domain, rev, key, "ws|/endpoint/new/latest", time.Minute))
	require.Equal(t, "ws|/endpoint/new/latest", rdb.Get(ctx, key).Val())
}

func TestCustomDomainCacheWriteWithoutRevisionHistory(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	rdb, err := NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	// Domain never invalidated: revision is empty on both sides, so a miss can be cached.
	require.True(t, SetCustomDomainCacheIfCurrent(ctx, rdb, "fresh.customer.io", "", CustomDomainHandlerCacheKey("fresh.customer.io"), "-", time.Minute))
	// Without Redis nothing is written.
	require.False(t, SetCustomDomainCacheIfCurrent(ctx, nil, "fresh.customer.io", "", "k", "v", time.Minute))
}
