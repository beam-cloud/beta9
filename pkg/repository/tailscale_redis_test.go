package repository

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestTailscaleHostnamesUseServiceIndexAndPruneExpiredEntries(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	repo := &TailscaleRedisRepository{rdb: rdb}
	require.NoError(t, repo.SetHostname("svc-a", "instance-a", "host-a"))
	require.NoError(t, repo.SetHostname("svc-a", "instance-b", "host-b"))

	ctx := context.Background()
	require.NoError(t, rdb.SAdd(ctx, common.RedisKeys.TailscaleServiceHostnameIndex("svc-a"), "expired").Err())
	require.NoError(t, rdb.Set(ctx, common.RedisKeys.TailscaleServiceHostname("svc-a", "expired"), "host-expired", time.Millisecond).Err())
	time.Sleep(2 * time.Millisecond)

	hostnames, err := repo.GetHostnamesForService("svc-a")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"host-a", "host-b"}, hostnames)

	indexMembers, err := rdb.SMembers(ctx, common.RedisKeys.TailscaleServiceHostnameIndex("svc-a")).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"instance-a", "instance-b"}, indexMembers)
}
