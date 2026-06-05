package dmap

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestMapKeysAndCountUseIndexAndPruneStaleEntries(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	service, err := NewRedisMapService(rdb)
	require.NoError(t, err)

	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Name: "workspace-a"},
		Token:     &types.Token{},
	})

	_, err = service.MapSet(ctx, &pb.MapSetRequest{Name: "map-a", Key: "live-a", Value: []byte("a"), Ttl: 3600})
	require.NoError(t, err)
	_, err = service.MapSet(ctx, &pb.MapSetRequest{Name: "map-a", Key: "live-b", Value: []byte("b"), Ttl: 3600})
	require.NoError(t, err)

	indexKey := Keys.MapIndex("workspace-a", "map-a")
	require.NoError(t, rdb.SAdd(ctx, indexKey, "stale").Err())
	require.NoError(t, rdb.Set(ctx, Keys.MapEntry("workspace-a", "map-a", "expired"), []byte("expired"), time.Millisecond).Err())
	require.NoError(t, rdb.SAdd(ctx, indexKey, "expired").Err())
	server.FastForward(2 * time.Millisecond)

	keys, err := service.MapKeys(ctx, &pb.MapKeysRequest{Name: "map-a"})
	require.NoError(t, err)
	require.True(t, keys.Ok)
	require.Equal(t, []string{"live-a", "live-b"}, keys.Keys)

	count, err := service.MapCount(ctx, &pb.MapCountRequest{Name: "map-a"})
	require.NoError(t, err)
	require.True(t, count.Ok)
	require.Equal(t, uint32(2), count.Count)

	indexMembers, err := rdb.SMembers(ctx, indexKey).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"live-a", "live-b"}, indexMembers)
}
