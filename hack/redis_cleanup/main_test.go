package main

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

func TestCleanupCacheFSLeaf(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := removeCacheFSLeaf.Load(ctx, rdb).Err(); err != nil {
		t.Fatal(err)
	}

	node := &cache.FSMetadata{ID: "leaf", PID: "parent"}
	data, err := cache.MarshalFSMetadata(node)
	if err != nil {
		t.Fatal(err)
	}
	dataKey := cache.MetadataKeys.MetadataFsNodeData(node.ID)
	index := cache.MetadataKeys.MetadataFsNodeIndex(node.ID)
	parent := cache.MetadataKeys.MetadataFsNodeChildren(node.PID)
	if err := rdb.HSet(ctx, dataKey, node.ID, data).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.ZAdd(ctx, index, redis.Z{Member: node.ID, Score: 1}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.SAdd(ctx, parent, node.ID).Err(); err != nil {
		t.Fatal(err)
	}

	result := &cleanupReport{}
	if err := cleanupCacheFSBatch(ctx, rdb, []string{node.ID}, dataKey, index, time.Now().Unix(), true, result); err != nil {
		t.Fatal(err)
	}
	if !rdb.HExists(ctx, dataKey, node.ID).Val() {
		t.Fatal("referenced leaf was removed")
	}

	if err := rdb.SRem(ctx, parent, node.ID).Err(); err != nil {
		t.Fatal(err)
	}
	if err := cleanupCacheFSBatch(ctx, rdb, []string{node.ID}, dataKey, index, time.Now().Unix(), true, result); err != nil {
		t.Fatal(err)
	}
	if rdb.HExists(ctx, dataKey, node.ID).Val() || result.CacheFSRemoved != 1 {
		t.Fatal("orphan leaf was not removed")
	}
}
