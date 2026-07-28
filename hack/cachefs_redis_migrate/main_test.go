package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestMigrateBatch(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		t.Fatal(err)
	}
	want := &cache.FSMetadata{ID: "node", PID: "parent", Name: "file", Gen: 3}
	key := cache.MetadataKeys.MetadataFsNode(want.ID)
	if err := rdb.HSet(context.Background(), key, cache.ToSlice(want)).Err(); err != nil {
		t.Fatal(err)
	}

	result := &report{}
	if err := replaceMetadata.Load(context.Background(), rdb).Err(); err != nil {
		t.Fatal(err)
	}
	if err := migrateBatch(context.Background(), rdb, []string{key}, true, result); err != nil {
		t.Fatal(err)
	}
	data, err := rdb.HGet(context.Background(), cache.MetadataKeys.MetadataFsNodeData(want.ID), want.ID).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	got, err := cache.UnmarshalFSMetadata(data)
	if err != nil || !reflect.DeepEqual(got, want) || result.Converted != 1 || rdb.Exists(context.Background(), key).Val() != 0 {
		t.Fatalf("got %#v, converted %d, err %v", got, result.Converted, err)
	}
	if rdb.ZScore(context.Background(), cache.MetadataKeys.MetadataFsNodeIndex(want.ID), want.ID).Err() != nil {
		t.Fatal("node was not indexed")
	}

	if err := restoreMetadata.Load(context.Background(), rdb).Err(); err != nil {
		t.Fatal(err)
	}
	rollback := &report{}
	if err := rollbackShard(context.Background(), rdb, cache.FSMetadataShard(want.ID), 10, 0, rollback); err != nil {
		t.Fatal(err)
	}
	legacy := &cache.FSMetadata{}
	if err := cache.ToStruct(rdb.HGetAll(context.Background(), key).Val(), legacy); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(legacy, want) || rollback.Restored != 1 ||
		rdb.HExists(context.Background(), cache.MetadataKeys.MetadataFsNodeData(want.ID), want.ID).Val() {
		t.Fatalf("rollback got %#v, restored %d", legacy, rollback.Restored)
	}
}
