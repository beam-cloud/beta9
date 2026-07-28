package cache

import (
	"context"
	"reflect"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestMetadataHostKeepAliveKeyIncludesLocalityAndHost(t *testing.T) {
	got := MetadataKeys.MetadataHostKeepAlive("locality-a", "host-b")
	want := "cache:host:keepalive:locality-a:host-b"

	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFSMetadataWorkerCacheProtoRoundTrip(t *testing.T) {
	metadata := &FSMetadata{
		ID:        "node-1",
		PID:       "parent-1",
		Name:      "file.txt",
		Path:      "/cache/file.txt",
		Hash:      "hash-1",
		Ino:       10,
		Size:      20,
		Blocks:    30,
		Atime:     40,
		Mtime:     50,
		Ctime:     60,
		Atimensec: 70,
		Mtimensec: 80,
		Ctimensec: 90,
		Mode:      100,
		Nlink:     110,
		Rdev:      120,
		Blksize:   130,
		Padding:   140,
		Uid:       150,
		Gid:       160,
		Gen:       170,
	}

	got := FSMetadataFromWorkerCacheProto(metadata.ToWorkerCacheProto())

	if !reflect.DeepEqual(got, metadata) {
		t.Fatalf("got %#v, want %#v", got, metadata)
	}

	encoded, err := MarshalFSMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	got, err = UnmarshalFSMetadata(encoded)
	if err != nil || !reflect.DeepEqual(got, metadata) {
		t.Fatalf("compact round trip: got %#v, err %v", got, err)
	}
}

func TestMetadataReadsMixedCacheFSFormats(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	metadata := NewMetadataWithRedisClient(client)
	ctx := context.Background()

	legacy := &FSMetadata{ID: "legacy", PID: "root", Name: "legacy"}
	if err := metadata.SetFsNode(ctx, legacy.ID, legacy); err != nil {
		t.Fatal(err)
	}
	if got := client.Type(ctx, MetadataKeys.MetadataFsNode(legacy.ID)).Val(); got != "hash" {
		t.Fatalf("legacy type = %q", got)
	}

	if err := client.Set(ctx, MetadataKeys.MetadataFsFormat(), FSMetadataFormatCompact, 0).Err(); err != nil {
		t.Fatal(err)
	}
	compact := &FSMetadata{ID: "compact", PID: "root", Name: "compact"}
	if err := metadata.SetFsNode(ctx, compact.ID, compact); err != nil {
		t.Fatal(err)
	}
	if got := client.HExists(ctx, MetadataKeys.MetadataFsNodeData(compact.ID), compact.ID).Val(); !got {
		t.Fatal("compact node not stored in shard")
	}

	for _, want := range []*FSMetadata{legacy, compact} {
		got, err := metadata.GetFsNode(ctx, want.ID)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("read %s: got %#v, err %v", want.ID, got, err)
		}
	}

	if err := client.Del(ctx, MetadataKeys.MetadataFsFormat()).Err(); err != nil {
		t.Fatal(err)
	}
	withoutMarker := NewMetadataWithRedisClient(client)
	if _, err := withoutMarker.GetFsNode(ctx, compact.ID); err != nil {
		t.Fatal(err)
	}
	legacyAfterCompactRead := &FSMetadata{ID: "still-legacy"}
	if err := withoutMarker.SetFsNode(ctx, legacyAfterCompactRead.ID, legacyAfterCompactRead); err != nil {
		t.Fatal(err)
	}
	if got := client.Type(ctx, MetadataKeys.MetadataFsNode(legacyAfterCompactRead.ID)).Val(); got != "hash" {
		t.Fatalf("unmarked write type = %q", got)
	}
}
