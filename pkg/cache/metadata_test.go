package cache

import (
	"reflect"
	"testing"
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
}
