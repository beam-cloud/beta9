//go:build linux

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeBlockDevice lays out /sys/block/<name> with the two attributes
// enableWriteCache uses. When linked, queue/write_cache is a symlink to
// cache_type, so a write to cache_type is reflected in the cache mode the way
// virtio_blk reflects it.
func fakeBlockDevice(t *testing.T, name, mode string, cacheTypePerm os.FileMode, linked bool) string {
	t.Helper()
	dir := filepath.Join(sysBlockDir, name)
	if err := os.MkdirAll(filepath.Join(dir, "queue"), 0o755); err != nil {
		t.Fatal(err)
	}
	cacheType := filepath.Join(dir, "cache_type")
	if err := os.WriteFile(cacheType, []byte(mode+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cacheType, cacheTypePerm); err != nil {
		t.Fatal(err)
	}
	writeCache := filepath.Join(dir, "queue", "write_cache")
	if linked {
		if err := os.Symlink(cacheType, writeCache); err != nil {
			t.Fatal(err)
		}
	} else if err := os.WriteFile(writeCache, []byte(mode+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return cacheType
}

func TestEnableWriteCache(t *testing.T) {
	previous := sysBlockDir
	sysBlockDir = t.TempDir()
	t.Cleanup(func() { sysBlockDir = previous })

	read := func(path string) string {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(data))
	}

	switched := fakeBlockDevice(t, "vda", "write through", 0o644, true)
	if err := enableWriteCache("/dev/vda"); err != nil {
		t.Fatalf("write-through device with a configurable cache: %v", err)
	}
	if got := read(switched); got != writeBack {
		t.Fatalf("cache_type is %q, want write back", got)
	}

	already := fakeBlockDevice(t, "vdb", "write back", 0o644, false)
	if err := os.WriteFile(already, []byte("untouched"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := enableWriteCache("/dev/vdb"); err != nil || read(already) != "untouched" {
		t.Fatalf("a write-back device must be left alone: %v, cache_type %q", err, read(already))
	}

	fixed := fakeBlockDevice(t, "vdc", "write through", 0o444, false)
	if err := enableWriteCache("/dev/vdc"); err != nil || read(fixed) != "write through" {
		t.Fatalf("a fixed cache mode must be left alone: %v, cache_type %q", err, read(fixed))
	}

	fakeBlockDevice(t, "vdd", "write through", 0o644, false)
	if err := enableWriteCache("/dev/vdd"); err == nil || !strings.Contains(err.Error(), "still reports a write through cache") {
		t.Fatalf("a device that ignores the switch must fail boot, got %v", err)
	}

	if err := enableWriteCache("/dev/vde"); err == nil {
		t.Fatal("a device without a queue must fail boot")
	}
}
