package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

type maxChunkReader struct {
	reader *bytes.Reader
	max    int
}

func (r *maxChunkReader) Read(p []byte) (int, error) {
	if len(p) > r.max {
		p = p[:r.max]
	}
	return r.reader.Read(p)
}

func newTestStore(t *testing.T, pageSize int64) *Store {
	t.Helper()

	InitLogger(false, false)

	store, err := NewStore(context.Background(), &Host{HostId: "test-host"}, "test", NewMockCacheMetadataStore(), Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        pageSize,
			ObjectTtlS:           300,
		},
	})
	require.NoError(t, err)

	t.Cleanup(store.Cleanup)
	return store
}

func TestStoreAddReaderStreamsToDiskCAS(t *testing.T) {
	store := newTestStore(t, 5)
	content := []byte("streamed content spanning several cache pages")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])

	hash, size, err := store.AddReader(context.Background(), bytes.NewReader(content))
	require.NoError(t, err)
	require.Equal(t, expectedHash, hash)
	require.Equal(t, int64(len(content)), size)
	require.True(t, store.Exists(hash))

	dst := make([]byte, len(content))
	n, err := store.Get(hash, 0, int64(len(dst)), dst)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)
}

func TestStoreAddReaderRepagesShortSourceReads(t *testing.T) {
	store := newTestStore(t, 7)
	content := bytes.Repeat([]byte("short source reads still need fixed cache pages."), 3)
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])

	hash, size, err := store.AddReader(context.Background(), &maxChunkReader{
		reader: bytes.NewReader(content),
		max:    3,
	})
	require.NoError(t, err)
	require.Equal(t, expectedHash, hash)
	require.Equal(t, int64(len(content)), size)

	dst := make([]byte, len(content))
	n, err := store.Get(hash, 0, int64(len(dst)), dst)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)

	ranges := []struct {
		offset int64
		length int64
	}{
		{offset: 1, length: 6},
		{offset: 5, length: 12},
		{offset: 8, length: int64(len(content)) - 9},
	}

	for _, tt := range ranges {
		dst := make([]byte, tt.length)
		n, err := store.Get(hash, tt.offset, tt.length, dst)
		require.NoError(t, err)
		require.Equal(t, tt.length, n)
		require.Equal(t, content[tt.offset:tt.offset+tt.length], dst)
	}
}

func TestStoreReadAtReadsOnlyRequestedPageRanges(t *testing.T) {
	store := newTestStore(t, 5)
	content := []byte("abcdefghijklmnopqrstuvwxyz")
	hash, size, err := store.AddReader(context.Background(), bytes.NewReader(content))
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), size)

	dst := make([]byte, 12)
	n, err := store.ReadAt(hash, 3, dst)
	require.NoError(t, err)
	require.Equal(t, int64(len(dst)), n)
	require.Equal(t, content[3:15], dst)

	pagePath, pageOffset, pageN, ok, err := store.PageRegion(hash, 6, 3)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(1), pageOffset)
	require.Equal(t, 3, pageN)
	require.FileExists(t, pagePath)
}

func TestStoreReadAtFallsBackToLegacyPageLayout(t *testing.T) {
	store := newTestStore(t, 5)
	hash := "legacy-hash"
	require.NoError(t, os.MkdirAll(store.legacyPageDir(hash), 0755))
	require.NoError(t, os.WriteFile(store.legacyPagePath(hash, 0), []byte("hello"), 0644))

	dst := make([]byte, 3)
	n, err := store.ReadAt(hash, 1, dst)
	require.NoError(t, err)
	require.Equal(t, int64(3), n)
	require.Equal(t, []byte("ell"), dst)
}

func TestStoreAddReaderStoresEmptyContent(t *testing.T) {
	store := newTestStore(t, 5)
	sum := sha256.Sum256(nil)
	expectedHash := hex.EncodeToString(sum[:])

	hash, size, err := store.AddReader(context.Background(), bytes.NewReader(nil))
	require.NoError(t, err)
	require.Equal(t, expectedHash, hash)
	require.Equal(t, int64(0), size)
	require.True(t, store.Exists(hash))

	n, err := store.Get(hash, 0, 0, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
}

func TestStoreAddReaderFallsBackToMemoryWhenDiskExceeded(t *testing.T) {
	InitLogger(false, false)

	cacheDir := filepath.Join(t.TempDir(), "cache-dir")
	store, err := NewStore(context.Background(), &Host{HostId: "test-host"}, "test", NewMockCacheMetadataStore(), Config{
		Server: ServerConfig{
			DiskCacheDir:         cacheDir,
			DiskCacheMaxUsagePct: 90,
			MaxCachePct:          1,
			PageSizeBytes:        5,
			ObjectTtlS:           300,
		},
	})
	require.NoError(t, err)
	t.Cleanup(store.Cleanup)

	store.diskCachedUsageExceeded = true
	content := []byte("memory fallback content")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])

	hash, size, err := store.AddReader(context.Background(), &maxChunkReader{
		reader: bytes.NewReader(content),
		max:    4,
	})
	require.NoError(t, err)

	require.Equal(t, expectedHash, hash)
	require.Equal(t, int64(len(content)), size)
	require.Eventually(t, func() bool {
		return store.Exists(hash)
	}, time.Second, 10*time.Millisecond)
	_, statErr := os.Stat(cacheDir)
	require.True(t, os.IsNotExist(statErr))

	dst := make([]byte, len(content))
	n, err := store.Get(hash, 0, int64(len(dst)), dst)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)
}

func TestStoreAddReaderRejectsWhenDiskExceededWithoutMemory(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache-dir")
	store := newTestStore(t, 5)
	store.diskCacheDir = cacheDir
	store.diskCachedUsageExceeded = true

	_, _, err := store.AddReader(context.Background(), bytes.NewReader([]byte("content")))
	require.ErrorContains(t, err, "disk cache capacity exceeded")
	_, statErr := os.Stat(cacheDir)
	require.True(t, os.IsNotExist(statErr))
}

func TestCacheFSMetadataFromFileInfoPreservesFileMode(t *testing.T) {
	path := t.TempDir() + "/tool"
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"), 0755))
	require.NoError(t, os.Chmod(path, 0755))

	info, err := os.Stat(path)
	require.NoError(t, err)

	metadata := cacheFSMetadataFromFileInfo("/volumes/workspace/tool", info)
	require.NotNil(t, metadata)
	require.Equal(t, uint32(fuse.S_IFREG|0755), metadata.Mode)
	require.Equal(t, uint64(info.Size()), metadata.Size)
	require.Equal(t, "/volumes/workspace/tool", metadata.Path)
}

func TestCacheFSReadLengthCapsFinalRead(t *testing.T) {
	tests := []struct {
		name      string
		fileSize  uint64
		offset    int64
		requested int
		expected  int64
	}{
		{name: "empty file", fileSize: 0, offset: 0, requested: 128, expected: 0},
		{name: "zero request", fileSize: 1024, offset: 0, requested: 0, expected: 0},
		{name: "negative offset", fileSize: 1024, offset: -1, requested: 128, expected: 0},
		{name: "full request", fileSize: 1024, offset: 128, requested: 256, expected: 256},
		{name: "final short read", fileSize: 1024, offset: 900, requested: 256, expected: 124},
		{name: "at eof", fileSize: 1024, offset: 1024, requested: 256, expected: 0},
		{name: "past eof", fileSize: 1024, offset: 2048, requested: 256, expected: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, cacheFSReadLength(tt.fileSize, tt.offset, tt.requested))
		})
	}
}
