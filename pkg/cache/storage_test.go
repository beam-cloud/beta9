package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T, pageSize int64) *Store {
	t.Helper()

	InitLogger(false, false)

	store, err := NewStore(context.Background(), &Host{HostId: "test-host"}, "test", NewMockRegistry(), Config{
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
