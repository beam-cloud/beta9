package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCreateDurableDiskSnapshotUploadsMissingContentAddressedChunks(t *testing.T) {
	ctx := context.Background()
	backingPath := filepath.Join(t.TempDir(), "backing.img")
	require.NoError(t, os.WriteFile(backingPath, []byte("aaaabbbb"), 0600))

	store := &fakeDurableDiskSnapshotStore{
		objects: map[string][]byte{
			"durable-disks/pg-data/chunks/61be55a8e2f6b4e172338bddf184d6dbee29c98853e0a0485ecee7f27b9af0b4": []byte("aaaa"),
		},
	}

	snapshot, manifest, err := createDurableDiskSnapshot(ctx, store, backingPath, "durable-disks/pg-data/snapshots/7", types.DiskSnapshot{
		DiskName:   "pg-data",
		Filesystem: "ext4",
		Generation: 7,
	}, 4)

	require.NoError(t, err)
	require.Equal(t, types.DiskSnapshotStatusAvailable, snapshot.Status)
	require.Equal(t, "durable-disks/pg-data/snapshots/7/manifest.json", snapshot.ManifestKey)
	require.Equal(t, int64(2), snapshot.ChunkCount)
	require.Equal(t, int64(8), snapshot.LogicalSizeBytes)
	require.Equal(t, int64(8), snapshot.StoredSizeBytes)
	require.Len(t, manifest.Chunks, 2)
	require.Equal(t, 2, store.existsCalls)
	require.Equal(t, 2, store.uploadCalls) // one new chunk plus the manifest

	var storedManifest types.DiskSnapshotManifest
	require.NoError(t, json.Unmarshal(store.objects["durable-disks/pg-data/snapshots/7/manifest.json"], &storedManifest))
	require.Equal(t, "pg-data", storedManifest.DiskName)
	require.Equal(t, int64(7), storedManifest.Generation)
	require.Equal(t, "durable-disks/pg-data/chunks/61be55a8e2f6b4e172338bddf184d6dbee29c98853e0a0485ecee7f27b9af0b4", storedManifest.Chunks[0].ObjectKey)

	restorePath := filepath.Join(t.TempDir(), "restored.img")
	restoredManifest, err := restoreDurableDiskSnapshot(ctx, store, "durable-disks/pg-data/snapshots/7/manifest.json", restorePath)
	require.NoError(t, err)
	require.Equal(t, manifest.LogicalSizeBytes, restoredManifest.LogicalSizeBytes)
	restored, err := os.ReadFile(restorePath)
	require.NoError(t, err)
	require.Equal(t, []byte("aaaabbbb"), restored)
}

type fakeDurableDiskSnapshotStore struct {
	objects     map[string][]byte
	existsCalls int
	uploadCalls int
}

func (s *fakeDurableDiskSnapshotStore) Exists(_ context.Context, key string) (bool, error) {
	s.existsCalls++
	_, ok := s.objects[key]
	return ok, nil
}

func (s *fakeDurableDiskSnapshotStore) Upload(_ context.Context, key string, data []byte) error {
	s.uploadCalls++
	if s.objects == nil {
		s.objects = map[string][]byte{}
	}
	s.objects[key] = append([]byte(nil), data...)
	return nil
}

func (s *fakeDurableDiskSnapshotStore) UploadWithReader(_ context.Context, key string, data io.Reader) error {
	body, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	return s.Upload(context.Background(), key, body)
}

func (s *fakeDurableDiskSnapshotStore) DownloadWithReader(_ context.Context, key string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.objects[key])), nil
}
