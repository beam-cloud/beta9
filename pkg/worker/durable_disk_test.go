package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDevDurableDiskRestoresMissingPrimaryFromReplica(t *testing.T) {
	primary := filepath.Join(t.TempDir(), "pg-data")
	mount := devDurableDiskTestMount(primary)

	require.NoError(t, prepareDevDurableDiskMount(mount))
	require.NoError(t, os.MkdirAll(filepath.Join(primary, "pgdata"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(primary, "pgdata", "state"), []byte("postgres-value"), 0o644))
	require.NoError(t, syncDevDurableDiskMount(mount))

	require.NoError(t, os.RemoveAll(primary))
	require.NoError(t, prepareDevDurableDiskMount(mount))

	data, err := os.ReadFile(filepath.Join(primary, "pgdata", "state"))
	require.NoError(t, err)
	require.Equal(t, "postgres-value", string(data))
}

func TestDevDurableDiskCleansStalePostgresPid(t *testing.T) {
	primary := filepath.Join(t.TempDir(), "pg-data")
	mount := devDurableDiskTestMount(primary)
	mount.MountPath = "/var/lib/postgresql/data"

	incomplete := filepath.Join(primary, "pgdata")
	require.NoError(t, os.MkdirAll(incomplete, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(incomplete, "PG_VERSION"), []byte("16"), 0o600))
	require.False(t, devDurableDiskHasRestorablePayload(mount, primary))
	require.ErrorContains(t, prepareDevDurableDiskMount(mount), "active or incomplete local payload")

	require.NoError(t, os.MkdirAll(filepath.Join(incomplete, "global"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(incomplete, "global", "pg_control"), []byte("control"), 0o600))
	require.True(t, devDurableDiskHasRestorablePayload(mount, primary))

	pidFile := filepath.Join(incomplete, "postmaster.pid")
	require.NoError(t, os.WriteFile(pidFile, []byte("123"), 0o600))
	require.NoError(t, prepareDevDurableDiskMount(mount))
	require.NoFileExists(t, pidFile)
}

func TestDevDurableDiskSyncRecreatesMissingReplicaFromPrimary(t *testing.T) {
	primary := filepath.Join(t.TempDir(), "redis-data")
	mount := devDurableDiskTestMount(primary)

	require.NoError(t, prepareDevDurableDiskMount(mount))
	require.NoError(t, os.MkdirAll(filepath.Join(primary, "appendonlydir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(primary, "appendonlydir", "state"), []byte("redis-value"), 0o644))
	require.NoError(t, syncDevDurableDiskMount(mount))

	replica := devDurableDiskReplicaPaths(mount)[0]
	require.NoError(t, os.RemoveAll(replica))
	require.NoError(t, syncDevDurableDiskMount(mount))

	data, err := os.ReadFile(filepath.Join(replica, "appendonlydir", "state"))
	require.NoError(t, err)
	require.Equal(t, "redis-value", string(data))
}

func TestAddRequestMountsPreparesDevDurableDisk(t *testing.T) {
	localPath := filepath.Join(t.TempDir(), "durable")
	spec := getTestBaseSpec()
	request := &types.ContainerRequest{
		ContainerId: "container-1",
		Mounts: []types.Mount{{
			LocalPath: localPath,
			MountPath: "/var/lib/postgresql/data",
			MountType: types.StorageModeDurableDisk,
			DurableDisk: &types.DurableDiskMountConfig{
				Name:       "pg-data",
				Size:       "10Gi",
				Filesystem: "ext4",
				Driver:     "dev",
				Replicas:   3,
				Mode:       "sync",
				Quorum:     "majority",
			},
		}},
	}

	volumeCacheMap, err := (&Worker{}).addRequestMounts(request, &spec)

	require.NoError(t, err)
	require.Empty(t, volumeCacheMap)
	require.DirExists(t, localPath)
	require.FileExists(t, filepath.Join(localPath, ".beta9-durable-disk"))
	require.Len(t, spec.Mounts, 1)
	require.Equal(t, localPath, spec.Mounts[0].Source)
	require.Equal(t, request.Mounts[0].MountPath, spec.Mounts[0].Destination)
	require.Equal(t, []string{"rbind", "rw"}, spec.Mounts[0].Options)
}

func TestCreateDurableDiskDirectorySnapshotDedupesChunks(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "pg-data")
	require.NoError(t, os.MkdirAll(filepath.Join(source, "pgdata", "base"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(source, "pgdata", "pg_wal"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(source, "pgdata", "base", "1"), []byte("base-data"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "pgdata", "pg_wal", "0001"), []byte("wal1"), 0o600))

	store := &fakeDurableDiskSnapshotStore{}
	first, firstManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/pg-data/snapshots/1", types.DiskSnapshot{
		ExternalId: "snap-1",
		DiskName:   "pg-data",
		Format:     types.DiskSnapshotFormatPostgresWalV1,
		Filesystem: "ext4",
		Generation: 1,
	}, 4, nil)
	require.NoError(t, err)
	require.Equal(t, types.DiskSnapshotFormatPostgresWalV1, first.Format)
	require.NotEmpty(t, firstManifest.Files)

	baseFile := snapshotTestFile(firstManifest, "pgdata/base/1")
	require.NotEmpty(t, baseFile.Chunks)

	store.existsCalls = 0
	store.uploadCalls = 0
	require.NoError(t, os.WriteFile(filepath.Join(source, "pgdata", "base", "1"), []byte("base-data-mutated"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "pgdata", "base", "2"), []byte("new-base-file"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(source, "pgdata", "pg_wal", "0002"), []byte("wal2"), 0o600))
	second, secondManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/pg-data/snapshots/2", types.DiskSnapshot{
		DiskName:         "pg-data",
		Format:           types.DiskSnapshotFormatPostgresWalV1,
		ParentSnapshotId: first.ExternalId,
		Filesystem:       "ext4",
		Generation:       2,
	}, 4, firstManifest)
	require.NoError(t, err)
	require.Equal(t, first.ExternalId, second.ParentSnapshotId)
	require.NotEqual(t, baseFile.Chunks, snapshotTestFile(secondManifest, "pgdata/base/1").Chunks)
	require.NotEmpty(t, snapshotTestFile(secondManifest, "pgdata/base/2").Chunks)
	require.Less(t, store.existsCalls, int(second.ChunkCount))

	walFile := snapshotTestFile(secondManifest, "pgdata/pg_wal/0002")
	require.NotEmpty(t, walFile.Chunks)
	chunk := walFile.Chunks[0]
	chunkHash := strings.TrimPrefix(chunk.Digest, "sha256:")
	cacheReader := &fakeDurableDiskSnapshotCacheReader{objects: map[string][]byte{chunkHash: []byte("wal2")}}
	delete(store.objects, chunk.ObjectKey)

	restored := filepath.Join(t.TempDir(), "restored")
	_, err = restoreDurableDiskDirectorySnapshotWithCache(ctx, store, cacheReader, second.ManifestKey, second.ManifestDigest, second.ManifestSizeBytes, restored)
	require.NoError(t, err)
	require.GreaterOrEqual(t, cacheReader.calls, 1)
	require.Equal(t, 1, cacheReader.hits)
	data, err := os.ReadFile(filepath.Join(restored, "pgdata", "base", "1"))
	require.NoError(t, err)
	require.Equal(t, "base-data-mutated", string(data))
	data, err = os.ReadFile(filepath.Join(restored, "pgdata", "base", "2"))
	require.NoError(t, err)
	require.Equal(t, "new-base-file", string(data))
	data, err = os.ReadFile(filepath.Join(restored, "pgdata", "pg_wal", "0002"))
	require.NoError(t, err)
	require.Equal(t, "wal2", string(data))
}

func TestCreateDurableDiskDirectorySnapshotReusesAppendOnlyTail(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "redis-data")
	aof := filepath.Join(source, "appendonlydir", "appendonly.aof.1.incr.aof")
	require.NoError(t, os.MkdirAll(filepath.Dir(aof), 0o700))
	require.NoError(t, os.WriteFile(aof, []byte("aaaabbbb"), 0o600))

	store := &fakeDurableDiskSnapshotStore{}
	_, firstManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/redis-data/snapshots/1", types.DiskSnapshot{
		DiskName:   "redis-data",
		Format:     types.DiskSnapshotFormatRedisAOFV1,
		Filesystem: "ext4",
		Generation: 1,
	}, 4, nil)
	require.NoError(t, err)
	firstFile := snapshotTestFile(firstManifest, "appendonlydir/appendonly.aof.1.incr.aof")
	require.Len(t, firstFile.Chunks, 2)

	store.uploadCalls = 0
	require.NoError(t, os.WriteFile(aof, []byte("aaaabbbbcccc"), 0o600))
	second, secondManifest, err := createDurableDiskDirectorySnapshot(ctx, store, source, "durable-disks/redis-data/snapshots/2", types.DiskSnapshot{
		DiskName:   "redis-data",
		Format:     types.DiskSnapshotFormatRedisAOFV1,
		Filesystem: "ext4",
		Generation: 2,
	}, 4, firstManifest)
	require.NoError(t, err)
	secondFile := snapshotTestFile(secondManifest, "appendonlydir/appendonly.aof.1.incr.aof")
	require.Len(t, secondFile.Chunks, 3)
	require.Equal(t, firstFile.Chunks, secondFile.Chunks[:2])
	require.Equal(t, int64(8), secondFile.Chunks[2].OffsetBytes)
	require.Equal(t, 2, store.uploadCalls)

	restored := filepath.Join(t.TempDir(), "restored")
	_, err = restoreDurableDiskDirectorySnapshotWithCache(ctx, store, nil, second.ManifestKey, second.ManifestDigest, second.ManifestSizeBytes, restored)
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(restored, "appendonlydir", "appendonly.aof.1.incr.aof"))
	require.NoError(t, err)
	require.Equal(t, "aaaabbbbcccc", string(data))
}

func TestDurableDiskSnapshotRequiredContentItems(t *testing.T) {
	items := durableDiskSnapshotRequiredContentItems(&types.DiskSnapshot{
		BucketName:        "disk-bucket",
		ManifestKey:       "durable-disks/pg-data/snapshots/7/manifest.json",
		ManifestDigest:    "sha256:" + strings.Repeat("a", 64),
		ManifestSizeBytes: 512,
	}, &types.DiskSnapshotManifest{
		Files: []types.DiskSnapshotFile{{
			Path: "pgdata/base/1",
			Type: "file",
			Chunks: []types.DiskSnapshotChunk{{
				ObjectKey: "durable-disks/pg-data/chunks/" + strings.Repeat("b", 64),
				Digest:    "sha256:" + strings.Repeat("b", 64),
				SizeBytes: 4096,
			}},
		}},
	})

	require.Len(t, items, 2)
	require.Equal(t, types.CacheContentKindDiskSnapshot, items[0].Kind)
	require.Equal(t, strings.Repeat("a", 64), items[0].Hash)
	require.Equal(t, "disk-bucket", items[0].SourceBucket)
	require.Equal(t, "durable-disks/pg-data/snapshots/7/manifest.json", items[0].Source)
	require.Equal(t, int64(512), items[0].SizeBytes)
	require.Equal(t, strings.Repeat("b", 64), items[1].Hash)
	require.Equal(t, "disk-bucket", items[1].SourceBucket)
}

func snapshotTestFile(manifest *types.DiskSnapshotManifest, name string) types.DiskSnapshotFile {
	for _, file := range manifest.Files {
		if file.Path == name {
			return file
		}
	}
	return types.DiskSnapshotFile{}
}

func devDurableDiskTestMount(primary string) *types.Mount {
	return &types.Mount{
		LocalPath: primary,
		MountPath: "/data",
		DurableDisk: &types.DurableDiskMountConfig{
			Name:     filepath.Base(primary),
			Driver:   types.DurableDiskDriverDev,
			Replicas: 3,
		},
	}
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

type fakeDurableDiskSnapshotCacheReader struct {
	objects map[string][]byte
	calls   int
	hits    int
}

func (s *fakeDurableDiskSnapshotCacheReader) GetContent(hash string, offset int64, length int64, _ struct{ RoutingKey string }) ([]byte, error) {
	s.calls++
	data, ok := s.objects[hash]
	if !ok {
		return nil, fmt.Errorf("cache miss")
	}
	s.hits++
	end := offset + length
	if offset < 0 || end > int64(len(data)) {
		return nil, fmt.Errorf("invalid cache range")
	}
	return append([]byte(nil), data[offset:end]...), nil
}
