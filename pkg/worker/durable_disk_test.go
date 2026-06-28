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

func TestDevDurableDiskRejectsIncompletePostgresPayload(t *testing.T) {
	primary := filepath.Join(t.TempDir(), "pg-data")
	mount := devDurableDiskTestMount(primary)
	mount.MountPath = "/var/lib/postgresql/data"

	incomplete := filepath.Join(primary, "pgdata")
	require.NoError(t, os.MkdirAll(incomplete, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(incomplete, "PG_VERSION"), []byte("16"), 0o600))
	require.False(t, devDurableDiskHasRestorablePayload(mount, primary))

	require.NoError(t, os.MkdirAll(filepath.Join(incomplete, "global"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(incomplete, "global", "pg_control"), []byte("control"), 0o600))
	require.True(t, devDurableDiskHasRestorablePayload(mount, primary))

	require.NoError(t, os.WriteFile(filepath.Join(incomplete, "postmaster.pid"), []byte("123"), 0o600))
	require.False(t, devDurableDiskHasRestorablePayload(mount, primary))
	require.ErrorContains(t, prepareDevDurableDiskMount(mount), "active or incomplete local payload")
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

func TestCreateDurableDiskDirectorySnapshotReusesUnchangedFiles(t *testing.T) {
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
	require.Equal(t, baseFile.Chunks, snapshotTestFile(secondManifest, "pgdata/base/1").Chunks)
	require.Empty(t, snapshotTestFile(secondManifest, "pgdata/base/2").Path)
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
	require.Equal(t, "base-data", string(data))
	data, err = os.ReadFile(filepath.Join(restored, "pgdata", "pg_wal", "0002"))
	require.NoError(t, err)
	require.Equal(t, "wal2", string(data))
}

func TestPreparePostgresWALRecoveryWritesRecoverySignal(t *testing.T) {
	localPath := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(localPath, "pgdata"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(localPath, "pgdata", "PG_VERSION"), []byte("16"), 0o600))

	require.NoError(t, preparePostgresWALRecovery(&types.Mount{
		LocalPath: localPath,
		MountPath: "/var/lib/postgresql/data",
	}))
	require.FileExists(t, filepath.Join(localPath, "pgdata", "recovery.signal"))
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
			Mode:     types.DurableDiskReplicationModeSync,
			Quorum:   types.DurableDiskReplicationQuorumMajority,
		},
	}
}

func TestDRBDDurableDiskRejectsNonPrimaryWorker(t *testing.T) {
	mount := drbdDurableDiskTestMount(filepath.Join(t.TempDir(), "pg-data"))
	worker := &Worker{workerId: "worker-b"}

	_, err := worker.newDRBDDiskConfig(mount, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing attach")
}

func TestDRBDDurableDiskRequiresReplicaAddresses(t *testing.T) {
	mount := drbdDurableDiskTestMount(filepath.Join(t.TempDir(), "pg-data"))
	worker := &Worker{workerId: "worker-a"}

	_, err := worker.newDRBDDiskConfig(mount, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), drbdNodeAddressesEnv)
}

func TestDRBDDurableDiskPreparesPrimary(t *testing.T) {
	stateDir := t.TempDir()
	configDir := t.TempDir()
	t.Setenv(drbdStateDirEnv, stateDir)
	t.Setenv(drbdConfigDirEnv, configDir)
	t.Setenv(drbdNodeAddressesEnv, `{"worker-a":"10.0.0.1","worker-b":"10.0.0.2","worker-c":"10.0.0.3"}`)
	t.Setenv(drbdNodeNamesEnv, `{"worker-a":"node-a","worker-b":"node-b","worker-c":"node-c"}`)

	runner := &fakeDurableDiskRunner{}
	withDurableDiskTestHooks(t, runner)

	mount := drbdDurableDiskTestMount(filepath.Join(t.TempDir(), "pg-data"))
	worker := &Worker{workerId: "worker-a", ctx: context.Background()}

	require.NoError(t, worker.prepareDRBDDurableDiskMount(&types.ContainerRequest{StubId: "stub-id"}, mount))

	files, err := filepath.Glob(filepath.Join(configDir, "*.res"))
	require.NoError(t, err)
	require.Len(t, files, 1)

	config, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Contains(t, string(config), "protocol C;")
	require.Contains(t, string(config), "quorum majority;")
	require.Contains(t, string(config), "on-no-quorum io-error;")
	require.Contains(t, string(config), "on node-a")
	require.Contains(t, string(config), "address ipv4 10.0.0.2:")

	require.True(t, runner.hasCallPrefix("truncate -s 1073741824 "))
	require.True(t, runner.hasCallPrefix("losetup --find --show "))
	require.True(t, runner.hasCallPrefix("drbdadm create-md beta9_"))
	require.True(t, runner.hasCallPrefix("drbdadm primary --force beta9_"))
	require.True(t, runner.hasCallPrefix("mkfs.ext4 -F /dev/drbd"))
	require.True(t, runner.hasCallPrefix("mount /dev/drbd"))
}

func TestDRBDDurableDiskTeardownDemotesPrimary(t *testing.T) {
	t.Setenv(drbdNodeAddressesEnv, `{"worker-a":"10.0.0.1","worker-b":"10.0.0.2","worker-c":"10.0.0.3"}`)
	t.Setenv(drbdNodeNamesEnv, `{"worker-a":"node-a","worker-b":"node-b","worker-c":"node-c"}`)

	runner := &fakeDurableDiskRunner{mounted: true}
	withDurableDiskTestHooks(t, runner)

	mount := drbdDurableDiskTestMount(filepath.Join(t.TempDir(), "pg-data"))
	worker := &Worker{workerId: "worker-a", ctx: context.Background()}

	require.NoError(t, worker.teardownDRBDDurableDiskMount(&types.ContainerRequest{ContainerId: "container-id"}, mount))
	require.True(t, runner.hasCallPrefix("umount "))
	require.True(t, runner.hasCallPrefix("drbdadm secondary beta9_"))
}

func TestDRBDDurableDiskRefusesBackingFileShrink(t *testing.T) {
	backingPath := filepath.Join(t.TempDir(), "backing.img")
	require.NoError(t, os.WriteFile(backingPath, []byte("existing-state"), 0o644))

	config := &drbdDiskConfig{
		BackingPath: backingPath,
		SizeBytes:   1,
	}
	err := config.ensureBackingFile(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing to shrink")
}

func TestDRBDAddressWithPortHandlesIPv6(t *testing.T) {
	require.Equal(t, drbdNetAddress{Value: "[fd00::1]:7799", Family: "ipv6"}, drbdAddressWithPort("fd00::1", 7799))
	require.Equal(t, drbdNetAddress{Value: "[fd00::1]:7799", Family: "ipv6"}, drbdAddressWithPort("[fd00::1]", 7799))
	require.Equal(t, drbdNetAddress{Value: "[fd00::1]:7788", Family: "ipv6"}, drbdAddressWithPort("[fd00::1]:7788", 7799))
	require.Equal(t, drbdNetAddress{Value: "10.0.0.1:7799", Family: "ipv4"}, drbdAddressWithPort("10.0.0.1", 7799))
}

func drbdDurableDiskTestMount(primary string) *types.Mount {
	return &types.Mount{
		LocalPath: primary,
		MountPath: "/var/lib/postgresql/data",
		DurableDisk: &types.DurableDiskMountConfig{
			Name:             filepath.Base(primary),
			Size:             "1Gi",
			Filesystem:       "ext4",
			Driver:           types.DurableDiskDriverDRBD,
			Replicas:         3,
			Mode:             types.DurableDiskReplicationModeSync,
			Quorum:           types.DurableDiskReplicationQuorumMajority,
			PrimaryWorkerID:  "worker-a",
			ReplicaWorkerIDs: []string{"worker-a", "worker-b", "worker-c"},
		},
	}
}

type fakeDurableDiskRunner struct {
	calls   []string
	mounted bool
}

func (f *fakeDurableDiskRunner) run(_ context.Context, name string, args ...string) (string, error) {
	call := strings.TrimSpace(name + " " + strings.Join(args, " "))
	f.calls = append(f.calls, call)

	switch {
	case callHasPrefix(call, "losetup -j "):
		return "", nil
	case callHasPrefix(call, "losetup --find --show "):
		return "/dev/loop7\n", nil
	case callHasPrefix(call, "findmnt --mountpoint "):
		if f.mounted {
			return "mounted\n", nil
		}
		return "", fmt.Errorf("not mounted")
	case callHasPrefix(call, "blkid -o value -s TYPE "):
		return "", fmt.Errorf("no filesystem")
	default:
		return "", nil
	}
}

func (f *fakeDurableDiskRunner) hasCallPrefix(prefix string) bool {
	for _, call := range f.calls {
		if callHasPrefix(call, prefix) {
			return true
		}
	}
	return false
}

func callHasPrefix(call, prefix string) bool {
	return strings.HasPrefix(strings.Join(strings.Fields(call), " "), strings.Join(strings.Fields(prefix), " "))
}

func withDurableDiskTestHooks(t *testing.T, runner *fakeDurableDiskRunner) {
	t.Helper()

	oldRun := durableDiskRun
	oldLookPath := durableDiskLookPath
	oldEUID := durableDiskEUID
	oldHostname := durableDiskHostname

	durableDiskRun = runner.run
	durableDiskLookPath = func(name string) (string, error) {
		return "/usr/bin/" + name, nil
	}
	durableDiskEUID = func() int {
		return 0
	}
	durableDiskHostname = func() (string, error) {
		return "node-a", nil
	}

	t.Cleanup(func() {
		durableDiskRun = oldRun
		durableDiskLookPath = oldLookPath
		durableDiskEUID = oldEUID
		durableDiskHostname = oldHostname
	})
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
