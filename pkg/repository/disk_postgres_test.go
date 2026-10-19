package repository

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestGetOrCreateDiskAcceptsQcowDriver(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	now := time.Now().UTC()

	mock.ExpectQuery(`WITH existing AS`).
		WithArgs(uint(7), "machine-root", "32Gi", "ext4", types.DurableDiskDriverQcow, "/", true).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "external_id", "workspace_id", "name", "size", "filesystem", "driver", "mount_path",
			"created_at", "updated_at", "deleted_at", "created",
		}).AddRow(
			uint(1),
			"disk-1",
			uint(7),
			"machine-root",
			"32Gi",
			"ext4",
			types.DurableDiskDriverQcow,
			"/",
			now,
			now,
			nil,
			true,
		))

	disk, err := postgresRepo.GetOrCreateDisk(context.Background(), 7, &types.Disk{
		Name:       "machine-root",
		Size:       "32Gi",
		Filesystem: "ext4",
		Driver:     types.DurableDiskDriverQcow,
		MountPath:  "/",
	})

	require.NoError(t, err)
	require.Equal(t, types.DurableDiskDriverQcow, disk.Driver)
	require.Equal(t, "/", disk.MountPath)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetOrCreateDiskRejectsUnsupportedDriver(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)

	mock.ExpectQuery(`WITH existing AS`).
		WithArgs(uint(7), "machine-root", "", "ext4", "unsupported", "", false).
		WillReturnError(sql.ErrNoRows)

	_, err := postgresRepo.GetOrCreateDisk(context.Background(), 7, &types.Disk{
		Name:   "machine-root",
		Driver: "unsupported",
	})

	require.EqualError(t, err, `unsupported durable disk driver "unsupported"`)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConcurrentDiskRegistrationStillValidatesWinningRow(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	mock.ExpectQuery(`WITH existing AS`).
		WithArgs(uint(7), "root", "32Gi", "ext4", types.DurableDiskDriverQcow, "/", true).
		WillReturnError(sql.ErrNoRows)
	now := time.Now()
	mock.ExpectQuery(`SELECT .* FROM disk`).WithArgs(uint(7), "root").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "external_id", "workspace_id", "name", "size", "filesystem", "driver", "mount_path",
			"created_at", "updated_at", "deleted_at",
		}).AddRow(1, "existing", 7, "root", "32Gi", "ext4", types.DurableDiskDriverSnapshot, "/", now, now, nil))
	_, err := repo.GetOrCreateDisk(context.Background(), 7, &types.Disk{
		Name: "root", Size: "32Gi", Driver: types.DurableDiskDriverQcow, MountPath: "/",
	})
	require.ErrorContains(t, err, "changing drivers requires an explicit data migration")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDiskRegistrationIntegration(t *testing.T) {
	db := postgresIntegrationSession(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `
		CREATE TEMP TABLE disk (LIKE public.disk INCLUDING DEFAULTS);
		CREATE TEMP SEQUENCE disk_registration_id;
		ALTER TABLE disk ALTER COLUMN id SET DEFAULT nextval('pg_temp.disk_registration_id');
		CREATE UNIQUE INDEX ON disk (workspace_id,name) WHERE deleted_at IS NULL;`)
	require.NoError(t, err)
	repo := &PostgresBackendRepository{client: db}
	spec := &types.Disk{Name: "root", Size: "32Gi", Driver: types.DurableDiskDriverQcow, MountPath: "/"}
	first, err := repo.GetOrCreateDisk(ctx, 7, spec)
	require.NoError(t, err)
	again, err := repo.GetOrCreateDisk(ctx, 7, spec)
	require.NoError(t, err)
	require.Equal(t, first, again, "reusing a disk must not write or touch its timestamp")
	lookup, err := repo.GetOrCreateDisk(ctx, 7, &types.Disk{Name: "root"})
	require.NoError(t, err)
	require.Equal(t, first, lookup, "lookup by name must not require creation options")
	_, err = repo.GetOrCreateDisk(ctx, 7, &types.Disk{Name: "root", Driver: types.DurableDiskDriverSnapshot})
	require.ErrorContains(t, err, "changing drivers")
	_, err = repo.GetOrCreateDisk(ctx, 7, &types.Disk{Name: "root", Filesystem: "xfs"})
	require.ErrorContains(t, err, "filesystem cannot be changed")
	_, err = repo.GetOrCreateDisk(ctx, 7, &types.Disk{Name: "root", Size: "16Gi"})
	require.ErrorContains(t, err, "cannot shrink")
	grown, err := repo.GetOrCreateDisk(ctx, 7, &types.Disk{Name: "root", Size: "64Gi"})
	require.NoError(t, err)
	require.Equal(t, first.Id, grown.Id)
	require.Equal(t, "64Gi", grown.Size)
	require.NoError(t, repo.DeleteDisk(ctx, 7, "root"))
	recreated, err := repo.GetOrCreateDisk(ctx, 7, spec)
	require.NoError(t, err)
	require.NotEqual(t, first.Id, recreated.Id)
	_, err = repo.GetOrCreateDisk(ctx, 7, &types.Disk{Name: "missing"})
	require.ErrorContains(t, err, "invalid disk size")
	var count int
	require.NoError(t, db.GetContext(ctx, &count, `SELECT count(*) FROM disk`))
	require.Equal(t, 2, count)
}

func TestGetDiskSnapshotOnlySharesAvailableSnapshots(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	mock.ExpectQuery(`public = TRUE AND status = \$3`).
		WithArgs(uint(7), "snapshot-1", types.DiskSnapshotStatusAvailable).
		WillReturnError(sql.ErrNoRows)

	_, err := repo.GetDiskSnapshot(context.Background(), 7, "snapshot-1")
	require.ErrorAs(t, err, new(*types.ErrDiskSnapshotNotFound))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetDiskSnapshotChainIsBoundedAndRequiresEveryParent(t *testing.T) {
	for _, test := range []struct {
		name  string
		count int
		root  string
		want  string
	}{
		{name: "complete", count: 16},
		{name: "missing head", want: "not found"},
		{name: "missing parent", count: 1, root: "hidden", want: "missing or inaccessible"},
		{name: "cycle or excessive depth", count: types.DiskSnapshotMaxChainDepth + 1, root: "cycle", want: "exceeds"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo, mock := NewBackendPostgresRepositoryForTest()
			rows := sqlmock.NewRows([]string{"external_id", "parent_snapshot_id"})
			for i := range test.count {
				parent := test.root
				if i > 0 {
					parent = fmt.Sprint(i - 1)
				}
				rows.AddRow(fmt.Sprint(i), parent)
			}
			mock.ExpectQuery(`WITH RECURSIVE chain AS .*parent.deleted_at IS NULL.*parent.workspace_id = \$1 OR \(parent.public = TRUE AND parent.status = \$3\).*chain.depth <= \$4.*ORDER BY depth DESC`).
				WithArgs(uint(7), "head", types.DiskSnapshotStatusAvailable, types.DiskSnapshotMaxChainDepth).
				WillReturnRows(rows)
			chain, err := repo.GetDiskSnapshotChain(context.Background(), 7, "head")
			if test.want == "" {
				require.NoError(t, err)
				require.Len(t, chain, test.count)
				require.Empty(t, chain[0].ParentSnapshotId)
			} else {
				require.ErrorContains(t, err, test.want)
				require.Nil(t, chain)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDeleteDiskOnlyUnregistersMutableDisk(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)

	mock.ExpectExec(`UPDATE disk SET deleted_at = CURRENT_TIMESTAMP`).
		WithArgs(uint(7), "template-source").
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, postgresRepo.DeleteDisk(context.Background(), 7, "template/source"))
	// No disk_snapshot expectation is deliberate: immutable snapshots may be
	// pinned by a template or consumed by another workspace after publication.
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateDiskSnapshotStoresManifestReference(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)

	now := time.Now()
	mock.ExpectQuery("INSERT INTO disk_snapshot").
		WithArgs(
			uint(7),
			uint(13),
			"pg-data",
			types.DiskSnapshotFormatDirV1,
			types.DiskSnapshotStatusPending,
			"",
			"",
			int64(42),
			int64(10<<30),
			"ext4",
			types.DurableDiskDriverSnapshot,
			"",
			"",
			int64(0),
			int64(0),
			int64(0),
			int64(0),
			"beta9-workspace-disk-pg-data",
			"snapshots/workspace/pg-data/snap-1",
			"private-pool",
			"worker-a",
			"node-a",
			nil,
		).
		WillReturnRows(diskSnapshotRows().AddRow(
			uint(1),
			"snapshot-1",
			uint(7),
			uint(13),
			"pg-data",
			types.DiskSnapshotFormatDirV1,
			types.DiskSnapshotStatusPending,
			"",
			"",
			int64(42),
			int64(10<<30),
			"ext4",
			types.DurableDiskDriverSnapshot,
			"",
			"",
			int64(0),
			int64(0),
			int64(0),
			int64(0),
			"beta9-workspace-disk-pg-data",
			"snapshots/workspace/pg-data/snap-1",
			"private-pool",
			"worker-a",
			"node-a",
			now,
			now,
			nil,
			nil,
			false,
		))

	snapshot, err := postgresRepo.CreateDiskSnapshot(context.Background(), &types.DiskSnapshot{
		WorkspaceId:         7,
		StubId:              13,
		DiskName:            "pg-data",
		Generation:          42,
		SizeBytes:           10 << 30,
		Filesystem:          "ext4",
		Driver:              types.DurableDiskDriverSnapshot,
		ObjectPrefix:        "snapshots/workspace/pg-data/snap-1",
		SourcePool:          "private-pool",
		BucketName:          "beta9-workspace-disk-pg-data",
		SourceWorkerId:      "worker-a",
		SourceStorageNodeId: "node-a",
	})

	require.NoError(t, err)
	require.Equal(t, "snapshot-1", snapshot.ExternalId)
	require.Equal(t, types.DiskSnapshotStatusPending, snapshot.Status)
	require.Equal(t, "snapshots/workspace/pg-data/snap-1", snapshot.ObjectPrefix)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateDiskSnapshotSetsCompletedAtForTerminalStatus(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)

	now := time.Now()
	mock.ExpectQuery("INSERT INTO disk_snapshot").
		WithArgs(
			uint(7),
			uint(13),
			"pg-data",
			types.DiskSnapshotFormatDirV1,
			types.DiskSnapshotStatusAvailable,
			"",
			"",
			int64(42),
			int64(10<<30),
			"ext4",
			types.DurableDiskDriverSnapshot,
			"snapshots/workspace/pg-data/snap-1/manifest.json",
			"sha256:manifest",
			int64(512),
			int64(128),
			int64(10<<30),
			int64(3<<30),
			"beta9-workspace-disk-pg-data",
			"snapshots/workspace/pg-data/snap-1",
			"private-pool",
			"worker-a",
			"node-a",
			sqlmock.AnyArg(),
		).
		WillReturnRows(diskSnapshotRows().AddRow(
			uint(1),
			"snapshot-1",
			uint(7),
			uint(13),
			"pg-data",
			types.DiskSnapshotFormatDirV1,
			types.DiskSnapshotStatusAvailable,
			"",
			"",
			int64(42),
			int64(10<<30),
			"ext4",
			types.DurableDiskDriverSnapshot,
			"snapshots/workspace/pg-data/snap-1/manifest.json",
			"sha256:manifest",
			int64(512),
			int64(128),
			int64(10<<30),
			int64(3<<30),
			"beta9-workspace-disk-pg-data",
			"snapshots/workspace/pg-data/snap-1",
			"private-pool",
			"worker-a",
			"node-a",
			now,
			now,
			now,
			nil,
			false,
		))

	snapshot, err := postgresRepo.CreateDiskSnapshot(context.Background(), &types.DiskSnapshot{
		WorkspaceId:         7,
		StubId:              13,
		DiskName:            "pg-data",
		Format:              types.DiskSnapshotFormatDirV1,
		Status:              types.DiskSnapshotStatusAvailable,
		Generation:          42,
		SizeBytes:           10 << 30,
		Filesystem:          "ext4",
		Driver:              types.DurableDiskDriverSnapshot,
		ManifestKey:         "snapshots/workspace/pg-data/snap-1/manifest.json",
		ManifestDigest:      "sha256:manifest",
		ManifestSizeBytes:   512,
		ChunkCount:          128,
		LogicalSizeBytes:    10 << 30,
		StoredSizeBytes:     3 << 30,
		ObjectPrefix:        "snapshots/workspace/pg-data/snap-1",
		SourcePool:          "private-pool",
		BucketName:          "beta9-workspace-disk-pg-data",
		SourceWorkerId:      "worker-a",
		SourceStorageNodeId: "node-a",
	})

	require.NoError(t, err)
	require.Equal(t, types.DiskSnapshotStatusAvailable, snapshot.Status)
	require.True(t, snapshot.CompletedAt.Valid)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateDiskSnapshotFinalizesManifestReference(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)

	now := time.Now()
	completedAt := now.Add(time.Minute)
	mock.ExpectQuery("UPDATE disk_snapshot").
		WillReturnRows(diskSnapshotRows().AddRow(
			uint(1),
			"snapshot-1",
			uint(7),
			uint(13),
			"pg-data",
			types.DiskSnapshotFormatDirV1,
			types.DiskSnapshotStatusAvailable,
			"",
			"",
			int64(42),
			int64(10<<30),
			"ext4",
			types.DurableDiskDriverSnapshot,
			"snapshots/workspace/pg-data/snap-1/manifest.json",
			"sha256:manifest",
			int64(512),
			int64(128),
			int64(10<<30),
			int64(3<<30),
			"beta9-workspace-disk-pg-data",
			"snapshots/workspace/pg-data/snap-1",
			"private-pool",
			"worker-a",
			"node-a",
			now,
			completedAt,
			completedAt,
			nil,
			true,
		))

	snapshot, err := postgresRepo.UpdateDiskSnapshot(context.Background(), &types.DiskSnapshot{
		ExternalId:        "snapshot-1",
		Status:            types.DiskSnapshotStatusAvailable,
		ManifestKey:       "snapshots/workspace/pg-data/snap-1/manifest.json",
		ManifestDigest:    "sha256:manifest",
		ManifestSizeBytes: 512,
		ChunkCount:        128,
		LogicalSizeBytes:  10 << 30,
		StoredSizeBytes:   3 << 30,
		BucketName:        "beta9-workspace-disk-pg-data",
		ObjectPrefix:      "snapshots/workspace/pg-data/snap-1",
		Public:            true,
	})

	require.NoError(t, err)
	require.Equal(t, types.DiskSnapshotStatusAvailable, snapshot.Status)
	require.Equal(t, "snapshots/workspace/pg-data/snap-1/manifest.json", snapshot.ManifestKey)
	require.Equal(t, int64(128), snapshot.ChunkCount)
	require.True(t, snapshot.CompletedAt.Valid)
	require.True(t, snapshot.Public)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetLatestDiskSnapshotOrdersConcurrentGenerationsDeterministically(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)

	now := time.Now()
	mock.ExpectQuery("ORDER BY generation DESC, created_at DESC, id DESC").
		WithArgs("{7}", "{\"pg-data\"}").
		WillReturnRows(diskSnapshotRows().AddRow(
			uint(2),
			"snapshot-2",
			uint(7),
			uint(13),
			"pg-data",
			types.DiskSnapshotFormatDirV1,
			types.DiskSnapshotStatusAvailable,
			"",
			"snapshot-1",
			int64(2),
			int64(10<<30),
			"ext4",
			types.DurableDiskDriverSnapshot,
			"durable-disks/pg-data/snapshots/2/manifest.json",
			"sha256:manifest-2",
			int64(512),
			int64(128),
			int64(10<<30),
			int64(3<<30),
			"disk-bucket",
			"durable-disks/pg-data/snapshots/2",
			"default",
			"worker-a",
			"node-a",
			now,
			now,
			now,
			nil,
			false,
		))

	snapshot, err := postgresRepo.GetLatestDiskSnapshot(context.Background(), 7, "pg-data")

	require.NoError(t, err)
	require.Equal(t, "snapshot-2", snapshot.ExternalId)
	require.Equal(t, int64(2), snapshot.Generation)
	require.NoError(t, mock.ExpectationsWereMet())
}

func diskSnapshotRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id",
		"external_id",
		"workspace_id",
		"stub_id",
		"disk_name",
		"format",
		"status",
		"reason",
		"parent_snapshot_id",
		"generation",
		"size_bytes",
		"filesystem",
		"driver",
		"manifest_key",
		"manifest_digest",
		"manifest_size_bytes",
		"chunk_count",
		"logical_size_bytes",
		"stored_size_bytes",
		"bucket_name",
		"object_prefix",
		"source_pool",
		"source_worker_id",
		"source_storage_node_id",
		"created_at",
		"updated_at",
		"completed_at",
		"deleted_at",
		"public",
	})
}
