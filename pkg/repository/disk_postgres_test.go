package repository

import (
	"context"
	"database/sql"
	"errors"
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

	mock.ExpectQuery(`SELECT .* FROM disk`).
		WithArgs(uint(7), "machine-root").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO disk`).
		WithArgs(uint(7), "machine-root", "32Gi", "ext4", types.DurableDiskDriverQcow, "/").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "external_id", "workspace_id", "name", "size", "filesystem", "driver", "mount_path",
			"created_at", "updated_at", "deleted_at",
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

	mock.ExpectQuery(`SELECT .* FROM disk`).
		WithArgs(uint(7), "machine-root").
		WillReturnError(sql.ErrNoRows)

	_, err := postgresRepo.GetOrCreateDisk(context.Background(), 7, &types.Disk{
		Name:   "machine-root",
		Driver: "unsupported",
	})

	require.EqualError(t, err, `unsupported durable disk driver "unsupported"`)
	require.NoError(t, mock.ExpectationsWereMet())
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
		WithArgs(uint(7), "pg-data", types.DiskSnapshotStatusAvailable).
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

func TestPublishSnapshotRequiresHeadOwnership(t *testing.T) {
	for _, public := range []bool{false, true} {
		backend, mock := NewBackendPostgresRepositoryForTest()
		mock.ExpectBegin()
		query := mock.ExpectQuery(`SELECT .* FROM disk_snapshot .* FOR UPDATE`).
			WithArgs(uint(8), "private-head", types.DiskSnapshotStatusAvailable)
		if public {
			query.WillReturnRows(sqlmock.NewRows([]string{"external_id", "workspace_id", "status", "public"}).
				AddRow("private-head", uint(7), types.DiskSnapshotStatusAvailable, true))
		} else {
			query.WillReturnError(sql.ErrNoRows)
		}
		mock.ExpectRollback() // No visibility update may be attempted, even for a public foreign head.
		err := backend.PublishDiskSnapshot(context.Background(), 8, "private-head")
		require.ErrorAs(t, err, new(*types.ErrDiskSnapshotNotFound))
		require.NoError(t, mock.ExpectationsWereMet())
	}
}

func TestPublishSnapshotChainIsAtomicAndDoesNotReplayStatus(t *testing.T) {
	for _, test := range []struct {
		name                                                                   string
		unavailable, updateFailure, withdrawn, artifactFailure, publicAncestor bool
	}{
		{name: "snapshot"},
		{name: "public ancestor", publicAncestor: true},
		{name: "unavailable ancestor", unavailable: true},
		{name: "later update failure", updateFailure: true},
		{name: "conditional update rejected", withdrawn: true},
		{name: "artifact insert failure", artifactFailure: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, mock := NewBackendPostgresRepositoryForTest()
			repo := backend.(*PostgresBackendRepository)
			mock.ExpectBegin()
			locked := func(id, parent string, status types.DiskSnapshotStatus, owner uint, public bool) {
				mock.ExpectQuery(`SELECT .* FROM disk_snapshot .* FOR UPDATE`).
					WithArgs(uint(7), id, types.DiskSnapshotStatusAvailable).
					WillReturnRows(sqlmock.NewRows([]string{"external_id", "workspace_id", "parent_snapshot_id", "status", "public"}).
						AddRow(id, owner, parent, status, public))
			}
			locked("head", "base", types.DiskSnapshotStatusAvailable, 7, false)
			status, owner := types.DiskSnapshotStatusAvailable, uint(7)
			if test.unavailable {
				status = types.DiskSnapshotStatusPending
			}
			if test.publicAncestor {
				owner = 8
			}
			locked("base", "", status, owner, test.publicAncestor)
			if !test.unavailable {
				// Publishing must update visibility alone and fence withdrawn rows.
				update := func(id string) *sqlmock.ExpectedExec {
					return mock.ExpectExec(`UPDATE disk_snapshot SET public=TRUE,updated_at=CURRENT_TIMESTAMP WHERE workspace_id=\$1 AND external_id=\$2 AND status=\$3 AND deleted_at IS NULL`).
						WithArgs(uint(7), id, types.DiskSnapshotStatusAvailable)
				}
				if !test.publicAncestor {
					update("base").WillReturnResult(sqlmock.NewResult(0, 1))
				}
				if test.updateFailure {
					update("head").WillReturnError(errors.New("update unavailable"))
				} else if test.withdrawn {
					update("head").WillReturnResult(sqlmock.NewResult(0, 0))
				} else {
					update("head").WillReturnResult(sqlmock.NewResult(0, 1))
				}
				if test.artifactFailure {
					mock.ExpectExec(`INSERT INTO vm_artifact`).WillReturnError(errors.New("artifact unavailable"))
				}
			}
			failed := test.unavailable || test.updateFailure || test.withdrawn || test.artifactFailure
			if failed {
				mock.ExpectRollback()
			} else {
				mock.ExpectCommit()
			}
			var err error
			if test.artifactFailure {
				err = repo.CreateVMArtifact(context.Background(), 7, &types.VMArtifact{ID: "template", Kind: types.VMArtifactTemplate, RootSnapshotID: "head", Public: true})
			} else {
				err = repo.PublishDiskSnapshot(context.Background(), 7, "head")
			}
			if failed {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
