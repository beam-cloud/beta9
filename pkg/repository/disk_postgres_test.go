package repository

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

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
			types.DurableDiskDriverDev,
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
			types.DurableDiskDriverDev,
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
		))

	snapshot, err := postgresRepo.CreateDiskSnapshot(context.Background(), &types.DiskSnapshot{
		WorkspaceId:         7,
		StubId:              13,
		DiskName:            "pg-data",
		Generation:          42,
		SizeBytes:           10 << 30,
		Filesystem:          "ext4",
		Driver:              types.DurableDiskDriverDev,
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
			types.DurableDiskDriverDev,
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
	})

	require.NoError(t, err)
	require.Equal(t, types.DiskSnapshotStatusAvailable, snapshot.Status)
	require.Equal(t, "snapshots/workspace/pg-data/snap-1/manifest.json", snapshot.ManifestKey)
	require.Equal(t, int64(128), snapshot.ChunkCount)
	require.True(t, snapshot.CompletedAt.Valid)
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
	})
}
