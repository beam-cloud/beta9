package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func stateVolumeRows() *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{"id", "external_id", "workspace_id", "name", "size", "mount_path", "current_generation_id", "created_at", "updated_at", "deleted_at"}).
		AddRow(uint(41), "21d4182a-4930-47b4-a987-e50c4a80156f", uint(7), "data", "4Gi", "/data", "", now, now, nil)
}

const testStateVolumeReleaseClaimID = "9bdcbd90-294f-4943-ae6f-a5dc7d325b1b"

func expectSourceStateVolumeReleaseIntent(mock sqlmock.Sqlmock, workerID, workerInstanceID, storageNodeID,
	attachmentToken string, fencingToken int64, completed bool,
) {
	mock.ExpectQuery(`SELECT external_id::text AS external_id.*state_volume_release_claim`).
		WithArgs(uint(7), "writer").
		WillReturnRows(sqlmock.NewRows([]string{"external_id", "workspace_id", "container_id", "source_worker_id",
			"source_worker_instance_id", "storage_node_id", "recovery_worker_id", "recovery_worker_instance_id",
			"journal_digest", "claim_generation", "phase", "completed"}).
			AddRow(testStateVolumeReleaseClaimID, uint(7), "writer", workerID, workerInstanceID, storageNodeID,
				workerID, workerInstanceID, "sha256:"+strings.Repeat("a", 64), int64(0),
				map[bool]string{true: "completed", false: "source"}[completed], completed))
	mock.ExpectQuery(`SELECT volume_id::text AS volume_id, attachment_kind`).
		WithArgs(testStateVolumeReleaseClaimID, workerID, workerInstanceID, storageNodeID).
		WillReturnRows(sqlmock.NewRows([]string{"volume_id", "attachment_kind", "attachment_plan_id",
			"attachment_token", "fencing_token", "lease_settled", "owner_worker_id",
			"owner_worker_instance_id", "storage_node_id", "next_fencing_token"}).
			AddRow("21d4182a-4930-47b4-a987-e50c4a80156f", "named",
				"7aee3365-2963-4a6d-b9fb-2c934924880d", attachmentToken, fencingToken, true,
				workerID, workerInstanceID, storageNodeID, fencingToken))
}

func TestDeleteDiskTargetsOnlyNewStateVolumeTable(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM state_volume`).
		WithArgs(uint(7), "template-source").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(uint(41)))
	mock.ExpectQuery(`SELECT count\(\*\) FROM state_volume_attachment`).
		WithArgs(uint(41)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`UPDATE state_volume SET deleted_at = CURRENT_TIMESTAMP`).
		WithArgs(uint(41)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	require.NoError(t, postgresRepo.DeleteDisk(context.Background(), 7, "template/source"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteDiskRejectsAnActiveStateVolumeAttachment(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id FROM state_volume`).
		WithArgs(uint(7), "data").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(uint(41)))
	mock.ExpectQuery(`SELECT count\(\*\) FROM state_volume_attachment`).WithArgs(uint(41)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectRollback()

	require.ErrorContains(t, postgresRepo.DeleteDisk(context.Background(), 7, "data"), "active attachments")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetOrCreateDiskRejectsGeometryMismatch(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	now := time.Now()
	mock.ExpectQuery(`SELECT .* FROM state_volume`).WithArgs(uint(7), "data").WillReturnRows(
		sqlmock.NewRows([]string{"id", "external_id", "workspace_id", "name", "size", "mount_path", "current_generation_id", "created_at", "updated_at", "deleted_at"}).
			AddRow(uint(41), "21d4182a-4930-47b4-a987-e50c4a80156f", uint(7), "data", "8Gi", "/data", "", now, now, nil),
	)
	_, err := postgresRepo.GetOrCreateDisk(context.Background(), 7, &types.Disk{Name: "data", Size: "4Gi", MountPath: "/data"})
	require.ErrorContains(t, err, "already exists")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetOrCreateDiskRejectsConcurrentGeometryMismatch(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.ExpectQuery(`SELECT .* FROM state_volume`).WithArgs(uint(7), "data").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`INSERT INTO state_volume`).WillReturnError(sql.ErrNoRows)
	now := time.Now()
	mock.ExpectQuery(`SELECT .* FROM state_volume`).WithArgs(uint(7), "data").WillReturnRows(
		sqlmock.NewRows([]string{"id", "external_id", "workspace_id", "name", "size", "mount_path", "current_generation_id", "created_at", "updated_at", "deleted_at"}).
			AddRow(uint(41), "21d4182a-4930-47b4-a987-e50c4a80156f", uint(7), "data", "8Gi", "/data", "", now, now, nil),
	)
	_, err := postgresRepo.GetOrCreateDisk(context.Background(), 7, &types.Disk{Name: "data", Size: "4Gi", MountPath: "/data"})
	require.ErrorContains(t, err, "already exists")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestResolveReadOnlyStateAttachmentAllowsMultiattachWithoutWriterLease(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	generationID := "7aee3365-2963-4a6d-b9fb-2c934924880d"
	for _, containerID := range []string{"reader-1", "reader-2"} {
		mock.ExpectBegin()
		mock.ExpectQuery(`INSERT INTO state_read_only_attachment`).
			WithArgs(uint(7), containerID, "21d4182a-4930-47b4-a987-e50c4a80156f", generationID, "data", "/data", false).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(uint(91)))
		mock.ExpectCommit()
		require.NoError(t, postgresRepo.ResolveReadOnlyStateAttachment(context.Background(), 7, containerID,
			"21d4182a-4930-47b4-a987-e50c4a80156f", generationID, "data", "/data", false))
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestResolveStateVolumeExpiredWriterFailsClosed(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT request_hash FROM state_volume_attachment_plan`).
		WithArgs(uint(7), "writer-old", "7aee3365-2963-4a6d-b9fb-2c934924880d", strings.Repeat("a", 64)).
		WillReturnRows(sqlmock.NewRows([]string{"request_hash"}).AddRow(strings.Repeat("a", 64)))
	mock.ExpectQuery(`SELECT .* FROM state_volume`).WithArgs(uint(7), "data").WillReturnRows(stateVolumeRows())
	mock.ExpectQuery(`SELECT COALESCE\(attachment_plan_id`).WithArgs(uint(41), "writer-old").WillReturnRows(
		sqlmock.NewRows([]string{"attachment_plan_id", "source_generation_id", "initialize", "attachment_token", "fencing_token", "expires_at"}).
			AddRow("7aee3365-2963-4a6d-b9fb-2c934924880d", "", true, "35141b8e-4591-4c72-856a-3ab7e831818e", int64(8), time.Now().Add(-time.Minute)),
	)
	mock.ExpectRollback()
	_, err := postgresRepo.ResolveStateVolumeAttachment(context.Background(), 7, "writer-old",
		"7aee3365-2963-4a6d-b9fb-2c934924880d", strings.Repeat("a", 64),
		&types.Disk{Name: "data", Size: "4Gi", MountPath: "/data"}, "")
	require.ErrorContains(t, err, "authoritative teardown is required")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestResolveStateVolumeConcurrentWriterIsFenced(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT request_hash FROM state_volume_attachment_plan`).
		WithArgs(uint(7), "writer-2", "7aee3365-2963-4a6d-b9fb-2c934924880d", strings.Repeat("a", 64)).
		WillReturnRows(sqlmock.NewRows([]string{"request_hash"}).AddRow(strings.Repeat("a", 64)))
	mock.ExpectQuery(`SELECT .* FROM state_volume`).WithArgs(uint(7), "data").WillReturnRows(stateVolumeRows())
	mock.ExpectQuery(`SELECT COALESCE\(attachment_plan_id`).WithArgs(uint(41), "writer-2").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`UPDATE state_volume SET`).WithArgs(uint(41)).
		WillReturnRows(sqlmock.NewRows([]string{"next_fencing_token"}).AddRow(int64(9)))
	mock.ExpectExec(`INSERT INTO state_volume_attachment`).WillReturnError(&pq.Error{Code: "23505"})
	mock.ExpectRollback()
	_, err := postgresRepo.ResolveStateVolumeAttachment(context.Background(), 7, "writer-2",
		"7aee3365-2963-4a6d-b9fb-2c934924880d", strings.Repeat("a", 64),
		&types.Disk{Name: "data", Size: "4Gi", MountPath: "/data"}, "")
	require.ErrorContains(t, err, "already attached")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExactWriterCanRenewAfterTTLButSupersededTokenCannot(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE state_volume_attachment.*next_fencing_token = \$5`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expiresAt, err := postgresRepo.RenewStateVolumeAttachments(context.Background(), 7, "writer-old", "worker-1", "instance-1", "node-1", []types.StateVolumeLease{{
		VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: 8,
	}})
	require.NoError(t, err)
	require.True(t, expiresAt.After(time.Now()))
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE state_volume_attachment`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`UPDATE state_branch_attachment.*next_fencing_token = \$5`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expiresAt, err = postgresRepo.RenewStateVolumeAttachments(context.Background(), 7, "writer-old", "worker-1", "instance-1", "node-1", []types.StateVolumeLease{{
		VolumeId: "6df3f5bb-6959-4da4-88e7-57d4f11bf54f", AttachmentToken: "b93ef4a4-43db-4625-9559-375fbbd6956a", FencingToken: 11,
	}})
	require.NoError(t, err)
	require.True(t, expiresAt.After(time.Now()))

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE state_volume_attachment`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`UPDATE state_branch_attachment`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	_, err = postgresRepo.RenewStateVolumeAttachments(context.Background(), 7, "writer-old", "worker-stale", "instance-stale", "node-1", []types.StateVolumeLease{{
		VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: 8,
	}})
	require.ErrorContains(t, err, "missing or fenced")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStateVolumeReleaseIsExactAndIdempotent(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.ExpectBegin()
	expectSourceStateVolumeReleaseIntent(mock, "worker-1", "instance-1", "node-1",
		"35141b8e-4591-4c72-856a-3ab7e831818e", 9, false)
	mock.ExpectQuery(`SELECT count\(\*\).*state_snapshot_member_plan`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`DELETE FROM state_volume_attachment`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM state_branch_attachment`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT.*state_volume_attachment`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`DELETE FROM state_read_only_attachment`).
		WithArgs(uint(7), "writer").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT.*state_volume_attachment`).
		WithArgs(uint(7), "writer").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`DELETE FROM state_volume_attachment_plan`).
		WithArgs(uint(7), "writer").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE state_volume_release_claim SET completed_at`).
		WithArgs(testStateVolumeReleaseClaimID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	require.NoError(t, postgresRepo.ReleaseStateVolumeAttachments(context.Background(), 7, "writer", "worker-1", "instance-1", "node-1", []types.StateVolumeLease{{
		VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: 9,
	}}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStateVolumeReleaseRejectsPendingSnapshotLease(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.ExpectBegin()
	expectSourceStateVolumeReleaseIntent(mock, "worker-1", "instance-1", "node-1",
		"35141b8e-4591-4c72-856a-3ab7e831818e", 9, false)
	mock.ExpectQuery(`SELECT count\(\*\).*state_snapshot_member_plan`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectRollback()
	err := postgresRepo.ReleaseStateVolumeAttachments(context.Background(), 7, "writer", "worker-1", "instance-1", "node-1", []types.StateVolumeLease{{
		VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: 9,
	}})
	require.ErrorContains(t, err, "pending snapshot operation")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStateVolumeReleaseRejectsStaleTupleWhileWriterRemains(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.ExpectBegin()
	expectSourceStateVolumeReleaseIntent(mock, "worker-stale", "instance-stale", "node-1",
		"35141b8e-4591-4c72-856a-3ab7e831818e", 8, false)
	mock.ExpectQuery(`SELECT count\(\*\).*state_snapshot_member_plan`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`DELETE FROM state_volume_attachment`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM state_branch_attachment`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT.*state_volume_attachment`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectRollback()
	err := postgresRepo.ReleaseStateVolumeAttachments(context.Background(), 7, "writer", "worker-stale", "instance-stale", "node-1", []types.StateVolumeLease{{
		VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: 8,
	}})
	require.ErrorContains(t, err, "stale or fenced")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPendingStateVolumeReleaseIsScopedToWorkspaceAndContainer(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT request_hash, admitted_at IS NOT NULL AS admitted`).
		WithArgs(uint(7), "pending-container", "7aee3365-2963-4a6d-b9fb-2c934924880d", strings.Repeat("a", 64)).
		WillReturnRows(sqlmock.NewRows([]string{"request_hash", "admitted", "aborted"}).AddRow(strings.Repeat("a", 64), true, false))
	mock.ExpectQuery(`SELECT.*state_volume_attachment`).
		WithArgs(uint(7), "pending-container", "7aee3365-2963-4a6d-b9fb-2c934924880d").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectExec(`DELETE FROM state_volume_attachment`).WithArgs(uint(7), "pending-container", "7aee3365-2963-4a6d-b9fb-2c934924880d").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM state_branch_attachment`).WithArgs(uint(7), "pending-container", "7aee3365-2963-4a6d-b9fb-2c934924880d").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM state_read_only_attachment`).WithArgs(uint(7), "pending-container").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE state_volume_attachment_plan`).
		WithArgs(uint(7), "pending-container", "7aee3365-2963-4a6d-b9fb-2c934924880d", strings.Repeat("a", 64),
			"scheduler authoritatively stopped pending unassigned container").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	require.NoError(t, postgresRepo.ReleasePendingStateVolumeAttachments(context.Background(), 7, "pending-container",
		"7aee3365-2963-4a6d-b9fb-2c934924880d", strings.Repeat("a", 64)))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAvailableStateSnapshotRequiresExactlyOneRoot(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	generationID := "3f95e0d4-e31b-4a76-91b8-0c5779c1a1fd"
	_, err := postgresRepo.CommitStateSnapshot(context.Background(), &types.StateSnapshot{
		ExternalId: "e4f41f9a-524c-4906-8ea3-b36b32f45c27", WorkspaceId: 7,
		Mode: "live", RestoreMode: "cold_state", Status: types.StateSnapshotStatusAvailable,
		Generations: []types.StateGeneration{{
			VolumeId: "data-volume", GenerationId: generationID, Name: "data", MountPath: "/data", Generation: 1,
		}},
	}, []types.VolumeGeneration{{
		ExternalId: generationID, WorkspaceId: 7, VolumeId: "data-volume", Name: "data", Generation: 1,
		Status: types.StateSnapshotStatusAvailable, ManifestKey: "manifest", ManifestDigest: "sha256:digest",
		ManifestSizeBytes: 1, LogicalSizeBytes: 1, BucketName: "bucket", ObjectPrefix: "prefix",
	}}, nil, "worker-1", "instance-1", "node-1", 0)
	require.ErrorContains(t, err, "exactly one root")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStateSnapshotMembershipComparisonIsOrderIndependentAndExact(t *testing.T) {
	root := types.StateGeneration{VolumeId: "root", GenerationId: "g1", Name: "root", MountPath: "/", Root: true, Generation: 1}
	data := types.StateGeneration{VolumeId: "data", GenerationId: "g2", Name: "data", MountPath: "/data", Generation: 8}
	require.True(t, sameStateGenerations([]types.StateGeneration{root, data}, []types.StateGeneration{data, root}))
	data.GenerationId = "different"
	require.False(t, sameStateGenerations([]types.StateGeneration{root, data}, []types.StateGeneration{root}))
}

func TestIndependentSourceAndForkLineagesAdvanceWithoutGenerationCollisions(t *testing.T) {
	const (
		sourceVolume  = "21d4182a-4930-47b4-a987-e50c4a80156f"
		forkOneVolume = "f0d82a87-d142-4333-a9c6-dc621128ee64"
		forkTwoVolume = "3f95e0d4-e31b-4a76-91b8-0c5779c1a1fd"
		sourceS1      = "7aee3365-2963-4a6d-b9fb-2c934924880d"
		sourceS2      = "acee3e88-20d7-4bbc-92cc-4b839ad6bc55"
		forkOneS1     = "aa54e1b5-e878-401d-8fe1-49992c2f4b31"
		forkTwoS1     = "e4f41f9a-524c-4906-8ea3-b36b32f45c27"
	)
	lineage := []types.VolumeGeneration{
		{ExternalId: sourceS1, VolumeId: sourceVolume, Name: "root", Generation: 1},
		{ExternalId: sourceS2, VolumeId: sourceVolume, Name: "root", Generation: 2, ParentGenerationId: sourceS1},
		{ExternalId: "2d49a110-b3f9-40bc-9d75-9a904f2b710b", VolumeId: sourceVolume, Name: "root", Generation: 3, ParentGenerationId: sourceS2},
		{ExternalId: forkOneS1, VolumeId: forkOneVolume, Name: "root", Generation: 1, CloneParentGenerationId: sourceS1},
		{ExternalId: "a449744a-6540-41cb-abcf-acb55ea537a3", VolumeId: forkOneVolume, Name: "root", Generation: 2, ParentGenerationId: forkOneS1},
		{ExternalId: forkTwoS1, VolumeId: forkTwoVolume, Name: "root", Generation: 1, CloneParentGenerationId: sourceS2},
		{ExternalId: "79e2c048-1ed1-41e9-a8d1-57723a946d60", VolumeId: forkTwoVolume, Name: "root", Generation: 2, ParentGenerationId: forkTwoS1},
	}
	seen := map[string]struct{}{}
	for index := range lineage {
		require.NoError(t, validateVolumeGeneration(&lineage[index]))
		key := fmt.Sprintf("%s:%d", lineage[index].VolumeId, lineage[index].Generation)
		_, duplicate := seen[key]
		require.False(t, duplicate, key)
		seen[key] = struct{}{}
	}
}

func TestRecoveryClaimCASHandsOffExactGenerationAfterOwnerDeathProof(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	const snapshotID = "e4f41f9a-524c-4906-8ea3-b36b32f45c27"
	const recoveryProofToken = "f4821650-87ad-49d7-8866-04e68f61e6f1"
	columns := []string{"id", "external_id", "source_container_id", "operation_id", "source_worker_id",
		"source_worker_instance_id", "recovery_worker_id", "recovery_worker_instance_id",
		"recovery_claim_generation", "recovery_proof_token", "storage_node_id", "armed", "mode", "status"}
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FROM state_snapshot`).
		WithArgs(snapshotID, "container", "operation", "node-1").
		WillReturnRows(sqlmock.NewRows(columns).AddRow(uint(51), snapshotID, "container", "operation",
			"source", "source-instance", "worker-one", "instance-one", int64(1), recoveryProofToken, "node-1", true,
			"terminal", types.StateSnapshotStatusAvailable))
	mock.ExpectExec(`UPDATE state_snapshot_recovery_claim`).
		WithArgs(uint(51), "worker-two", "instance-two", "node-1", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT .* FROM state_snapshot WHERE id`).WithArgs(uint(51)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(uint(51), snapshotID, "container", "operation",
			"source", "source-instance", "worker-two", "instance-two", int64(2), recoveryProofToken, "node-1", true,
			"terminal", types.StateSnapshotStatusAvailable))
	mock.ExpectCommit()

	claimed, err := postgresRepo.ClaimStateSnapshotRecovery(context.Background(), snapshotID, "container", "operation",
		"worker-two", "instance-two", "node-1", recoveryProofToken, 1)
	require.NoError(t, err)
	require.Equal(t, "worker-two", claimed.RecoveryWorkerId)
	require.EqualValues(t, 2, claimed.RecoveryClaimGeneration)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAttachmentGenerationBindsExactCloneSourceAndCurrentHead(t *testing.T) {
	const (
		source = "7aee3365-2963-4a6d-b9fb-2c934924880d"
		head   = "acee3e88-20d7-4bbc-92cc-4b839ad6bc55"
	)
	cloneAttachment := stateAttachmentLineage{SourceGenerationId: source, CloneSource: true, Active: true}
	require.NoError(t, validateAttachmentGeneration("root", types.VolumeGeneration{
		Generation: 1, CloneParentGenerationId: source,
	}, "", cloneAttachment, types.StateGenerationCompaction{}, false))
	require.ErrorContains(t, validateAttachmentGeneration("root", types.VolumeGeneration{
		Generation: 1, CloneParentGenerationId: "aa54e1b5-e878-401d-8fe1-49992c2f4b31",
	}, "", cloneAttachment, types.StateGenerationCompaction{}, false), "exact attachment source")

	// A long-lived writer keeps the immutable source used when it attached. Once
	// the lineage has a head, every later pivot is instead bound to that exact
	// head, so repeated live snapshots can advance independently.
	require.NoError(t, validateAttachmentGeneration("root", types.VolumeGeneration{
		Generation: 3, ParentGenerationId: head,
	}, head, cloneAttachment, types.StateGenerationCompaction{}, false))
	require.ErrorContains(t, validateAttachmentGeneration("root", types.VolumeGeneration{
		Generation: 3, ParentGenerationId: source,
	}, head, cloneAttachment, types.StateGenerationCompaction{}, false), "exact current head")
}

func TestAttachmentGenerationInitialStateCannotInventCloneLineage(t *testing.T) {
	initialized := stateAttachmentLineage{Initialize: true, Active: true}
	require.NoError(t, validateAttachmentGeneration("root", types.VolumeGeneration{Generation: 1}, "", initialized, types.StateGenerationCompaction{}, false))
	require.ErrorContains(t, validateAttachmentGeneration("root", types.VolumeGeneration{
		Generation: 1, CloneParentGenerationId: "7aee3365-2963-4a6d-b9fb-2c934924880d",
	}, "", initialized, types.StateGenerationCompaction{}, false), "initialize attachment")
	require.ErrorContains(t, validateAttachmentGeneration("data", types.VolumeGeneration{
		Generation: 1, CloneParentGenerationId: "7aee3365-2963-4a6d-b9fb-2c934924880d",
	}, "", initialized, types.StateGenerationCompaction{}, true), "initialize attachment")
}

func TestAttachmentGenerationAllowsOnlyExactParentlessCompactionOfCurrentHead(t *testing.T) {
	const (
		volumeID     = "36eb1f5c-e9ed-464a-bd98-cc35d5d068bc"
		generationID = "12614665-148e-405b-9cc3-6e1b06f659d9"
		currentHead  = "1670704b-7589-49b8-be95-0015315125f7"
	)
	generation := types.VolumeGeneration{
		ExternalId: generationID, VolumeId: volumeID, Name: "root", Generation: 9,
	}
	compaction := types.StateGenerationCompaction{
		VolumeId: volumeID, GenerationId: generationID, SourceGenerationId: currentHead,
	}
	require.NoError(t, validateAttachmentGeneration("root", generation, currentHead,
		stateAttachmentLineage{Active: true}, compaction, false))

	wrongSource := compaction
	wrongSource.SourceGenerationId = "4f96d83a-0eb8-4a9a-afd4-fcae79069302"
	require.ErrorContains(t, validateAttachmentGeneration("root", generation, currentHead,
		stateAttachmentLineage{Active: true}, wrongSource, false), "exact current head")

	parented := generation
	parented.ParentGenerationId = currentHead
	require.ErrorContains(t, validateAttachmentGeneration("root", parented, currentHead,
		stateAttachmentLineage{Active: true}, compaction, false), "exact current head")
	require.ErrorContains(t, validateAttachmentGeneration("root", generation, currentHead,
		stateAttachmentLineage{Active: true}, types.StateGenerationCompaction{}, false), "advance its exact current head")
}

func TestStateMemberPlanDistinguishesNewCompactionFromReadOnlyAnchorReuse(t *testing.T) {
	const (
		rootVolume = "36eb1f5c-e9ed-464a-bd98-cc35d5d068bc"
		rootGen    = "12614665-148e-405b-9cc3-6e1b06f659d9"
		rootSource = "1670704b-7589-49b8-be95-0015315125f7"
		roVolume   = "4f96d83a-0eb8-4a9a-afd4-fcae79069302"
		roGen      = "4d4740d2-6af4-4d1b-8357-5981d42e5886"
		token      = "09737b39-ae66-4fa5-93da-d6ca4b6d60c9"
	)
	members := []types.StateGeneration{
		{VolumeId: rootVolume, GenerationId: rootGen, Generation: 9, Name: "root", MountPath: "/", Root: true},
		// This immutable read-only member may itself be an already-published
		// parentless compacted anchor. Reuse is not a new compaction operation.
		{VolumeId: roVolume, GenerationId: roGen, Generation: 7, Name: "data", MountPath: "/data", ReadOnly: true},
	}
	leases := []types.StateVolumeLease{{VolumeId: rootVolume, AttachmentToken: token, FencingToken: 3}}
	compactions := []types.StateGenerationCompaction{{
		VolumeId: rootVolume, GenerationId: rootGen, SourceGenerationId: rootSource,
	}}
	_, plannedCompactions, err := validateStateMemberPlan(members, compactions, leases)
	require.NoError(t, err)
	require.Equal(t, rootSource, plannedCompactions[rootVolume].SourceGenerationId)

	compactions = append(compactions, types.StateGenerationCompaction{
		VolumeId: roVolume, GenerationId: roGen, SourceGenerationId: rootSource,
	})
	_, _, err = validateStateMemberPlan(members, compactions, leases)
	require.ErrorContains(t, err, "invalid parentless compaction authorization")
}

func TestCreateStateSnapshotTerminalReplayReturnsExactCommittedGroup(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	const (
		snapshotID   = "e4f41f9a-524c-4906-8ea3-b36b32f45c27"
		volumeID     = "21d4182a-4930-47b4-a987-e50c4a80156f"
		generationID = "7aee3365-2963-4a6d-b9fb-2c934924880d"
		attachmentID = "35141b8e-4591-4c72-856a-3ab7e831818e"
	)
	snapshot := &types.StateSnapshot{
		OperationId: "operation", WorkspaceId: 7, StubId: 13, SourceContainerId: "container",
		SourceWorkerId: "worker-1", SourceWorkerInstanceId: "instance-1", StorageNodeId: "node-1",
		SourceStubExternalId: "stub", SourceStubName: "machine", SourceStubType: "pod",
		Mode: "terminal", IncludeMemory: true, Visible: true, ImageId: "image", ImageDigest: "sha256:image",
		RuntimeProfile: "runc", RestoreMode: "cold_state",
	}
	members := []types.StateGeneration{{
		VolumeId: volumeID, GenerationId: generationID, Name: "root", MountPath: "/", Root: true, Generation: 1,
	}}
	leases := []types.StateVolumeLease{{VolumeId: volumeID, AttachmentToken: attachmentID, FencingToken: 9}}
	now := time.Now()

	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO state_snapshot`).WillReturnRows(stateSnapshotRowsForTest(now).
		AddRow(uint(51), snapshotID, "operation", uint(7), uint(13), "container", "worker-1", "instance-1", "", "", int64(0), "f4821650-87ad-49d7-8866-04e68f61e6f1", "node-1", true, "stub", "machine", "pod",
			"terminal", true, true, types.StateSnapshotStatusAvailable, "", "image", "sha256:image", "runc",
			"", "", "", int64(0), "", "", "", "cold_state", "", false, now, now, now))
	mock.ExpectQuery(`SELECT volume_id::text AS volume_id`).WithArgs(uint(51)).WillReturnRows(
		sqlmock.NewRows([]string{"volume_id", "generation_id", "parent_generation_id", "clone_parent_generation_id", "compaction", "compaction_source_generation_id", "generation", "name", "mount_path", "read_only", "root", "attachment_token", "fencing_token"}).
			AddRow(volumeID, generationID, "", "", false, "", int64(1), "root", "/", false, true, attachmentID, int64(9)),
	)
	mock.ExpectCommit()
	mock.ExpectQuery(`SELECT .* FROM state_snapshot`).WithArgs(snapshotID, uint(7), types.StateSnapshotStatusAvailable).
		WillReturnRows(stateSnapshotRowsForTest(now).AddRow(uint(51), snapshotID, "operation", uint(7), uint(13), "container", "worker-1", "instance-1", "", "", int64(0), "f4821650-87ad-49d7-8866-04e68f61e6f1", "node-1", true, "stub", "machine", "pod",
			"terminal", true, true, types.StateSnapshotStatusAvailable, "", "image", "sha256:image", "runc",
			"", "", "", int64(0), "", "", "", "cold_state", "", false, now, now, now))
	mock.ExpectQuery(`SELECT ssg.volume_id`).WithArgs(uint(51)).WillReturnRows(
		sqlmock.NewRows([]string{"volume_id", "generation_id", "name", "mount_path", "parent_generation_id", "clone_parent_generation_id", "read_only", "root", "generation"}).
			AddRow(volumeID, generationID, "root", "/", "", "", false, true, int64(1)),
	)

	created, err := postgresRepo.CreateStateSnapshot(context.Background(), snapshot, members, nil, leases)
	require.NoError(t, err)
	require.Equal(t, snapshotID, created.ExternalId)
	require.Equal(t, types.StateSnapshotStatusAvailable, created.Status)
	require.Equal(t, members, created.Generations)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateStateSnapshotRejectsASecondPendingOperationForContainer(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	const (
		volumeID     = "21d4182a-4930-47b4-a987-e50c4a80156f"
		generationID = "7aee3365-2963-4a6d-b9fb-2c934924880d"
		attachmentID = "35141b8e-4591-4c72-856a-3ab7e831818e"
	)
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO state_snapshot`).WillReturnError(&pq.Error{
		Code: "23505", Constraint: "idx_state_snapshot_one_pending_container",
	})
	mock.ExpectRollback()
	_, err := postgresRepo.CreateStateSnapshot(context.Background(), &types.StateSnapshot{
		OperationId: "operation-2", WorkspaceId: 7, StubId: 13, SourceContainerId: "container",
		SourceWorkerId: "worker-1", SourceWorkerInstanceId: "instance-1", StorageNodeId: "node-1", SourceStubExternalId: "stub",
		SourceStubName: "machine", SourceStubType: "pod", Mode: "terminal", ImageId: "image",
		ImageDigest: "sha256:image", RuntimeProfile: "runc",
	}, []types.StateGeneration{{
		VolumeId: volumeID, GenerationId: generationID, Name: "root", MountPath: "/", Root: true, Generation: 1,
	}}, nil, []types.StateVolumeLease{{VolumeId: volumeID, AttachmentToken: attachmentID, FencingToken: 9}})
	require.ErrorContains(t, err, "another pending")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStateSnapshotMustArmBeforeCommitAndFailureReleasesEscrowPlan(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	const snapshotID = "e4f41f9a-524c-4906-8ea3-b36b32f45c27"
	const recoveryProofToken = "f4821650-87ad-49d7-8866-04e68f61e6f1"
	now := time.Now()
	pendingRow := func(armed bool) *sqlmock.Rows {
		return stateSnapshotRowsForTest(now).AddRow(uint(51), snapshotID, "operation", uint(7), uint(13),
			"container", "worker-1", "instance-1", "", "", int64(0), recoveryProofToken, "node-1", armed, "stub", "machine", "pod", "terminal", false, true,
			types.StateSnapshotStatusPending, "", "image", "sha256:image", "runc",
			"", "", "", int64(0), "", "", "", "cold_state", "", false, now, now, nil)
	}

	mock.ExpectQuery(`UPDATE state_snapshot`).
		WithArgs(snapshotID, "container", "operation", "worker-1", "instance-1", "node-1", recoveryProofToken).
		WillReturnRows(pendingRow(true))
	armed, err := postgresRepo.ArmStateSnapshot(context.Background(), snapshotID, "container", "operation", "worker-1", "instance-1", "node-1", recoveryProofToken)
	require.NoError(t, err)
	require.True(t, armed.Armed)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FROM state_snapshot`).
		WithArgs(snapshotID, "container", "operation", "node-1").
		WillReturnRows(pendingRow(true))
	mock.ExpectExec(`UPDATE volume_generation`).WithArgs(uint(51), "qmp pivot failed", uint(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE state_snapshot SET status = 'failed'`).WithArgs(uint(51), "qmp pivot failed").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM state_snapshot_member_plan`).WithArgs(uint(51)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	failed, err := postgresRepo.FailStateSnapshot(context.Background(), snapshotID, "container", "operation",
		"worker-1", "instance-1", "node-1", "qmp pivot failed", 0)
	require.NoError(t, err)
	require.Equal(t, types.StateSnapshotStatusFailed, failed.Status)
	require.Equal(t, "qmp pivot failed", failed.Reason)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEscrowedStateLeaseRejectsChangedGenerationOrSupersededFence(t *testing.T) {
	const (
		volumeID     = "21d4182a-4930-47b4-a987-e50c4a80156f"
		generationID = "7aee3365-2963-4a6d-b9fb-2c934924880d"
		attachmentID = "35141b8e-4591-4c72-856a-3ab7e831818e"
	)
	member := types.StateGeneration{VolumeId: volumeID, GenerationId: generationID, Name: "root", MountPath: "/", Root: true, Generation: 1}
	lease := types.StateVolumeLease{VolumeId: volumeID, AttachmentToken: attachmentID, FencingToken: 9}
	for _, test := range []struct {
		name, generation string
		planned, fence   int
		wantError        string
	}{
		{name: "exact escrow", generation: generationID, planned: 1, fence: 1},
		{name: "generation changed", generation: "acee3e88-20d7-4bbc-92cc-4b839ad6bc55", planned: 0, wantError: "not escrowed"},
		{name: "writer superseded", generation: generationID, planned: 1, fence: 0, wantError: "superseded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo, mock := NewBackendPostgresRepositoryForTest()
			postgresRepo := repo.(*PostgresBackendRepository)
			mock.ExpectBegin()
			tx, err := postgresRepo.client.BeginTxx(context.Background(), nil)
			require.NoError(t, err)
			requested := member
			requested.GenerationId = test.generation
			mock.ExpectQuery(`SELECT count\(\*\) FROM state_snapshot_member_plan`).
				WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(test.planned))
			if test.planned == 1 {
				mock.ExpectQuery(`SELECT.*next_fencing_token`).
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(test.fence))
			}
			err = verifyEscrowedStateLease(context.Background(), tx, 51, requested, lease)
			if test.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantError)
			}
			mock.ExpectRollback()
			require.NoError(t, tx.Rollback())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTerminalCommitReleaseSupportsRootAndExtraWritableBranchMembers(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.MatchExpectationsInOrder(false)
	mock.ExpectBegin()
	tx, err := postgresRepo.client.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	leases := map[string]types.StateVolumeLease{
		"21d4182a-4930-47b4-a987-e50c4a80156f": {VolumeId: "21d4182a-4930-47b4-a987-e50c4a80156f", AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e", FencingToken: 9},
		"f0d82a87-d142-4333-a9c6-dc621128ee64": {VolumeId: "f0d82a87-d142-4333-a9c6-dc621128ee64", AttachmentToken: "aa54e1b5-e878-401d-8fe1-49992c2f4b31", FencingToken: 11},
	}
	mock.ExpectQuery(`SELECT external_id::text AS external_id.*state_volume_release_claim`).
		WithArgs(uint(7), "container").WillReturnError(sql.ErrNoRows)
	for _, lease := range leases {
		mock.ExpectExec(`DELETE FROM state_volume_attachment`).
			WithArgs(uint(7), "container", lease.AttachmentToken, lease.FencingToken, lease.VolumeId).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`DELETE FROM state_branch_attachment`).
			WithArgs(uint(7), "container", lease.AttachmentToken, lease.FencingToken, lease.VolumeId).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec(`DELETE FROM state_read_only_attachment`).
		WithArgs(uint(7), "container").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT.*state_volume_attachment`).
		WithArgs(uint(7), "container").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`DELETE FROM state_volume_attachment_plan`).
		WithArgs(uint(7), "container").
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, releaseCommittedTerminalStateLeases(context.Background(), tx, 7, "container",
		"worker", "instance", "node", leases))
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTerminalCommitRollsBackWhenEscrowedWriterIsMissing(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	postgresRepo := repo.(*PostgresBackendRepository)
	mock.ExpectBegin()
	tx, err := postgresRepo.client.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	lease := types.StateVolumeLease{
		VolumeId:        "21d4182a-4930-47b4-a987-e50c4a80156f",
		AttachmentToken: "35141b8e-4591-4c72-856a-3ab7e831818e",
		FencingToken:    9,
	}
	mock.ExpectQuery(`SELECT external_id::text AS external_id.*state_volume_release_claim`).
		WithArgs(uint(7), "container").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`DELETE FROM state_volume_attachment`).
		WithArgs(uint(7), "container", lease.AttachmentToken, lease.FencingToken, lease.VolumeId).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM state_branch_attachment`).
		WithArgs(uint(7), "container", lease.AttachmentToken, lease.FencingToken, lease.VolumeId).
		WillReturnResult(sqlmock.NewResult(0, 0))
	err = releaseCommittedTerminalStateLeases(context.Background(), tx, 7, "container",
		"worker", "instance", "node", map[string]types.StateVolumeLease{
			lease.VolumeId: lease,
		})
	require.ErrorContains(t, err, "exactly one escrowed writer lease")
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func stateSnapshotRowsForTest(_ time.Time) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "external_id", "operation_id", "workspace_id", "stub_id", "source_container_id",
		"source_worker_id", "source_worker_instance_id", "recovery_worker_id", "recovery_worker_instance_id", "recovery_claim_generation", "recovery_proof_token", "storage_node_id", "armed",
		"source_stub_external_id", "source_stub_name", "source_stub_type", "mode", "include_memory", "visible",
		"status", "reason", "image_id", "image_digest", "runtime_profile", "checkpoint_id", "checkpoint_digest",
		"checkpoint_cache_hash", "checkpoint_size_bytes", "checkpoint_origin_key", "checkpoint_accelerator",
		"checkpoint_locality", "restore_mode", "fallback_reason", "public", "created_at", "updated_at", "completed_at",
	})
}

func volumeGenerationRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "external_id", "workspace_id", "stub_id", "volume_id", "name", "parent_generation_id",
		"clone_parent_generation_id",
		"generation", "status", "reason", "manifest_key", "manifest_digest", "manifest_size_bytes",
		"chunk_count", "logical_size_bytes", "stored_size_bytes", "bucket_name", "object_prefix", "public",
		"created_at", "updated_at", "completed_at",
	})
}
