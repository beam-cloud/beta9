package repository

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

const stateSnapshotColumns = `id, external_id::text AS external_id, operation_id, workspace_id,
	COALESCE(stub_id, 0) AS stub_id, source_container_id, source_worker_id, source_worker_instance_id,
	COALESCE((SELECT c.worker_id FROM state_snapshot_recovery_claim c WHERE c.state_snapshot_id = state_snapshot.id), '') AS recovery_worker_id,
	COALESCE((SELECT c.worker_instance_id FROM state_snapshot_recovery_claim c WHERE c.state_snapshot_id = state_snapshot.id), '') AS recovery_worker_instance_id,
	COALESCE((SELECT c.claim_generation FROM state_snapshot_recovery_claim c WHERE c.state_snapshot_id = state_snapshot.id), 0) AS recovery_claim_generation,
	recovery_proof_token::text AS recovery_proof_token, storage_node_id,
	armed_at IS NOT NULL AS armed, source_stub_external_id, source_stub_name,
	source_stub_type, mode, include_memory, visible, status, reason, image_id, image_digest, runtime_profile, checkpoint_id, checkpoint_digest,
	checkpoint_cache_hash, checkpoint_size_bytes, checkpoint_origin_key, checkpoint_accelerator,
	checkpoint_locality, restore_mode, fallback_reason, public, created_at, updated_at, completed_at`

const volumeGenerationColumns = `id, external_id::text AS external_id, workspace_id, COALESCE(stub_id, 0) AS stub_id,
	volume_id, name, COALESCE(parent_generation_id::text, '') AS parent_generation_id,
	COALESCE(clone_parent_generation_id::text, '') AS clone_parent_generation_id,
	generation, status, reason, manifest_key, manifest_digest,
	manifest_size_bytes, chunk_count, logical_size_bytes, stored_size_bytes, bucket_name, object_prefix,
	public, created_at, updated_at, completed_at`

func stateStatusTerminal(status types.StateSnapshotStatus) bool {
	return status == types.StateSnapshotStatusAvailable || status == types.StateSnapshotStatusFailed
}

func validateStateSnapshotBinding(snapshot *types.StateSnapshot) error {
	switch snapshot.Mode {
	case "live":
		if snapshot.IncludeMemory {
			return fmt.Errorf("live state snapshot cannot include memory")
		}
	case "terminal":
	default:
		return fmt.Errorf("state snapshot mode must be live or terminal")
	}
	if snapshot.Status == types.StateSnapshotStatusFailed && snapshot.Reason == "" {
		return fmt.Errorf("failed state snapshot requires a reason")
	}
	if snapshot.RestoreMode == "" {
		snapshot.RestoreMode = "cold_state"
	}
	switch snapshot.RestoreMode {
	case "memory":
		if snapshot.CheckpointId == "" || snapshot.CheckpointDigest == "" ||
			snapshot.CheckpointCacheHash == "" || snapshot.CheckpointSizeBytes <= 0 ||
			snapshot.CheckpointOriginKey == "" {
			return fmt.Errorf("memory state requires an exact checkpoint id, digest, cache hash, size, and origin key")
		}
		if snapshot.FallbackReason != "" {
			return fmt.Errorf("memory state cannot have a cold fallback reason")
		}
	case "cold_state":
		if snapshot.CheckpointId != "" || snapshot.CheckpointDigest != "" ||
			snapshot.CheckpointCacheHash != "" || snapshot.CheckpointSizeBytes != 0 ||
			snapshot.CheckpointOriginKey != "" || snapshot.CheckpointAccelerator != "" ||
			snapshot.CheckpointLocality != "" {
			return fmt.Errorf("cold state cannot reference a memory checkpoint")
		}
	default:
		return fmt.Errorf("restore mode must be memory or cold_state")
	}
	return nil
}

func (r *PostgresBackendRepository) CreateStateSnapshot(ctx context.Context, snapshot *types.StateSnapshot, members []types.StateGeneration, compactions []types.StateGenerationCompaction, leases []types.StateVolumeLease) (*types.StateSnapshot, error) {
	if snapshot == nil || snapshot.OperationId == "" || snapshot.SourceContainerId == "" ||
		snapshot.SourceWorkerId == "" || snapshot.SourceWorkerInstanceId == "" || snapshot.StorageNodeId == "" ||
		snapshot.SourceStubExternalId == "" || snapshot.SourceStubName == "" || snapshot.SourceStubType == "" {
		return nil, fmt.Errorf("state snapshot operation, source container, and source stub identity are required")
	}
	if snapshot.Public {
		return nil, fmt.Errorf("public whole-root state publishing is disabled")
	}
	if snapshot.ImageId == "" || snapshot.ImageDigest == "" || snapshot.RuntimeProfile == "" {
		return nil, fmt.Errorf("state snapshot image id, digest, and runtime profile are required")
	}
	if snapshot.Status == "" {
		snapshot.Status = types.StateSnapshotStatusPending
	}
	if snapshot.Status != types.StateSnapshotStatusPending {
		return nil, fmt.Errorf("state snapshot must be created pending and published with CommitStateSnapshot")
	}
	if len(snapshot.Generations) != 0 {
		return nil, fmt.Errorf("pending state snapshot cannot include generation memberships")
	}
	leaseByVolume, compactionByVolume, err := validateStateMemberPlan(members, compactions, leases)
	if err != nil {
		return nil, err
	}
	if err := validateStateSnapshotBinding(snapshot); err != nil {
		return nil, err
	}
	tx, err := r.client.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var stored types.StateSnapshot
	err = tx.GetContext(ctx, &stored, fmt.Sprintf(`
		INSERT INTO state_snapshot (operation_id, workspace_id, stub_id, source_container_id, source_worker_id, source_worker_instance_id, storage_node_id,
			source_stub_external_id, source_stub_name, source_stub_type, mode, include_memory, visible,
			status, reason, image_id, image_digest, runtime_profile, restore_mode, public)
		VALUES ($1,$2,NULLIF($3,0),$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'',$15,$16,$17,'cold_state',FALSE)
		ON CONFLICT (workspace_id, source_container_id, operation_id)
		DO UPDATE SET operation_id = EXCLUDED.operation_id
		WHERE state_snapshot.stub_id IS NOT DISTINCT FROM EXCLUDED.stub_id
		  AND state_snapshot.source_stub_external_id = EXCLUDED.source_stub_external_id
		  AND state_snapshot.source_stub_name = EXCLUDED.source_stub_name
		  AND state_snapshot.source_stub_type = EXCLUDED.source_stub_type
		  AND state_snapshot.source_worker_id = EXCLUDED.source_worker_id
		  AND state_snapshot.source_worker_instance_id = EXCLUDED.source_worker_instance_id
		  AND state_snapshot.storage_node_id = EXCLUDED.storage_node_id
		  AND state_snapshot.mode = EXCLUDED.mode
		  AND state_snapshot.include_memory = EXCLUDED.include_memory
		  AND state_snapshot.visible = EXCLUDED.visible
		  AND state_snapshot.image_id = EXCLUDED.image_id
		  AND state_snapshot.image_digest = EXCLUDED.image_digest
		  AND state_snapshot.runtime_profile = EXCLUDED.runtime_profile
		RETURNING %s;`, stateSnapshotColumns),
		snapshot.OperationId, snapshot.WorkspaceId, snapshot.StubId, snapshot.SourceContainerId,
		snapshot.SourceWorkerId, snapshot.SourceWorkerInstanceId, snapshot.StorageNodeId,
		snapshot.SourceStubExternalId, snapshot.SourceStubName, snapshot.SourceStubType,
		snapshot.Mode, snapshot.IncludeMemory, snapshot.Visible, types.StateSnapshotStatusPending,
		snapshot.ImageId, snapshot.ImageDigest, snapshot.RuntimeProfile)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("operation id conflicts with different immutable state snapshot inputs")
		}
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "23505" && pgErr.Constraint == "idx_state_snapshot_one_pending_container" {
			return nil, fmt.Errorf("source container already has another pending state snapshot operation")
		}
		return nil, err
	}

	if stateStatusTerminal(stored.Status) {
		if err := verifyStateMemberPlan(ctx, tx, stored.Id, members, compactionByVolume, leaseByVolume); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return r.GetStateSnapshot(ctx, snapshot.WorkspaceId, stored.ExternalId)
	}
	if err := preventCompactionFromDroppingRetainedState(ctx, tx, stored.WorkspaceId,
		stored.SourceStubExternalId, compactionByVolume); err != nil {
		return nil, err
	}
	var planCount int
	if err := tx.GetContext(ctx, &planCount, `SELECT count(*) FROM state_snapshot_member_plan
		WHERE state_snapshot_id = $1;`, stored.Id); err != nil {
		return nil, err
	}
	if planCount == 0 {
		for _, member := range members {
			lease := leaseByVolume[member.VolumeId]
			compaction := compactionByVolume[member.VolumeId]
			if _, err := tx.ExecContext(ctx, `INSERT INTO state_snapshot_member_plan
				(state_snapshot_id, volume_id, generation_id, parent_generation_id,
				 clone_parent_generation_id, compaction, compaction_source_generation_id, generation,
				 name, mount_path, read_only, is_root, attachment_token, fencing_token)
				VALUES ($1,$2::uuid,$3::uuid,NULLIF($4, '')::uuid,NULLIF($5, '')::uuid,
				 $6,NULLIF($7, '')::uuid,$8,$9,$10,$11,$12,NULLIF($13, '')::uuid,NULLIF($14, 0));`,
				stored.Id, member.VolumeId, member.GenerationId, member.ParentGenerationId,
				member.CloneParentGenerationId, compaction.SourceGenerationId != "", compaction.SourceGenerationId, member.Generation,
				member.Name, member.MountPath, member.ReadOnly, member.Root,
				lease.AttachmentToken, lease.FencingToken); err != nil {
				return nil, err
			}
		}
	}
	if err := verifyStateMemberPlan(ctx, tx, stored.Id, members, compactionByVolume, leaseByVolume); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return r.GetStateSnapshot(ctx, snapshot.WorkspaceId, stored.ExternalId)
}

// preventCompactionFromDroppingRetainedState guards the scoped replacement
// contract used by the state cache. A parentless anchor intentionally stops
// physical Parent/Clone traversal. It therefore cannot be planned while an
// older generation in the same stub/volume scope is still protected by an
// authoritative reference, or the worker's new scoped revision would omit
// required objects for that retained state. The exact compaction source may be
// the state being atomically advanced; every historical pin must be released
// first.
func preventCompactionFromDroppingRetainedState(ctx context.Context, tx *sqlx.Tx, workspaceId uint,
	stubExternalId string, compactions map[string]types.StateGenerationCompaction,
) error {
	if len(compactions) == 0 {
		return nil
	}
	volumeIds := make([]string, 0, len(compactions))
	for volumeId := range compactions {
		volumeIds = append(volumeIds, volumeId)
	}
	sort.Strings(volumeIds)
	for _, volumeId := range volumeIds {
		compaction := compactions[volumeId]
		if err := lockStateCacheScope(ctx, tx, workspaceId, stubExternalId, volumeId); err != nil {
			return err
		}
		var retainedHistorical int
		if err := tx.GetContext(ctx, &retainedHistorical, `SELECT count(*)
			FROM state_snapshot_reference reference
			JOIN state_snapshot retained_snapshot ON retained_snapshot.id=reference.state_snapshot_id
			JOIN state_snapshot_generation retained_member ON retained_member.state_snapshot_id=retained_snapshot.id
			JOIN volume_generation retained_generation ON retained_generation.id=retained_member.volume_generation_id
			WHERE reference.workspace_id=$1 AND reference.released_at IS NULL
			  AND retained_snapshot.source_stub_external_id=$2
			  AND retained_member.volume_id=$3
			  AND retained_generation.external_id<>$4::uuid;`, workspaceId, stubExternalId,
			volumeId, compaction.SourceGenerationId); err != nil {
			return err
		}
		if retainedHistorical != 0 {
			return fmt.Errorf("state volume %s compaction is blocked by a retained historical state snapshot", volumeId)
		}
	}
	return nil
}

func validateStateSnapshotOwner(snapshotId, sourceContainerId, operationId, workerId, workerInstanceId, storageNodeId string) error {
	if parsed, err := uuid.Parse(snapshotId); err != nil || parsed.String() != snapshotId {
		return fmt.Errorf("state snapshot id must be a canonical RFC4122 UUID")
	}
	if strings.TrimSpace(sourceContainerId) == "" || strings.TrimSpace(operationId) == "" ||
		strings.TrimSpace(workerId) == "" || strings.TrimSpace(workerInstanceId) == "" || strings.TrimSpace(storageNodeId) == "" {
		return fmt.Errorf("state snapshot source, operation, worker process, and storage node identities are required")
	}
	return nil
}

func validateStateSnapshotRecoveryProofToken(token string) error {
	parsed, err := uuid.Parse(strings.TrimSpace(token))
	if err != nil || parsed.String() != token {
		return fmt.Errorf("state snapshot recovery proof token must be a canonical RFC4122 UUID")
	}
	return nil
}

func (r *PostgresBackendRepository) ArmStateSnapshot(
	ctx context.Context,
	snapshotId, sourceContainerId, operationId, workerId, workerInstanceId, storageNodeId, recoveryProofToken string,
) (*types.StateSnapshot, error) {
	if err := validateStateSnapshotOwner(snapshotId, sourceContainerId, operationId, workerId, workerInstanceId, storageNodeId); err != nil {
		return nil, err
	}
	if err := validateStateSnapshotRecoveryProofToken(recoveryProofToken); err != nil {
		return nil, err
	}
	var armed types.StateSnapshot
	err := r.client.GetContext(ctx, &armed, `UPDATE state_snapshot
		SET armed_at = COALESCE(armed_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP
		WHERE external_id = $1::uuid AND source_container_id = $2 AND operation_id = $3
		  AND source_worker_id = $4 AND source_worker_instance_id = $5 AND storage_node_id = $6
		  AND recovery_proof_token = $7::uuid AND status = 'pending'
		RETURNING `+stateSnapshotColumns+`;`, snapshotId, sourceContainerId, operationId, workerId,
		workerInstanceId, storageNodeId, recoveryProofToken)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("state snapshot cannot be armed by this worker or is no longer pending")
	}
	return &armed, err
}

func (r *PostgresBackendRepository) ClaimStateSnapshotRecovery(
	ctx context.Context,
	snapshotId, sourceContainerId, operationId, workerId, workerInstanceId, storageNodeId, recoveryProofToken string,
	previousClaimGeneration int64,
) (*types.StateSnapshot, error) {
	if err := validateStateSnapshotOwner(snapshotId, sourceContainerId, operationId, workerId, workerInstanceId, storageNodeId); err != nil {
		return nil, err
	}
	if previousClaimGeneration < 0 {
		return nil, fmt.Errorf("previous recovery claim generation cannot be negative")
	}
	if err := validateStateSnapshotRecoveryProofToken(recoveryProofToken); err != nil {
		return nil, err
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var stored types.StateSnapshot
	if err := tx.GetContext(ctx, &stored, `SELECT `+stateSnapshotColumns+` FROM state_snapshot
		WHERE external_id = $1::uuid AND source_container_id = $2 AND operation_id = $3
		  AND storage_node_id = $4 AND status IN ('pending', 'available')
		  AND mode = 'terminal' AND armed_at IS NOT NULL FOR UPDATE;`,
		snapshotId, sourceContainerId, operationId, storageNodeId); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("state snapshot recovery claim is unavailable")
		}
		return nil, err
	}
	if stored.RecoveryProofToken != recoveryProofToken {
		return nil, fmt.Errorf("state snapshot recovery proof is invalid")
	}
	if stored.RecoveryWorkerId == workerId && stored.RecoveryWorkerInstanceId == workerInstanceId {
		if stored.RecoveryClaimGeneration != previousClaimGeneration &&
			stored.RecoveryClaimGeneration != previousClaimGeneration+1 {
			return nil, fmt.Errorf("recovery claim replay generation mismatch")
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &stored, nil
	}
	if stored.RecoveryClaimGeneration != previousClaimGeneration {
		return nil, fmt.Errorf("recovery claim was superseded")
	}
	if previousClaimGeneration == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO state_snapshot_recovery_claim
			(state_snapshot_id, worker_id, worker_instance_id, storage_node_id, claim_generation)
			VALUES ($1,$2,$3,$4,1);`, stored.Id, workerId, workerInstanceId, storageNodeId)
	} else {
		var rows sql.Result
		rows, err = tx.ExecContext(ctx, `UPDATE state_snapshot_recovery_claim
			SET worker_id = $2, worker_instance_id = $3, claim_generation = claim_generation + 1,
				updated_at = CURRENT_TIMESTAMP
			WHERE state_snapshot_id = $1 AND storage_node_id = $4 AND claim_generation = $5;`,
			stored.Id, workerId, workerInstanceId, storageNodeId, previousClaimGeneration)
		if err == nil {
			if count, _ := rows.RowsAffected(); count != 1 {
				err = fmt.Errorf("recovery claim was superseded")
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if err := tx.GetContext(ctx, &stored, `SELECT `+stateSnapshotColumns+` FROM state_snapshot WHERE id = $1;`, stored.Id); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &stored, nil
}

func stateSnapshotActiveWorker(snapshot *types.StateSnapshot) (string, string) {
	if snapshot != nil && snapshot.RecoveryWorkerId != "" {
		return snapshot.RecoveryWorkerId, snapshot.RecoveryWorkerInstanceId
	}
	if snapshot == nil {
		return "", ""
	}
	return snapshot.SourceWorkerId, snapshot.SourceWorkerInstanceId
}

func (r *PostgresBackendRepository) FailStateSnapshot(
	ctx context.Context,
	snapshotId, sourceContainerId, operationId, workerId, workerInstanceId, storageNodeId, reason string,
	recoveryClaimGeneration int64,
) (*types.StateSnapshot, error) {
	if err := validateStateSnapshotOwner(snapshotId, sourceContainerId, operationId, workerId, workerInstanceId, storageNodeId); err != nil {
		return nil, err
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil, fmt.Errorf("failed state snapshot requires a reason")
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var stored types.StateSnapshot
	if err := tx.GetContext(ctx, &stored, `SELECT `+stateSnapshotColumns+` FROM state_snapshot
		WHERE external_id = $1::uuid AND source_container_id = $2 AND operation_id = $3
		  AND storage_node_id = $4 FOR UPDATE;`,
		snapshotId, sourceContainerId, operationId, storageNodeId); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("state snapshot failure owner does not match")
		}
		return nil, err
	}
	activeWorker, activeInstance := stateSnapshotActiveWorker(&stored)
	if activeWorker != workerId || activeInstance != workerInstanceId {
		return nil, fmt.Errorf("state snapshot failure worker does not hold the active operation claim")
	}
	if stored.RecoveryClaimGeneration != recoveryClaimGeneration {
		return nil, fmt.Errorf("state snapshot failure recovery claim was superseded")
	}
	if stored.Status == types.StateSnapshotStatusAvailable {
		return nil, fmt.Errorf("available state snapshot cannot be failed")
	}
	if stored.Status == types.StateSnapshotStatusFailed {
		if stored.Reason != reason {
			return nil, fmt.Errorf("failed state snapshot replay reason mismatch")
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &stored, nil
	}
	if err := failPendingStateSnapshot(ctx, tx, &stored, reason); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	stored.Status = types.StateSnapshotStatusFailed
	stored.Reason = reason
	stored.RestoreMode = "cold_state"
	stored.FallbackReason = ""
	stored.CheckpointId, stored.CheckpointDigest, stored.CheckpointCacheHash = "", "", ""
	stored.CheckpointSizeBytes, stored.CheckpointOriginKey = 0, ""
	stored.CheckpointAccelerator, stored.CheckpointLocality = "", ""
	return &stored, nil
}

func (r *PostgresBackendRepository) FailUnarmedStateSnapshot(ctx context.Context, snapshotId, reason string) (*types.StateSnapshot, error) {
	if parsed, err := uuid.Parse(snapshotId); err != nil || parsed.String() != snapshotId {
		return nil, fmt.Errorf("state snapshot id must be a canonical RFC4122 UUID")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil, fmt.Errorf("failed state snapshot requires a reason")
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var stored types.StateSnapshot
	if err := tx.GetContext(ctx, &stored, `SELECT `+stateSnapshotColumns+` FROM state_snapshot
		WHERE external_id = $1::uuid FOR UPDATE;`, snapshotId); err != nil {
		return nil, err
	}
	if stored.Status == types.StateSnapshotStatusFailed {
		if stored.Reason != reason {
			return nil, fmt.Errorf("failed state snapshot replay reason mismatch")
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &stored, nil
	}
	if stored.Status != types.StateSnapshotStatusPending || stored.Armed {
		return nil, fmt.Errorf("only an unarmed pending state snapshot can be reaped")
	}
	if err := failPendingStateSnapshot(ctx, tx, &stored, reason); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	stored.Status, stored.Reason, stored.RestoreMode = types.StateSnapshotStatusFailed, reason, "cold_state"
	return &stored, nil
}

func failPendingStateSnapshot(ctx context.Context, tx *sqlx.Tx, stored *types.StateSnapshot, reason string) error {
	if stored == nil {
		return fmt.Errorf("pending state snapshot is required")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE volume_generation vg
		SET status = 'failed', reason = $2, updated_at = CURRENT_TIMESTAMP, completed_at = CURRENT_TIMESTAMP
		FROM state_snapshot_member_plan p
		WHERE p.state_snapshot_id = $1 AND vg.external_id = p.generation_id
		  AND vg.workspace_id = $3 AND vg.status = 'pending';`, stored.Id, reason, stored.WorkspaceId); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE state_snapshot SET status = 'failed', reason = $2,
		restore_mode = 'cold_state', fallback_reason = '', checkpoint_id = '', checkpoint_digest = '',
		checkpoint_cache_hash = '', checkpoint_size_bytes = 0, checkpoint_origin_key = '',
		checkpoint_accelerator = '', checkpoint_locality = '', updated_at = CURRENT_TIMESTAMP,
		completed_at = CURRENT_TIMESTAMP WHERE id = $1 AND status = 'pending';`, stored.Id, reason)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("state snapshot was not pending")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM state_snapshot_member_plan WHERE state_snapshot_id = $1;`, stored.Id); err != nil {
		return err
	}
	return nil
}

type plannedStateMember struct {
	VolumeId                     string `db:"volume_id"`
	GenerationId                 string `db:"generation_id"`
	ParentGenerationId           string `db:"parent_generation_id"`
	CloneParentGenerationId      string `db:"clone_parent_generation_id"`
	Compaction                   bool   `db:"compaction"`
	CompactionSourceGenerationId string `db:"compaction_source_generation_id"`
	Generation                   int64  `db:"generation"`
	Name                         string `db:"name"`
	MountPath                    string `db:"mount_path"`
	ReadOnly                     bool   `db:"read_only"`
	Root                         bool   `db:"root"`
	AttachmentToken              string `db:"attachment_token"`
	FencingToken                 int64  `db:"fencing_token"`
}

func validateStateMemberPlan(members []types.StateGeneration, compactions []types.StateGenerationCompaction, leases []types.StateVolumeLease) (map[string]types.StateVolumeLease, map[string]types.StateGenerationCompaction, error) {
	if len(members) == 0 {
		return nil, nil, fmt.Errorf("state snapshot operation requires planned members")
	}
	leaseByVolume := make(map[string]types.StateVolumeLease, len(leases))
	for _, lease := range leases {
		if parsed, err := uuid.Parse(lease.VolumeId); err != nil || parsed.String() != lease.VolumeId {
			return nil, nil, fmt.Errorf("planned lease volume id must be a canonical RFC4122 UUID")
		}
		if parsed, err := uuid.Parse(lease.AttachmentToken); err != nil || parsed.String() != lease.AttachmentToken || lease.FencingToken <= 0 {
			return nil, nil, fmt.Errorf("planned lease requires a canonical attachment token and positive fencing token")
		}
		if _, duplicate := leaseByVolume[lease.VolumeId]; duplicate {
			return nil, nil, fmt.Errorf("duplicate planned lease for volume %q", lease.VolumeId)
		}
		leaseByVolume[lease.VolumeId] = lease
	}
	compactionByVolume := make(map[string]types.StateGenerationCompaction, len(compactions))
	for _, compaction := range compactions {
		if parsed, err := uuid.Parse(compaction.VolumeId); err != nil || parsed.String() != compaction.VolumeId {
			return nil, nil, fmt.Errorf("compaction volume id must be a canonical RFC4122 UUID")
		}
		if parsed, err := uuid.Parse(compaction.GenerationId); err != nil || parsed.String() != compaction.GenerationId {
			return nil, nil, fmt.Errorf("compaction generation id must be a canonical RFC4122 UUID")
		}
		if parsed, err := uuid.Parse(compaction.SourceGenerationId); err != nil || parsed.String() != compaction.SourceGenerationId {
			return nil, nil, fmt.Errorf("compaction source generation id must be a canonical RFC4122 UUID")
		}
		if _, duplicate := compactionByVolume[compaction.VolumeId]; duplicate {
			return nil, nil, fmt.Errorf("duplicate compaction plan for volume %q", compaction.VolumeId)
		}
		compactionByVolume[compaction.VolumeId] = compaction
	}
	seenVolumes := make(map[string]struct{}, len(members))
	seenNames := make(map[string]struct{}, len(members))
	seenMounts := make(map[string]struct{}, len(members))
	roots, writable := 0, 0
	for _, member := range members {
		if parsed, err := uuid.Parse(member.VolumeId); err != nil || parsed.String() != member.VolumeId {
			return nil, nil, fmt.Errorf("planned member volume id must be a canonical RFC4122 UUID")
		}
		if member.Name == "" || !filepath.IsAbs(member.MountPath) {
			return nil, nil, fmt.Errorf("planned member requires a name and absolute mount path")
		}
		plannedGeneration := &types.VolumeGeneration{
			ExternalId: member.GenerationId, VolumeId: member.VolumeId, Name: member.Name,
			ParentGenerationId:      member.ParentGenerationId,
			CloneParentGenerationId: member.CloneParentGenerationId,
			Generation:              member.Generation, Status: types.StateSnapshotStatusPending,
		}
		if err := validateVolumeGeneration(plannedGeneration); err != nil {
			return nil, nil, fmt.Errorf("planned member %q: %w", member.Name, err)
		}
		compaction, compacting := compactionByVolume[member.VolumeId]
		if compacting {
			if compaction.GenerationId != member.GenerationId || member.ReadOnly || member.Generation <= 1 ||
				member.ParentGenerationId != "" || member.CloneParentGenerationId != "" {
				return nil, nil, fmt.Errorf("planned member %q has an invalid parentless compaction authorization", member.Name)
			}
		} else if !member.ReadOnly && member.Generation > 1 && member.ParentGenerationId == "" {
			return nil, nil, fmt.Errorf("planned member %q generation greater than one requires a parent or exact compaction authorization", member.Name)
		}
		if _, duplicate := seenVolumes[member.VolumeId]; duplicate {
			return nil, nil, fmt.Errorf("duplicate planned volume %q", member.VolumeId)
		}
		if _, duplicate := seenNames[member.Name]; duplicate {
			return nil, nil, fmt.Errorf("duplicate planned member name %q", member.Name)
		}
		if _, duplicate := seenMounts[member.MountPath]; duplicate {
			return nil, nil, fmt.Errorf("duplicate planned mount path %q", member.MountPath)
		}
		seenVolumes[member.VolumeId], seenNames[member.Name], seenMounts[member.MountPath] = struct{}{}, struct{}{}, struct{}{}
		if member.Root {
			roots++
			if member.Name != "root" || member.MountPath != "/" || member.ReadOnly {
				return nil, nil, fmt.Errorf("planned root must be named root, mounted at /, and writable")
			}
		} else if member.Name == "root" || member.MountPath == "/" {
			return nil, nil, fmt.Errorf("planned root name and mount path are reserved")
		}
		if !member.ReadOnly {
			writable++
			if _, ok := leaseByVolume[member.VolumeId]; !ok {
				return nil, nil, fmt.Errorf("planned writable member %q requires its exact lease", member.Name)
			}
		} else if _, ok := leaseByVolume[member.VolumeId]; ok {
			return nil, nil, fmt.Errorf("planned read-only member %q cannot carry a writer lease", member.Name)
		}
	}
	if roots != 1 || len(leaseByVolume) != writable {
		return nil, nil, fmt.Errorf("state snapshot plan requires exactly one root and one lease per writable member")
	}
	if len(compactionByVolume) != len(compactions) {
		return nil, nil, fmt.Errorf("state snapshot compaction plan is inconsistent")
	}
	for volumeID := range compactionByVolume {
		if _, exists := seenVolumes[volumeID]; !exists {
			return nil, nil, fmt.Errorf("compaction plan references unknown volume %q", volumeID)
		}
	}
	return leaseByVolume, compactionByVolume, nil
}

func verifyStateMemberPlan(ctx context.Context, tx *sqlx.Tx, snapshotId uint, members []types.StateGeneration, compactions map[string]types.StateGenerationCompaction, leases map[string]types.StateVolumeLease) error {
	var stored []plannedStateMember
	if err := tx.SelectContext(ctx, &stored, `SELECT volume_id::text AS volume_id,
		generation_id::text AS generation_id,
		COALESCE(parent_generation_id::text, '') AS parent_generation_id,
		COALESCE(clone_parent_generation_id::text, '') AS clone_parent_generation_id,
		compaction, COALESCE(compaction_source_generation_id::text, '') AS compaction_source_generation_id,
		generation, name, mount_path,
		read_only, is_root AS root, COALESCE(attachment_token::text, '') AS attachment_token,
		COALESCE(fencing_token, 0) AS fencing_token
		FROM state_snapshot_member_plan WHERE state_snapshot_id = $1;`, snapshotId); err != nil {
		return err
	}
	if len(stored) != len(members) {
		return fmt.Errorf("state snapshot operation replay has a different member set")
	}
	byVolume := make(map[string]plannedStateMember, len(stored))
	for _, member := range stored {
		byVolume[member.VolumeId] = member
	}
	for _, requested := range members {
		storedMember, ok := byVolume[requested.VolumeId]
		lease := leases[requested.VolumeId]
		compaction := compactions[requested.VolumeId]
		if !ok || storedMember.GenerationId != requested.GenerationId ||
			storedMember.ParentGenerationId != requested.ParentGenerationId ||
			storedMember.CloneParentGenerationId != requested.CloneParentGenerationId ||
			storedMember.Compaction != (compaction.SourceGenerationId != "") ||
			storedMember.CompactionSourceGenerationId != compaction.SourceGenerationId ||
			storedMember.Generation != requested.Generation ||
			storedMember.Name != requested.Name || storedMember.MountPath != requested.MountPath ||
			storedMember.ReadOnly != requested.ReadOnly || storedMember.Root != requested.Root ||
			storedMember.AttachmentToken != lease.AttachmentToken || storedMember.FencingToken != lease.FencingToken {
			return fmt.Errorf("state snapshot operation replay member %q does not match its immutable plan", requested.Name)
		}
	}
	return nil
}

func (r *PostgresBackendRepository) CommitStateSnapshot(ctx context.Context, snapshot *types.StateSnapshot, generations []types.VolumeGeneration, leases []types.StateVolumeLease, workerId, workerInstanceId, storageNodeId string, recoveryClaimGeneration int64) (*types.StateSnapshot, error) {
	if snapshot == nil || snapshot.ExternalId == "" {
		return nil, fmt.Errorf("state snapshot id is required")
	}
	if strings.TrimSpace(workerId) == "" || strings.TrimSpace(storageNodeId) == "" {
		return nil, fmt.Errorf("state snapshot commit requires an authenticated worker and storage node")
	}
	if snapshot.Public {
		return nil, fmt.Errorf("public whole-root state publishing is disabled")
	}
	if !stateStatusTerminal(snapshot.Status) {
		return nil, fmt.Errorf("state snapshot commit must transition pending state to available or failed")
	}
	if err := validateStateSnapshotBinding(snapshot); err != nil {
		return nil, err
	}
	leaseByVolume := make(map[string]types.StateVolumeLease, len(leases))
	if snapshot.Status == types.StateSnapshotStatusAvailable {
		if len(snapshot.Generations) == 0 || len(generations) != len(snapshot.Generations) {
			return nil, fmt.Errorf("available state snapshot requires one terminal generation record per member")
		}
		roots := 0
		members := make(map[string]types.StateGeneration, len(snapshot.Generations))
		generationIDs := make(map[string]struct{}, len(snapshot.Generations))
		names := make(map[string]struct{}, len(snapshot.Generations))
		mountPaths := make(map[string]struct{}, len(snapshot.Generations))
		for _, member := range snapshot.Generations {
			if member.VolumeId == "" || member.GenerationId == "" || member.Name == "" || member.Generation <= 0 {
				return nil, fmt.Errorf("state snapshot member requires volume, generation id, name, and generation number")
			}
			if !filepath.IsAbs(member.MountPath) {
				return nil, fmt.Errorf("state snapshot member %q mount path must be absolute", member.Name)
			}
			if _, duplicate := members[member.VolumeId]; duplicate {
				return nil, fmt.Errorf("state snapshot has duplicate volume %q", member.VolumeId)
			}
			if _, duplicate := generationIDs[member.GenerationId]; duplicate {
				return nil, fmt.Errorf("state snapshot has duplicate generation %q", member.GenerationId)
			}
			if _, duplicate := names[member.Name]; duplicate {
				return nil, fmt.Errorf("state snapshot has duplicate member name %q", member.Name)
			}
			if _, duplicate := mountPaths[member.MountPath]; duplicate {
				return nil, fmt.Errorf("state snapshot has duplicate mount path %q", member.MountPath)
			}
			members[member.VolumeId] = member
			generationIDs[member.GenerationId] = struct{}{}
			names[member.Name] = struct{}{}
			mountPaths[member.MountPath] = struct{}{}
			if member.Root {
				roots++
				if member.Name != "root" || member.MountPath != "/" || member.ReadOnly {
					return nil, fmt.Errorf("root state member must be named root, mounted at /, and writable")
				}
			} else if member.Name == "root" || member.MountPath == "/" {
				return nil, fmt.Errorf("root name and mount path are reserved for the root state member")
			}
		}
		if roots != 1 {
			return nil, fmt.Errorf("available state snapshot requires exactly one root")
		}
		for _, lease := range leases {
			if _, err := uuid.Parse(lease.VolumeId); err != nil {
				return nil, fmt.Errorf("state-volume lease volume id must be an RFC4122 UUID")
			}
			if _, err := uuid.Parse(lease.AttachmentToken); err != nil || lease.FencingToken <= 0 {
				return nil, fmt.Errorf("state-volume lease requires a canonical attachment token and positive fencing token")
			}
			if _, duplicate := leaseByVolume[lease.VolumeId]; duplicate {
				return nil, fmt.Errorf("duplicate state-volume lease for %q", lease.VolumeId)
			}
			leaseByVolume[lease.VolumeId] = lease
		}
		if len(leases) != 0 {
			writableMembers := 0
			for _, member := range snapshot.Generations {
				if member.ReadOnly {
					continue
				}
				writableMembers++
				if _, ok := leaseByVolume[member.VolumeId]; !ok {
					return nil, fmt.Errorf("writable state member %q requires its exact active lease", member.Name)
				}
			}
			if len(leaseByVolume) != writableMembers {
				return nil, fmt.Errorf("state snapshot commit includes a lease that is not a writable member")
			}
		}
		for index := range generations {
			generation := &generations[index]
			generation.WorkspaceId = snapshot.WorkspaceId
			if generation.Status != types.StateSnapshotStatusAvailable {
				return nil, fmt.Errorf("available state snapshot cannot commit a non-available generation")
			}
			if err := validateVolumeGeneration(generation); err != nil {
				return nil, err
			}
			member, ok := members[generation.VolumeId]
			if !ok || member.GenerationId != generation.ExternalId || member.Generation != generation.Generation ||
				member.Name != generation.Name || member.ParentGenerationId != generation.ParentGenerationId ||
				member.CloneParentGenerationId != generation.CloneParentGenerationId {
				return nil, fmt.Errorf("terminal generation %q does not exactly match state membership", generation.ExternalId)
			}
		}
	} else {
		if len(snapshot.Generations) != 0 {
			return nil, fmt.Errorf("failed state snapshot cannot publish generation memberships")
		}
		for index := range generations {
			generation := &generations[index]
			generation.WorkspaceId = snapshot.WorkspaceId
			if generation.Status != types.StateSnapshotStatusFailed {
				return nil, fmt.Errorf("failed state snapshot can only fail pending generations")
			}
			if err := validateVolumeGeneration(generation); err != nil {
				return nil, err
			}
		}
	}

	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var stored types.StateSnapshot
	if err := tx.GetContext(ctx, &stored, `SELECT `+stateSnapshotColumns+` FROM state_snapshot
		WHERE external_id = $1::uuid AND workspace_id = $2 FOR UPDATE;`, snapshot.ExternalId, snapshot.WorkspaceId); err != nil {
		if err == sql.ErrNoRows {
			return nil, &types.ErrStateSnapshotNotFound{StateSnapshotId: snapshot.ExternalId}
		}
		return nil, err
	}
	if stateStatusTerminal(stored.Status) {
		activeWorker, activeInstance := stateSnapshotActiveWorker(&stored)
		if activeWorker != workerId || activeInstance != workerInstanceId || stored.StorageNodeId != storageNodeId {
			return nil, fmt.Errorf("terminal state snapshot replay worker or storage node mismatch")
		}
		if stored.RecoveryClaimGeneration != recoveryClaimGeneration {
			return nil, fmt.Errorf("terminal state snapshot replay recovery claim was superseded")
		}
		if !sameStateSnapshotTerminal(&stored, snapshot) {
			return nil, fmt.Errorf("terminal state snapshot is immutable")
		}
		members, err := stateSnapshotMembers(ctx, tx, stored.Id)
		if err != nil {
			return nil, err
		}
		if !sameStateGenerations(members, snapshot.Generations) {
			return nil, fmt.Errorf("terminal state snapshot membership replay mismatch")
		}
		if err := verifyCommittedGenerationReplay(ctx, tx, snapshot.WorkspaceId, generations); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return r.GetStateSnapshot(ctx, snapshot.WorkspaceId, snapshot.ExternalId)
	}
	activeWorker, activeInstance := stateSnapshotActiveWorker(&stored)
	if activeWorker != workerId || activeInstance != workerInstanceId || stored.StorageNodeId != storageNodeId {
		return nil, fmt.Errorf("state snapshot commit worker or storage node does not match its immutable owner")
	}
	if stored.RecoveryClaimGeneration != recoveryClaimGeneration {
		return nil, fmt.Errorf("state snapshot commit recovery claim was superseded")
	}
	if snapshot.Status == types.StateSnapshotStatusAvailable && !stored.Armed {
		return nil, fmt.Errorf("state snapshot must be durably armed before publication")
	}
	if len(leaseByVolume) == 0 {
		leaseByVolume, err = escrowedStateLeases(ctx, tx, stored.Id)
		if err != nil {
			return nil, err
		}
	}
	compactionByVolume, err := stateSnapshotCompactions(ctx, tx, stored.Id)
	if err != nil {
		return nil, err
	}
	if snapshot.Status == types.StateSnapshotStatusAvailable {
		if err := verifyStateMemberPlan(ctx, tx, stored.Id, snapshot.Generations, compactionByVolume, leaseByVolume); err != nil {
			return nil, err
		}
	}

	byID := make(map[string]types.VolumeGeneration, len(generations))
	for index := range generations {
		requested := &generations[index]
		var current types.VolumeGeneration
		if err := tx.GetContext(ctx, &current, `SELECT `+volumeGenerationColumns+` FROM volume_generation
			WHERE external_id = $1::uuid AND workspace_id = $2 FOR UPDATE;`, requested.ExternalId, snapshot.WorkspaceId); err != nil {
			if err != sql.ErrNoRows {
				return nil, err
			}
			err = tx.GetContext(ctx, &current, `INSERT INTO volume_generation
				(external_id, workspace_id, stub_id, volume_id, name, parent_generation_id,
				 clone_parent_generation_id, generation, status, reason, manifest_key, manifest_digest,
				 manifest_size_bytes, chunk_count, logical_size_bytes, stored_size_bytes, bucket_name,
				 object_prefix, public, completed_at)
				SELECT $1::uuid, $2, NULLIF($3, 0), $4, $5, NULLIF($6, '')::uuid,
				 NULLIF($7, '')::uuid, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
				 FALSE, CURRENT_TIMESTAMP
				FROM state_snapshot_member_plan p
				WHERE p.state_snapshot_id = $19 AND p.generation_id = $1::uuid
				  AND p.volume_id::text = $4 AND p.name = $5
				  AND COALESCE(p.parent_generation_id::text, '') = $6
				  AND COALESCE(p.clone_parent_generation_id::text, '') = $7 AND p.generation = $8
				RETURNING `+volumeGenerationColumns+`;`, requested.ExternalId, snapshot.WorkspaceId,
				stored.StubId, requested.VolumeId, requested.Name, requested.ParentGenerationId,
				requested.CloneParentGenerationId, requested.Generation, requested.Status, requested.Reason,
				requested.ManifestKey, requested.ManifestDigest, requested.ManifestSizeBytes, requested.ChunkCount,
				requested.LogicalSizeBytes, requested.StoredSizeBytes, requested.BucketName, requested.ObjectPrefix,
				stored.Id)
			if err == sql.ErrNoRows {
				return nil, fmt.Errorf("generation %q does not match the immutable snapshot plan", requested.ExternalId)
			}
			if err != nil {
				return nil, err
			}
		}
		if current.VolumeId != requested.VolumeId || current.Name != requested.Name ||
			current.ParentGenerationId != requested.ParentGenerationId ||
			current.CloneParentGenerationId != requested.CloneParentGenerationId || current.Generation != requested.Generation {
			return nil, fmt.Errorf("terminal generation %q identity mismatch", requested.ExternalId)
		}
		if stateStatusTerminal(current.Status) {
			if !sameVolumeGenerationTerminal(&current, requested) {
				return nil, fmt.Errorf("terminal generation %q replay mismatch", requested.ExternalId)
			}
		} else {
			result, err := tx.ExecContext(ctx, `UPDATE volume_generation SET status = $2, reason = $3,
				manifest_key = $4, manifest_digest = $5, manifest_size_bytes = $6, chunk_count = $7,
				logical_size_bytes = $8, stored_size_bytes = $9, bucket_name = $10, object_prefix = $11,
				updated_at = CURRENT_TIMESTAMP, completed_at = CURRENT_TIMESTAMP
				WHERE id = $1 AND status = 'pending';`, current.Id, requested.Status, requested.Reason,
				requested.ManifestKey, requested.ManifestDigest, requested.ManifestSizeBytes, requested.ChunkCount,
				requested.LogicalSizeBytes, requested.StoredSizeBytes, requested.BucketName, requested.ObjectPrefix)
			if err != nil {
				return nil, err
			}
			if rows, _ := result.RowsAffected(); rows != 1 {
				return nil, fmt.Errorf("generation %q was not pending", requested.ExternalId)
			}
		}
		if _, duplicate := byID[requested.ExternalId]; duplicate {
			return nil, fmt.Errorf("duplicate terminal generation %q", requested.ExternalId)
		}
		byID[requested.ExternalId] = *requested
	}

	if snapshot.Status == types.StateSnapshotStatusAvailable {
		for _, member := range snapshot.Generations {
			generation, ok := byID[member.GenerationId]
			if !ok {
				return nil, fmt.Errorf("state member generation %q is absent from atomic commit", member.GenerationId)
			}
			result, err := tx.ExecContext(ctx, `INSERT INTO state_snapshot_generation
				(state_snapshot_id, volume_generation_id, volume_id, name, mount_path, read_only, is_root, generation)
				SELECT $1, id, $3, $4, $5, $6, $7, $8 FROM volume_generation
				WHERE external_id = $2::uuid AND workspace_id = $9 AND status = 'available';`,
				stored.Id, member.GenerationId, member.VolumeId, member.Name, member.MountPath,
				member.ReadOnly, member.Root, generation.Generation, snapshot.WorkspaceId)
			if err != nil {
				return nil, err
			}
			if rows, _ := result.RowsAffected(); rows != 1 {
				return nil, fmt.Errorf("generation %q was not atomically published", member.GenerationId)
			}
		}
		if err := advanceStateVolumeHeads(ctx, tx, stored.Id, snapshot.WorkspaceId, stored.SourceContainerId,
			snapshot.Generations, byID, compactionByVolume, leaseByVolume); err != nil {
			return nil, err
		}
	}

	result, err := tx.ExecContext(ctx, `UPDATE state_snapshot SET status = $2, reason = $3,
		checkpoint_id = $4, checkpoint_digest = $5, checkpoint_cache_hash = $6,
		checkpoint_size_bytes = $7, checkpoint_origin_key = $8, checkpoint_accelerator = $9,
		checkpoint_locality = $10, restore_mode = $11, fallback_reason = $12,
		updated_at = CURRENT_TIMESTAMP, completed_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND status = 'pending';`, stored.Id, snapshot.Status, snapshot.Reason,
		snapshot.CheckpointId, snapshot.CheckpointDigest, snapshot.CheckpointCacheHash,
		snapshot.CheckpointSizeBytes, snapshot.CheckpointOriginKey, snapshot.CheckpointAccelerator,
		snapshot.CheckpointLocality, snapshot.RestoreMode, snapshot.FallbackReason)
	if err != nil {
		return nil, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return nil, fmt.Errorf("state snapshot %q was not pending", snapshot.ExternalId)
	}
	if snapshot.Mode == "terminal" {
		if err := releaseCommittedTerminalStateLeases(ctx, tx, snapshot.WorkspaceId, stored.SourceContainerId,
			workerId, workerInstanceId, storageNodeId, leaseByVolume); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return r.GetStateSnapshot(ctx, snapshot.WorkspaceId, snapshot.ExternalId)
}

func escrowedStateLeases(ctx context.Context, tx *sqlx.Tx, snapshotId uint) (map[string]types.StateVolumeLease, error) {
	var stored []plannedStateMember
	if err := tx.SelectContext(ctx, &stored, `SELECT volume_id::text AS volume_id,
		COALESCE(attachment_token::text, '') AS attachment_token,
		COALESCE(fencing_token, 0) AS fencing_token, read_only
		FROM state_snapshot_member_plan WHERE state_snapshot_id = $1;`, snapshotId); err != nil {
		return nil, err
	}
	if len(stored) == 0 {
		return nil, fmt.Errorf("state snapshot has no immutable member plan")
	}
	leases := make(map[string]types.StateVolumeLease, len(stored))
	for _, member := range stored {
		if member.ReadOnly {
			continue
		}
		if member.AttachmentToken == "" || member.FencingToken <= 0 {
			return nil, fmt.Errorf("writable state snapshot plan has no exact writer lease")
		}
		leases[member.VolumeId] = types.StateVolumeLease{VolumeId: member.VolumeId,
			AttachmentToken: member.AttachmentToken, FencingToken: member.FencingToken}
	}
	if len(leases) == 0 {
		return nil, fmt.Errorf("state snapshot plan has no writable member lease")
	}
	return leases, nil
}

func releaseCommittedTerminalStateLeases(ctx context.Context, tx *sqlx.Tx, workspaceId uint, containerId,
	workerId, workerInstanceId, storageNodeId string, leases map[string]types.StateVolumeLease,
) error {
	if err := completeMatchingTerminalStateVolumeReleaseIntent(ctx, tx, workspaceId, containerId,
		workerId, workerInstanceId, storageNodeId, leases); err != nil {
		return err
	}
	for _, lease := range leases {
		named, err := tx.ExecContext(ctx, `DELETE FROM state_volume_attachment
			WHERE workspace_id = $1 AND container_id = $2 AND attachment_token = $3::uuid
			  AND fencing_token = $4 AND state_volume_id =
			      (SELECT id FROM state_volume WHERE external_id = $5::uuid AND workspace_id = $1);`,
			workspaceId, containerId, lease.AttachmentToken, lease.FencingToken, lease.VolumeId)
		if err != nil {
			return err
		}
		branch, err := tx.ExecContext(ctx, `DELETE FROM state_branch_attachment a USING state_branch_lineage l
			WHERE a.lineage_id = l.id AND a.workspace_id = $1 AND a.container_id = $2
			  AND a.attachment_token = $3::uuid AND a.fencing_token = $4
			  AND l.volume_id = $5::uuid AND l.workspace_id = $1;`,
			workspaceId, containerId, lease.AttachmentToken, lease.FencingToken, lease.VolumeId)
		if err != nil {
			return err
		}
		namedRows, _ := named.RowsAffected()
		branchRows, _ := branch.RowsAffected()
		if namedRows+branchRows != 1 {
			return fmt.Errorf("terminal state commit did not release exactly one escrowed writer lease")
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM state_read_only_attachment
		WHERE workspace_id = $1 AND container_id = $2;`, workspaceId, containerId); err != nil {
		return err
	}
	var remaining int
	if err := tx.GetContext(ctx, &remaining, `SELECT
		(SELECT count(*) FROM state_volume_attachment WHERE workspace_id = $1 AND container_id = $2) +
		(SELECT count(*) FROM state_branch_attachment WHERE workspace_id = $1 AND container_id = $2);`,
		workspaceId, containerId); err != nil {
		return err
	}
	if remaining != 0 {
		return fmt.Errorf("terminal state commit did not release every writer attachment")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM state_volume_attachment_plan
		WHERE workspace_id = $1 AND container_id = $2;`, workspaceId, containerId); err != nil {
		return err
	}
	return nil
}

func sameStateSnapshotTerminal(stored, requested *types.StateSnapshot) bool {
	return stored.Mode == requested.Mode && stored.IncludeMemory == requested.IncludeMemory &&
		stored.Visible == requested.Visible && stored.Status == requested.Status && stored.Reason == requested.Reason &&
		stored.CheckpointId == requested.CheckpointId && stored.CheckpointDigest == requested.CheckpointDigest &&
		stored.CheckpointCacheHash == requested.CheckpointCacheHash && stored.CheckpointSizeBytes == requested.CheckpointSizeBytes &&
		stored.CheckpointOriginKey == requested.CheckpointOriginKey && stored.CheckpointAccelerator == requested.CheckpointAccelerator &&
		stored.CheckpointLocality == requested.CheckpointLocality && stored.RestoreMode == requested.RestoreMode &&
		stored.FallbackReason == requested.FallbackReason
}

func advanceStateVolumeHeads(
	ctx context.Context,
	tx *sqlx.Tx,
	snapshotId uint,
	workspaceId uint,
	containerId string,
	members []types.StateGeneration,
	generations map[string]types.VolumeGeneration,
	compactions map[string]types.StateGenerationCompaction,
	leases map[string]types.StateVolumeLease,
) error {
	for _, member := range members {
		if member.ReadOnly {
			continue
		}
		generation, ok := generations[member.GenerationId]
		if !ok {
			return fmt.Errorf("writable member %q has no committed generation", member.Name)
		}
		lease, ok := leases[member.VolumeId]
		if !ok {
			return fmt.Errorf("writable member %q has no active lease", member.Name)
		}

		var named struct {
			Id                  uint   `db:"id"`
			CurrentGenerationId string `db:"current_generation_id"`
		}
		err := tx.GetContext(ctx, &named, `SELECT id,
			COALESCE(current_generation_id::text, '') AS current_generation_id
			FROM state_volume WHERE workspace_id = $1 AND external_id = $2::uuid
			  AND deleted_at IS NULL FOR UPDATE;`, workspaceId, member.VolumeId)
		if err == nil {
			var attachment stateAttachmentLineage
			if err := tx.GetContext(ctx, &attachment, `SELECT
				COALESCE(source_generation_id::text, '') AS source_generation_id,
				initialize, FALSE AS clone_source, expires_at > CURRENT_TIMESTAMP AS active
				FROM state_volume_attachment
				WHERE state_volume_id = $1 AND workspace_id = $2 AND container_id = $3
				  AND attachment_token = $4::uuid AND fencing_token = $5;`, named.Id, workspaceId,
				containerId, lease.AttachmentToken, lease.FencingToken); err != nil {
				if err == sql.ErrNoRows {
					return fmt.Errorf("named disk %q writer attachment is missing or fenced", member.Name)
				}
				return err
			}
			if err := validateAttachmentGeneration(member.Name, generation, named.CurrentGenerationId, attachment, compactions[member.VolumeId], true); err != nil {
				return err
			}
			if !attachment.Active {
				if err := verifyEscrowedStateLease(ctx, tx, snapshotId, member, lease); err != nil {
					return fmt.Errorf("named disk %q writer lease is expired or fenced: %w", member.Name, err)
				}
			}
			result, err := tx.ExecContext(ctx, `UPDATE state_volume SET current_generation_id = $2::uuid,
				updated_at = CURRENT_TIMESTAMP WHERE id = $1
				  AND COALESCE(current_generation_id::text, '') = $3;`, named.Id,
				generation.ExternalId, named.CurrentGenerationId)
			if err != nil {
				return err
			}
			if rows, _ := result.RowsAffected(); rows != 1 {
				return fmt.Errorf("named disk %q head changed during commit", member.Name)
			}
			continue
		}
		if err != sql.ErrNoRows {
			return err
		}

		var branch struct {
			Id                  uint   `db:"id"`
			CurrentGenerationId string `db:"current_generation_id"`
		}
		if err := tx.GetContext(ctx, &branch, `SELECT id,
			COALESCE(current_generation_id::text, '') AS current_generation_id
			FROM state_branch_lineage WHERE workspace_id = $1 AND volume_id = $2::uuid
			FOR UPDATE;`, workspaceId, member.VolumeId); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("writable member %q has no registered state lineage", member.Name)
			}
			return err
		}
		var attachment stateAttachmentLineage
		if err := tx.GetContext(ctx, &attachment, `SELECT
			COALESCE(source_generation_id::text, '') AS source_generation_id,
			initialize, clone_source, expires_at > CURRENT_TIMESTAMP AS active
			FROM state_branch_attachment
			WHERE lineage_id = $1 AND workspace_id = $2 AND container_id = $3
			  AND attachment_token = $4::uuid AND fencing_token = $5;`, branch.Id, workspaceId, containerId,
			lease.AttachmentToken, lease.FencingToken); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("branch member %q writer attachment is missing or fenced", member.Name)
			}
			return err
		}
		if err := validateAttachmentGeneration(member.Name, generation, branch.CurrentGenerationId, attachment, compactions[member.VolumeId], false); err != nil {
			return err
		}
		if !attachment.Active {
			if err := verifyEscrowedStateLease(ctx, tx, snapshotId, member, lease); err != nil {
				return fmt.Errorf("branch member %q writer lease is expired or fenced: %w", member.Name, err)
			}
		}
		result, err := tx.ExecContext(ctx, `UPDATE state_branch_lineage SET current_generation_id = $2::uuid,
			updated_at = CURRENT_TIMESTAMP WHERE id = $1
			  AND COALESCE(current_generation_id::text, '') = $3;`, branch.Id,
			generation.ExternalId, branch.CurrentGenerationId)
		if err != nil {
			return err
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return fmt.Errorf("branch member %q head changed during commit", member.Name)
		}
	}
	return nil
}

type stateAttachmentLineage struct {
	SourceGenerationId string `db:"source_generation_id"`
	Initialize         bool   `db:"initialize"`
	CloneSource        bool   `db:"clone_source"`
	Active             bool   `db:"active"`
}

func validateAttachmentGeneration(
	memberName string,
	generation types.VolumeGeneration,
	currentHead string,
	attachment stateAttachmentLineage,
	compaction types.StateGenerationCompaction,
	named bool,
) error {
	if currentHead != "" {
		if compaction.SourceGenerationId != "" {
			if compaction.VolumeId != generation.VolumeId || compaction.GenerationId != generation.ExternalId ||
				compaction.SourceGenerationId != currentHead || generation.Generation <= 1 ||
				generation.ParentGenerationId != "" || generation.CloneParentGenerationId != "" {
				return fmt.Errorf("state member %q compaction does not replace its exact current head", memberName)
			}
			return nil
		}
		if generation.CloneParentGenerationId != "" || generation.ParentGenerationId != currentHead {
			return fmt.Errorf("state member %q generation does not advance its exact current head", memberName)
		}
		return nil
	}

	if compaction.SourceGenerationId != "" || generation.Generation != 1 || generation.ParentGenerationId != "" {
		return fmt.Errorf("state member %q first generation must be initial or clone-backed", memberName)
	}
	if named {
		if !attachment.Initialize || attachment.SourceGenerationId != "" || attachment.CloneSource || generation.CloneParentGenerationId != "" {
			return fmt.Errorf("named disk %q first generation must match its initialize attachment", memberName)
		}
		return nil
	}
	if attachment.Initialize {
		if attachment.SourceGenerationId != "" || attachment.CloneSource || generation.CloneParentGenerationId != "" {
			return fmt.Errorf("branch member %q first generation must match its initialize attachment", memberName)
		}
		return nil
	}
	if !attachment.CloneSource || attachment.SourceGenerationId == "" ||
		generation.CloneParentGenerationId != attachment.SourceGenerationId {
		return fmt.Errorf("branch member %q clone parent does not match its exact attachment source", memberName)
	}
	return nil
}

func verifyEscrowedStateLease(ctx context.Context, tx *sqlx.Tx, snapshotId uint, member types.StateGeneration, lease types.StateVolumeLease) error {
	var count int
	if err := tx.GetContext(ctx, &count, `SELECT count(*) FROM state_snapshot_member_plan
		WHERE state_snapshot_id = $1 AND volume_id = $2::uuid AND generation_id = $3::uuid
		  AND COALESCE(parent_generation_id::text, '') = $4
		  AND COALESCE(clone_parent_generation_id::text, '') = $5
		  AND generation = $6 AND name = $7 AND mount_path = $8 AND read_only = FALSE
		  AND attachment_token = $9::uuid AND fencing_token = $10;`,
		snapshotId, member.VolumeId, member.GenerationId, member.ParentGenerationId,
		member.CloneParentGenerationId, member.Generation,
		member.Name, member.MountPath, lease.AttachmentToken, lease.FencingToken); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("lease was not escrowed by this pending operation")
	}
	var currentFence int
	if err := tx.GetContext(ctx, &currentFence, `SELECT
		(SELECT count(*) FROM state_volume
		 WHERE workspace_id = (SELECT workspace_id FROM state_snapshot WHERE id = $1)
		   AND external_id = $2::uuid AND next_fencing_token = $3) +
		(SELECT count(*) FROM state_branch_lineage
		 WHERE workspace_id = (SELECT workspace_id FROM state_snapshot WHERE id = $1)
		   AND volume_id = $2::uuid AND next_fencing_token = $3);`,
		snapshotId, member.VolumeId, lease.FencingToken); err != nil {
		return err
	}
	if currentFence != 1 {
		return fmt.Errorf("escrowed writer lease was superseded")
	}
	return nil
}

func sameVolumeGenerationTerminal(stored, requested *types.VolumeGeneration) bool {
	return stored.Status == requested.Status && stored.Reason == requested.Reason &&
		stored.ManifestKey == requested.ManifestKey && stored.ManifestDigest == requested.ManifestDigest &&
		stored.ManifestSizeBytes == requested.ManifestSizeBytes && stored.ChunkCount == requested.ChunkCount &&
		stored.LogicalSizeBytes == requested.LogicalSizeBytes && stored.StoredSizeBytes == requested.StoredSizeBytes &&
		stored.BucketName == requested.BucketName && stored.ObjectPrefix == requested.ObjectPrefix
}

func verifyCommittedGenerationReplay(ctx context.Context, tx *sqlx.Tx, workspaceId uint, generations []types.VolumeGeneration) error {
	for index := range generations {
		requested := &generations[index]
		var current types.VolumeGeneration
		if err := tx.GetContext(ctx, &current, `SELECT `+volumeGenerationColumns+` FROM volume_generation
			WHERE external_id = $1::uuid AND workspace_id = $2 FOR UPDATE;`, requested.ExternalId, workspaceId); err != nil {
			return err
		}
		if current.VolumeId != requested.VolumeId || current.Name != requested.Name ||
			current.ParentGenerationId != requested.ParentGenerationId ||
			current.CloneParentGenerationId != requested.CloneParentGenerationId || current.Generation != requested.Generation ||
			!sameVolumeGenerationTerminal(&current, requested) {
			return fmt.Errorf("terminal generation %q replay mismatch", requested.ExternalId)
		}
	}
	return nil
}

func (r *PostgresBackendRepository) GetStateSnapshot(ctx context.Context, workspaceId uint, snapshotId string) (*types.StateSnapshot, error) {
	var snapshot types.StateSnapshot
	err := r.client.GetContext(ctx, &snapshot, `SELECT `+stateSnapshotColumns+` FROM state_snapshot
		WHERE external_id = $1 AND (workspace_id = $2 OR (public = TRUE AND status = $3))
		AND NOT EXISTS (SELECT 1 FROM state_cache_retirement_outbox retirement
		                WHERE retirement.state_snapshot_id=state_snapshot.id
		                  AND retirement.status IN ('delivering','delivered'))
		LIMIT 1;`,
		snapshotId, workspaceId, types.StateSnapshotStatusAvailable)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, &types.ErrStateSnapshotNotFound{StateSnapshotId: snapshotId}
		}
		return nil, err
	}
	err = r.client.SelectContext(ctx, &snapshot.Generations, `
		SELECT ssg.volume_id, vg.external_id::text AS generation_id, ssg.name, ssg.mount_path,
			COALESCE(vg.parent_generation_id::text, '') AS parent_generation_id,
			COALESCE(vg.clone_parent_generation_id::text, '') AS clone_parent_generation_id,
			ssg.read_only, ssg.is_root AS root, ssg.generation
		FROM state_snapshot_generation ssg JOIN volume_generation vg ON vg.id = ssg.volume_generation_id
		WHERE ssg.state_snapshot_id = $1 ORDER BY ssg.is_root DESC, ssg.volume_id;`, snapshot.Id)
	return &snapshot, err
}

// GetStateSnapshotByOperation is an internal worker recovery lookup. Container
// IDs and operation IDs are globally unique in the new state schema, so a
// worker can replay a terminal result after its in-memory instance has gone
// without trusting caller-provided workspace identity.
func (r *PostgresBackendRepository) GetStateSnapshotByOperation(ctx context.Context, sourceContainerId, operationId string) (*types.StateSnapshot, error) {
	return r.getStateSnapshotByOperation(ctx, 0, sourceContainerId, operationId)
}

func (r *PostgresBackendRepository) GetStateSnapshotByOperationForWorkspace(ctx context.Context, workspaceId uint, sourceContainerId, operationId string) (*types.StateSnapshot, error) {
	if workspaceId == 0 {
		return nil, fmt.Errorf("workspace id is required")
	}
	return r.getStateSnapshotByOperation(ctx, workspaceId, sourceContainerId, operationId)
}

func (r *PostgresBackendRepository) GetPendingStateSnapshotByContainer(ctx context.Context, sourceContainerId string) (*types.StateSnapshot, error) {
	sourceContainerId = strings.TrimSpace(sourceContainerId)
	if sourceContainerId == "" {
		return nil, fmt.Errorf("source container id is required")
	}
	var snapshot types.StateSnapshot
	if err := r.client.GetContext(ctx, &snapshot, `SELECT `+stateSnapshotColumns+` FROM state_snapshot
		WHERE source_container_id = $1 AND status = 'pending';`, sourceContainerId); err != nil {
		if err == sql.ErrNoRows {
			return nil, &types.ErrStateSnapshotNotFound{StateSnapshotId: "pending:" + sourceContainerId}
		}
		return nil, err
	}
	return &snapshot, nil
}

func (r *PostgresBackendRepository) ListUnarmedPendingStateSnapshots(ctx context.Context, olderThan time.Time) ([]types.StateSnapshot, error) {
	snapshots := []types.StateSnapshot{}
	err := r.client.SelectContext(ctx, &snapshots, `SELECT `+stateSnapshotColumns+` FROM state_snapshot
		WHERE status = 'pending' AND armed_at IS NULL AND created_at < $1
		ORDER BY created_at, id;`, olderThan)
	return snapshots, err
}

func (r *PostgresBackendRepository) GetStateSnapshotPlan(ctx context.Context, workspaceId uint, snapshotId string) ([]types.StateGeneration, error) {
	if workspaceId == 0 || strings.TrimSpace(snapshotId) == "" {
		return nil, fmt.Errorf("workspace and state snapshot ids are required")
	}
	var stored []plannedStateMember
	if err := r.client.SelectContext(ctx, &stored, `SELECT p.volume_id::text AS volume_id,
		p.generation_id::text AS generation_id,
		COALESCE(p.parent_generation_id::text, '') AS parent_generation_id,
		COALESCE(p.clone_parent_generation_id::text, '') AS clone_parent_generation_id,
		p.compaction, COALESCE(p.compaction_source_generation_id::text, '') AS compaction_source_generation_id,
		p.generation, p.name, p.mount_path, p.read_only, p.is_root AS root,
		COALESCE(p.attachment_token::text, '') AS attachment_token,
		COALESCE(p.fencing_token, 0) AS fencing_token
		FROM state_snapshot_member_plan p JOIN state_snapshot s ON s.id = p.state_snapshot_id
		WHERE s.workspace_id = $1 AND s.external_id = $2::uuid
		ORDER BY p.is_root DESC, p.volume_id;`, workspaceId, snapshotId); err != nil {
		return nil, err
	}
	if len(stored) == 0 {
		return nil, fmt.Errorf("state snapshot %q has no immutable member plan", snapshotId)
	}
	members := make([]types.StateGeneration, 0, len(stored))
	for _, planned := range stored {
		members = append(members, types.StateGeneration{
			VolumeId: planned.VolumeId, GenerationId: planned.GenerationId,
			ParentGenerationId:      planned.ParentGenerationId,
			CloneParentGenerationId: planned.CloneParentGenerationId,
			Generation:              planned.Generation, Name: planned.Name, MountPath: planned.MountPath,
			ReadOnly: planned.ReadOnly, Root: planned.Root,
		})
	}
	return members, nil
}

func (r *PostgresBackendRepository) GetStateSnapshotCompactionPlan(ctx context.Context, workspaceId uint, snapshotId string) ([]types.StateGenerationCompaction, error) {
	if workspaceId == 0 || strings.TrimSpace(snapshotId) == "" {
		return nil, fmt.Errorf("workspace and state snapshot ids are required")
	}
	var plans []types.StateGenerationCompaction
	if err := r.client.SelectContext(ctx, &plans, `SELECT p.volume_id::text AS volume_id,
		p.generation_id::text AS generation_id,
		p.compaction_source_generation_id::text AS compaction_source_generation_id
		FROM state_snapshot_member_plan p JOIN state_snapshot s ON s.id = p.state_snapshot_id
		WHERE s.workspace_id = $1 AND s.external_id = $2::uuid AND p.compaction = TRUE
		ORDER BY p.volume_id;`, workspaceId, snapshotId); err != nil {
		return nil, err
	}
	return plans, nil
}

func stateSnapshotCompactions(ctx context.Context, tx *sqlx.Tx, snapshotID uint) (map[string]types.StateGenerationCompaction, error) {
	var plans []types.StateGenerationCompaction
	if err := tx.SelectContext(ctx, &plans, `SELECT volume_id::text AS volume_id,
		generation_id::text AS generation_id,
		compaction_source_generation_id::text AS compaction_source_generation_id
		FROM state_snapshot_member_plan WHERE state_snapshot_id = $1 AND compaction = TRUE;`, snapshotID); err != nil {
		return nil, err
	}
	byVolume := make(map[string]types.StateGenerationCompaction, len(plans))
	for _, plan := range plans {
		byVolume[plan.VolumeId] = plan
	}
	return byVolume, nil
}

func (r *PostgresBackendRepository) getStateSnapshotByOperation(ctx context.Context, workspaceId uint, sourceContainerId, operationId string) (*types.StateSnapshot, error) {
	if sourceContainerId == "" || operationId == "" {
		return nil, fmt.Errorf("source container id and operation id are required")
	}
	query := `SELECT ` + stateSnapshotColumns + ` FROM state_snapshot
		WHERE source_container_id = $1 AND operation_id = $2`
	args := []any{sourceContainerId, operationId}
	if workspaceId != 0 {
		query += ` AND workspace_id = $3`
		args = append(args, workspaceId)
	}
	query += ` LIMIT 1;`
	var snapshot types.StateSnapshot
	if err := r.client.GetContext(ctx, &snapshot, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return nil, &types.ErrStateSnapshotNotFound{StateSnapshotId: sourceContainerId + ":" + operationId}
		}
		return nil, err
	}
	if err := r.client.SelectContext(ctx, &snapshot.Generations, `
		SELECT ssg.volume_id, vg.external_id::text AS generation_id, ssg.name, ssg.mount_path,
			COALESCE(vg.parent_generation_id::text, '') AS parent_generation_id,
			COALESCE(vg.clone_parent_generation_id::text, '') AS clone_parent_generation_id,
			ssg.read_only, ssg.is_root AS root, ssg.generation
		FROM state_snapshot_generation ssg JOIN volume_generation vg ON vg.id = ssg.volume_generation_id
		WHERE ssg.state_snapshot_id = $1 ORDER BY ssg.is_root DESC, ssg.volume_id;`, snapshot.Id); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func stateSnapshotMembers(ctx context.Context, tx *sqlx.Tx, snapshotId uint) ([]types.StateGeneration, error) {
	members := []types.StateGeneration{}
	err := tx.SelectContext(ctx, &members, `SELECT ssg.volume_id, vg.external_id::text AS generation_id,
		ssg.name, ssg.mount_path,
		COALESCE(vg.parent_generation_id::text, '') AS parent_generation_id,
		COALESCE(vg.clone_parent_generation_id::text, '') AS clone_parent_generation_id,
		ssg.read_only, ssg.is_root AS root, ssg.generation
		FROM state_snapshot_generation ssg JOIN volume_generation vg ON vg.id = ssg.volume_generation_id
		WHERE ssg.state_snapshot_id = $1 ORDER BY ssg.is_root DESC, ssg.volume_id;`, snapshotId)
	return members, err
}

func sameStateGenerations(stored, requested []types.StateGeneration) bool {
	if len(stored) != len(requested) {
		return false
	}
	byVolume := make(map[string]types.StateGeneration, len(stored))
	for _, generation := range stored {
		byVolume[generation.VolumeId] = generation
	}
	for _, generation := range requested {
		match, ok := byVolume[generation.VolumeId]
		if !ok || match != generation {
			return false
		}
	}
	return true
}

func validateVolumeGeneration(generation *types.VolumeGeneration) error {
	if generation.VolumeId == "" || generation.Name == "" || generation.Generation <= 0 {
		return fmt.Errorf("volume id, name, and a positive generation are required")
	}
	if generation.ExternalId != "" {
		if _, err := uuid.Parse(generation.ExternalId); err != nil {
			return fmt.Errorf("generation id must be an RFC4122 UUID: %w", err)
		}
	}
	if generation.Generation == 1 {
		if generation.ParentGenerationId != "" {
			return fmt.Errorf("generation one may only be initial or clone-backed")
		}
	} else {
		if generation.CloneParentGenerationId != "" {
			return fmt.Errorf("generation greater than one cannot be clone-backed")
		}
	}
	if generation.ParentGenerationId != "" {
		if _, err := uuid.Parse(generation.ParentGenerationId); err != nil {
			return fmt.Errorf("parent generation id must be an RFC4122 UUID: %w", err)
		}
		if generation.Generation <= 1 {
			return fmt.Errorf("a parented generation must follow a positive prior generation")
		}
	}
	if generation.CloneParentGenerationId != "" {
		if generation.ParentGenerationId != "" || generation.Generation != 1 {
			return fmt.Errorf("a cloned generation must be generation one without a same-volume parent")
		}
		if _, err := uuid.Parse(generation.CloneParentGenerationId); err != nil {
			return fmt.Errorf("clone parent generation id must be an RFC4122 UUID: %w", err)
		}
	}
	if generation.Status == types.StateSnapshotStatusAvailable {
		if generation.ManifestKey == "" || generation.ManifestDigest == "" ||
			generation.ManifestSizeBytes <= 0 || generation.LogicalSizeBytes <= 0 ||
			generation.BucketName == "" || generation.ObjectPrefix == "" {
			return fmt.Errorf("available volume generation requires a published manifest and canonical object location")
		}
	}
	if generation.Status == types.StateSnapshotStatusPending &&
		(generation.Reason != "" || generation.ManifestKey != "" || generation.ManifestDigest != "" ||
			generation.ManifestSizeBytes != 0 || generation.ChunkCount != 0 ||
			generation.LogicalSizeBytes != 0 || generation.StoredSizeBytes != 0 ||
			generation.BucketName != "" || generation.ObjectPrefix != "") {
		return fmt.Errorf("pending volume generation cannot reference objects before manifest publication")
	}
	if generation.Status == types.StateSnapshotStatusFailed && generation.Reason == "" {
		return fmt.Errorf("failed volume generation requires a reason")
	}
	return nil
}

func (r *PostgresBackendRepository) GetVolumeGeneration(ctx context.Context, workspaceId uint, generationId string) (*types.VolumeGeneration, error) {
	var generation types.VolumeGeneration
	err := r.client.GetContext(ctx, &generation, `SELECT `+volumeGenerationColumns+` FROM volume_generation
		WHERE external_id = $1 AND (workspace_id = $2 OR (public = TRUE AND status = $3)) LIMIT 1;`,
		generationId, workspaceId, types.StateSnapshotStatusAvailable)
	if err == sql.ErrNoRows {
		return nil, &types.ErrVolumeGenerationNotFound{GenerationId: generationId}
	}
	return &generation, err
}
