package repository

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

var stateVolumeReleaseJournalDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

const stateVolumeReleaseClaimColumns = `external_id::text AS external_id, workspace_id, container_id,
	source_worker_id, source_worker_instance_id, storage_node_id, recovery_worker_id,
	recovery_worker_instance_id, journal_digest, claim_generation, phase,
	completed_at IS NOT NULL AS completed`

type stateVolumeReleaseEscrow struct {
	VolumeId        string `db:"volume_id"`
	AttachmentKind  string `db:"attachment_kind"`
	AttachmentPlan  string `db:"attachment_plan_id"`
	AttachmentToken string `db:"attachment_token"`
	FencingToken    int64  `db:"fencing_token"`
	LeaseSettled    bool   `db:"lease_settled"`
	OwnerWorker     string `db:"owner_worker_id"`
	OwnerInstance   string `db:"owner_worker_instance_id"`
	StorageNode     string `db:"storage_node_id"`
	NextFence       int64  `db:"next_fencing_token"`
}

func validateStateVolumeReleaseIdentity(containerId, sourceWorkerId, sourceWorkerInstanceId, storageNodeId,
	recoveryWorkerId, recoveryWorkerInstanceId, journalDigest string, previousClaimGeneration int64,
	members []types.StateVolumeReleaseMember,
) (map[string]int64, error) {
	if strings.TrimSpace(containerId) == "" || strings.TrimSpace(sourceWorkerId) == "" ||
		strings.TrimSpace(sourceWorkerInstanceId) == "" || strings.TrimSpace(storageNodeId) == "" ||
		strings.TrimSpace(recoveryWorkerId) == "" || strings.TrimSpace(recoveryWorkerInstanceId) == "" {
		return nil, fmt.Errorf("release source, recovery worker process, storage node, and container identities are required")
	}
	if sourceWorkerId == recoveryWorkerId && sourceWorkerInstanceId == recoveryWorkerInstanceId {
		return nil, fmt.Errorf("release recovery must use a replacement worker process epoch")
	}
	if !stateVolumeReleaseJournalDigest.MatchString(journalDigest) {
		return nil, fmt.Errorf("release journal digest must be canonical sha256")
	}
	if previousClaimGeneration < 0 {
		return nil, fmt.Errorf("release claim generation is invalid")
	}
	return validateStateVolumeReleaseMembers(members)
}

func validateStateVolumeReleaseMembers(members []types.StateVolumeReleaseMember) (map[string]int64, error) {
	if len(members) == 0 {
		return nil, fmt.Errorf("release claim generation and members are required")
	}
	requested := make(map[string]int64, len(members))
	for _, member := range members {
		parsed, err := uuid.Parse(member.VolumeId)
		if err != nil || parsed.String() != member.VolumeId || member.FencingToken <= 0 {
			return nil, fmt.Errorf("release member requires a canonical volume id and positive fencing token")
		}
		if _, duplicate := requested[member.VolumeId]; duplicate {
			return nil, fmt.Errorf("duplicate release member volume %q", member.VolumeId)
		}
		requested[member.VolumeId] = member.FencingToken
	}
	return requested, nil
}

func validateStateVolumeReleaseSourceIdentity(containerId, sourceWorkerId, sourceWorkerInstanceId,
	storageNodeId, journalDigest string, members []types.StateVolumeReleaseMember,
) (map[string]int64, error) {
	if strings.TrimSpace(containerId) == "" || strings.TrimSpace(sourceWorkerId) == "" ||
		strings.TrimSpace(sourceWorkerInstanceId) == "" || strings.TrimSpace(storageNodeId) == "" {
		return nil, fmt.Errorf("release source worker process, storage node, and container identities are required")
	}
	if !stateVolumeReleaseJournalDigest.MatchString(journalDigest) {
		return nil, fmt.Errorf("release journal digest must be canonical sha256")
	}
	return validateStateVolumeReleaseMembers(members)
}

func loadStateVolumeReleaseEscrow(ctx context.Context, tx *sqlx.Tx, workspaceId uint, containerId string) ([]stateVolumeReleaseEscrow, error) {
	var escrow []stateVolumeReleaseEscrow
	err := tx.SelectContext(ctx, &escrow, `
		SELECT v.external_id::text AS volume_id, 'named' AS attachment_kind,
			a.attachment_plan_id::text, a.attachment_token::text, a.fencing_token,
			a.expires_at <= CURRENT_TIMESTAMP - INTERVAL '30 seconds' AS lease_settled,
			a.owner_worker_id, a.owner_worker_instance_id, a.storage_node_id, v.next_fencing_token
		FROM state_volume_attachment a JOIN state_volume v ON v.id = a.state_volume_id
		WHERE a.workspace_id = $1 AND a.container_id = $2
		UNION ALL
		SELECT l.volume_id::text AS volume_id, 'branch' AS attachment_kind,
			a.attachment_plan_id::text, a.attachment_token::text, a.fencing_token,
			a.expires_at <= CURRENT_TIMESTAMP - INTERVAL '30 seconds' AS lease_settled,
			a.owner_worker_id, a.owner_worker_instance_id, a.storage_node_id, l.next_fencing_token
		FROM state_branch_attachment a JOIN state_branch_lineage l ON l.id = a.lineage_id
		WHERE a.workspace_id = $1 AND a.container_id = $2
		ORDER BY volume_id;`, workspaceId, containerId)
	return escrow, err
}

func validateStateVolumeReleaseEscrow(escrow []stateVolumeReleaseEscrow, requested map[string]int64,
	sourceWorkerId, sourceWorkerInstanceId, storageNodeId string, requireSettled bool,
) error {
	if len(escrow) != len(requested) {
		return fmt.Errorf("release journal member set does not exactly match the server-owned attachments")
	}
	for _, attachment := range escrow {
		fence, ok := requested[attachment.VolumeId]
		if !ok || fence != attachment.FencingToken || attachment.NextFence != attachment.FencingToken {
			return fmt.Errorf("release attachment %q was superseded or is not in the exact journal set", attachment.VolumeId)
		}
		if attachment.OwnerWorker != sourceWorkerId || attachment.OwnerInstance != sourceWorkerInstanceId ||
			attachment.StorageNode != storageNodeId {
			return fmt.Errorf("release attachment %q is not owned by the dead source worker epoch", attachment.VolumeId)
		}
		if requireSettled && !attachment.LeaseSettled {
			return fmt.Errorf("release attachment %q lease deadline has not passed its safety settle interval", attachment.VolumeId)
		}
		if _, err := uuid.Parse(attachment.AttachmentPlan); err != nil {
			return fmt.Errorf("release attachment %q has invalid scheduler plan identity", attachment.VolumeId)
		}
		if _, err := uuid.Parse(attachment.AttachmentToken); err != nil {
			return fmt.Errorf("release attachment %q has invalid server escrow token", attachment.VolumeId)
		}
	}
	return nil
}

func loadStateVolumeReleaseClaimMembers(ctx context.Context, queryer sqlx.QueryerContext, claimId string) ([]types.StateVolumeReleaseMember, error) {
	var members []types.StateVolumeReleaseMember
	err := sqlx.SelectContext(ctx, queryer, &members, `SELECT volume_id::text AS volume_id, fencing_token
		FROM state_volume_release_claim_member WHERE claim_id =
		(SELECT id FROM state_volume_release_claim WHERE external_id = $1::uuid) ORDER BY volume_id;`, claimId)
	return members, err
}

func (r *PostgresBackendRepository) GetStateVolumeReleaseClaim(ctx context.Context, workspaceId uint, containerId string) (*types.StateVolumeReleaseClaim, error) {
	var claim types.StateVolumeReleaseClaim
	if err := r.client.GetContext(ctx, &claim, `SELECT `+stateVolumeReleaseClaimColumns+`
		FROM state_volume_release_claim WHERE workspace_id = $1 AND container_id = $2;`, workspaceId, containerId); err != nil {
		return nil, err
	}
	members, err := loadStateVolumeReleaseClaimMembers(ctx, r.client, claim.ExternalId)
	if err != nil {
		return nil, err
	}
	claim.Members = members
	return &claim, nil
}

func (r *PostgresBackendRepository) BeginStateVolumeReleaseIntent(ctx context.Context, workspaceId uint,
	containerId, sourceWorkerId, sourceWorkerInstanceId, storageNodeId, journalDigest string,
	members []types.StateVolumeReleaseMember,
) (*types.StateVolumeReleaseClaim, error) {
	requested, err := validateStateVolumeReleaseSourceIdentity(containerId, sourceWorkerId,
		sourceWorkerInstanceId, storageNodeId, journalDigest, members)
	if err != nil {
		return nil, err
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var stored types.StateVolumeReleaseClaim
	err = tx.GetContext(ctx, &stored, `SELECT `+stateVolumeReleaseClaimColumns+`
		FROM state_volume_release_claim WHERE workspace_id = $1 AND container_id = $2 FOR UPDATE;`, workspaceId, containerId)
	if err == nil {
		if stored.SourceWorkerId != sourceWorkerId || stored.SourceWorkerInstanceId != sourceWorkerInstanceId ||
			stored.StorageNodeId != storageNodeId || stored.JournalDigest != journalDigest {
			return nil, fmt.Errorf("release intent conflicts with a different immutable source obligation")
		}
		recorded, memberErr := loadStateVolumeReleaseClaimMembers(ctx, tx, stored.ExternalId)
		if memberErr != nil {
			return nil, memberErr
		}
		if !equalStateVolumeReleaseMembers(recorded, members) {
			return nil, fmt.Errorf("release intent replay member set mismatch")
		}
		if !stored.Completed {
			if stored.Phase != "source" {
				return nil, fmt.Errorf("release intent is already owned by a recovery claimant")
			}
			escrow, escrowErr := loadStateVolumeReleaseEscrow(ctx, tx, workspaceId, containerId)
			if escrowErr != nil {
				return nil, escrowErr
			}
			if err := validateStateVolumeReleaseEscrow(escrow, requested, sourceWorkerId,
				sourceWorkerInstanceId, storageNodeId, false); err != nil {
				return nil, err
			}
		}
	} else if err == sql.ErrNoRows {
		escrow, escrowErr := loadStateVolumeReleaseEscrow(ctx, tx, workspaceId, containerId)
		if escrowErr != nil {
			return nil, escrowErr
		}
		if err := validateStateVolumeReleaseEscrow(escrow, requested, sourceWorkerId,
			sourceWorkerInstanceId, storageNodeId, false); err != nil {
			return nil, err
		}
		if err := tx.GetContext(ctx, &stored, `INSERT INTO state_volume_release_claim
			(workspace_id, container_id, source_worker_id, source_worker_instance_id, storage_node_id,
			 recovery_worker_id, recovery_worker_instance_id, journal_digest, claim_generation, phase)
			VALUES ($1,$2,$3,$4,$5,$3,$4,$6,0,'source') RETURNING `+stateVolumeReleaseClaimColumns+`;`,
			workspaceId, containerId, sourceWorkerId, sourceWorkerInstanceId, storageNodeId, journalDigest); err != nil {
			return nil, err
		}
		for _, attachment := range escrow {
			if _, err := tx.ExecContext(ctx, `INSERT INTO state_volume_release_claim_member
				(claim_id, volume_id, attachment_kind, attachment_plan_id, attachment_token, fencing_token, lease_expires_at)
				VALUES ((SELECT id FROM state_volume_release_claim WHERE external_id = $1::uuid),
				$2::uuid,$3,$4::uuid,$5::uuid,$6,
				(SELECT expires_at FROM `+stateVolumeAttachmentTable(attachment.AttachmentKind)+` WHERE attachment_token = $5::uuid));`,
				stored.ExternalId, attachment.VolumeId, attachment.AttachmentKind, attachment.AttachmentPlan,
				attachment.AttachmentToken, attachment.FencingToken); err != nil {
				return nil, err
			}
		}
	} else {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	stored.Members = sortedStateVolumeReleaseMembers(members)
	return &stored, nil
}

func (r *PostgresBackendRepository) ClaimStateVolumeRelease(ctx context.Context, workspaceId uint, containerId,
	sourceWorkerId, sourceWorkerInstanceId, storageNodeId, recoveryWorkerId, recoveryWorkerInstanceId,
	journalDigest string, previousClaimGeneration int64, members []types.StateVolumeReleaseMember,
) (*types.StateVolumeReleaseClaim, error) {
	requested, err := validateStateVolumeReleaseIdentity(containerId, sourceWorkerId, sourceWorkerInstanceId,
		storageNodeId, recoveryWorkerId, recoveryWorkerInstanceId, journalDigest, previousClaimGeneration, members)
	if err != nil {
		return nil, err
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var stored types.StateVolumeReleaseClaim
	err = tx.GetContext(ctx, &stored, `SELECT `+stateVolumeReleaseClaimColumns+`
		FROM state_volume_release_claim WHERE workspace_id = $1 AND container_id = $2 FOR UPDATE;`, workspaceId, containerId)
	switch err {
	case nil:
		if stored.SourceWorkerId != sourceWorkerId || stored.SourceWorkerInstanceId != sourceWorkerInstanceId ||
			stored.StorageNodeId != storageNodeId || stored.JournalDigest != journalDigest {
			return nil, fmt.Errorf("release claim conflicts with a different immutable source obligation")
		}
		recorded, err := loadStateVolumeReleaseClaimMembers(ctx, tx, stored.ExternalId)
		if err != nil {
			return nil, err
		}
		if !equalStateVolumeReleaseMembers(recorded, members) {
			return nil, fmt.Errorf("release claim replay member set mismatch")
		}
		if stored.Completed {
			if previousClaimGeneration != stored.ClaimGeneration {
				return nil, fmt.Errorf("completed release claim generation mismatch")
			}
			break
		}
		escrow, err := loadStateVolumeReleaseEscrow(ctx, tx, workspaceId, containerId)
		if err != nil {
			return nil, err
		}
		if err := validateStateVolumeReleaseEscrow(escrow, requested, sourceWorkerId, sourceWorkerInstanceId, storageNodeId, true); err != nil {
			return nil, err
		}
		if stored.RecoveryWorkerId == recoveryWorkerId && stored.RecoveryWorkerInstanceId == recoveryWorkerInstanceId {
			if previousClaimGeneration != stored.ClaimGeneration && previousClaimGeneration+1 != stored.ClaimGeneration {
				return nil, fmt.Errorf("release claim replay generation mismatch")
			}
			break
		}
		if previousClaimGeneration != stored.ClaimGeneration {
			return nil, fmt.Errorf("release claim was superseded")
		}
		result, err := tx.ExecContext(ctx, `UPDATE state_volume_release_claim
			SET recovery_worker_id = $2, recovery_worker_instance_id = $3,
				claim_generation = claim_generation + 1, phase = 'claimed', updated_at = CURRENT_TIMESTAMP
			WHERE external_id = $1::uuid AND claim_generation = $4 AND completed_at IS NULL;`,
			stored.ExternalId, recoveryWorkerId, recoveryWorkerInstanceId, previousClaimGeneration)
		if err != nil {
			return nil, err
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return nil, fmt.Errorf("release claim handoff was superseded")
		}
		stored.RecoveryWorkerId, stored.RecoveryWorkerInstanceId = recoveryWorkerId, recoveryWorkerInstanceId
		stored.ClaimGeneration++
	case sql.ErrNoRows:
		if previousClaimGeneration != 0 {
			return nil, fmt.Errorf("state-volume release intent does not exist at the requested generation")
		}
		escrow, escrowErr := loadStateVolumeReleaseEscrow(ctx, tx, workspaceId, containerId)
		if escrowErr != nil {
			return nil, escrowErr
		}
		if err := validateStateVolumeReleaseEscrow(escrow, requested, sourceWorkerId,
			sourceWorkerInstanceId, storageNodeId, true); err != nil {
			return nil, err
		}
		if err := tx.GetContext(ctx, &stored, `INSERT INTO state_volume_release_claim
			(workspace_id, container_id, source_worker_id, source_worker_instance_id, storage_node_id,
			 recovery_worker_id, recovery_worker_instance_id, journal_digest, claim_generation, phase)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,'claimed') RETURNING `+stateVolumeReleaseClaimColumns+`;`,
			workspaceId, containerId, sourceWorkerId, sourceWorkerInstanceId, storageNodeId,
			recoveryWorkerId, recoveryWorkerInstanceId, journalDigest); err != nil {
			return nil, err
		}
		for _, attachment := range escrow {
			if _, err := tx.ExecContext(ctx, `INSERT INTO state_volume_release_claim_member
				(claim_id, volume_id, attachment_kind, attachment_plan_id, attachment_token, fencing_token, lease_expires_at)
				VALUES ((SELECT id FROM state_volume_release_claim WHERE external_id = $1::uuid),
				$2::uuid,$3,$4::uuid,$5::uuid,$6,
				(SELECT expires_at FROM `+stateVolumeAttachmentTable(attachment.AttachmentKind)+` WHERE attachment_token = $5::uuid));`,
				stored.ExternalId, attachment.VolumeId, attachment.AttachmentKind, attachment.AttachmentPlan,
				attachment.AttachmentToken, attachment.FencingToken); err != nil {
				return nil, err
			}
		}
	default:
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	stored.Members = sortedStateVolumeReleaseMembers(members)
	return &stored, nil
}

func stateVolumeAttachmentTable(kind string) string {
	if kind == "branch" {
		return "state_branch_attachment"
	}
	return "state_volume_attachment"
}

func sortedStateVolumeReleaseMembers(in []types.StateVolumeReleaseMember) []types.StateVolumeReleaseMember {
	out := append([]types.StateVolumeReleaseMember(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeId < out[j].VolumeId })
	return out
}

func equalStateVolumeReleaseMembers(left, right []types.StateVolumeReleaseMember) bool {
	left, right = sortedStateVolumeReleaseMembers(left), sortedStateVolumeReleaseMembers(right)
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func lockStateVolumeReleaseIntentForSource(ctx context.Context, tx *sqlx.Tx, workspaceId uint,
	containerId, workerId, workerInstanceId, storageNodeId string, leases []types.StateVolumeLease,
) (*types.StateVolumeReleaseClaim, error) {
	var claim types.StateVolumeReleaseClaim
	if err := tx.GetContext(ctx, &claim, `SELECT `+stateVolumeReleaseClaimColumns+`
		FROM state_volume_release_claim WHERE workspace_id = $1 AND container_id = $2 FOR UPDATE;`,
		workspaceId, containerId); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("state-volume release intent must be durably begun before detach")
		}
		return nil, err
	}
	if claim.SourceWorkerId != workerId || claim.SourceWorkerInstanceId != workerInstanceId ||
		claim.StorageNodeId != storageNodeId || claim.RecoveryWorkerId != workerId ||
		claim.RecoveryWorkerInstanceId != workerInstanceId || claim.ClaimGeneration != 0 {
		return nil, fmt.Errorf("state-volume release intent is owned by another worker process or recovery claimant")
	}
	var escrow []stateVolumeReleaseEscrow
	if err := tx.SelectContext(ctx, &escrow, `SELECT volume_id::text AS volume_id, attachment_kind,
		attachment_plan_id::text, attachment_token::text, fencing_token, TRUE AS lease_settled,
		$2 AS owner_worker_id, $3 AS owner_worker_instance_id, $4 AS storage_node_id,
		fencing_token AS next_fencing_token
		FROM state_volume_release_claim_member WHERE claim_id =
		(SELECT id FROM state_volume_release_claim WHERE external_id = $1::uuid) ORDER BY volume_id;`,
		claim.ExternalId, workerId, workerInstanceId, storageNodeId); err != nil {
		return nil, err
	}
	if len(escrow) != len(leases) {
		return nil, fmt.Errorf("state-volume release lease set does not match its durable intent")
	}
	leaseByVolume := make(map[string]types.StateVolumeLease, len(leases))
	for _, lease := range leases {
		if _, duplicate := leaseByVolume[lease.VolumeId]; duplicate {
			return nil, fmt.Errorf("duplicate state-volume release lease")
		}
		leaseByVolume[lease.VolumeId] = lease
	}
	for _, member := range escrow {
		lease, ok := leaseByVolume[member.VolumeId]
		if !ok || lease.AttachmentToken != member.AttachmentToken || lease.FencingToken != member.FencingToken {
			return nil, fmt.Errorf("state-volume release lease does not match its server-escrowed intent")
		}
	}
	claim.Members = make([]types.StateVolumeReleaseMember, 0, len(escrow))
	for _, member := range escrow {
		claim.Members = append(claim.Members, types.StateVolumeReleaseMember{
			VolumeId: member.VolumeId, FencingToken: member.FencingToken,
		})
	}
	return &claim, nil
}

func completeSourceStateVolumeReleaseIntent(ctx context.Context, tx *sqlx.Tx, claimId string) error {
	result, err := tx.ExecContext(ctx, `UPDATE state_volume_release_claim SET completed_at = CURRENT_TIMESTAMP,
		phase = 'completed', updated_at = CURRENT_TIMESTAMP WHERE external_id = $1::uuid AND claim_generation = 0
		AND phase = 'source' AND completed_at IS NULL;`, claimId)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("state-volume release intent completion was superseded")
	}
	return nil
}

func completeMatchingTerminalStateVolumeReleaseIntent(ctx context.Context, tx *sqlx.Tx, workspaceId uint,
	containerId, activeWorkerId, activeWorkerInstanceId, storageNodeId string,
	leases map[string]types.StateVolumeLease,
) error {
	var claim types.StateVolumeReleaseClaim
	err := tx.GetContext(ctx, &claim, `SELECT `+stateVolumeReleaseClaimColumns+`
		FROM state_volume_release_claim WHERE workspace_id = $1 AND container_id = $2 FOR UPDATE;`,
		workspaceId, containerId)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if claim.Completed {
		return nil
	}
	if claim.StorageNodeId != storageNodeId ||
		(claim.Phase == "source" && (claim.SourceWorkerId != activeWorkerId || claim.SourceWorkerInstanceId != activeWorkerInstanceId)) ||
		(claim.Phase == "claimed" && (claim.RecoveryWorkerId != activeWorkerId || claim.RecoveryWorkerInstanceId != activeWorkerInstanceId)) ||
		(claim.Phase != "source" && claim.Phase != "claimed") {
		return fmt.Errorf("terminal state release intent is not owned by the active snapshot worker process")
	}
	var escrow []stateVolumeReleaseEscrow
	if err := tx.SelectContext(ctx, &escrow, `SELECT volume_id::text AS volume_id, attachment_kind,
		attachment_plan_id::text, attachment_token::text, fencing_token, TRUE AS lease_settled,
		$2 AS owner_worker_id, $3 AS owner_worker_instance_id, $4 AS storage_node_id,
		fencing_token AS next_fencing_token
		FROM state_volume_release_claim_member WHERE claim_id =
		(SELECT id FROM state_volume_release_claim WHERE external_id = $1::uuid) ORDER BY volume_id;`,
		claim.ExternalId, activeWorkerId, activeWorkerInstanceId, storageNodeId); err != nil {
		return err
	}
	if len(escrow) != len(leases) {
		return fmt.Errorf("terminal state release intent does not match its committed member set")
	}
	for _, member := range escrow {
		lease, ok := leases[member.VolumeId]
		if !ok || lease.AttachmentToken != member.AttachmentToken || lease.FencingToken != member.FencingToken {
			return fmt.Errorf("terminal state release intent member does not match its committed lease")
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE state_volume_release_claim SET phase = 'completed',
		completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE external_id = $1::uuid AND claim_generation = $2 AND phase = $3 AND completed_at IS NULL;`,
		claim.ExternalId, claim.ClaimGeneration, claim.Phase)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("terminal state release intent completion was superseded")
	}
	return nil
}

func (r *PostgresBackendRepository) CompleteClaimedStateVolumeRelease(ctx context.Context, workspaceId uint,
	containerId, claimId, recoveryWorkerId, recoveryWorkerInstanceId, storageNodeId string, claimGeneration int64,
) error {
	if parsed, err := uuid.Parse(claimId); err != nil || parsed.String() != claimId || claimGeneration <= 0 ||
		strings.TrimSpace(containerId) == "" || strings.TrimSpace(recoveryWorkerId) == "" ||
		strings.TrimSpace(recoveryWorkerInstanceId) == "" || strings.TrimSpace(storageNodeId) == "" {
		return fmt.Errorf("exact release claim and recovery worker process identities are required")
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var stored types.StateVolumeReleaseClaim
	if err := tx.GetContext(ctx, &stored, `SELECT `+stateVolumeReleaseClaimColumns+`
		FROM state_volume_release_claim WHERE external_id = $1::uuid AND workspace_id = $2
		AND container_id = $3 FOR UPDATE;`, claimId, workspaceId, containerId); err != nil {
		return err
	}
	if stored.RecoveryWorkerId != recoveryWorkerId || stored.RecoveryWorkerInstanceId != recoveryWorkerInstanceId ||
		stored.StorageNodeId != storageNodeId || stored.ClaimGeneration != claimGeneration {
		return fmt.Errorf("release claim recovery owner or generation was superseded")
	}
	if stored.Completed {
		return tx.Commit()
	}
	var escrow []stateVolumeReleaseEscrow
	if err := tx.SelectContext(ctx, &escrow, `SELECT m.volume_id::text, m.attachment_kind,
		m.attachment_plan_id::text, m.attachment_token::text, m.fencing_token,
		TRUE AS lease_settled, c.source_worker_id AS owner_worker_id,
		c.source_worker_instance_id AS owner_worker_instance_id, c.storage_node_id,
		m.fencing_token AS next_fencing_token
		FROM state_volume_release_claim_member m JOIN state_volume_release_claim c ON c.id = m.claim_id
		WHERE c.external_id = $1::uuid ORDER BY m.volume_id;`, claimId); err != nil {
		return err
	}
	if len(escrow) == 0 {
		return fmt.Errorf("release claim has no escrowed attachments")
	}
	for _, attachment := range escrow {
		var result sql.Result
		if attachment.AttachmentKind == "named" {
			result, err = tx.ExecContext(ctx, `DELETE FROM state_volume_attachment a USING state_volume v
				WHERE a.state_volume_id = v.id AND a.workspace_id = $1 AND a.container_id = $2
				  AND v.external_id = $3::uuid AND v.next_fencing_token = $4
				  AND a.attachment_plan_id = $5::uuid AND a.attachment_token = $6::uuid AND a.fencing_token = $4
				  AND a.owner_worker_id = $7 AND a.owner_worker_instance_id = $8 AND a.storage_node_id = $9
				  AND a.expires_at <= CURRENT_TIMESTAMP - INTERVAL '30 seconds';`, workspaceId, containerId,
				attachment.VolumeId, attachment.FencingToken, attachment.AttachmentPlan, attachment.AttachmentToken,
				stored.SourceWorkerId, stored.SourceWorkerInstanceId, storageNodeId)
		} else {
			result, err = tx.ExecContext(ctx, `DELETE FROM state_branch_attachment a USING state_branch_lineage l
				WHERE a.lineage_id = l.id AND a.workspace_id = $1 AND a.container_id = $2
				  AND l.volume_id = $3::uuid AND l.next_fencing_token = $4
				  AND a.attachment_plan_id = $5::uuid AND a.attachment_token = $6::uuid AND a.fencing_token = $4
				  AND a.owner_worker_id = $7 AND a.owner_worker_instance_id = $8 AND a.storage_node_id = $9
				  AND a.expires_at <= CURRENT_TIMESTAMP - INTERVAL '30 seconds';`, workspaceId, containerId,
				attachment.VolumeId, attachment.FencingToken, attachment.AttachmentPlan, attachment.AttachmentToken,
				stored.SourceWorkerId, stored.SourceWorkerInstanceId, storageNodeId)
		}
		if err != nil {
			return err
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return fmt.Errorf("escrowed release attachment %q changed or is still live", attachment.VolumeId)
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
		return fmt.Errorf("claimed release did not remove every escrowed writer attachment")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM state_volume_attachment_plan
		WHERE workspace_id = $1 AND container_id = $2;`, workspaceId, containerId); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE state_volume_release_claim SET completed_at = CURRENT_TIMESTAMP,
		phase = 'completed', updated_at = CURRENT_TIMESTAMP WHERE external_id = $1::uuid
		AND phase = 'claimed' AND completed_at IS NULL;`, claimId)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("release claim completion was superseded")
	}
	return tx.Commit()
}
