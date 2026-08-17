package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type stateCacheScopeIdentity struct {
	StubExternalId string `db:"source_stub_external_id"`
	VolumeId       string `db:"volume_id"`
}

func lockStateCacheScope(ctx context.Context, tx *sqlx.Tx, workspaceId uint, stubExternalId, volumeId string) error {
	key := fmt.Sprintf("state-cache-scope:%d:%s:%s", workspaceId, stubExternalId, volumeId)
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0));`, key)
	return err
}

func lockStateCacheSnapshotScopes(ctx context.Context, tx *sqlx.Tx, workspaceId uint, snapshotId string) error {
	var scopes []stateCacheScopeIdentity
	if err := tx.SelectContext(ctx, &scopes, `SELECT DISTINCT s.source_stub_external_id,
		sg.volume_id FROM state_snapshot s JOIN state_snapshot_generation sg ON sg.state_snapshot_id=s.id
		WHERE s.workspace_id=$1 AND s.external_id=$2::uuid;`, workspaceId, snapshotId); err != nil {
		return err
	}
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].StubExternalId != scopes[j].StubExternalId {
			return scopes[i].StubExternalId < scopes[j].StubExternalId
		}
		return scopes[i].VolumeId < scopes[j].VolumeId
	})
	for _, scope := range scopes {
		if err := lockStateCacheScope(ctx, tx, workspaceId, scope.StubExternalId, scope.VolumeId); err != nil {
			return err
		}
	}
	return nil
}

const (
	stateSnapshotRetirementGrace = 15 * time.Minute
	stateSnapshotRetirementRetry = time.Minute
)

type stateCacheSnapshotMember struct {
	VolumeId   string `db:"volume_id"`
	Generation int64  `db:"generation"`
}

const stateSnapshotReferenceColumns = `r.external_id::text AS external_id, r.workspace_id,
	COALESCE(r.state_snapshot_id, 0) AS state_snapshot_id,
	r.state_snapshot_external_id::text AS snapshot_external_id, r.kind, r.reference_id,
	r.released_at IS NOT NULL AS released, r.created_at, r.updated_at`

func validateStateSnapshotReferenceIdentity(snapshotId, kind, referenceId string) error {
	if parsed, err := uuid.Parse(snapshotId); err != nil || parsed.String() != snapshotId {
		return fmt.Errorf("state snapshot id must be a canonical RFC4122 UUID")
	}
	switch kind {
	case "machine", "snapshot", "template", "internal":
	default:
		return fmt.Errorf("state snapshot reference kind must be machine, snapshot, template, or internal")
	}
	if strings.TrimSpace(referenceId) != referenceId || referenceId == "" || len(referenceId) > 512 {
		return fmt.Errorf("state snapshot reference id must be a nonempty canonical identity")
	}
	return nil
}

func (r *PostgresBackendRepository) RetainStateSnapshotReference(ctx context.Context, workspaceId uint,
	snapshotId, kind, referenceId string,
) (*types.StateSnapshotReference, error) {
	if workspaceId == 0 {
		return nil, fmt.Errorf("state snapshot reference workspace is required")
	}
	if err := validateStateSnapshotReferenceIdentity(snapshotId, kind, referenceId); err != nil {
		return nil, err
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Serialize retain with parentless compaction planning for every physical
	// lineage in the immutable snapshot. This lock is acquired before the
	// snapshot row lock so a late Retain cannot appear after compaction has
	// already proven that no historical pin would be dropped from the scoped S2
	// replacement.
	if err := lockStateCacheSnapshotScopes(ctx, tx, workspaceId, snapshotId); err != nil {
		return nil, err
	}
	var snapshot struct {
		Id             uint   `db:"id"`
		StubExternalId string `db:"source_stub_external_id"`
		Status         string `db:"status"`
	}
	if err := tx.GetContext(ctx, &snapshot, `SELECT id, source_stub_external_id, status FROM state_snapshot
		WHERE workspace_id = $1 AND external_id = $2::uuid FOR UPDATE;`, workspaceId, snapshotId); err != nil {
		return nil, err
	}
	if snapshot.Status != string(types.StateSnapshotStatusAvailable) || snapshot.StubExternalId == "" {
		return nil, fmt.Errorf("only an available state snapshot with an immutable source stub can be retained")
	}
	var irreversible int
	if err := tx.GetContext(ctx, &irreversible, `SELECT
		(SELECT count(*) FROM state_cache_retirement_outbox
		 WHERE state_snapshot_id=$1 AND status IN ('delivering','delivered')) +
		(SELECT count(*) FROM state_snapshot_generation sg
		 JOIN state_cache_scope_subscription subscription
		   ON subscription.workspace_id=$2 AND subscription.stub_external_id=$3
		  AND subscription.volume_id=sg.volume_id::uuid
			 WHERE sg.state_snapshot_id=$1 AND subscription.retirement_authorized_at IS NOT NULL) +
		(SELECT count(*) FROM state_snapshot_generation retained
		 JOIN volume_generation retained_generation ON retained_generation.id=retained.volume_generation_id
		 JOIN state_snapshot_member_plan planned ON planned.volume_id=retained.volume_id::uuid AND planned.compaction=TRUE
		 JOIN state_snapshot compacting ON compacting.id=planned.state_snapshot_id
		 WHERE retained.state_snapshot_id=$1 AND compacting.workspace_id=$2
		   AND compacting.source_stub_external_id=$3 AND compacting.status='pending'
		   AND planned.compaction_source_generation_id IS DISTINCT FROM retained_generation.external_id);`,
		snapshot.Id, workspaceId, snapshot.StubExternalId); err != nil {
		return nil, err
	}
	if irreversible != 0 {
		return nil, fmt.Errorf("state snapshot cache retirement is already irreversibly authorized")
	}
	var reference types.StateSnapshotReference
	err = tx.GetContext(ctx, &reference, `INSERT INTO state_snapshot_reference
		(workspace_id, state_snapshot_id, state_snapshot_external_id, kind, reference_id)
		VALUES ($1,$2,$5::uuid,$3,$4)
		ON CONFLICT (workspace_id, kind, reference_id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
		WHERE state_snapshot_reference.state_snapshot_id = EXCLUDED.state_snapshot_id
		  AND state_snapshot_reference.released_at IS NULL
		RETURNING external_id::text AS external_id, workspace_id, state_snapshot_id,
		$5 AS snapshot_external_id, kind, reference_id, released_at IS NOT NULL AS released,
		created_at, updated_at;`, workspaceId, snapshot.Id, kind, referenceId, snapshotId)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("state snapshot reference identity conflicts with another or already released reference")
	}
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE state_snapshot SET cache_retire_after = NULL,
		updated_at = CURRENT_TIMESTAMP WHERE id = $1;`, snapshot.Id); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE state_cache_retirement_outbox SET status = 'cancelled',
		last_error = 'cancelled by a new authoritative reference', updated_at = CURRENT_TIMESTAMP
		WHERE state_snapshot_id = $1 AND status = 'pending';`, snapshot.Id); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO state_cache_scope_subscription
		(workspace_id, stub_external_id, volume_id, max_generation)
		SELECT $1, $2, sg.volume_id::uuid, max(sg.generation)
		FROM state_snapshot_generation sg WHERE sg.state_snapshot_id = $3 GROUP BY sg.volume_id
		ON CONFLICT (workspace_id, stub_external_id, volume_id) DO UPDATE SET
		max_generation = GREATEST(state_cache_scope_subscription.max_generation, EXCLUDED.max_generation),
		updated_at = CURRENT_TIMESTAMP;`, workspaceId, snapshot.StubExternalId, snapshot.Id); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &reference, nil
}

func (r *PostgresBackendRepository) ReleaseStateSnapshotReference(ctx context.Context, workspaceId uint,
	snapshotId, kind, referenceId string,
) (*types.StateSnapshotReference, error) {
	if workspaceId == 0 {
		return nil, fmt.Errorf("state snapshot reference workspace is required")
	}
	if err := validateStateSnapshotReferenceIdentity(snapshotId, kind, referenceId); err != nil {
		return nil, err
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var reference types.StateSnapshotReference
	// Release is also the durable negative acknowledgement for a Retain that
	// may have been lost before reaching Beta9. Creating a terminal exact-identity
	// tombstone closes the create/persist/delete crash window: retries succeed,
	// and a delayed Retain for the same (kind, reference_id) can never resurrect
	// the state. The nullable FK permits this even after metadata GC, while the
	// immutable external UUID keeps the tombstone precisely scoped.
	err = tx.GetContext(ctx, &reference, `INSERT INTO state_snapshot_reference
		(workspace_id, state_snapshot_id, state_snapshot_external_id, kind, reference_id, released_at)
		VALUES ($1,(SELECT id FROM state_snapshot WHERE workspace_id=$1 AND external_id=$2::uuid),$2::uuid,$3,$4,CURRENT_TIMESTAMP)
		ON CONFLICT (workspace_id, kind, reference_id) DO UPDATE SET
			released_at=COALESCE(state_snapshot_reference.released_at, CURRENT_TIMESTAMP),
			updated_at=CURRENT_TIMESTAMP
		WHERE state_snapshot_reference.state_snapshot_external_id=EXCLUDED.state_snapshot_external_id
		RETURNING external_id::text AS external_id, workspace_id, COALESCE(state_snapshot_id,0) AS state_snapshot_id,
			state_snapshot_external_id::text AS snapshot_external_id, kind, reference_id,
			released_at IS NOT NULL AS released, created_at, updated_at;`, workspaceId, snapshotId, kind, referenceId)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("state snapshot reference identity conflicts with another snapshot")
	}
	if err != nil {
		return nil, err
	}
	if reference.StateSnapshotId != 0 {
		var active int
		if err := tx.GetContext(ctx, &active, `SELECT count(*) FROM state_snapshot_reference
			WHERE state_snapshot_id = $1 AND released_at IS NULL;`, reference.StateSnapshotId); err != nil {
			return nil, err
		}
		if active == 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE state_snapshot SET
				cache_retire_after = COALESCE(cache_retire_after, $2), updated_at = CURRENT_TIMESTAMP
				WHERE id = $1;`, reference.StateSnapshotId, time.Now().UTC().Add(stateSnapshotRetirementGrace)); err != nil {
				return nil, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &reference, nil
}

func activeStateCacheScopeReferenceCount(ctx context.Context, tx *sqlx.Tx, workspaceId uint,
	stubExternalId, volumeId string,
) (int, error) {
	var count int
	err := tx.GetContext(ctx, &count, `SELECT count(DISTINCT r.id)
		FROM state_snapshot_reference r
		JOIN state_snapshot s ON s.id = r.state_snapshot_id
		JOIN state_snapshot_generation sg ON sg.state_snapshot_id = s.id
		WHERE r.workspace_id = $1 AND r.released_at IS NULL
		  AND s.source_stub_external_id = $2 AND sg.volume_id = $3;`, workspaceId, stubExternalId, volumeId)
	return count, err
}

func liveStateCacheScopeCount(ctx context.Context, tx *sqlx.Tx, workspaceId uint, volumeId string) (int, error) {
	var count int
	err := tx.GetContext(ctx, &count, `SELECT
		(SELECT count(*) FROM state_volume WHERE workspace_id = $1 AND external_id = $2::uuid
		 AND current_generation_id IS NOT NULL AND deleted_at IS NULL) +
		(SELECT count(*) FROM state_branch_lineage WHERE workspace_id = $1 AND volume_id = $2::uuid
		 AND current_generation_id IS NOT NULL) +
		(SELECT count(*) FROM state_volume_attachment a JOIN state_volume v ON v.id = a.state_volume_id
		 WHERE a.workspace_id = $1 AND v.external_id = $2::uuid) +
		(SELECT count(*) FROM state_branch_attachment a JOIN state_branch_lineage l ON l.id = a.lineage_id
		 WHERE a.workspace_id = $1 AND l.volume_id = $2::uuid) +
		(SELECT count(*) FROM state_read_only_attachment WHERE workspace_id = $1 AND volume_id = $2::uuid) +
		(SELECT count(*) FROM state_snapshot_member_plan p JOIN state_snapshot s ON s.id = p.state_snapshot_id
		 WHERE s.workspace_id = $1 AND s.status = 'pending' AND p.volume_id = $2);`, workspaceId, volumeId)
	return count, err
}

func stateCacheSnapshotMembers(ctx context.Context, tx *sqlx.Tx, snapshotId uint) ([]stateCacheSnapshotMember, error) {
	var members []stateCacheSnapshotMember
	err := tx.SelectContext(ctx, &members, `SELECT volume_id, max(generation) AS generation
		FROM state_snapshot_generation WHERE state_snapshot_id = $1
		GROUP BY volume_id ORDER BY volume_id;`, snapshotId)
	return members, err
}

// stateCacheSnapshotPhysicalVolumeIds returns only the concrete Parent/Clone
// ancestry reachable from this exact snapshot group. It intentionally does
// not follow audit or compaction-history metadata, and bounds pruning to the
// scopes whose reachability can change when this snapshot is deleted.
func stateCacheSnapshotPhysicalVolumeIds(ctx context.Context, tx *sqlx.Tx, snapshotId uint) ([]string, error) {
	var volumeIds []string
	err := tx.SelectContext(ctx, &volumeIds, `WITH RECURSIVE physical AS (
		SELECT vg.external_id, vg.volume_id, vg.parent_generation_id, vg.clone_parent_generation_id, 0 AS depth
		FROM state_snapshot_generation sg JOIN volume_generation vg ON vg.id=sg.volume_generation_id
		WHERE sg.state_snapshot_id=$1
		UNION ALL
		SELECT parent.external_id, parent.volume_id, parent.parent_generation_id,
		       parent.clone_parent_generation_id, child.depth+1
		FROM volume_generation parent JOIN physical child
		  ON parent.external_id=child.parent_generation_id OR parent.external_id=child.clone_parent_generation_id
	)
	SELECT volume_id FROM physical GROUP BY volume_id ORDER BY min(depth), volume_id;`, snapshotId)
	return volumeIds, err
}

// stateCacheSnapshotIsRetirable deliberately treats a state snapshot as one
// consistency group. It is not safe to publish a tombstone for only the root
// (or only one attached volume) while another member is still a live head,
// attachment, pending pivot, or authoritative reference.
func stateCacheSnapshotIsRetirable(ctx context.Context, tx *sqlx.Tx, snapshotId, workspaceId uint,
	stubExternalId string, members []stateCacheSnapshotMember,
) (bool, error) {
	var activeSnapshotReferences int
	if err := tx.GetContext(ctx, &activeSnapshotReferences, `SELECT count(*) FROM state_snapshot_reference
		WHERE state_snapshot_id=$1 AND released_at IS NULL;`, snapshotId); err != nil {
		return false, err
	}
	if activeSnapshotReferences != 0 {
		return false, nil
	}
	for _, member := range members {
		active, err := activeStateCacheScopeReferenceCount(ctx, tx, workspaceId, stubExternalId, member.VolumeId)
		if err != nil {
			return false, err
		}
		live, err := liveStateCacheScopeCount(ctx, tx, workspaceId, member.VolumeId)
		if err != nil {
			return false, err
		}
		if active != 0 || live != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (r *PostgresBackendRepository) planStateCacheRetirements(ctx context.Context, limit int) error {
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var snapshots []struct {
		Id             uint   `db:"id"`
		WorkspaceId    uint   `db:"workspace_id"`
		StubExternalId string `db:"source_stub_external_id"`
	}
	if err := tx.SelectContext(ctx, &snapshots, `SELECT s.id, s.workspace_id, s.source_stub_external_id
		FROM state_snapshot s WHERE s.cache_retire_after <= CURRENT_TIMESTAMP
		AND NOT EXISTS (SELECT 1 FROM state_snapshot_reference r
		                WHERE r.state_snapshot_id = s.id AND r.released_at IS NULL)
		ORDER BY s.cache_retire_after, s.id LIMIT $1 FOR UPDATE SKIP LOCKED;`, limit); err != nil {
		return err
	}
	for _, snapshot := range snapshots {
		members, err := stateCacheSnapshotMembers(ctx, tx, snapshot.Id)
		if err != nil {
			return err
		}
		retirable, err := stateCacheSnapshotIsRetirable(ctx, tx, snapshot.Id, snapshot.WorkspaceId,
			snapshot.StubExternalId, members)
		if err != nil {
			return err
		}
		if !retirable {
			// Do not create a partial consistency-group retirement. Pending
			// records from an earlier safe observation are cancelled and a
			// later, strictly newer revision will replace them after the live
			// dependency disappears. Keeping a retry timestamp is essential:
			// head/attachment teardown is not required to know which historical
			// snapshots it just unblocked.
			if _, err := tx.ExecContext(ctx, `UPDATE state_cache_retirement_outbox SET status='cancelled',
				last_error='deferred by authoritative reference, head, or attachment', updated_at=CURRENT_TIMESTAMP
				WHERE state_snapshot_id=$1 AND status='pending';`, snapshot.Id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE state_snapshot SET cache_retire_after=$2,
				updated_at=CURRENT_TIMESTAMP WHERE id=$1;`, snapshot.Id,
				time.Now().UTC().Add(stateSnapshotRetirementRetry)); err != nil {
				return err
			}
			continue
		}
		var otherRetirement int
		if err := tx.GetContext(ctx, &otherRetirement, `SELECT count(*)
			FROM state_snapshot_generation sg JOIN state_cache_retirement_outbox o
			  ON o.workspace_id=$2 AND o.stub_external_id=$3 AND o.volume_id=sg.volume_id::uuid
			WHERE sg.state_snapshot_id=$1 AND o.state_snapshot_id<>$1
			  AND o.status IN ('pending','delivering');`, snapshot.Id, snapshot.WorkspaceId,
			snapshot.StubExternalId); err != nil {
			return err
		}
		if otherRetirement != 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE state_snapshot SET cache_retire_after=$2,
				updated_at=CURRENT_TIMESTAMP WHERE id=$1;`, snapshot.Id,
				time.Now().UTC().Add(stateSnapshotRetirementRetry)); err != nil {
				return err
			}
			continue
		}
		if len(members) == 0 {
			if _, err := tx.ExecContext(ctx, `DELETE FROM state_snapshot WHERE id=$1;`, snapshot.Id); err != nil {
				return err
			}
			continue
		}
		planned := 0
		for _, member := range members {
			var alreadyAuthorized bool
			if err := tx.GetContext(ctx, &alreadyAuthorized, `SELECT EXISTS(
				SELECT 1 FROM state_cache_scope_subscription WHERE workspace_id=$1
				AND stub_external_id=$2 AND volume_id=$3::uuid
				AND retirement_authorized_at IS NOT NULL);`, snapshot.WorkspaceId,
				snapshot.StubExternalId, member.VolumeId); err != nil {
				return err
			}
			if alreadyAuthorized {
				continue
			}
			var revisionGeneration int64
			if err := tx.GetContext(ctx, &revisionGeneration, `INSERT INTO state_cache_scope_subscription
				(workspace_id, stub_external_id, volume_id, max_generation)
				VALUES ($1,$2,$3::uuid,$4)
				ON CONFLICT (workspace_id, stub_external_id, volume_id) DO UPDATE SET
				max_generation = GREATEST(state_cache_scope_subscription.max_generation, EXCLUDED.max_generation)
				RETURNING max_generation;`, snapshot.WorkspaceId, snapshot.StubExternalId,
				member.VolumeId, member.Generation); err != nil {
				return err
			}
			var previous int64
			if err := tx.GetContext(ctx, &previous, `SELECT COALESCE(max(revision_generation), 0)
				FROM state_cache_retirement_outbox WHERE workspace_id = $1 AND stub_external_id = $2
				AND volume_id = $3::uuid;`, snapshot.WorkspaceId, snapshot.StubExternalId, member.VolumeId); err != nil {
				return err
			}
			if previous > revisionGeneration {
				revisionGeneration = previous
			}
			revisionGeneration++
			if _, err := tx.ExecContext(ctx, `UPDATE state_cache_scope_subscription SET max_generation = $4,
				updated_at = CURRENT_TIMESTAMP WHERE workspace_id = $1 AND stub_external_id = $2
				AND volume_id = $3::uuid;`, snapshot.WorkspaceId, snapshot.StubExternalId,
				member.VolumeId, revisionGeneration); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO state_cache_retirement_outbox
				(workspace_id, state_snapshot_id, stub_external_id, volume_id, revision_generation)
				VALUES ($1,$2,$3,$4::uuid,$5)
				ON CONFLICT (workspace_id, stub_external_id, volume_id, revision_generation) DO NOTHING;`,
				snapshot.WorkspaceId, snapshot.Id, snapshot.StubExternalId, member.VolumeId,
				revisionGeneration); err != nil {
				return err
			}
			planned++
		}
		if planned == 0 {
			affectedVolumeIds, err := stateCacheSnapshotPhysicalVolumeIds(ctx, tx, snapshot.Id)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM state_snapshot WHERE id=$1;`, snapshot.Id); err != nil {
				return err
			}
			for _, volumeId := range affectedVolumeIds {
				if err := pruneUnreachableStateVolumeGenerations(ctx, tx, snapshot.WorkspaceId, volumeId); err != nil {
					return err
				}
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE state_snapshot SET cache_retire_after = NULL,
			updated_at = CURRENT_TIMESTAMP WHERE id = $1;`, snapshot.Id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (r *PostgresBackendRepository) authorizeStateCacheRetirements(ctx context.Context, limit int) error {
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var snapshots []struct {
		Id             uint   `db:"id"`
		WorkspaceId    uint   `db:"workspace_id"`
		StubExternalId string `db:"source_stub_external_id"`
	}
	if err := tx.SelectContext(ctx, &snapshots, `SELECT s.id, s.workspace_id, s.source_stub_external_id
		FROM state_snapshot s WHERE EXISTS (SELECT 1 FROM state_cache_retirement_outbox o
			WHERE o.state_snapshot_id=s.id AND o.status='pending')
		ORDER BY s.id LIMIT $1 FOR UPDATE OF s SKIP LOCKED;`, limit); err != nil {
		return err
	}
	for _, snapshot := range snapshots {
		members, err := stateCacheSnapshotMembers(ctx, tx, snapshot.Id)
		if err != nil {
			return err
		}
		retirable, err := stateCacheSnapshotIsRetirable(ctx, tx, snapshot.Id, snapshot.WorkspaceId,
			snapshot.StubExternalId, members)
		if err != nil {
			return err
		}
		if !retirable {
			if _, err := tx.ExecContext(ctx, `UPDATE state_cache_retirement_outbox SET status='cancelled',
				last_error='deferred by authoritative reference, head, or attachment', updated_at=CURRENT_TIMESTAMP
				WHERE state_snapshot_id=$1 AND status='pending';`, snapshot.Id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE state_snapshot SET cache_retire_after=$2,
				updated_at=CURRENT_TIMESTAMP WHERE id=$1;`, snapshot.Id,
				time.Now().UTC().Add(stateSnapshotRetirementRetry)); err != nil {
				return err
			}
			continue
		}
		var pendingScopes int
		if err := tx.GetContext(ctx, &pendingScopes, `SELECT count(DISTINCT volume_id)
			FROM state_cache_retirement_outbox WHERE state_snapshot_id=$1 AND status='pending';`, snapshot.Id); err != nil {
			return err
		}
		if pendingScopes == 0 {
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE state_cache_scope_subscription subscription SET
			retirement_revision_generation=o.revision_generation,
			retirement_revision_id=o.revision_id,
			retirement_authorized_at=CURRENT_TIMESTAMP,
			max_generation=GREATEST(subscription.max_generation,o.revision_generation),
			updated_at=CURRENT_TIMESTAMP
			FROM state_cache_retirement_outbox o
			WHERE o.state_snapshot_id=$1 AND o.status='pending'
			  AND subscription.workspace_id=o.workspace_id
			  AND subscription.stub_external_id=o.stub_external_id
			  AND subscription.volume_id=o.volume_id
			  AND subscription.retirement_authorized_at IS NULL;`, snapshot.Id); err != nil {
			return err
		}
		var authorizedScopes int
		if err := tx.GetContext(ctx, &authorizedScopes, `SELECT count(*)
			FROM state_cache_retirement_outbox o JOIN state_cache_scope_subscription subscription
			ON subscription.workspace_id=o.workspace_id AND subscription.stub_external_id=o.stub_external_id
			AND subscription.volume_id=o.volume_id
			AND subscription.retirement_revision_generation=o.revision_generation
			AND subscription.retirement_revision_id=o.revision_id
			WHERE o.state_snapshot_id=$1 AND o.status='pending'
			AND subscription.retirement_authorized_at IS NOT NULL;`, snapshot.Id); err != nil {
			return err
		}
		if authorizedScopes != pendingScopes {
			return fmt.Errorf("state cache retirement authorization did not bind every member scope")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE state_cache_retirement_outbox SET status='delivering',
			updated_at=CURRENT_TIMESTAMP WHERE state_snapshot_id=$1 AND status='pending';`, snapshot.Id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE state_snapshot SET cache_retire_after=NULL,
			updated_at=CURRENT_TIMESTAMP WHERE id=$1;`, snapshot.Id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (r *PostgresBackendRepository) finalizeStateCacheRetirement(ctx context.Context, snapshotId uint) (bool, error) {
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var snapshot struct {
		Id             uint   `db:"id"`
		WorkspaceId    uint   `db:"workspace_id"`
		StubExternalId string `db:"source_stub_external_id"`
	}
	if err := tx.GetContext(ctx, &snapshot, `SELECT id, workspace_id, source_stub_external_id
		FROM state_snapshot WHERE id=$1 FOR UPDATE;`, snapshotId); err != nil {
		if err == sql.ErrNoRows {
			return true, nil
		}
		return false, err
	}
	var unfinished int
	if err := tx.GetContext(ctx, &unfinished, `SELECT count(*) FROM state_cache_retirement_outbox
		WHERE state_snapshot_id=$1 AND status IN ('pending','delivering');`, snapshotId); err != nil {
		return false, err
	}
	if unfinished != 0 {
		return false, nil
	}
	members, err := stateCacheSnapshotMembers(ctx, tx, snapshot.Id)
	if err != nil {
		return false, err
	}
	retirable, err := stateCacheSnapshotIsRetirable(ctx, tx, snapshot.Id, snapshot.WorkspaceId,
		snapshot.StubExternalId, members)
	if err != nil {
		return false, err
	}
	if !retirable {
		return false, fmt.Errorf("irreversibly authorized state cache retirement gained a live dependency")
	}
	var unauthorizedScopes int
	if err := tx.GetContext(ctx, &unauthorizedScopes, `SELECT count(*)
		FROM state_snapshot_generation sg WHERE sg.state_snapshot_id=$1 AND NOT EXISTS (
			SELECT 1 FROM state_cache_scope_subscription subscription
			WHERE subscription.workspace_id=$2 AND subscription.stub_external_id=$3
			AND subscription.volume_id=sg.volume_id::uuid
			AND subscription.retirement_authorized_at IS NOT NULL);`, snapshot.Id, snapshot.WorkspaceId,
		snapshot.StubExternalId); err != nil {
		return false, err
	}
	if unauthorizedScopes != 0 {
		return false, nil
	}
	affectedVolumeIds, err := stateCacheSnapshotPhysicalVolumeIds(ctx, tx, snapshot.Id)
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM state_cache_retirement_outbox
		WHERE state_snapshot_id=$1;`, snapshot.Id); err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM state_snapshot WHERE id=$1;`, snapshot.Id)
	if err != nil {
		return false, err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return false, fmt.Errorf("state cache retirement lost its exact snapshot delete fence")
	}
	for _, volumeId := range affectedVolumeIds {
		if err := pruneUnreachableStateVolumeGenerations(ctx, tx, snapshot.WorkspaceId, volumeId); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (r *PostgresBackendRepository) finalizeReadyStateCacheRetirements(ctx context.Context, limit int) error {
	var snapshotIds []uint
	if err := r.client.SelectContext(ctx, &snapshotIds, `SELECT DISTINCT state_snapshot_id
		FROM state_cache_retirement_outbox o
		WHERE NOT EXISTS (SELECT 1 FROM state_cache_retirement_outbox unfinished
			WHERE unfinished.state_snapshot_id=o.state_snapshot_id
			AND unfinished.status IN ('pending','delivering'))
		ORDER BY state_snapshot_id LIMIT $1;`, limit); err != nil {
		return err
	}
	for _, snapshotId := range snapshotIds {
		if _, err := r.finalizeStateCacheRetirement(ctx, snapshotId); err != nil {
			return err
		}
	}
	return nil
}

func (r *PostgresBackendRepository) ProcessStateCacheRetirements(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 100 {
		return 0, fmt.Errorf("state cache retirement limit must be between 1 and 100")
	}
	if r.eventRepo == nil || !r.eventRepo.HasDurableScopedStateSink() {
		return 0, ErrEventWriteUnsupported
	}
	if err := r.planStateCacheRetirements(ctx, limit); err != nil {
		return 0, err
	}
	if err := r.authorizeStateCacheRetirements(ctx, limit); err != nil {
		return 0, err
	}
	if err := r.finalizeReadyStateCacheRetirements(ctx, limit); err != nil {
		return 0, err
	}
	delivered := 0
	for delivered < limit {
		var outbox types.StateCacheRetirement
		err := r.client.GetContext(ctx, &outbox, `SELECT o.id, o.workspace_id,
			w.external_id::text AS workspace_external_id, o.state_snapshot_id, o.stub_external_id,
			o.volume_id::text AS volume_id, o.revision_generation, o.revision_id::text AS revision_id
			FROM state_cache_retirement_outbox o JOIN workspace w ON w.id=o.workspace_id
			WHERE o.status='delivering' AND o.next_attempt_at <= CURRENT_TIMESTAMP
			ORDER BY o.next_attempt_at, o.id LIMIT 1;`)
		if err == sql.ErrNoRows {
			break
		}
		if err != nil {
			return delivered, err
		}
		records, err := types.BuildScopedCacheRequiredContentRevision(outbox.WorkspaceExternalId,
			outbox.StubExternalId, "state-retirement", outbox.VolumeId,
			outbox.RevisionGeneration, outbox.RevisionId, nil, true)
		if err == nil {
			for _, record := range records {
				if err = r.eventRepo.PushStubCacheRequiredContent(record); err != nil {
					break
				}
			}
		}
		if err == nil && r.stateCacheAfterWrite != nil {
			err = r.stateCacheAfterWrite(outbox)
		}
		if err != nil {
			if _, updateErr := r.client.ExecContext(ctx, `UPDATE state_cache_retirement_outbox SET
				attempts=attempts+1, last_error=$2, next_attempt_at=CURRENT_TIMESTAMP + INTERVAL '30 seconds',
				updated_at=CURRENT_TIMESTAMP WHERE id=$1 AND status='delivering';`, outbox.Id, err.Error()); updateErr != nil {
				return delivered, updateErr
			}
			return delivered, err
		}
		result, err := r.client.ExecContext(ctx, `UPDATE state_cache_retirement_outbox SET status='delivered',
			delivered_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP
			WHERE id=$1 AND status='delivering';`, outbox.Id)
		if err != nil {
			return delivered, err
		}
		if affected, err := result.RowsAffected(); err != nil || affected > 1 {
			return delivered, fmt.Errorf("state cache retirement delivery lost its exact outbox fence")
		}
		delivered++
		if _, err := r.finalizeStateCacheRetirement(ctx, outbox.StateSnapshotId); err != nil {
			return delivered, err
		}
	}
	if err := r.finalizeReadyStateCacheRetirements(ctx, limit); err != nil {
		return delivered, err
	}
	return delivered, nil
}

func pruneUnreachableStateVolumeGenerations(ctx context.Context, tx *sqlx.Tx, workspaceId uint, volumeId string) error {
	var candidates []uint
	if err := tx.SelectContext(ctx, &candidates, `WITH RECURSIVE reachable AS (
		SELECT DISTINCT vg.id, vg.parent_generation_id, vg.clone_parent_generation_id
		FROM volume_generation vg JOIN state_snapshot_generation sg ON sg.volume_generation_id=vg.id
		JOIN state_snapshot s ON s.id=sg.state_snapshot_id WHERE s.workspace_id=$1
		UNION
		SELECT vg.id, vg.parent_generation_id, vg.clone_parent_generation_id FROM volume_generation vg
		JOIN state_volume v ON v.current_generation_id=vg.external_id WHERE v.workspace_id=$1 AND v.deleted_at IS NULL
		UNION
		SELECT vg.id, vg.parent_generation_id, vg.clone_parent_generation_id FROM volume_generation vg
		JOIN state_branch_lineage l ON l.current_generation_id=vg.external_id WHERE l.workspace_id=$1
		UNION
		SELECT vg.id, vg.parent_generation_id, vg.clone_parent_generation_id FROM volume_generation vg
		JOIN state_snapshot_member_plan p ON p.generation_id=vg.external_id
		JOIN state_snapshot s ON s.id=p.state_snapshot_id WHERE s.workspace_id=$1 AND s.status='pending'
		UNION
		SELECT vg.id, vg.parent_generation_id, vg.clone_parent_generation_id FROM volume_generation vg
		JOIN state_volume_attachment a ON a.source_generation_id=vg.external_id WHERE a.workspace_id=$1
		UNION
		SELECT vg.id, vg.parent_generation_id, vg.clone_parent_generation_id FROM volume_generation vg
		JOIN state_branch_attachment a ON a.source_generation_id=vg.external_id WHERE a.workspace_id=$1
		UNION
		SELECT vg.id, vg.parent_generation_id, vg.clone_parent_generation_id FROM volume_generation vg
		JOIN state_read_only_attachment a ON a.source_generation_id=vg.external_id WHERE a.workspace_id=$1
		UNION
		SELECT vg.id, vg.parent_generation_id, vg.clone_parent_generation_id FROM volume_generation vg
		JOIN state_snapshot_member_plan p ON p.parent_generation_id=vg.external_id OR p.clone_parent_generation_id=vg.external_id
		JOIN state_snapshot s ON s.id=p.state_snapshot_id WHERE s.workspace_id=$1 AND s.status='pending'
		UNION
		SELECT parent.id, parent.parent_generation_id, parent.clone_parent_generation_id
		FROM volume_generation parent JOIN reachable child
		ON parent.external_id=child.parent_generation_id OR parent.external_id=child.clone_parent_generation_id
	)
	SELECT vg.id FROM volume_generation vg WHERE vg.workspace_id=$1 AND vg.volume_id=$2
	AND NOT EXISTS (SELECT 1 FROM reachable r WHERE r.id=vg.id)
	ORDER BY vg.generation DESC, vg.id DESC;`, workspaceId, volumeId); err != nil {
		return err
	}
	for _, generationId := range candidates {
		if _, err := tx.ExecContext(ctx, `DELETE FROM volume_generation vg WHERE vg.id=$1
			AND NOT EXISTS (SELECT 1 FROM volume_generation child
			 WHERE child.parent_generation_id=vg.external_id OR child.clone_parent_generation_id=vg.external_id);`, generationId); err != nil {
			return err
		}
	}
	return nil
}
