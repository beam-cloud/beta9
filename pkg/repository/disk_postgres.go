package repository

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"k8s.io/apimachinery/pkg/api/resource"
)

const diskColumns = "id, external_id::text AS external_id, workspace_id, name, size, mount_path, " +
	"COALESCE(current_generation_id::text, '') AS current_generation_id, created_at, updated_at, deleted_at"
const stateVolumeAttachmentLease = 2 * time.Minute

func validateStateVolumeAttachmentPlanInput(workspaceId uint, containerId, requestHash string, expectedWritableMembers int) error {
	if workspaceId == 0 || strings.TrimSpace(containerId) == "" || expectedWritableMembers <= 0 {
		return fmt.Errorf("workspace, container, and a positive writable-member count are required")
	}
	decoded, err := hex.DecodeString(requestHash)
	if err != nil || len(decoded) != 32 || requestHash != strings.ToLower(requestHash) {
		return fmt.Errorf("state-volume attachment plan requires a lowercase SHA-256 request hash")
	}
	return nil
}

func lockActiveStateVolumeAttachmentPlan(ctx context.Context, tx *sqlx.Tx, workspaceId uint, containerId, planId, requestHash string) error {
	if err := validateStateVolumeAttachmentPlanInput(workspaceId, containerId, requestHash, 1); err != nil {
		return err
	}
	if parsed, err := uuid.Parse(planId); err != nil || parsed.String() != planId {
		return fmt.Errorf("state-volume attachment plan id must be a canonical RFC4122 UUID")
	}
	var storedHash string
	if err := tx.GetContext(ctx, &storedHash, `SELECT request_hash FROM state_volume_attachment_plan
		WHERE workspace_id = $1 AND container_id = $2 AND plan_id = $3::uuid AND request_hash = $4
		  AND admitted_at IS NULL AND aborted_at IS NULL FOR UPDATE;`, workspaceId, containerId, planId, requestHash); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("state-volume attachment plan is absent, admitted, aborted, or conflicts with immutable inputs")
		}
		return err
	}
	return nil
}

// BeginStateVolumeAttachmentPlan durably records scheduler intent before the
// first writer lease is acquired. Exact retries are idempotent; a request that
// reuses a container ID with different volume inputs fails closed.
func (r *PostgresBackendRepository) BeginStateVolumeAttachmentPlan(ctx context.Context, workspaceId uint, containerId, requestHash string, expectedWritableMembers int) (*types.StateVolumeAttachmentPlan, error) {
	if err := validateStateVolumeAttachmentPlanInput(workspaceId, containerId, requestHash, expectedWritableMembers); err != nil {
		return nil, err
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var plan types.StateVolumeAttachmentPlan
	err = tx.GetContext(ctx, &plan, `INSERT INTO state_volume_attachment_plan
		(workspace_id, container_id, request_hash, expected_writable_members)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (workspace_id, container_id) DO NOTHING
		RETURNING plan_id::text AS plan_id, workspace_id, container_id, request_hash,
			expected_writable_members, created_at, admitted_at IS NOT NULL AS admitted,
			enqueued_at IS NOT NULL AS enqueued, aborted_at IS NOT NULL AS aborted,
			abort_reason;`, workspaceId, containerId, requestHash, expectedWritableMembers)
	created := err == nil
	if err == sql.ErrNoRows {
		err = tx.GetContext(ctx, &plan, `SELECT plan_id::text AS plan_id, workspace_id, container_id,
			request_hash, expected_writable_members, created_at, admitted_at IS NOT NULL AS admitted,
			enqueued_at IS NOT NULL AS enqueued, aborted_at IS NOT NULL AS aborted, abort_reason
			FROM state_volume_attachment_plan
			WHERE workspace_id = $1 AND container_id = $2 FOR UPDATE;`, workspaceId, containerId)
	}
	if err != nil {
		return nil, err
	}
	if plan.RequestHash != requestHash || plan.ExpectedWritableMembers != expectedWritableMembers {
		return nil, fmt.Errorf("container id conflicts with a different immutable state-volume attachment plan")
	}
	if plan.Aborted {
		return nil, fmt.Errorf("state-volume attachment plan was durably aborted: %s", plan.AbortReason)
	}
	plan.Owned = created
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &plan, nil
}

// CompleteStateVolumeAttachmentPlan marks the PostgreSQL side of admission
// complete only after every planned writable attachment exists. The row stays
// until terminal detach so a pending, unassigned Redis container can still be
// authoritatively aborted without relying on lease expiry.
func (r *PostgresBackendRepository) CompleteStateVolumeAttachmentPlan(ctx context.Context, workspaceId uint, containerId, planId, requestHash string) error {
	if err := validateStateVolumeAttachmentPlanInput(workspaceId, containerId, requestHash, 1); err != nil {
		return err
	}
	if parsed, err := uuid.Parse(planId); err != nil || parsed.String() != planId {
		return fmt.Errorf("state-volume attachment plan id must be a canonical RFC4122 UUID")
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var expected int
	if err := tx.GetContext(ctx, &expected, `SELECT expected_writable_members
		FROM state_volume_attachment_plan WHERE workspace_id = $1 AND container_id = $2
		  AND plan_id = $3::uuid AND request_hash = $4 AND aborted_at IS NULL FOR UPDATE;`, workspaceId, containerId, planId, requestHash); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("state-volume attachment plan is absent or conflicts with immutable inputs")
		}
		return err
	}
	var actual int
	if err := tx.GetContext(ctx, &actual, `SELECT
		(SELECT count(*) FROM state_volume_attachment WHERE workspace_id = $1 AND container_id = $2
		 AND attachment_plan_id = $3::uuid) +
		(SELECT count(*) FROM state_branch_attachment WHERE workspace_id = $1 AND container_id = $2
		 AND attachment_plan_id = $3::uuid);`, workspaceId, containerId, planId); err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("state-volume attachment plan expected %d writable members but resolved %d", expected, actual)
	}
	result, err := tx.ExecContext(ctx, `UPDATE state_volume_attachment_plan SET admitted_at = CURRENT_TIMESTAMP
		WHERE workspace_id = $1 AND container_id = $2 AND plan_id = $3::uuid AND request_hash = $4;`,
		workspaceId, containerId, planId, requestHash)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("state-volume attachment plan was not completed")
	}
	return tx.Commit()
}

func (r *PostgresBackendRepository) MarkStateVolumeAttachmentPlanEnqueued(ctx context.Context, workspaceId uint, containerId, planId, requestHash string) error {
	if err := validateStateVolumeAttachmentPlanInput(workspaceId, containerId, requestHash, 1); err != nil {
		return err
	}
	if parsed, err := uuid.Parse(planId); err != nil || parsed.String() != planId {
		return fmt.Errorf("state-volume attachment plan id must be a canonical RFC4122 UUID")
	}
	result, err := r.client.ExecContext(ctx, `UPDATE state_volume_attachment_plan
		SET enqueued_at = COALESCE(enqueued_at, CURRENT_TIMESTAMP)
		WHERE workspace_id = $1 AND container_id = $2 AND plan_id = $3::uuid AND request_hash = $4
		  AND admitted_at IS NOT NULL AND aborted_at IS NULL;`, workspaceId, containerId, planId, requestHash)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("state-volume attachment plan is not admitted or its immutable identity changed")
	}
	return nil
}

func (r *PostgresBackendRepository) ListIncompleteStateVolumeAttachmentPlans(ctx context.Context, olderThan time.Time) ([]types.StateVolumeAttachmentPlan, error) {
	plans := []types.StateVolumeAttachmentPlan{}
	err := r.client.SelectContext(ctx, &plans, `SELECT plan_id::text AS plan_id, workspace_id,
		container_id, request_hash, expected_writable_members, created_at,
		admitted_at IS NOT NULL AS admitted, enqueued_at IS NOT NULL AS enqueued,
		aborted_at IS NOT NULL AS aborted, abort_reason
		FROM state_volume_attachment_plan WHERE created_at < $1
		ORDER BY created_at, workspace_id, container_id;`, olderThan)
	return plans, err
}

func (r *PostgresBackendRepository) GetDisk(ctx context.Context, workspaceId uint, name string) (*types.Disk, error) {
	name, err := normalizedDiskName(name)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`SELECT %s FROM state_volume WHERE workspace_id = $1 AND name = $2 AND deleted_at IS NULL LIMIT 1;`, diskColumns)

	var disk types.Disk
	if err := r.client.GetContext(ctx, &disk, query, workspaceId, name); err != nil {
		return nil, err
	}
	return &disk, nil
}

func (r *PostgresBackendRepository) GetOrCreateDisk(ctx context.Context, workspaceId uint, disk *types.Disk) (*types.Disk, error) {
	if disk == nil {
		return nil, fmt.Errorf("disk is nil")
	}

	name, err := normalizedDiskName(disk.Name)
	if err != nil {
		return nil, err
	}
	if name == "root" {
		return nil, fmt.Errorf("disk name root is reserved for persistent root state")
	}
	size, mountPath, err := normalizedDiskGeometry(disk.Size, disk.MountPath)
	if err != nil {
		return nil, err
	}

	if existing, err := r.GetDisk(ctx, workspaceId, name); err == nil {
		return exactDiskGeometry(existing, size, mountPath)
	} else if err != sql.ErrNoRows {
		return nil, err
	}

	query := fmt.Sprintf(`
		INSERT INTO state_volume (workspace_id, name, size, mount_path)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (workspace_id, name) WHERE deleted_at IS NULL DO NOTHING
		RETURNING %s;`, diskColumns)

	var created types.Disk
	if err := r.client.GetContext(ctx, &created, query,
		workspaceId,
		name,
		size,
		mountPath,
	); err != nil {
		if err == sql.ErrNoRows {
			existing, getErr := r.GetDisk(ctx, workspaceId, name)
			if getErr != nil {
				return nil, getErr
			}
			return exactDiskGeometry(existing, size, mountPath)
		}
		return nil, err
	}
	return &created, nil
}

func exactDiskGeometry(existing *types.Disk, size, mountPath string) (*types.Disk, error) {
	if existing == nil {
		return nil, fmt.Errorf("registered disk is unavailable")
	}
	existingSize, existingMountPath, err := normalizedDiskGeometry(existing.Size, existing.MountPath)
	if err != nil || existingSize != size || existingMountPath != mountPath {
		return nil, fmt.Errorf("disk %q already exists with size %q and mount path %q", existing.Name, existing.Size, existing.MountPath)
	}
	return existing, nil
}

func normalizedDiskGeometry(size, mountPath string) (string, string, error) {
	quantity, err := resource.ParseQuantity(strings.TrimSpace(size))
	if err != nil || quantity.Sign() <= 0 {
		return "", "", fmt.Errorf("disk size must be a positive resource quantity")
	}
	mountPath = filepath.Clean(strings.TrimSpace(mountPath))
	if !filepath.IsAbs(mountPath) || mountPath == "/" {
		return "", "", fmt.Errorf("disk mount path must be an absolute directory below root")
	}
	return quantity.String(), mountPath, nil
}

func (r *PostgresBackendRepository) GetLatestVolumeGeneration(ctx context.Context, workspaceId uint, volumeId string) (*types.VolumeGeneration, error) {
	var generation types.VolumeGeneration
	err := r.client.GetContext(ctx, &generation, `SELECT `+volumeGenerationColumns+` FROM volume_generation
		WHERE external_id = (SELECT current_generation_id FROM state_volume
			WHERE workspace_id = $1 AND external_id = $2::uuid AND deleted_at IS NULL)
		  AND workspace_id = $1 AND status = $3 LIMIT 1;`, workspaceId, volumeId, types.StateSnapshotStatusAvailable)
	if err == sql.ErrNoRows {
		return nil, &types.ErrVolumeGenerationNotFound{GenerationId: "latest:" + volumeId}
	}
	return &generation, err
}

func (r *PostgresBackendRepository) ResolveStateVolumeAttachment(ctx context.Context, workspaceId uint, containerId, planId, requestHash string, requested *types.Disk, sourceGenerationId string) (*types.StateVolumeAttachment, error) {
	if requested == nil || strings.TrimSpace(containerId) == "" {
		return nil, fmt.Errorf("disk and container id are required")
	}
	name, err := normalizedDiskName(requested.Name)
	if err != nil {
		return nil, err
	}
	size, mountPath, err := normalizedDiskGeometry(requested.Size, requested.MountPath)
	if err != nil {
		return nil, err
	}
	sourceGenerationId = strings.TrimSpace(sourceGenerationId)
	if sourceGenerationId != "" {
		if _, err := uuid.Parse(sourceGenerationId); err != nil {
			return nil, fmt.Errorf("source generation id must be an RFC4122 UUID")
		}
	}

	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := lockActiveStateVolumeAttachmentPlan(ctx, tx, workspaceId, containerId, planId, requestHash); err != nil {
		return nil, err
	}

	var disk types.Disk
	if err := tx.GetContext(ctx, &disk, `SELECT `+diskColumns+` FROM state_volume
		WHERE workspace_id = $1 AND name = $2 AND deleted_at IS NULL FOR UPDATE;`, workspaceId, name); err != nil {
		return nil, err
	}
	storedSize, storedMountPath, err := normalizedDiskGeometry(disk.Size, disk.MountPath)
	if err != nil || storedSize != size || storedMountPath != mountPath {
		return nil, fmt.Errorf("disk %q geometry does not match its registered state volume", name)
	}
	var existing struct {
		AttachmentPlanId   string    `db:"attachment_plan_id"`
		SourceGenerationId string    `db:"source_generation_id"`
		Initialize         bool      `db:"initialize"`
		AttachmentToken    string    `db:"attachment_token"`
		FencingToken       int64     `db:"fencing_token"`
		ExpiresAt          time.Time `db:"expires_at"`
	}
	err = tx.GetContext(ctx, &existing, `SELECT COALESCE(attachment_plan_id::text, '') AS attachment_plan_id,
		COALESCE(source_generation_id::text, '') AS source_generation_id,
		initialize, attachment_token::text AS attachment_token, fencing_token, expires_at
		FROM state_volume_attachment WHERE state_volume_id = $1 AND container_id = $2 FOR UPDATE;`, disk.Id, containerId)
	if err == nil {
		if !existing.ExpiresAt.After(time.Now()) {
			return nil, fmt.Errorf("disk %q has a stale writer attachment; authoritative teardown is required", name)
		}
		if existing.AttachmentPlanId != planId ||
			(sourceGenerationId != "" && sourceGenerationId != existing.SourceGenerationId) {
			return nil, fmt.Errorf("container attachment replay does not match immutable disk inputs")
		}
		expiresAt := time.Now().Add(stateVolumeAttachmentLease)
		if _, err := tx.ExecContext(ctx, `UPDATE state_volume_attachment SET expires_at = $2, updated_at = CURRENT_TIMESTAMP
			WHERE attachment_token = $1::uuid;`, existing.AttachmentToken, expiresAt); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &types.StateVolumeAttachment{VolumeId: disk.ExternalId, Name: disk.Name, Size: disk.Size,
			MountPath: disk.MountPath, ContainerId: containerId, SourceGenerationId: existing.SourceGenerationId,
			Initialize: existing.Initialize, AttachmentToken: existing.AttachmentToken,
			FencingToken: existing.FencingToken, ExpiresAt: expiresAt, Replayed: true}, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	resolvedGenerationId := sourceGenerationId
	if resolvedGenerationId != "" {
		var count int
		if err := tx.GetContext(ctx, &count, `SELECT count(*) FROM volume_generation
			WHERE external_id = $1::uuid AND workspace_id = $2 AND volume_id = $3 AND status = 'available';`,
			resolvedGenerationId, workspaceId, disk.ExternalId); err != nil {
			return nil, err
		}
		if count != 1 {
			return nil, fmt.Errorf("source generation does not belong to this available state volume")
		}
		if resolvedGenerationId != disk.CurrentGenerationId {
			return nil, fmt.Errorf("writable disk %q source generation is not the current head", name)
		}
	} else {
		resolvedGenerationId = disk.CurrentGenerationId
	}
	initialize := resolvedGenerationId == ""
	var fencingToken int64
	if err := tx.GetContext(ctx, &fencingToken, `UPDATE state_volume SET
		next_fencing_token = next_fencing_token + 1, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1 RETURNING next_fencing_token;`, disk.Id); err != nil {
		return nil, err
	}
	attachmentToken := uuid.NewString()
	expiresAt := time.Now().Add(stateVolumeAttachmentLease)
	_, err = tx.ExecContext(ctx, `INSERT INTO state_volume_attachment
		(state_volume_id, attachment_plan_id, workspace_id, container_id, source_generation_id, initialize,
		 attachment_token, fencing_token, expires_at)
		VALUES ($1,$2::uuid,$3,$4,NULLIF($5, '')::uuid,$6,$7::uuid,$8,$9);`,
		disk.Id, planId, workspaceId, containerId, resolvedGenerationId, initialize,
		attachmentToken, fencingToken, expiresAt)
	if err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "23505" {
			return nil, fmt.Errorf("writable disk %q is already attached", name)
		}
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &types.StateVolumeAttachment{VolumeId: disk.ExternalId, Name: disk.Name, Size: disk.Size,
		MountPath: disk.MountPath, ContainerId: containerId, SourceGenerationId: resolvedGenerationId,
		Initialize: initialize, AttachmentToken: attachmentToken,
		FencingToken: fencingToken, ExpiresAt: expiresAt}, nil
}

func (r *PostgresBackendRepository) ResolveReadOnlyStateAttachment(
	ctx context.Context,
	workspaceId uint,
	containerId, volumeId, generationId, name, mountPath string,
	root bool,
) error {
	containerId, volumeId, generationId = strings.TrimSpace(containerId), strings.TrimSpace(volumeId), strings.TrimSpace(generationId)
	name, mountPath = strings.TrimSpace(name), filepath.Clean(strings.TrimSpace(mountPath))
	if workspaceId == 0 || containerId == "" {
		return fmt.Errorf("read-only state attachment requires workspace and container identities")
	}
	if parsed, err := uuid.Parse(volumeId); err != nil || parsed.String() != volumeId {
		return fmt.Errorf("read-only state volume id must be a canonical RFC4122 UUID")
	}
	if parsed, err := uuid.Parse(generationId); err != nil || parsed.String() != generationId {
		return fmt.Errorf("read-only state generation id must be a canonical RFC4122 UUID")
	}
	if name == "" || !filepath.IsAbs(mountPath) ||
		(root && (name != "root" || mountPath != "/")) || (!root && (name == "root" || mountPath == "/")) {
		return fmt.Errorf("read-only state attachment has an invalid member role")
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := insertReadOnlyStateAttachment(ctx, tx, workspaceId, containerId, volumeId,
		generationId, name, mountPath, root); err != nil {
		return err
	}
	return tx.Commit()
}

func insertReadOnlyStateAttachment(
	ctx context.Context,
	tx *sqlx.Tx,
	workspaceId uint,
	containerId, volumeId, generationId, name, mountPath string,
	root bool,
) error {
	var id uint
	err := tx.GetContext(ctx, &id, `INSERT INTO state_read_only_attachment
		(workspace_id, container_id, volume_id, source_generation_id, name, mount_path, is_root)
		VALUES ($1,$2,$3::uuid,$4::uuid,$5,$6,$7)
		ON CONFLICT (workspace_id, container_id, volume_id) DO UPDATE SET container_id = EXCLUDED.container_id
		WHERE state_read_only_attachment.source_generation_id = EXCLUDED.source_generation_id
		  AND state_read_only_attachment.name = EXCLUDED.name
		  AND state_read_only_attachment.mount_path = EXCLUDED.mount_path
		  AND state_read_only_attachment.is_root = EXCLUDED.is_root
		RETURNING id;`, workspaceId, containerId, volumeId, generationId, name, mountPath, root)
	if err == sql.ErrNoRows {
		return fmt.Errorf("read-only state attachment replay conflicts with its immutable source or member role")
	}
	return err
}

func (r *PostgresBackendRepository) ResolveBranchStateAttachment(
	ctx context.Context,
	workspaceId uint,
	stubExternalId, containerId, planId, requestHash, volumeId, name, size, mountPath, sourceGenerationId string,
	root, cloneSource bool,
) (*types.StateVolumeAttachment, error) {
	stubExternalId, containerId, volumeId = strings.TrimSpace(stubExternalId), strings.TrimSpace(containerId), strings.TrimSpace(volumeId)
	name, mountPath = strings.TrimSpace(name), filepath.Clean(strings.TrimSpace(mountPath))
	if stubExternalId == "" || containerId == "" {
		return nil, fmt.Errorf("branch-state stub and container identities are required")
	}
	if name == "" || !filepath.IsAbs(mountPath) ||
		(root && (name != "root" || mountPath != "/")) || (!root && (name == "root" || mountPath == "/")) {
		return nil, fmt.Errorf("branch-state member name and mount path are invalid")
	}
	if parsed, err := uuid.Parse(volumeId); err != nil || parsed.String() != strings.ToLower(volumeId) {
		return nil, fmt.Errorf("branch-state volume id must be a canonical RFC4122 UUID")
	}
	quantity, err := resource.ParseQuantity(strings.TrimSpace(size))
	if err != nil || quantity.Sign() <= 0 {
		return nil, fmt.Errorf("branch-state size must be a positive resource quantity")
	}
	size = quantity.String()
	sourceGenerationId = strings.TrimSpace(sourceGenerationId)
	if sourceGenerationId != "" {
		if parsed, err := uuid.Parse(sourceGenerationId); err != nil || parsed.String() != strings.ToLower(sourceGenerationId) {
			return nil, fmt.Errorf("branch-state source generation id must be a canonical RFC4122 UUID")
		}
	}
	if cloneSource && sourceGenerationId == "" {
		return nil, fmt.Errorf("branch-state clone requires an exact source generation")
	}

	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := lockActiveStateVolumeAttachmentPlan(ctx, tx, workspaceId, containerId, planId, requestHash); err != nil {
		return nil, err
	}
	var lineage struct {
		Id                  uint   `db:"id"`
		VolumeId            string `db:"volume_id"`
		Size                string `db:"size"`
		CurrentGenerationId string `db:"current_generation_id"`
	}
	err = tx.GetContext(ctx, &lineage, `INSERT INTO state_branch_lineage
		(workspace_id, stub_external_id, member_name, mount_path, is_root, volume_id, size)
		VALUES ($1,$2,$3,$4,$5,$6::uuid,$7)
		ON CONFLICT (workspace_id, stub_external_id, member_name) DO UPDATE SET member_name = EXCLUDED.member_name
		WHERE state_branch_lineage.volume_id = EXCLUDED.volume_id
		  AND state_branch_lineage.size = EXCLUDED.size
		  AND state_branch_lineage.mount_path = EXCLUDED.mount_path
		  AND state_branch_lineage.is_root = EXCLUDED.is_root
		RETURNING id, volume_id::text AS volume_id, size,
			COALESCE(current_generation_id::text, '') AS current_generation_id;`,
		workspaceId, stubExternalId, name, mountPath, root, volumeId, size)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("branch-state lineage conflicts with immutable volume identity or geometry")
	}
	if err != nil {
		return nil, err
	}

	var existing struct {
		AttachmentPlanId   string    `db:"attachment_plan_id"`
		ContainerId        string    `db:"container_id"`
		SourceGenerationId string    `db:"source_generation_id"`
		Initialize         bool      `db:"initialize"`
		CloneSource        bool      `db:"clone_source"`
		AttachmentToken    string    `db:"attachment_token"`
		FencingToken       int64     `db:"fencing_token"`
		ExpiresAt          time.Time `db:"expires_at"`
	}
	err = tx.GetContext(ctx, &existing, `SELECT container_id, attachment_plan_id::text AS attachment_plan_id,
		COALESCE(source_generation_id::text, '') AS source_generation_id, initialize, clone_source,
		attachment_token::text AS attachment_token, fencing_token, expires_at
		FROM state_branch_attachment WHERE lineage_id = $1 FOR UPDATE;`, lineage.Id)
	if err == nil {
		if existing.ContainerId != containerId {
			return nil, fmt.Errorf("branch-state lineage is already attached by container %q", existing.ContainerId)
		}
		if !existing.ExpiresAt.After(time.Now()) {
			return nil, fmt.Errorf("branch-state lineage has a stale writer attachment; authoritative teardown is required")
		}
		if existing.AttachmentPlanId != planId || existing.SourceGenerationId != sourceGenerationId || existing.CloneSource != cloneSource {
			return nil, fmt.Errorf("branch-state attachment replay does not match immutable lineage inputs")
		}
		expiresAt := time.Now().Add(stateVolumeAttachmentLease)
		if _, err := tx.ExecContext(ctx, `UPDATE state_branch_attachment SET expires_at = $2, updated_at = CURRENT_TIMESTAMP
			WHERE attachment_token = $1::uuid;`, existing.AttachmentToken, expiresAt); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &types.StateVolumeAttachment{VolumeId: lineage.VolumeId, Name: name, Size: lineage.Size,
			MountPath: mountPath, ContainerId: containerId, SourceGenerationId: existing.SourceGenerationId,
			Initialize: existing.Initialize, CloneSource: existing.CloneSource, AttachmentToken: existing.AttachmentToken,
			FencingToken: existing.FencingToken, ExpiresAt: expiresAt, Replayed: true}, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}

	initialize := sourceGenerationId == ""
	if initialize {
		if lineage.CurrentGenerationId != "" {
			return nil, fmt.Errorf("branch-state lineage already has a committed head; an exact state snapshot is required")
		}
	} else if cloneSource {
		if lineage.CurrentGenerationId != "" {
			return nil, fmt.Errorf("branch-state fork destination already has a committed head")
		}
		var sources int
		if err := tx.GetContext(ctx, &sources, `SELECT count(*) FROM volume_generation
			WHERE external_id = $1::uuid AND workspace_id = $2 AND volume_id <> $3 AND status = 'available';`,
			sourceGenerationId, workspaceId, lineage.VolumeId); err != nil {
			return nil, err
		}
		if sources != 1 {
			return nil, fmt.Errorf("branch-state clone source is unavailable or belongs to the destination lineage")
		}
	} else if sourceGenerationId != lineage.CurrentGenerationId {
		return nil, fmt.Errorf("branch-state resume source is not the current lineage head")
	}

	var fencingToken int64
	if err := tx.GetContext(ctx, &fencingToken, `UPDATE state_branch_lineage SET
		next_fencing_token = next_fencing_token + 1, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1 RETURNING next_fencing_token;`, lineage.Id); err != nil {
		return nil, err
	}
	attachmentToken := uuid.NewString()
	expiresAt := time.Now().Add(stateVolumeAttachmentLease)
	if _, err := tx.ExecContext(ctx, `INSERT INTO state_branch_attachment
		(lineage_id, attachment_plan_id, workspace_id, container_id, source_generation_id, initialize, clone_source,
		 attachment_token, fencing_token, expires_at)
		VALUES ($1,$2::uuid,$3,$4,NULLIF($5, '')::uuid,$6,$7,$8::uuid,$9,$10);`,
		lineage.Id, planId, workspaceId, containerId, sourceGenerationId, initialize, cloneSource,
		attachmentToken, fencingToken, expiresAt); err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "23505" {
			return nil, fmt.Errorf("branch-state lineage is already attached")
		}
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &types.StateVolumeAttachment{VolumeId: lineage.VolumeId, Name: name, Size: lineage.Size,
		MountPath: mountPath, ContainerId: containerId, SourceGenerationId: sourceGenerationId,
		Initialize: initialize, CloneSource: cloneSource, AttachmentToken: attachmentToken, FencingToken: fencingToken,
		ExpiresAt: expiresAt}, nil
}

func (r *PostgresBackendRepository) RenewStateVolumeAttachments(ctx context.Context, workspaceId uint, containerId, workerId, workerInstanceId, storageNodeId string, leases []types.StateVolumeLease) (time.Time, error) {
	if strings.TrimSpace(containerId) == "" || strings.TrimSpace(workerId) == "" ||
		strings.TrimSpace(workerInstanceId) == "" || strings.TrimSpace(storageNodeId) == "" || len(leases) == 0 {
		return time.Time{}, fmt.Errorf("container, worker process, storage node, and state-volume leases are required")
	}
	tx, err := r.client.BeginTxx(ctx, nil)
	if err != nil {
		return time.Time{}, err
	}
	defer tx.Rollback()
	expiresAt := time.Now().Add(stateVolumeAttachmentLease)
	for _, lease := range leases {
		if _, err := uuid.Parse(lease.VolumeId); err != nil {
			return time.Time{}, fmt.Errorf("volume id must be an RFC4122 UUID")
		}
		if _, err := uuid.Parse(lease.AttachmentToken); err != nil || lease.FencingToken <= 0 {
			return time.Time{}, fmt.Errorf("attachment token and positive fencing token are required")
		}
		result, err := tx.ExecContext(ctx, `UPDATE state_volume_attachment SET expires_at = $4,
			owner_worker_id = CASE WHEN owner_worker_id = '' THEN $7 ELSE owner_worker_id END,
			owner_worker_instance_id = CASE WHEN owner_worker_instance_id = '' THEN $8 ELSE owner_worker_instance_id END,
			storage_node_id = CASE WHEN storage_node_id = '' THEN $9 ELSE storage_node_id END,
			updated_at = CURRENT_TIMESTAMP
			WHERE workspace_id = $1 AND container_id = $2 AND attachment_token = $3::uuid
			  AND fencing_token = $5 AND state_volume_id =
			      (SELECT id FROM state_volume WHERE external_id = $6::uuid AND workspace_id = $1
			         AND next_fencing_token = $5)
			  AND ((owner_worker_id = '' AND owner_worker_instance_id = '' AND storage_node_id = '') OR
			       (owner_worker_id = $7 AND owner_worker_instance_id = $8 AND storage_node_id = $9));`,
			workspaceId, containerId, lease.AttachmentToken, expiresAt, lease.FencingToken, lease.VolumeId,
			workerId, workerInstanceId, storageNodeId)
		if err != nil {
			return time.Time{}, err
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			result, err = tx.ExecContext(ctx, `UPDATE state_branch_attachment a
				SET expires_at = $4,
				    owner_worker_id = CASE WHEN a.owner_worker_id = '' THEN $7 ELSE a.owner_worker_id END,
				    owner_worker_instance_id = CASE WHEN a.owner_worker_instance_id = '' THEN $8 ELSE a.owner_worker_instance_id END,
				    storage_node_id = CASE WHEN a.storage_node_id = '' THEN $9 ELSE a.storage_node_id END,
				    updated_at = CURRENT_TIMESTAMP
				FROM state_branch_lineage l
				WHERE a.lineage_id = l.id AND a.workspace_id = $1 AND a.container_id = $2
				  AND a.attachment_token = $3::uuid AND a.fencing_token = $5
				  AND l.volume_id = $6::uuid AND l.workspace_id = $1
				  AND l.next_fencing_token = $5
				  AND ((a.owner_worker_id = '' AND a.owner_worker_instance_id = '' AND a.storage_node_id = '') OR
				       (a.owner_worker_id = $7 AND a.owner_worker_instance_id = $8 AND a.storage_node_id = $9));`,
				workspaceId, containerId, lease.AttachmentToken, expiresAt, lease.FencingToken, lease.VolumeId,
				workerId, workerInstanceId, storageNodeId)
			if err != nil {
				return time.Time{}, err
			}
			rows, _ = result.RowsAffected()
		}
		if rows != 1 {
			return time.Time{}, fmt.Errorf("state-volume attachment lease is missing or fenced")
		}
	}
	if err := tx.Commit(); err != nil {
		return time.Time{}, err
	}
	return expiresAt, nil
}

func (r *PostgresBackendRepository) ReleaseStateVolumeAttachments(ctx context.Context, workspaceId uint, containerId, workerId, workerInstanceId, storageNodeId string, leases []types.StateVolumeLease) error {
	if strings.TrimSpace(containerId) == "" || strings.TrimSpace(workerId) == "" ||
		strings.TrimSpace(workerInstanceId) == "" || strings.TrimSpace(storageNodeId) == "" || len(leases) == 0 {
		return fmt.Errorf("container, worker process, storage node, and state-volume leases are required")
	}
	tx, err := r.client.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	releaseIntent, err := lockStateVolumeReleaseIntentForSource(ctx, tx, workspaceId, containerId,
		workerId, workerInstanceId, storageNodeId, leases)
	if err != nil {
		return err
	}
	if releaseIntent.Completed {
		return tx.Commit()
	}
	for _, lease := range leases {
		if _, err := uuid.Parse(lease.VolumeId); err != nil {
			return fmt.Errorf("volume id must be an RFC4122 UUID")
		}
		if _, err := uuid.Parse(lease.AttachmentToken); err != nil || lease.FencingToken <= 0 {
			return fmt.Errorf("attachment token and positive fencing token are required")
		}
		var pending int
		if err := tx.GetContext(ctx, &pending, `SELECT count(*)
			FROM state_snapshot_member_plan p JOIN state_snapshot s ON s.id = p.state_snapshot_id
			WHERE s.workspace_id = $1 AND s.source_container_id = $2 AND s.status = 'pending'
			  AND p.volume_id = $3::uuid AND p.attachment_token = $4::uuid AND p.fencing_token = $5;`,
			workspaceId, containerId, lease.VolumeId, lease.AttachmentToken, lease.FencingToken); err != nil {
			return err
		}
		if pending != 0 {
			return fmt.Errorf("state-volume attachment is held by a pending snapshot operation")
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM state_volume_attachment
			WHERE workspace_id = $1 AND container_id = $2 AND attachment_token = $3::uuid
			  AND fencing_token = $4 AND state_volume_id =
			      (SELECT id FROM state_volume WHERE external_id = $5::uuid AND workspace_id = $1)
			  AND owner_worker_id = $6 AND owner_worker_instance_id = $7 AND storage_node_id = $8;`,
			workspaceId, containerId, lease.AttachmentToken, lease.FencingToken, lease.VolumeId,
			workerId, workerInstanceId, storageNodeId)
		if err != nil {
			return err
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			result, err = tx.ExecContext(ctx, `DELETE FROM state_branch_attachment a USING state_branch_lineage l
				WHERE a.lineage_id = l.id AND a.workspace_id = $1 AND a.container_id = $2
				  AND a.attachment_token = $3::uuid AND a.fencing_token = $4
				  AND l.volume_id = $5::uuid AND l.workspace_id = $1
				  AND a.owner_worker_id = $6 AND a.owner_worker_instance_id = $7 AND a.storage_node_id = $8;`,
				workspaceId, containerId, lease.AttachmentToken, lease.FencingToken, lease.VolumeId,
				workerId, workerInstanceId, storageNodeId)
			if err != nil {
				return err
			}
			rows, _ = result.RowsAffected()
		}
		if rows == 0 {
			var remaining int
			if err := tx.GetContext(ctx, &remaining, `SELECT
				(SELECT count(*) FROM state_volume_attachment a JOIN state_volume v ON v.id = a.state_volume_id
				 WHERE a.workspace_id = $1 AND a.container_id = $2 AND v.external_id = $3::uuid) +
				(SELECT count(*) FROM state_branch_attachment a JOIN state_branch_lineage l ON l.id = a.lineage_id
				 WHERE a.workspace_id = $1 AND a.container_id = $2 AND l.volume_id = $3::uuid);`,
				workspaceId, containerId, lease.VolumeId); err != nil {
				return err
			}
			if remaining != 0 {
				return fmt.Errorf("state-volume attachment release tuple is stale or fenced")
			}
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
	if remaining == 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM state_volume_attachment_plan
			WHERE workspace_id = $1 AND container_id = $2;`, workspaceId, containerId); err != nil {
			return err
		}
	}
	if err := completeSourceStateVolumeReleaseIntent(ctx, tx, releaseIntent.ExternalId); err != nil {
		return err
	}
	return tx.Commit()
}

// ReleasePendingStateVolumeAttachments is reserved for the scheduler path
// that atomically proved a container stopped before worker assignment. No QSD
// could have attached these volumes, so releasing every lease for the scoped
// workspace/container cannot race a live writer.
func (r *PostgresBackendRepository) AbortStateVolumeAttachmentPlan(ctx context.Context, workspaceId uint, containerId, planId, requestHash string) error {
	return r.releaseStateVolumeAttachmentPlan(ctx, workspaceId, containerId, planId, requestHash, true)
}

func (r *PostgresBackendRepository) ReleasePendingStateVolumeAttachments(ctx context.Context, workspaceId uint, containerId, planId, requestHash string) error {
	return r.releaseStateVolumeAttachmentPlan(ctx, workspaceId, containerId, planId, requestHash, false)
}

func (r *PostgresBackendRepository) releaseStateVolumeAttachmentPlan(ctx context.Context, workspaceId uint, containerId, planId, requestHash string, requireUnadmitted bool) error {
	if strings.TrimSpace(containerId) == "" {
		return fmt.Errorf("container id is required")
	}
	if err := validateStateVolumeAttachmentPlanInput(workspaceId, containerId, requestHash, 1); err != nil {
		return err
	}
	if parsed, err := uuid.Parse(planId); err != nil || parsed.String() != planId {
		return fmt.Errorf("state-volume attachment plan id must be a canonical RFC4122 UUID")
	}
	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var plan struct {
		RequestHash string `db:"request_hash"`
		Admitted    bool   `db:"admitted"`
		Aborted     bool   `db:"aborted"`
	}
	planQuery := `SELECT request_hash, admitted_at IS NOT NULL AS admitted, aborted_at IS NOT NULL AS aborted
		FROM state_volume_attachment_plan
		WHERE workspace_id = $1 AND container_id = $2 AND plan_id = $3::uuid AND request_hash = $4 FOR UPDATE;`
	planErr := tx.GetContext(ctx, &plan, planQuery, workspaceId, containerId, planId, requestHash)
	if planErr != nil && planErr != sql.ErrNoRows {
		return planErr
	}
	if planErr == nil && plan.RequestHash == "" {
		return fmt.Errorf("state-volume attachment plan has no immutable request hash")
	}
	if planErr == nil && requireUnadmitted && plan.Admitted && !plan.Aborted {
		return fmt.Errorf("admitted state-volume attachment plan cannot be aborted without authoritative pending-container teardown")
	}
	var attachments int
	if err := tx.GetContext(ctx, &attachments, `SELECT
		(SELECT count(*) FROM state_volume_attachment WHERE workspace_id = $1 AND container_id = $2
		 AND attachment_plan_id = $3::uuid) +
		(SELECT count(*) FROM state_branch_attachment WHERE workspace_id = $1 AND container_id = $2
		 AND attachment_plan_id = $3::uuid);`, workspaceId, containerId, planId); err != nil {
		return err
	}
	if planErr == sql.ErrNoRows && attachments != 0 {
		return fmt.Errorf("unassigned state-volume attachments do not match the exact durable scheduler plan")
	}
	if planErr == sql.ErrNoRows {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM state_volume_attachment
		WHERE workspace_id = $1 AND container_id = $2 AND attachment_plan_id = $3::uuid;`, workspaceId, containerId, planId); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM state_branch_attachment
		WHERE workspace_id = $1 AND container_id = $2 AND attachment_plan_id = $3::uuid;`, workspaceId, containerId, planId); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM state_read_only_attachment
		WHERE workspace_id = $1 AND container_id = $2;`, workspaceId, containerId); err != nil {
		return err
	}
	reason := "scheduler aborted before Redis admission"
	if !requireUnadmitted {
		reason = "scheduler authoritatively stopped pending unassigned container"
	}
	result, err := tx.ExecContext(ctx, `UPDATE state_volume_attachment_plan
		SET aborted_at = COALESCE(aborted_at, CURRENT_TIMESTAMP),
		    abort_reason = CASE WHEN abort_reason = '' THEN $5 ELSE abort_reason END
		WHERE workspace_id = $1 AND container_id = $2 AND plan_id = $3::uuid AND request_hash = $4;`,
		workspaceId, containerId, planId, requestHash, reason)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("state-volume attachment plan abort lost its exact durable tombstone")
	}
	return tx.Commit()
}

func (r *PostgresBackendRepository) DeleteDisk(ctx context.Context, workspaceId uint, name string) error {
	name, err := normalizedDiskName(name)
	if err != nil {
		return err
	}

	tx, err := r.client.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var diskID uint
	if err := tx.GetContext(ctx, &diskID, `SELECT id FROM state_volume
		WHERE workspace_id = $1 AND name = $2 AND deleted_at IS NULL FOR UPDATE;`, workspaceId, name); err != nil {
		return err
	}
	var active int
	if err := tx.GetContext(ctx, &active, `SELECT count(*) FROM state_volume_attachment
		WHERE state_volume_id = $1;`, diskID); err != nil {
		return err
	}
	if active != 0 {
		return fmt.Errorf("disk %q has active attachments", name)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE state_volume SET deleted_at = CURRENT_TIMESTAMP
		WHERE id = $1;`, diskID); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *PostgresBackendRepository) ListDisksWithRelated(ctx context.Context, workspaceId uint) ([]types.DiskWithRelated, error) {
	query := `
		SELECT d.id, d.external_id::text AS external_id, d.workspace_id, d.name, d.size,
		       d.mount_path, d.created_at, d.updated_at, d.deleted_at,
		       w.external_id::text AS "workspace.external_id", w.name AS "workspace.name"
		FROM state_volume d
		JOIN workspace w ON d.workspace_id = w.id
		WHERE d.workspace_id = $1 AND d.deleted_at IS NULL
		ORDER BY d.created_at DESC;`

	var disks []types.DiskWithRelated
	if err := r.client.SelectContext(ctx, &disks, query, workspaceId); err != nil {
		return nil, err
	}
	return disks, nil
}

func normalizedDiskName(name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", fmt.Errorf("disk name is required")
	}
	return types.SafeDurableDiskName(name), nil
}
