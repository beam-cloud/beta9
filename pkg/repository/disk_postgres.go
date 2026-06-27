package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/beam-cloud/beta9/pkg/types"
)

const defaultDiskSnapshotListLimit uint64 = 100

func diskSnapshotColumns(alias string) string {
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}

	return strings.Join([]string{
		prefix + "id",
		prefix + "external_id::text AS external_id",
		prefix + "workspace_id",
		"COALESCE(" + prefix + "stub_id, 0) AS stub_id",
		prefix + "disk_name",
		prefix + "format",
		prefix + "status",
		prefix + "reason",
		prefix + "parent_snapshot_id",
		prefix + "generation",
		prefix + "size_bytes",
		prefix + "filesystem",
		prefix + "driver",
		prefix + "manifest_key",
		prefix + "manifest_digest",
		prefix + "manifest_size_bytes",
		prefix + "chunk_count",
		prefix + "logical_size_bytes",
		prefix + "stored_size_bytes",
		prefix + "bucket_name",
		prefix + "object_prefix",
		prefix + "source_pool",
		prefix + "source_worker_id",
		prefix + "source_storage_node_id",
		prefix + "created_at",
		prefix + "updated_at",
		prefix + "completed_at",
		prefix + "deleted_at",
	}, ", ")
}

func (r *PostgresBackendRepository) CreateDiskSnapshot(ctx context.Context, snapshot *types.DiskSnapshot) (*types.DiskSnapshot, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("disk snapshot is nil")
	}

	if snapshot.Format == "" {
		snapshot.Format = types.DiskSnapshotFormatBlockV1
	}
	if snapshot.Status == "" {
		snapshot.Status = types.DiskSnapshotStatusPending
	}

	query := fmt.Sprintf(`
		INSERT INTO disk_snapshot (
			workspace_id, stub_id, disk_name, format, status, reason, parent_snapshot_id,
			generation, size_bytes, filesystem, driver, manifest_key, manifest_digest,
			manifest_size_bytes, chunk_count, logical_size_bytes, stored_size_bytes,
			bucket_name, object_prefix, source_pool, source_worker_id, source_storage_node_id
		) VALUES (
			$1, NULLIF($2, 0), $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			$13, $14, $15, $16, $17, $18, $19, $20, $21, $22
		)
		RETURNING %s;`, diskSnapshotColumns(""))

	var created types.DiskSnapshot
	if err := r.client.GetContext(ctx, &created, query,
		snapshot.WorkspaceId,
		snapshot.StubId,
		snapshot.DiskName,
		snapshot.Format,
		snapshot.Status,
		snapshot.Reason,
		snapshot.ParentSnapshotId,
		snapshot.Generation,
		snapshot.SizeBytes,
		snapshot.Filesystem,
		snapshot.Driver,
		snapshot.ManifestKey,
		snapshot.ManifestDigest,
		snapshot.ManifestSizeBytes,
		snapshot.ChunkCount,
		snapshot.LogicalSizeBytes,
		snapshot.StoredSizeBytes,
		snapshot.BucketName,
		snapshot.ObjectPrefix,
		snapshot.SourcePool,
		snapshot.SourceWorkerId,
		snapshot.SourceStorageNodeId,
	); err != nil {
		return nil, err
	}

	return &created, nil
}

func (r *PostgresBackendRepository) UpdateDiskSnapshot(ctx context.Context, snapshot *types.DiskSnapshot) (*types.DiskSnapshot, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("disk snapshot is nil")
	}
	if snapshot.ExternalId == "" {
		return nil, fmt.Errorf("disk snapshot external id is required")
	}

	builder := squirrel.Update("disk_snapshot").
		Set("updated_at", squirrel.Expr("CURRENT_TIMESTAMP")).
		Where(squirrel.Eq{"external_id": snapshot.ExternalId}).
		Where(squirrel.Eq{"deleted_at": nil}).
		Suffix(fmt.Sprintf("RETURNING %s", diskSnapshotColumns(""))).
		PlaceholderFormat(squirrel.Dollar)

	if snapshot.Status != "" {
		builder = builder.Set("status", snapshot.Status)
		if diskSnapshotStatusTerminal(snapshot.Status) && !snapshot.CompletedAt.Valid {
			builder = builder.Set("completed_at", squirrel.Expr("COALESCE(completed_at, CURRENT_TIMESTAMP)"))
		}
	}
	if snapshot.Reason != "" {
		builder = builder.Set("reason", snapshot.Reason)
	}
	if snapshot.ParentSnapshotId != "" {
		builder = builder.Set("parent_snapshot_id", snapshot.ParentSnapshotId)
	}
	if snapshot.ManifestKey != "" {
		builder = builder.Set("manifest_key", snapshot.ManifestKey)
	}
	if snapshot.ManifestDigest != "" {
		builder = builder.Set("manifest_digest", snapshot.ManifestDigest)
	}
	if snapshot.ManifestSizeBytes > 0 {
		builder = builder.Set("manifest_size_bytes", snapshot.ManifestSizeBytes)
	}
	if snapshot.ChunkCount > 0 {
		builder = builder.Set("chunk_count", snapshot.ChunkCount)
	}
	if snapshot.LogicalSizeBytes > 0 {
		builder = builder.Set("logical_size_bytes", snapshot.LogicalSizeBytes)
	}
	if snapshot.StoredSizeBytes > 0 {
		builder = builder.Set("stored_size_bytes", snapshot.StoredSizeBytes)
	}
	if snapshot.BucketName != "" {
		builder = builder.Set("bucket_name", snapshot.BucketName)
	}
	if snapshot.ObjectPrefix != "" {
		builder = builder.Set("object_prefix", snapshot.ObjectPrefix)
	}
	if snapshot.CompletedAt.Valid {
		builder = builder.Set("completed_at", snapshot.CompletedAt.Time)
	}

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	var updated types.DiskSnapshot
	if err := r.client.GetContext(ctx, &updated, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return nil, &types.ErrDiskSnapshotNotFound{SnapshotId: snapshot.ExternalId}
		}
		return nil, err
	}
	return &updated, nil
}

func (r *PostgresBackendRepository) GetDiskSnapshot(ctx context.Context, workspaceId uint, snapshotId string) (*types.DiskSnapshot, error) {
	query := fmt.Sprintf(`
		SELECT %s
		FROM disk_snapshot
		WHERE workspace_id = $1 AND external_id = $2 AND deleted_at IS NULL
		LIMIT 1;`, diskSnapshotColumns(""))

	var snapshot types.DiskSnapshot
	if err := r.client.GetContext(ctx, &snapshot, query, workspaceId, snapshotId); err != nil {
		if err == sql.ErrNoRows {
			return nil, &types.ErrDiskSnapshotNotFound{SnapshotId: snapshotId}
		}
		return nil, err
	}
	return &snapshot, nil
}

func (r *PostgresBackendRepository) GetLatestDiskSnapshot(ctx context.Context, workspaceId uint, diskName string) (*types.DiskSnapshot, error) {
	query := fmt.Sprintf(`
		SELECT %s
		FROM disk_snapshot
		WHERE workspace_id = $1
		  AND disk_name = $2
		  AND status = $3
		  AND deleted_at IS NULL
		ORDER BY generation DESC, created_at DESC
		LIMIT 1;`, diskSnapshotColumns(""))

	var snapshot types.DiskSnapshot
	if err := r.client.GetContext(ctx, &snapshot, query, workspaceId, diskName, types.DiskSnapshotStatusAvailable); err != nil {
		if err == sql.ErrNoRows {
			return nil, &types.ErrDiskSnapshotNotFound{SnapshotId: fmt.Sprintf("latest:%d:%s", workspaceId, diskName)}
		}
		return nil, err
	}
	return &snapshot, nil
}

func (r *PostgresBackendRepository) ListDiskSnapshots(ctx context.Context, filter types.DiskSnapshotFilter) ([]types.DiskSnapshot, error) {
	builder := squirrel.Select(diskSnapshotColumns("ds")).
		From("disk_snapshot ds").
		Where("ds.deleted_at IS NULL").
		OrderBy("ds.created_at DESC").
		PlaceholderFormat(squirrel.Dollar)

	if filter.WorkspaceId > 0 {
		builder = builder.Where(squirrel.Eq{"ds.workspace_id": filter.WorkspaceId})
	}
	if filter.WorkspaceExternalId != "" {
		builder = builder.Join("workspace w ON ds.workspace_id = w.id").
			Where(squirrel.Eq{"w.external_id": filter.WorkspaceExternalId})
	}
	if filter.DiskName != "" {
		builder = builder.Where(squirrel.Eq{"ds.disk_name": filter.DiskName})
	}
	if len(filter.Statuses) > 0 {
		statuses := make([]string, 0, len(filter.Statuses))
		for _, status := range filter.Statuses {
			if status != "" {
				statuses = append(statuses, string(status))
			}
		}
		if len(statuses) > 0 {
			builder = builder.Where(squirrel.Eq{"ds.status": statuses})
		}
	}
	limit := filter.Limit
	if limit == 0 {
		limit = defaultDiskSnapshotListLimit
	}
	builder = builder.Limit(limit)

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	var snapshots []types.DiskSnapshot
	if err := r.client.SelectContext(ctx, &snapshots, query, args...); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func diskSnapshotStatusTerminal(status types.DiskSnapshotStatus) bool {
	return status == types.DiskSnapshotStatusAvailable || status == types.DiskSnapshotStatusFailed
}
