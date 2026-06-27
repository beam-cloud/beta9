package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddDiskSnapshot, downAddDiskSnapshot)
}

func upAddDiskSnapshot(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS disk_snapshot (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			stub_id INT REFERENCES stub(id) ON DELETE SET NULL,
			disk_name TEXT NOT NULL,
			format TEXT NOT NULL DEFAULT 'block.v1',
			status TEXT NOT NULL DEFAULT 'pending',
			reason TEXT NOT NULL DEFAULT '',
			parent_snapshot_id TEXT NOT NULL DEFAULT '',
			generation BIGINT NOT NULL DEFAULT 0,
			size_bytes BIGINT NOT NULL DEFAULT 0,
			filesystem TEXT NOT NULL DEFAULT '',
			driver TEXT NOT NULL DEFAULT '',
			manifest_key TEXT NOT NULL DEFAULT '',
			manifest_digest TEXT NOT NULL DEFAULT '',
			manifest_size_bytes BIGINT NOT NULL DEFAULT 0,
			chunk_count BIGINT NOT NULL DEFAULT 0,
			logical_size_bytes BIGINT NOT NULL DEFAULT 0,
			stored_size_bytes BIGINT NOT NULL DEFAULT 0,
			object_prefix TEXT NOT NULL DEFAULT '',
			source_pool TEXT NOT NULL DEFAULT '',
			source_worker_id TEXT NOT NULL DEFAULT '',
			source_storage_node_id TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			completed_at TIMESTAMP WITH TIME ZONE,
			deleted_at TIMESTAMP WITH TIME ZONE
		);`,
		`CREATE INDEX IF NOT EXISTS idx_disk_snapshot_workspace_disk_created ON disk_snapshot(workspace_id, disk_name, created_at DESC) WHERE deleted_at IS NULL;`,
		`CREATE INDEX IF NOT EXISTS idx_disk_snapshot_workspace_disk_available ON disk_snapshot(workspace_id, disk_name, generation DESC, created_at DESC) WHERE deleted_at IS NULL AND status = 'available';`,
		`CREATE INDEX IF NOT EXISTS idx_disk_snapshot_stub ON disk_snapshot(stub_id) WHERE deleted_at IS NULL;`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func downAddDiskSnapshot(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`DROP INDEX IF EXISTS idx_disk_snapshot_stub;`,
		`DROP INDEX IF EXISTS idx_disk_snapshot_workspace_disk_available;`,
		`DROP INDEX IF EXISTS idx_disk_snapshot_workspace_disk_created;`,
		`DROP TABLE IF EXISTS disk_snapshot;`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
