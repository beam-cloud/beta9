package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddPublicDiskSnapshots, downAddPublicDiskSnapshots)
}

func upAddPublicDiskSnapshots(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `ALTER TABLE disk_snapshot ADD COLUMN IF NOT EXISTS public BOOLEAN NOT NULL DEFAULT FALSE;`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_disk_snapshot_public_external_id ON disk_snapshot(external_id) WHERE public = TRUE AND deleted_at IS NULL;`)
	return err
}

func downAddPublicDiskSnapshots(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `DROP INDEX IF EXISTS idx_disk_snapshot_public_external_id;`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `ALTER TABLE disk_snapshot DROP COLUMN IF EXISTS public;`)
	return err
}
