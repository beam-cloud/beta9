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
	_, err := tx.ExecContext(ctx, `ALTER TABLE disk_snapshot ADD COLUMN IF NOT EXISTS public BOOLEAN NOT NULL DEFAULT FALSE;`)
	return err
}

func downAddPublicDiskSnapshots(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE disk_snapshot DROP COLUMN IF EXISTS public;`)
	return err
}
