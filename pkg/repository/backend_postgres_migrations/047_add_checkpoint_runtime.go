package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddCheckpointRuntime, downAddCheckpointRuntime)
}

func upAddCheckpointRuntime(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE checkpoint ADD COLUMN IF NOT EXISTS runtime TEXT NOT NULL DEFAULT '';`)
	return err
}

func downAddCheckpointRuntime(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE checkpoint DROP COLUMN IF EXISTS runtime;`)
	return err
}
