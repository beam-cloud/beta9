package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddCheckpointCompatibilityKey, downAddCheckpointCompatibilityKey)
}

func upAddCheckpointCompatibilityKey(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE checkpoint ADD COLUMN IF NOT EXISTS compatibility_key TEXT NOT NULL DEFAULT '';`)
	return err
}

func downAddCheckpointCompatibilityKey(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE checkpoint DROP COLUMN IF EXISTS compatibility_key;`)
	return err
}
