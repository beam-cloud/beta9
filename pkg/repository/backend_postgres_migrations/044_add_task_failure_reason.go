package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddTaskFailureReason, downAddTaskFailureReason)
}

func upAddTaskFailureReason(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE task ADD COLUMN IF NOT EXISTS failure_reason TEXT NOT NULL DEFAULT '';`)
	return err
}

func downAddTaskFailureReason(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE task DROP COLUMN IF EXISTS failure_reason;`)
	return err
}
