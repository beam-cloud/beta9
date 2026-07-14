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
	// Avoid queueing production task traffic behind an ALTER that cannot take
	// its metadata lock immediately. The gateway will retry the migration on
	// its next startup while existing replicas continue serving.
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '2s';`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `ALTER TABLE task ADD COLUMN IF NOT EXISTS failure_reason TEXT NOT NULL DEFAULT '';`)
	return err
}

func downAddTaskFailureReason(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '2s';`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `ALTER TABLE task DROP COLUMN IF EXISTS failure_reason;`)
	return err
}
