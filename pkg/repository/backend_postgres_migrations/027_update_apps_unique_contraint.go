package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upUpdateAppsUniqueConstraint, downUpdateAppsUniqueConstraint)
}

func upUpdateAppsUniqueConstraint(ctx context.Context, tx *sql.Tx) error {
	// Drop the existing unique constraint
	_, err := tx.ExecContext(ctx, `
		ALTER TABLE app
		DROP CONSTRAINT IF EXISTS app_name_workspace_id_key;
	`)
	if err != nil {
		return err
	}

	// Add the new unique constraint including deleted_at
	_, err = tx.ExecContext(ctx, `
		ALTER TABLE app
		ADD CONSTRAINT app_name_workspace_id_deleted_at_key UNIQUE (name, workspace_id, deleted_at);
	`)
	return err
}

func downUpdateAppsUniqueConstraint(ctx context.Context, tx *sql.Tx) error {
	// Drop the new unique constraint
	_, err := tx.ExecContext(ctx, `
		ALTER TABLE app
		DROP CONSTRAINT IF EXISTS app_name_workspace_id_deleted_at_key;
	`)
	if err != nil {
		return err
	}

	// Restore the original unique constraint
	_, err = tx.ExecContext(ctx, `
		ALTER TABLE app
		ADD CONSTRAINT app_name_workspace_id_key UNIQUE (name, workspace_id);
	`)
	return err
}
