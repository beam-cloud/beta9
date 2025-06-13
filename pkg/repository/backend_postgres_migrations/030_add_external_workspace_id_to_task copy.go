package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddExternalWorkspaceIdToTask, downAddExternalWorkspaceIdToTask)
}

func upAddExternalWorkspaceIdToTask(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		ALTER TABLE task 
		ADD COLUMN external_workspace_id BIGINT REFERENCES workspace(id) ON DELETE SET NULL,
		ADD COLUMN app_id INT NULL REFERENCES app(id) ON DELETE SET NULL;
		`)

	return err
}

func downAddExternalWorkspaceIdToTask(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		ALTER TABLE task 
		DROP COLUMN IF EXISTS external_workspace_id,
		DROP COLUMN IF EXISTS app_id;
	`)
	return err
}
