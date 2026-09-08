package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upDropTaskExternalWorkspaceId, downDropTaskExternalWorkspaceId)
}

// The public-priced-stub feature let foreign workspaces invoke a stub and
// tracked the caller on task.external_workspace_id. The feature is gone, so
// the column and its dashboard index go with it.
func upDropTaskExternalWorkspaceId(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`DROP INDEX CONCURRENTLY IF EXISTS idx_task_external_workspace_created_id;`,
		`ALTER TABLE task DROP COLUMN IF EXISTS external_workspace_id;`,
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func downDropTaskExternalWorkspaceId(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`ALTER TABLE task ADD COLUMN IF NOT EXISTS external_workspace_id BIGINT REFERENCES workspace(id) ON DELETE SET NULL;`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_external_workspace_created_id ON task (external_workspace_id, created_at DESC, id DESC);`,
	}
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
