package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddCreateTaskWorkspaceIdIndex, downRemoveTaskWorkspaceIdIndex)
}

func upAddCreateTaskWorkspaceIdIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(
		ctx,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS task_workspace_id_idx ON task (workspace_id);`,
	)
	if err != nil {
		return err
	}

	return nil
}

func downRemoveTaskWorkspaceIdIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(
		ctx,
		`DROP INDEX CONCURRENTLY IF EXISTS task_workspace_id_idx;`,
	)
	if err != nil {
		return err
	}

	return nil
}
