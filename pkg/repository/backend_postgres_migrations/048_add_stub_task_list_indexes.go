package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddStubTaskListIndexes, downAddStubTaskListIndexes)
}

// The stub list (and therefore the sandbox list) filters by workspace and type
// and pages on (created_at, id); until now stub had no workspace-prefixed index
// so it walked every stub in the join. The task list adds a status filter on
// top of the workspace keyset, which the 040 index cannot serve once the
// workspace has many finished tasks.
func upAddStubTaskListIndexes(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stub_workspace_created_id ON stub (workspace_id, created_at DESC, id DESC);`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stub_workspace_type_created_id ON stub (workspace_id, type, created_at DESC, id DESC);`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_workspace_status_created_id ON task (workspace_id, status, created_at DESC, id DESC);`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_stub_status_created_id ON task (stub_id, status, created_at DESC, id DESC);`,
	}

	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func downAddStubTaskListIndexes(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`DROP INDEX CONCURRENTLY IF EXISTS idx_task_stub_status_created_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_task_workspace_status_created_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_stub_workspace_type_created_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_stub_workspace_created_id;`,
	}

	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
