package backend_postgres_migrations

import (
	"context"
	"database/sql"
	"fmt"

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
	indexes := []struct{ name, definition string }{
		{"idx_stub_workspace_created_id", "ON stub (workspace_id, created_at DESC, id DESC)"},
		{"idx_stub_workspace_type_created_id", "ON stub (workspace_id, type, created_at DESC, id DESC)"},
		{"idx_task_workspace_status_created_id", "ON task (workspace_id, status, created_at DESC, id DESC)"},
		{"idx_task_stub_status_created_id", "ON task (stub_id, status, created_at DESC, id DESC)"},
	}

	for _, index := range indexes {
		if err := createIndexConcurrently(ctx, db, index.name, index.definition); err != nil {
			return err
		}
	}
	return nil
}

// createIndexConcurrently builds the index outside a transaction. An
// interrupted CREATE INDEX CONCURRENTLY leaves an INVALID index behind, which
// IF NOT EXISTS would then keep forever; drop such a leftover first so a retry
// rebuilds it.
func createIndexConcurrently(ctx context.Context, db *sql.DB, name, definition string) error {
	var invalid bool
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE((SELECT NOT i.indisvalid FROM pg_index i WHERE i.indexrelid = to_regclass($1)), false);
	`, name).Scan(&invalid)
	if err != nil {
		return fmt.Errorf("check index %s: %w", name, err)
	}
	if invalid {
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`DROP INDEX CONCURRENTLY IF EXISTS %s;`, name)); err != nil {
			return fmt.Errorf("drop invalid index %s: %w", name, err)
		}
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX CONCURRENTLY IF NOT EXISTS %s %s;`, name, definition)); err != nil {
		return fmt.Errorf("create index %s: %w", name, err)
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
