package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddWorkspaceStack, downAddWorkspaceStack)
}

// A stack is a dashboard board; spec is opaque JSON owned by the dashboard.
func upAddWorkspaceStack(tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS workspace_stack (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT REFERENCES workspace(id) ON DELETE CASCADE NOT NULL,
			name TEXT NOT NULL,
			spec JSONB NOT NULL DEFAULT '{}',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_workspace_stack_workspace_id ON workspace_stack(workspace_id);
	`)
	return err
}

func downAddWorkspaceStack(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE IF EXISTS workspace_stack;`)
	return err
}
