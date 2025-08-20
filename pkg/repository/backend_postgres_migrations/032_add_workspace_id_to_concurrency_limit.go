package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddWorkspaceIdToConcurrencyLimit, downAddWorkspaceIdToConcurrencyLimit)
}

func upAddWorkspaceIdToConcurrencyLimit(tx *sql.Tx) error {
	// Add workspace_id column to concurrency_limit table
	_, err := tx.Exec(`ALTER TABLE concurrency_limit ADD COLUMN workspace_id INT REFERENCES workspace(id);`)
	if err != nil {
		return err
	}

	// Populate workspace_id using existing workspace.concurrency_limit_id relationships
	_, err = tx.Exec(`
		UPDATE concurrency_limit 
		SET workspace_id = w.id 
		FROM workspace w 
		WHERE w.concurrency_limit_id = concurrency_limit.id;
	`)
	if err != nil {
		return err
	}

	return nil
}

func downAddWorkspaceIdToConcurrencyLimit(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE concurrency_limit DROP COLUMN IF EXISTS workspace_id;`)
	return err
}