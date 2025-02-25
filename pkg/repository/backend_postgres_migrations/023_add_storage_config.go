package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddWorkspaceStorage, downDropWorkspaceStorage)
}

func upAddWorkspaceStorage(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE TABLE workspace_storage (
			id SERIAL PRIMARY KEY,
			bucket_name TEXT NOT NULL,
			access_key TEXT NOT NULL,
			secret_key TEXT NOT NULL,
			endpoint_url TEXT NOT NULL,
			region TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
	`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		ALTER TABLE workspace
		ADD COLUMN storage_id INTEGER NULL REFERENCES workspace_storage(id);
	`)

	return err
}

func downDropWorkspaceStorage(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		ALTER TABLE workspace
		DROP COLUMN storage_id;
	`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DROP TABLE workspace_storage;`)
	return err
}
