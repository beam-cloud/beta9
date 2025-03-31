package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upCreateAppTable, downDropAppTable)
}

func upCreateAppTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE TABLE app (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() NOT NULL,
			name VARCHAR(255) NOT NULL,
			description TEXT NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id),
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
			deleted_at TIMESTAMP NULL,
			UNIQUE (name, workspace_id)
		);
	`)

	// Update stubs table to add app_id column
	_, err = tx.Exec(`
		ALTER TABLE stub
		ADD COLUMN app_id INT NULL REFERENCES app(id);
	`)
	if err != nil {
		return err
	}

	// Update deployments table to add app_id column
	_, err = tx.Exec(`
		ALTER TABLE deployment
		ADD COLUMN app_id INT NULL REFERENCES app(id);
	`)
	if err != nil {
		return err
	}

	return err
}

func downDropAppTable(ctx context.Context, tx *sql.Tx) error {
	// Remove app_id column from deployments table
	_, err := tx.Exec(`
		ALTER TABLE deployment
		DROP COLUMN app_id;
	`)
	if err != nil {
		return err
	}

	// Remove app_id column from stubs table
	_, err = tx.Exec(`
		ALTER TABLE stub
		DROP COLUMN app_id;
	`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DROP TABLE IF EXISTS app;`)
	return err
}
