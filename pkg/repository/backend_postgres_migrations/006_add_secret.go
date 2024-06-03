package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upCreateSecretTable, downDropSecretTable)
}

func upCreateSecretTable(tx *sql.Tx) error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS workspace_secret (
		id SERIAL PRIMARY KEY,
		external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		name TEXT NOT NULL,
		value TEXT NOT NULL,
		workspace_id INT REFERENCES workspace(id) ON DELETE CASCADE NOT NULL,
		last_updated_by INT REFERENCES token(id) ON DELETE SET NULL
	);`)
	if err != nil {
		return err
	}

	// Add unique constraint on workspace_id and name
	_, err = tx.Exec(`ALTER TABLE workspace_secret ADD CONSTRAINT workspace_secret_workspace_id_name_unique UNIQUE (workspace_id, name);`)
	if err != nil {
		return err
	}

	return err
}

func downDropSecretTable(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE IF EXISTS workspace_secret;`)
	return err
}
