package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(Up, Down)
}

func Up(tx *sql.Tx) error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS identity_identity (
		id SERIAL PRIMARY KEY,
		external_id VARCHAR(255) UNIQUE NOT NULL,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);`)
	if err != nil {
		return err
	}
	return nil
}

func Down(tx *sql.Tx) error {
	_, err := tx.Exec("DROP TABLE IF EXISTS identity_identity;")
	if err != nil {
		return err
	}
	return nil
}
