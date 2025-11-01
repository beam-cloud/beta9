package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddImageCredentialFields, downAddImageCredentialFields)
}

func upAddImageCredentialFields(tx *sql.Tx) error {
	// Add columns for storing OCI credential secret references
	_, err := tx.Exec(`
		ALTER TABLE image
		ADD COLUMN IF NOT EXISTS credential_secret_name VARCHAR(255),
		ADD COLUMN IF NOT EXISTS credential_secret_id VARCHAR(36);
	`)
	if err != nil {
		return err
	}

	// Add index on credential_secret_name for faster lookups
	_, err = tx.Exec(`
		CREATE INDEX IF NOT EXISTS idx_image_credential_secret_name
		ON image(credential_secret_name);
	`)
	return err
}

func downAddImageCredentialFields(tx *sql.Tx) error {
	// Drop index first
	_, err := tx.Exec(`
		DROP INDEX IF EXISTS idx_image_credential_secret_name;
	`)
	if err != nil {
		return err
	}

	// Drop columns
	_, err = tx.Exec(`
		ALTER TABLE image
		DROP COLUMN IF EXISTS credential_secret_id,
		DROP COLUMN IF EXISTS credential_secret_name;
	`)
	return err
}
