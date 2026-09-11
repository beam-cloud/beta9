package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upImageCredentialSecretNames, downImageCredentialSecretNames)
}

// An image names the workspace secrets holding its registry credentials.
func upImageCredentialSecretNames(tx *sql.Tx) error {
	_, err := tx.Exec(`
		DROP INDEX IF EXISTS idx_image_credential_secret_name;
		ALTER TABLE image
			DROP COLUMN IF EXISTS credential_secret_id,
			DROP COLUMN IF EXISTS credential_secret_name,
			ADD COLUMN IF NOT EXISTS credential_secret_names TEXT[];
	`)
	return err
}

func downImageCredentialSecretNames(tx *sql.Tx) error {
	_, err := tx.Exec(`
		ALTER TABLE image
			DROP COLUMN IF EXISTS credential_secret_names,
			ADD COLUMN IF NOT EXISTS credential_secret_name VARCHAR(255),
			ADD COLUMN IF NOT EXISTS credential_secret_id VARCHAR(36);
	`)
	return err
}
