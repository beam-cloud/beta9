package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddSigningKey, downRemoveSigningKey)
}

func upAddSigningKey(tx *sql.Tx) error {
	addFieldSQL := `
	ALTER TABLE workspace
	ADD COLUMN signing_key VARCHAR(256);
	`
	if _, err := tx.Exec(addFieldSQL); err != nil {
		return err
	}
	return nil
}

func downRemoveSigningKey(tx *sql.Tx) error {
	removeFieldSQL := `
	ALTER TABLE workspace
	DROP COLUMN signing_key;
	`
	if _, err := tx.Exec(removeFieldSQL); err != nil {
		return err
	}
	return nil
}
