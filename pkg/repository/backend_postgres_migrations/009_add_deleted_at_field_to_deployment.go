package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddFieldDeletedAtToDeploymentTable, downDropFieldDeletedAtFromDeploymentTable)
}

func upAddFieldDeletedAtToDeploymentTable(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE deployment ADD COLUMN deleted_at TIMESTAMP WITH TIME ZONE`)
	return err
}

func downDropFieldDeletedAtFromDeploymentTable(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE deployment DROP COLUMN deleted_at`)
	return err
}
