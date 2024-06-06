package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddFieldDisabledByClusterAdminToTokenTable, downDropFieldDisabledByClusterAdminFromTokenTable)
}

func upAddFieldDisabledByClusterAdminToTokenTable(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE token ADD COLUMN disabled_by_cluster_admin BOOLEAN DEFAULT FALSE;`)
	return err
}

func downDropFieldDisabledByClusterAdminFromTokenTable(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE token DROP COLUMN disabled_by_cluster_admin;`)
	return err
}
