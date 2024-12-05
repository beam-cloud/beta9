package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddFieldWorkspaceMultiGpuEnabled, downDropFieldWorkspaceMultiGpuEnabled)
}

func upAddFieldWorkspaceMultiGpuEnabled(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE workspace ADD COLUMN multi_gpu_enabled BOOLEAN DEFAULT FALSE;`)
	return err
}

func downDropFieldWorkspaceMultiGpuEnabled(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE workspace DROP COLUMN multi_gpu_enabled;`)
	return err
}
