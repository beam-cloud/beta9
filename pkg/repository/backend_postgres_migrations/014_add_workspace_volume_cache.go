package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddFieldWorkspaceVolumeCachedEnabled, downDropFieldWorkspaceVolumeCachedEnabled)
}

func upAddFieldWorkspaceVolumeCachedEnabled(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE workspace ADD COLUMN volume_cache_enabled BOOLEAN DEFAULT FALSE;`)
	return err
}

func downDropFieldWorkspaceVolumeCachedEnabled(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE workspace DROP COLUMN volume_cache_enabled;`)
	return err
}
