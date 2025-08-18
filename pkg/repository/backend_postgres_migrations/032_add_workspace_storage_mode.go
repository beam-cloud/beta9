package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddWorkspaceStorageMode, downRemoveWorkspaceStorageMode)
}

func upAddWorkspaceStorageMode(ctx context.Context, tx *sql.Tx) error {
	createEnumSQL := `
DO $$
BEGIN
   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'storage_mode') THEN
      CREATE TYPE storage_mode AS ENUM ('alluxio', 'geese');
   END IF;
END
$$;`

	if _, err := tx.Exec(createEnumSQL); err != nil {
		return err
	}

	// Add storage_mode column to workspace_storage table with default value 'geese'
	addColumnSQL := `
ALTER TABLE workspace_storage 
ADD COLUMN IF NOT EXISTS storage_mode storage_mode DEFAULT 'geese';`

	if _, err := tx.Exec(addColumnSQL); err != nil {
		return err
	}

	return nil
}

func downRemoveWorkspaceStorageMode(ctx context.Context, tx *sql.Tx) error {
	removeColumnSQL := `ALTER TABLE workspace_storage DROP COLUMN IF EXISTS storage_mode;`

	if _, err := tx.Exec(removeColumnSQL); err != nil {
		return err
	}

	// Drop the storage_mode enum type if it exists
	dropEnumSQL := `DROP TYPE IF EXISTS storage_mode;`

	if _, err := tx.Exec(dropEnumSQL); err != nil {
		return err
	}

	return nil
}
