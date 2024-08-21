package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddTaskStatusExpired, downRemoveTaskStatusExpired)
}

func upAddTaskStatusExpired(tx *sql.Tx) error {
	addEnumSQL := `
DO $$
BEGIN
   IF NOT EXISTS (
      SELECT 1 FROM pg_type t
      JOIN pg_enum e ON t.oid = e.enumtypid
      JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
      WHERE n.nspname = 'public' AND t.typname = 'task_status' AND e.enumlabel = 'EXPIRED'
   ) THEN
      EXECUTE 'ALTER TYPE task_status ADD VALUE ' || quote_literal('EXPIRED');
   END IF;
END
$$;`

	if _, err := tx.Exec(addEnumSQL); err != nil {
		return err
	}

	return nil
}

func downRemoveTaskStatusExpired(tx *sql.Tx) error {
	// PostgreSQL doesn't support removing values from an ENUM directly
	return nil
}
