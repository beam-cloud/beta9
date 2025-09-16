package backend_postgres_migrations

import (
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddPublishableTokenType, downRemovePublishableTokenType)
}

func upAddPublishableTokenType(tx *sql.Tx) error {
	newTokenTypes := []string{"workspace_restricted"}

	for _, tokenType := range newTokenTypes {
		addEnumSQL := fmt.Sprintf(`
DO $$
BEGIN
   IF NOT EXISTS (
      SELECT 1 FROM pg_type t
      JOIN pg_enum e ON t.oid = e.enumtypid
      JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
      WHERE n.nspname = 'public' AND t.typname = 'token_type' AND e.enumlabel = '%s'
   ) THEN
      EXECUTE 'ALTER TYPE token_type ADD VALUE ' || quote_literal('%s');
   END IF;
END
$$;`, tokenType, tokenType)

		if _, err := tx.Exec(addEnumSQL); err != nil {
			return err
		}
	}

	return nil
}

func downRemovePublishableTokenType(tx *sql.Tx) error {
	// PostgreSQL doesn't support removing values from an ENUM directly
	return nil
}