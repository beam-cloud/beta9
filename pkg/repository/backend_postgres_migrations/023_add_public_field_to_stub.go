package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddFieldStubPublicField, downDropFieldStubPublicField)
}

func upAddFieldStubPublicField(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE stub ADD COLUMN public BOOLEAN DEFAULT FALSE;`)

	return err
}

func downDropFieldStubPublicField(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE stub DROP COLUMN public;`)
	return err
}
