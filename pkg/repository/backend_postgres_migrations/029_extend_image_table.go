package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upExtendImageTable, downExtendImageTable)
}

func upExtendImageTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		ALTER TABLE image 
		ADD COLUMN external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
		ADD COLUMN created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
		`)
	return err
}

func downExtendImageTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		ALTER TABLE image 
		DROP COLUMN IF EXISTS external_id,
		DROP COLUMN IF EXISTS created_at,
		DROP COLUMN IF EXISTS updated_at;
	`)
	return err
}
