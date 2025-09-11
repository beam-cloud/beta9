package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddCheckpointDeletedAt, downAddCheckpointDeletedAt)
}

func upAddCheckpointDeletedAt(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`
		ALTER TABLE checkpoint ADD COLUMN deleted_at TIMESTAMP WITH TIME ZONE DEFAULT NULL;
	`)
	return err
}

func downAddCheckpointDeletedAt(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE checkpoint DROP COLUMN deleted_at;`)
	return err
}
