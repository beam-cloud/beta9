package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upMirrorGenerationHome, downMirrorGenerationHome) }

// The home directory a generation's files came from; empty for generations
// that do not record one.
func upMirrorGenerationHome(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE mirror_generation ADD COLUMN home TEXT NOT NULL DEFAULT ''`)
	return err
}

func downMirrorGenerationHome(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE mirror_generation DROP COLUMN home`)
	return err
}
