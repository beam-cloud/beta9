package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upSecretBrokering, downSecretBrokering) }
func upSecretBrokering(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE workspace_secret
 ADD COLUMN hosts TEXT[] NOT NULL DEFAULT '{}',
 ADD COLUMN placeholder TEXT NOT NULL DEFAULT ('beam_secret_' || replace(uuid_generate_v4()::text, '-', ''));
 CREATE UNIQUE INDEX workspace_secret_placeholder ON workspace_secret (placeholder);`)
	return err
}
func downSecretBrokering(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE workspace_secret DROP COLUMN hosts, DROP COLUMN placeholder;`)
	return err
}
