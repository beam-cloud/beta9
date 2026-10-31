package backend_postgres_migrations

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upTokenAttributes, downTokenAttributes) }

func upTokenAttributes(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE workspace ALTER COLUMN signing_key TYPE TEXT; ALTER TABLE token ALTER COLUMN key TYPE TEXT; ALTER TABLE token ADD COLUMN worker_id TEXT NOT NULL DEFAULT '', ADD COLUMN expires_at TIMESTAMPTZ, ADD COLUMN key_hash TEXT; CREATE UNIQUE INDEX token_key_hash ON token(key_hash) WHERE key_hash IS NOT NULL;`)
	return err
}

func downTokenAttributes(ctx context.Context, tx *sql.Tx) error {
	return fmt.Errorf("token attributes require a forward migration")
}
