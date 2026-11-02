package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upTokenWorkerBinding, downTokenWorkerBinding)
}

func upTokenWorkerBinding(ctx context.Context, db *sql.DB) error {
	for _, statement := range []string{
		`ALTER TABLE token ADD COLUMN IF NOT EXISTS worker_id VARCHAR(255) NOT NULL DEFAULT ''`,
		`ALTER TABLE token ADD COLUMN IF NOT EXISTS key_hash VARCHAR(64)`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func downTokenWorkerBinding(ctx context.Context, db *sql.DB) error {
	for _, statement := range []string{
		`DROP INDEX CONCURRENTLY IF EXISTS idx_token_key_hash`,
		`ALTER TABLE token DROP COLUMN IF EXISTS key_hash`,
		`ALTER TABLE token DROP COLUMN IF EXISTS worker_id`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
