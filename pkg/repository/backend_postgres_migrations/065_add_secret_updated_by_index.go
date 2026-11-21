package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddSecretUpdatedByIndex, downAddSecretUpdatedByIndex)
}

func upAddSecretUpdatedByIndex(ctx context.Context, db *sql.DB) error {
	// Deleting a token clears last_updated_by on its secrets; without an index
	// every deleted token scans workspace_secret.
	return createIndexConcurrently(ctx, db, "idx_workspace_secret_last_updated_by", "ON workspace_secret (last_updated_by)")
}

func downAddSecretUpdatedByIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP INDEX CONCURRENTLY IF EXISTS idx_workspace_secret_last_updated_by;`)
	return err
}
