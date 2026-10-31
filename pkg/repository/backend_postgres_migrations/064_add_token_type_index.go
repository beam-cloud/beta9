package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddTokenTypeIndex, downAddTokenTypeIndex)
}

func upAddTokenTypeIndex(ctx context.Context, db *sql.DB) error {
	// The admin workspace keeps a worker token per worker; lookup by type must not scan them.
	return createIndexConcurrently(ctx, db, "idx_token_workspace_type_created_id", "ON token (workspace_id, token_type, created_at DESC, id DESC)")
}

func downAddTokenTypeIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP INDEX CONCURRENTLY IF EXISTS idx_token_workspace_type_created_id;`)
	return err
}
