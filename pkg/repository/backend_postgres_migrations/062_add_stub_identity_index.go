package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddStubIdentityIndex, downAddStubIdentityIndex)
}

func upAddStubIdentityIndex(ctx context.Context, db *sql.DB) error {
	// GetOrCreateStub checks JSON equality after identifying the stub. Keep
	// that check independent of the workspace's accumulated launch history.
	return createIndexConcurrently(ctx, db, "idx_stub_identity", "ON stub (workspace_id, name, type, object_id, app_id)")
}

func downAddStubIdentityIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "DROP INDEX CONCURRENTLY IF EXISTS idx_stub_identity")
	return err
}
