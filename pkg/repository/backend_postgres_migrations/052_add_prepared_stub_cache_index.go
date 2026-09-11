package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddPreparedStubCacheIndex, downAddPreparedStubCacheIndex)
}

func upAddPreparedStubCacheIndex(ctx context.Context, db *sql.DB) error {
	return createIndexConcurrently(
		ctx,
		db,
		"idx_stub_preparation_cache_key",
		"ON stub (workspace_id, type, (config->>'preparation_cache_key'), updated_at DESC, id DESC) WHERE config->>'preparation_cache_key' IS NOT NULL",
	)
}

func downAddPreparedStubCacheIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "DROP INDEX CONCURRENTLY IF EXISTS idx_stub_preparation_cache_key")
	return err
}
