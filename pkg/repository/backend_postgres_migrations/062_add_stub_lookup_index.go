package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddStubLookupIndex, downAddStubLookupIndex)
}

func upAddStubLookupIndex(ctx context.Context, db *sql.DB) error {
	// VM stubs share an app and type; lookup by name must not scan its history.
	return createIndexConcurrently(ctx, db, "idx_stub_workspace_name", "ON stub (workspace_id, name)")
}

func downAddStubLookupIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP INDEX CONCURRENTLY IF EXISTS idx_stub_workspace_name;`)
	return err
}
