package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddCheckpointCleanupIndexes, downAddCheckpointCleanupIndexes)
}

func upAddCheckpointCleanupIndexes(ctx context.Context, db *sql.DB) error {
	// Cleanup can start from a small set of inactive stubs. Both its checkpoint
	// join and the saved-memory protection must remain indexed in that plan.
	if err := createIndexConcurrently(ctx, db, "idx_checkpoint_active_stub", "ON checkpoint (stub_id) WHERE deleted_at IS NULL"); err != nil {
		return err
	}
	return createIndexConcurrently(ctx, db, "idx_vm_memory_checkpoint", "ON persistent_vm ((data->>'memory_checkpoint_id')) WHERE data->>'desired_state'<>'deleted'")
}

func downAddCheckpointCleanupIndexes(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "DROP INDEX CONCURRENTLY IF EXISTS idx_checkpoint_active_stub"); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, "DROP INDEX CONCURRENTLY IF EXISTS idx_vm_memory_checkpoint")
	return err
}
