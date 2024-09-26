package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddTaskCreatedAtIndex, downAddTaskCreatedAtIndex)
}

func upAddTaskCreatedAtIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE INDEX  CONCURRENTLY IF NOT EXISTS task_created_at on task(created_at);`)
	return err
}

func downAddTaskCreatedAtIndex(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP INDEX CONCURRENTLY IF EXISTS task_created_at `)
	return err
}
