package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddStubIdIndexTask, downAddStubIdIndexTask)
}

func upAddStubIdIndexTask(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE INDEX CONCURRENTLY IF NOT EXISTS task_stub_id on task(stub_id);`)
	return err
}

func downAddStubIdIndexTask(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DROP INDEX CONCURRENTLY IF EXISTS task_stub_id `)
	return err
}
