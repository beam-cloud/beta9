package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upAddDashboardQueryIndexes, downAddDashboardQueryIndexes)
}

func upAddDashboardQueryIndexes(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE EXTENSION IF NOT EXISTS pg_trgm;`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_app_workspace_active_updated_id ON app (workspace_id, updated_at DESC, id DESC) WHERE deleted_at IS NULL;`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_app_workspace_external_id_active ON app (workspace_id, external_id) WHERE deleted_at IS NULL;`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_app_workspace_name_trgm_active ON app USING gin (LOWER(name) gin_trgm_ops) WHERE deleted_at IS NULL;`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stub_app_created_id ON stub (app_id, created_at DESC, id DESC);`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stub_app_type_created_id ON stub (app_id, type, created_at DESC, id DESC);`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_deployment_app_active_created_id ON deployment (app_id, created_at DESC, id DESC) WHERE deleted_at IS NULL;`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_deployment_workspace_active_created_id ON deployment (workspace_id, created_at DESC, id DESC) WHERE deleted_at IS NULL;`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_deployment_stub_active ON deployment (stub_id) WHERE deleted_at IS NULL;`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_workspace_created_id ON task (workspace_id, created_at DESC, id DESC);`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_external_workspace_created_id ON task (external_workspace_id, created_at DESC, id DESC);`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_stub_created_id ON task (stub_id, created_at DESC, id DESC);`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_container_id ON task (container_id);`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_token_workspace_created_id ON token (workspace_id, created_at DESC, id DESC);`,
	}

	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func downAddDashboardQueryIndexes(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`DROP INDEX CONCURRENTLY IF EXISTS idx_token_workspace_created_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_task_container_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_task_stub_created_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_task_external_workspace_created_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_task_workspace_created_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_deployment_stub_active;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_deployment_workspace_active_created_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_deployment_app_active_created_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_stub_app_type_created_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_stub_app_created_id;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_app_workspace_name_trgm_active;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_app_workspace_external_id_active;`,
		`DROP INDEX CONCURRENTLY IF EXISTS idx_app_workspace_active_updated_id;`,
	}

	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
