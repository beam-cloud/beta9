package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTxContext(upMarkClusterAdminWorkspace, downMarkClusterAdminWorkspace)
}

func upMarkClusterAdminWorkspace(ctx context.Context, db *sql.DB) error {
	// Existing installations have no canonical marker. Preserve their current
	// admin workspace once; all subsequent lookups use the explicit marker.
	statements := []string{
		`ALTER TABLE workspace ADD COLUMN IF NOT EXISTS is_cluster_admin BOOLEAN NOT NULL DEFAULT FALSE;`,
		`UPDATE workspace
		 SET is_cluster_admin = TRUE
		 WHERE id = (
			 SELECT workspace_id
			 FROM token
			 WHERE token_type = 'admin'
			 ORDER BY id
			 LIMIT 1
		 )
		 AND NOT EXISTS (SELECT 1 FROM workspace WHERE is_cluster_admin);`,
		`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_workspace_cluster_admin ON workspace (is_cluster_admin) WHERE is_cluster_admin;`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func downMarkClusterAdminWorkspace(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`DROP INDEX CONCURRENTLY IF EXISTS idx_workspace_cluster_admin;`,
		`ALTER TABLE workspace DROP COLUMN IF EXISTS is_cluster_admin;`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
