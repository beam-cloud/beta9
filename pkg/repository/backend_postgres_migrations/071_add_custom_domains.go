package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddCustomDomains, downAddCustomDomains)
}

func upAddCustomDomains(ctx context.Context, tx *sql.Tx) error {
	// Ownership model:
	//   - Any workspace may hold a pending claim for a domain; pending claims
	//     never block other workspaces (no squatting).
	//   - At most one claim per domain can be active. Activation requires DNS
	//     proof and demotes any previously active claim held by another
	//     workspace, so the current DNS owner can always take a domain back.
	_, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS custom_domain (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT REFERENCES workspace(id) ON DELETE CASCADE NOT NULL,
			domain VARCHAR(255) NOT NULL,
			deployment_name TEXT NOT NULL,
			stub_type TEXT NOT NULL DEFAULT '',
			version INT NOT NULL DEFAULT 0,
			port INT NOT NULL DEFAULT 0,
			status VARCHAR(32) NOT NULL DEFAULT 'pending'
				CONSTRAINT custom_domain_status_check CHECK (status IN ('pending', 'active', 'failed')),
			source VARCHAR(16) NOT NULL DEFAULT 'api'
				CONSTRAINT custom_domain_source_check CHECK (source IN ('api', 'deploy')),
			verification_token VARCHAR(128) NOT NULL,
			verified_at TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
			CONSTRAINT custom_domain_workspace_domain_key UNIQUE (workspace_id, domain)
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_custom_domain_active_domain ON custom_domain(domain) WHERE status = 'active';
		CREATE INDEX IF NOT EXISTS idx_custom_domain_domain ON custom_domain(domain);
		CREATE INDEX IF NOT EXISTS idx_custom_domain_deployment ON custom_domain(workspace_id, deployment_name);

		-- Records which version of a deployment last saved its declared domains.
		-- A deploy may only be skipped as stale once a newer deploy has actually
		-- saved its domains; a newer deployment row alone proves nothing, because
		-- that deploy can still fail and be rolled back.
		CREATE TABLE IF NOT EXISTS custom_domain_sync (
			workspace_id INT REFERENCES workspace(id) ON DELETE CASCADE NOT NULL,
			deployment_name TEXT NOT NULL,
			stub_type TEXT NOT NULL,
			synced_version INT NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
			PRIMARY KEY (workspace_id, deployment_name, stub_type)
		);
	`)
	return err
}

func downAddCustomDomains(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS custom_domain_sync;
		DROP TABLE IF EXISTS custom_domain;`)
	return err
}
