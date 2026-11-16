package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upGitHubRunners, downGitHubRunners) }

func upGitHubRunners(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE github_runner_installation (
 id BIGINT PRIMARY KEY,
 workspace_id BIGINT NOT NULL UNIQUE REFERENCES workspace(id),
 token_id TEXT NOT NULL,
 account TEXT NOT NULL,
 account_type TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE github_runner_job (
 id BIGINT PRIMARY KEY,
 installation_id BIGINT NOT NULL,
 workspace_id BIGINT NOT NULL REFERENCES workspace(id),
 repository TEXT NOT NULL,
 workflow TEXT NOT NULL,
 name TEXT NOT NULL,
 url TEXT NOT NULL,
 label TEXT NOT NULL,
 status TEXT NOT NULL,
 conclusion TEXT NOT NULL,
 runner_name TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL,
 started_at TIMESTAMPTZ,
 completed_at TIMESTAMPTZ,
 vm TEXT NOT NULL DEFAULT '',
 attempts INTEGER NOT NULL DEFAULT 0,
 provision_at TIMESTAMPTZ,
 error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX github_runner_job_history ON github_runner_job (workspace_id, id DESC);
CREATE INDEX github_runner_job_due ON github_runner_job (provision_at) WHERE provision_at IS NOT NULL;
CREATE INDEX github_runner_job_vm ON github_runner_job (workspace_id, vm) WHERE vm <> '';
CREATE INDEX github_runner_job_runner ON github_runner_job (workspace_id, runner_name) WHERE runner_name <> '';`)
	return err
}

func downGitHubRunners(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP TABLE github_runner_job; DROP TABLE github_runner_installation;`)
	return err
}
