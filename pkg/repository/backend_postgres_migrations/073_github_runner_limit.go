package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upGitHubRunnerLimit, downGitHubRunnerLimit) }

// A workspace may be allowed more runners at once than the gateway allows
// every workspace, or fewer. What it is allowed is kept with the workspace
// and not with the account it connected, so that it stays when the account
// is disconnected and connected again. Zero allows any number.
func upGitHubRunnerLimit(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE github_runner_limit (
 workspace_id BIGINT PRIMARY KEY REFERENCES workspace(id),
 runner_limit INTEGER NOT NULL CHECK (runner_limit >= 0),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`)
	return err
}

func downGitHubRunnerLimit(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP TABLE github_runner_limit;`)
	return err
}
