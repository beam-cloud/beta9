package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upGitHubRunnerReady, downGitHubRunnerReady) }

// A job keeps when a runner that could take it was first ready, whichever
// runner that was, so that its time in the queue splits into the time that
// it waited for Beam and the time that it waited for GitHub.
func upGitHubRunnerReady(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE github_runner_job ADD COLUMN runner_ready_at TIMESTAMPTZ DEFAULT NULL;`)
	return err
}

func downGitHubRunnerReady(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE github_runner_job DROP COLUMN runner_ready_at;`)
	return err
}
