package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upGitHubRunnerStops, downGitHubRunnerStops) }

// Both columns have defaults, as gateways of the version before this one
// write these tables without them until they are replaced.
func upGitHubRunnerStops(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
ALTER TABLE github_runner_job ADD COLUMN run_id BIGINT NOT NULL DEFAULT 0;
ALTER TABLE github_runner_installation ADD COLUMN paused BOOLEAN NOT NULL DEFAULT FALSE;`)
	return err
}

func downGitHubRunnerStops(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE github_runner_installation DROP COLUMN paused; ALTER TABLE github_runner_job DROP COLUMN run_id;`)
	return err
}
