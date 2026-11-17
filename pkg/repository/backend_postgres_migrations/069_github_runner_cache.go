package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upGitHubRunnerCache, downGitHubRunnerCache) }

// A workspace has one cache disk that is not retired, which the index holds
// it to. The retired ones wait there to be deleted.
//
// Gateways of the version before this one write accounts and jobs without
// the new columns until they are replaced, so each has a default: the cache
// is on, and a job is not known to run on a default branch.
func upGitHubRunnerCache(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
ALTER TABLE github_runner_installation ADD COLUMN enhanced_cache BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE github_runner_job ADD COLUMN default_branch BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE github_runner_cache (
 workspace_id BIGINT NOT NULL REFERENCES workspace(id),
 disk_name TEXT NOT NULL,
 snapshot_id TEXT NOT NULL DEFAULT '',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 publisher_at TIMESTAMPTZ,
 retired_at TIMESTAMPTZ,
 PRIMARY KEY (workspace_id, disk_name)
);
CREATE UNIQUE INDEX github_runner_cache_current ON github_runner_cache (workspace_id) WHERE retired_at IS NULL;`)
	return err
}

func downGitHubRunnerCache(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
DROP TABLE github_runner_cache;
ALTER TABLE github_runner_job DROP COLUMN default_branch;
ALTER TABLE github_runner_installation DROP COLUMN enhanced_cache;`)
	return err
}
