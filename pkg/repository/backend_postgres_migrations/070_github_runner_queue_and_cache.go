package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upGitHubRunnerQueueAndCache, downGitHubRunnerQueueAndCache) }

// An account can be paused and have its enhanced cache turned off. A job
// keeps its workflow run, to cancel it by, whether it is on its repository's
// default branch, what it waits for, and when each step of its start was.
//
// The indexes are of the few jobs that wait, so that those of a workspace
// are counted without reading its history: the jobs that GitHub has queued,
// by what runner they take, and the jobs that are to get a runner, in the
// order they are due.
//
// A cache disk is one repository's: what a job of a repository leaves is
// never in front of the jobs of another. The index holds a repository of a
// workspace to one disk that is not retired. The retired ones wait there to
// be deleted.
func upGitHubRunnerQueueAndCache(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
ALTER TABLE github_runner_installation
 ADD COLUMN paused BOOLEAN NOT NULL DEFAULT FALSE,
 ADD COLUMN enhanced_cache BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE github_runner_job
 ADD COLUMN run_id BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN default_branch BOOLEAN NOT NULL DEFAULT FALSE,
 ADD COLUMN waiting TEXT NOT NULL DEFAULT '',
 ADD COLUMN received_at TIMESTAMPTZ DEFAULT NULL,
 ADD COLUMN registered_at TIMESTAMPTZ DEFAULT NULL,
 ADD COLUMN vm_ready_at TIMESTAMPTZ DEFAULT NULL;
CREATE INDEX github_runner_job_queued ON github_runner_job (workspace_id, repository, label) WHERE status = 'queued';
CREATE INDEX github_runner_job_order ON github_runner_job (workspace_id, provision_at, id) WHERE provision_at IS NOT NULL;
CREATE TABLE github_runner_cache (
 workspace_id BIGINT NOT NULL REFERENCES workspace(id),
 repository TEXT NOT NULL DEFAULT '',
 disk_name TEXT NOT NULL,
 snapshot_id TEXT NOT NULL DEFAULT '',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 publisher_at TIMESTAMPTZ,
 retired_at TIMESTAMPTZ,
 PRIMARY KEY (workspace_id, disk_name)
);
CREATE UNIQUE INDEX github_runner_cache_current ON github_runner_cache (workspace_id, repository) WHERE retired_at IS NULL;`)
	return err
}

func downGitHubRunnerQueueAndCache(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
DROP TABLE github_runner_cache;
DROP INDEX github_runner_job_order;
DROP INDEX github_runner_job_queued;
ALTER TABLE github_runner_job
 DROP COLUMN vm_ready_at, DROP COLUMN registered_at, DROP COLUMN received_at, DROP COLUMN waiting,
 DROP COLUMN default_branch, DROP COLUMN run_id;
ALTER TABLE github_runner_installation DROP COLUMN enhanced_cache, DROP COLUMN paused;`)
	return err
}
