package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upGitHubRunnerQueue, downGitHubRunnerQueue) }

// Gateways of the version before this one write these rows without the new
// columns until they are replaced, so each has a default: nothing is waited
// for, and a step that was not timed has no time.
//
// The indexes are of the few jobs that wait, so that those of a workspace
// are counted without reading its history: the jobs that GitHub has queued,
// by what runner they take, and the jobs that are to get a runner, in the
// order they are due.
func upGitHubRunnerQueue(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
ALTER TABLE github_runner_job
 ADD COLUMN waiting TEXT NOT NULL DEFAULT '',
 ADD COLUMN received_at TIMESTAMPTZ DEFAULT NULL,
 ADD COLUMN registered_at TIMESTAMPTZ DEFAULT NULL,
 ADD COLUMN vm_ready_at TIMESTAMPTZ DEFAULT NULL;
CREATE INDEX github_runner_job_queued ON github_runner_job (workspace_id, repository, label) WHERE status = 'queued';
CREATE INDEX github_runner_job_order ON github_runner_job (workspace_id, provision_at, id) WHERE provision_at IS NOT NULL;`)
	return err
}

func downGitHubRunnerQueue(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
DROP INDEX github_runner_job_order;
DROP INDEX github_runner_job_queued;
ALTER TABLE github_runner_job
 DROP COLUMN vm_ready_at, DROP COLUMN registered_at, DROP COLUMN received_at, DROP COLUMN waiting;`)
	return err
}
