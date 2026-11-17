package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upGitHubRunnerCacheRepository, downGitHubRunnerCacheRepository)
}

// A cache disk is one repository's: what a job of a repository leaves is
// never in front of the jobs of another. The index holds a repository of a
// workspace to one disk that is not retired.
//
// A disk from before held what the jobs of every repository of its
// workspace left. It is retired, and deleted as every retired one is.
func upGitHubRunnerCacheRepository(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
ALTER TABLE github_runner_cache ADD COLUMN repository TEXT NOT NULL DEFAULT '';
UPDATE github_runner_cache SET retired_at = NOW() WHERE retired_at IS NULL;
DROP INDEX github_runner_cache_current;
CREATE UNIQUE INDEX github_runner_cache_current ON github_runner_cache (workspace_id, repository) WHERE retired_at IS NULL;`)
	return err
}

// A workspace may have a disk for each of its repositories by now, and can
// keep none of them as the one it has.
func downGitHubRunnerCacheRepository(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
UPDATE github_runner_cache SET retired_at = NOW() WHERE retired_at IS NULL;
DROP INDEX github_runner_cache_current;
CREATE UNIQUE INDEX github_runner_cache_current ON github_runner_cache (workspace_id) WHERE retired_at IS NULL;
ALTER TABLE github_runner_cache DROP COLUMN repository;`)
	return err
}
