package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upGitHubCompaction, downGitHubCompaction) }

func upGitHubCompaction(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
ALTER TABLE github_runner_installation ADD COLUMN ci_compaction BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE github_compaction (
 installation_id BIGINT NOT NULL REFERENCES github_runner_installation(id) ON DELETE CASCADE,
 workspace_id BIGINT NOT NULL REFERENCES workspace(id),
 repository TEXT NOT NULL,
 branch TEXT NOT NULL,
 label TEXT NOT NULL,
 head TEXT NOT NULL,
 marked_at TIMESTAMPTZ NOT NULL,
 last_commit TEXT NOT NULL DEFAULT '',
 next_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 wait_weeks INTEGER NOT NULL DEFAULT 1,
 run_id TEXT NOT NULL DEFAULT '',
 run_commit TEXT NOT NULL DEFAULT '',
 run_expires TIMESTAMPTZ,
 status TEXT NOT NULL DEFAULT 'Waiting to analyze',
 pr_number INTEGER NOT NULL DEFAULT 0,
 pr_url TEXT NOT NULL DEFAULT '',
 pr_open BOOLEAN NOT NULL DEFAULT FALSE,
 branch_sha TEXT NOT NULL DEFAULT '',
 savings TEXT NOT NULL DEFAULT '',
 report JSONB NOT NULL DEFAULT '{}',
 PRIMARY KEY (installation_id, repository)
);
CREATE UNIQUE INDEX github_compaction_active ON github_compaction (workspace_id) WHERE run_id <> '';
CREATE INDEX github_compaction_due ON github_compaction (next_at) WHERE NOT pr_open AND head <> last_commit;`)
	return err
}

func downGitHubCompaction(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
DROP TABLE github_compaction;
ALTER TABLE github_runner_installation DROP COLUMN ci_compaction;`)
	return err
}
