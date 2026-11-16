package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/lib/pq"
)

// ErrGitHubRunnerConflict means the installation is connected to another
// workspace, or the workspace to another installation.
var ErrGitHubRunnerConflict = errors.New("GitHub runner connection conflict")

const (
	githubRunnerInstallationColumns = `id, workspace_id, token_id, account, account_type, created_at`
	githubRunnerJobColumns          = `id, installation_id, workspace_id, repository, workflow, name, url, label, status,
	conclusion, runner_name, created_at, started_at, completed_at, vm, attempts, provision_at, error`
)

func (r *PostgresBackendRepository) GetGitHubRunnerInstallation(ctx context.Context, workspaceId uint) (*types.GitHubRunnerInstallation, error) {
	query := `SELECT ` + githubRunnerInstallationColumns + ` FROM github_runner_installation WHERE workspace_id = $1;`
	return r.getGitHubRunnerInstallation(ctx, query, workspaceId)
}

func (r *PostgresBackendRepository) GetGitHubRunnerInstallationById(ctx context.Context, installationId int64) (*types.GitHubRunnerInstallation, error) {
	query := `SELECT ` + githubRunnerInstallationColumns + ` FROM github_runner_installation WHERE id = $1;`
	return r.getGitHubRunnerInstallation(ctx, query, installationId)
}

func (r *PostgresBackendRepository) getGitHubRunnerInstallation(ctx context.Context, query string, args ...any) (*types.GitHubRunnerInstallation, error) {
	var installation types.GitHubRunnerInstallation
	err := r.client.GetContext(ctx, &installation, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &installation, nil
}

// SaveGitHubRunnerInstallation connects an installation to a workspace.
// Connecting it again refreshes the token its runners launch with; an
// installation never moves to another workspace.
func (r *PostgresBackendRepository) SaveGitHubRunnerInstallation(ctx context.Context, installation *types.GitHubRunnerInstallation) error {
	query := `
	INSERT INTO github_runner_installation (id, workspace_id, token_id, account, account_type)
	VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (id) DO UPDATE
	SET token_id = EXCLUDED.token_id, account = EXCLUDED.account, account_type = EXCLUDED.account_type
	WHERE github_runner_installation.workspace_id = EXCLUDED.workspace_id;
	`

	result, err := r.client.ExecContext(ctx, query, installation.ID, installation.WorkspaceID, installation.TokenID,
		installation.Account, installation.AccountType)
	if err != nil {
		// The workspace is connected to another installation.
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code.Name() == "unique_violation" {
			return ErrGitHubRunnerConflict
		}
		return err
	}

	// The installation is connected to another workspace.
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrGitHubRunnerConflict
	}

	return nil
}

// DeleteGitHubRunnerInstallation disconnects an installation. Its jobs are
// kept, but the runners they still wait for are not started.
func (r *PostgresBackendRepository) DeleteGitHubRunnerInstallation(ctx context.Context, installationId int64) error {
	query := `
	WITH disconnected AS (
		DELETE FROM github_runner_installation WHERE id = $1
	)
	UPDATE github_runner_job
	SET provision_at = NULL
	WHERE installation_id = $1 AND provision_at IS NOT NULL;
	`

	_, err := r.client.ExecContext(ctx, query, installationId)
	return err
}

// SaveGitHubRunnerJob records a delivery of a workflow job. A job is due for
// a runner when it is first seen. Deliveries repeat and arrive out of order,
// so a job's status never moves backwards.
func (r *PostgresBackendRepository) SaveGitHubRunnerJob(ctx context.Context, job *types.GitHubRunnerJob) error {
	query := `
	INSERT INTO github_runner_job (
		id, installation_id, workspace_id, repository, workflow, name, url, label, status,
		conclusion, runner_name, created_at, started_at, completed_at, provision_at
	)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
	ON CONFLICT (id) DO UPDATE
	SET status = EXCLUDED.status, conclusion = EXCLUDED.conclusion, runner_name = EXCLUDED.runner_name,
		started_at = EXCLUDED.started_at, completed_at = EXCLUDED.completed_at
	WHERE github_runner_job.workspace_id = EXCLUDED.workspace_id
		AND array_position(ARRAY['queued', 'in_progress', 'completed'], github_runner_job.status)
			<= array_position(ARRAY['queued', 'in_progress', 'completed'], EXCLUDED.status);
	`

	_, err := r.client.ExecContext(ctx, query, job.ID, job.InstallationID, job.WorkspaceID, job.Repository, job.Workflow,
		job.Name, job.URL, job.Label, job.Status, job.Conclusion, job.RunnerName, job.CreatedAt, job.StartedAt, job.CompletedAt)
	return err
}

// ListGitHubRunnerJobs returns a page of a workspace's jobs, newest first,
// and how many it has. The jobs that a disconnected account left unfinished
// are not among them, because nothing reports how they end. They are back if
// the account connects again.
func (r *PostgresBackendRepository) ListGitHubRunnerJobs(ctx context.Context, workspaceId uint, limit, offset int) ([]types.GitHubRunnerJob, int, error) {
	const history = `
	FROM github_runner_job j
	WHERE workspace_id = $1 AND (
		status = 'completed'
		OR EXISTS (SELECT 1 FROM github_runner_installation i WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id)
	)
	`

	var total int
	if err := r.client.GetContext(ctx, &total, `SELECT COUNT(*)`+history, workspaceId); err != nil {
		return nil, 0, err
	}

	jobs := []types.GitHubRunnerJob{}
	if limit == 0 || offset >= total {
		return jobs, total, nil
	}

	query := `
	SELECT ` + githubRunnerJobColumns + history + `
	ORDER BY id DESC
	LIMIT $2 OFFSET $3;
	`

	if err := r.client.SelectContext(ctx, &jobs, query, workspaceId, limit, offset); err != nil {
		return nil, 0, err
	}

	return jobs, total, nil
}

// ListDueGitHubRunnerJobs returns the jobs of connected accounts that are due
// for a runner, longest due first.
func (r *PostgresBackendRepository) ListDueGitHubRunnerJobs(ctx context.Context) ([]types.GitHubRunnerJob, error) {
	query := `
	SELECT ` + githubRunnerJobColumns + `
	FROM github_runner_job j
	WHERE provision_at <= NOW()
		AND EXISTS (SELECT 1 FROM github_runner_installation i WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id)
	ORDER BY provision_at
	LIMIT 100;
	`

	jobs := []types.GitHubRunnerJob{}
	if err := r.client.SelectContext(ctx, &jobs, query); err != nil {
		return nil, err
	}

	return jobs, nil
}

// ClaimGitHubRunnerJob gives a due job to one caller, to start its runner.
// The claim names that runner before it exists, so that a crash cannot leave
// one no job refers to, and makes the job due again later, for an attempt
// that fails or never finishes.
func (r *PostgresBackendRepository) ClaimGitHubRunnerJob(ctx context.Context, job *types.GitHubRunnerJob, vmName string) (bool, error) {
	query := `
	UPDATE github_runner_job
	SET vm = $3, attempts = attempts + 1, provision_at = NOW() + (attempts + 1) * INTERVAL '1 minute'
	WHERE id = $1 AND attempts = $2 AND provision_at <= NOW();
	`

	result, err := r.client.ExecContext(ctx, query, job.ID, job.Attempts, vmName)
	if err != nil {
		return false, err
	}

	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return false, err
	}

	job.VM, job.Attempts = vmName, job.Attempts+1
	return true, nil
}

// FinishGitHubRunnerJob stops starting a runner for a job: it has one, or it
// will never get one, for the reason in message.
func (r *PostgresBackendRepository) FinishGitHubRunnerJob(ctx context.Context, jobId int64, message string) error {
	query := `UPDATE github_runner_job SET provision_at = NULL, error = $2 WHERE id = $1;`
	_, err := r.client.ExecContext(ctx, query, jobId, message)
	return err
}

// FailGitHubRunnerJob records why an attempt to start a job's runner failed.
// The job stays due when its claim said it would be.
func (r *PostgresBackendRepository) FailGitHubRunnerJob(ctx context.Context, jobId int64, message string) error {
	query := `UPDATE github_runner_job SET error = $2 WHERE id = $1;`
	_, err := r.client.ExecContext(ctx, query, jobId, message)
	return err
}

// GetGitHubRunner describes the runner that a VM was started as, or returns
// nil for a VM that is not a runner. The conditions on empty names are those
// of the partial indexes, which the planner must see to use them.
func (r *PostgresBackendRepository) GetGitHubRunner(ctx context.Context, workspaceId uint, vmName string) (*types.GitHubRunner, error) {
	query := `
	SELECT j.installation_id, j.repository,
		EXISTS (
			SELECT 1 FROM github_runner_installation i WHERE i.id = j.installation_id AND i.workspace_id = j.workspace_id
		) AS connected,
		COALESCE((
			SELECT a.status FROM github_runner_job a
			WHERE a.workspace_id = j.workspace_id AND a.runner_name = j.vm AND a.runner_name <> ''
			LIMIT 1
		), '') AS job_status
	FROM github_runner_job j
	WHERE j.workspace_id = $1 AND j.vm = $2 AND j.vm <> ''
	LIMIT 1;
	`

	var runner types.GitHubRunner
	err := r.client.GetContext(ctx, &runner, query, workspaceId, vmName)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &runner, nil
}

// ReplaceGitHubRunner deletes a runner VM that stopped before GitHub gave it
// a job, and makes the job it was started for due again, as some queued job
// is still counting on a runner. One statement does both, so that a crash
// cannot do only one of them.
func (r *PostgresBackendRepository) ReplaceGitHubRunner(ctx context.Context, workspaceId uint, vmName, message string) error {
	query := `
	WITH deleted AS (
		UPDATE persistent_vm
		SET data = jsonb_set(data, '{desired_state}', '"deleted"')
		WHERE workspace_id = $1 AND name = $2 AND data->>'desired_state' <> 'deleted'
	)
	UPDATE github_runner_job
	SET provision_at = NOW(), error = $3
	WHERE workspace_id = $1 AND vm = $2 AND vm <> '';
	`

	_, err := r.client.ExecContext(ctx, query, workspaceId, vmName, message)
	return err
}
