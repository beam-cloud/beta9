package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/lib/pq"
)

// ErrGitHubRunnerConflict means the installation belongs to another workspace,
// or the workspace already has another installation.
var ErrGitHubRunnerConflict = errors.New("GitHub runner connection conflict")

type GitHubRunnerRepository interface {
	GetGitHubRunnerInstallation(context.Context, uint) (*types.GitHubRunnerInstallation, error)
	GetGitHubRunnerInstallationByID(context.Context, int64) (*types.GitHubRunnerInstallation, error)
	SaveGitHubRunnerInstallation(context.Context, *types.GitHubRunnerInstallation) error
	DeleteGitHubRunnerInstallation(context.Context, int64) error
	SaveGitHubRunnerJob(context.Context, *types.GitHubRunnerJob) error
	ListGitHubRunnerJobs(context.Context, uint, int, int) ([]types.GitHubRunnerJob, int, error)
	ListDueGitHubRunnerJobs(context.Context) ([]types.GitHubRunnerJob, error)
	ClaimGitHubRunnerJob(context.Context, *types.GitHubRunnerJob, string) (bool, error)
	FinishGitHubRunnerJob(context.Context, int64, bool, string) error
	GetGitHubRunner(context.Context, uint, string) (*types.GitHubRunner, error)
	ReleaseGitHubRunner(context.Context, uint, string, string) error
}

func (r *PostgresBackendRepository) GetGitHubRunnerInstallation(ctx context.Context, ws uint) (*types.GitHubRunnerInstallation, error) {
	return getRow[types.GitHubRunnerInstallation](ctx, r, `SELECT * FROM github_runner_installation WHERE workspace_id=$1`, ws)
}

func (r *PostgresBackendRepository) GetGitHubRunnerInstallationByID(ctx context.Context, id int64) (*types.GitHubRunnerInstallation, error) {
	return getRow[types.GitHubRunnerInstallation](ctx, r, `SELECT * FROM github_runner_installation WHERE id=$1`, id)
}

// Reconnecting refreshes the launching token; an installation never moves to
// another workspace.
func (r *PostgresBackendRepository) SaveGitHubRunnerInstallation(ctx context.Context, i *types.GitHubRunnerInstallation) error {
	result, err := r.client.ExecContext(ctx, `INSERT INTO github_runner_installation (id,workspace_id,token_id,account,account_type)
 VALUES ($1,$2,$3,$4,$5) ON CONFLICT (id) DO UPDATE SET token_id=EXCLUDED.token_id, account=EXCLUDED.account
 WHERE github_runner_installation.workspace_id=EXCLUDED.workspace_id`, i.ID, i.WorkspaceID, i.TokenID, i.Account, i.AccountType)
	var pgErr *pq.Error
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrGitHubRunnerConflict
	}
	if err != nil {
		return err
	}

	count, err := result.RowsAffected()
	if err == nil && count != 1 {
		return ErrGitHubRunnerConflict
	}
	return err
}

// Job history is kept, but its pending runners are not started.
func (r *PostgresBackendRepository) DeleteGitHubRunnerInstallation(ctx context.Context, id int64) error {
	_, err := r.client.ExecContext(ctx, `WITH deleted AS (DELETE FROM github_runner_installation WHERE id=$1)
 UPDATE github_runner_job SET provision_at=NULL WHERE installation_id=$1 AND provision_at IS NOT NULL`, id)
	return err
}

// Deliveries repeat and arrive out of order, so a job's status never moves
// backwards. Queued jobs become due for a runner.
func (r *PostgresBackendRepository) SaveGitHubRunnerJob(ctx context.Context, j *types.GitHubRunnerJob) error {
	_, err := r.client.ExecContext(ctx, `INSERT INTO github_runner_job
 (id,installation_id,workspace_id,repository,workflow,name,url,label,status,conclusion,runner_name,created_at,started_at,completed_at,provision_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,CASE WHEN $9='queued' THEN NOW() END)
 ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status, conclusion=EXCLUDED.conclusion, runner_name=EXCLUDED.runner_name,
 started_at=EXCLUDED.started_at, completed_at=EXCLUDED.completed_at
 WHERE github_runner_job.workspace_id=EXCLUDED.workspace_id
 AND array_position(ARRAY['queued','in_progress','completed'], github_runner_job.status)
 <= array_position(ARRAY['queued','in_progress','completed'], EXCLUDED.status)`,
		j.ID, j.InstallationID, j.WorkspaceID, j.Repository, j.Workflow, j.Name, j.URL, j.Label, j.Status, j.Conclusion,
		j.RunnerName, j.CreatedAt, j.StartedAt, j.CompletedAt)
	return err
}

func (r *PostgresBackendRepository) ListGitHubRunnerJobs(ctx context.Context, ws uint, limit, offset int) ([]types.GitHubRunnerJob, int, error) {
	var total int
	if err := r.client.GetContext(ctx, &total, `SELECT COUNT(*) FROM github_runner_job WHERE workspace_id=$1`, ws); err != nil {
		return nil, 0, err
	}

	jobs := []types.GitHubRunnerJob{}
	err := r.client.SelectContext(ctx, &jobs, `SELECT * FROM github_runner_job WHERE workspace_id=$1 ORDER BY id DESC LIMIT $2 OFFSET $3`, ws, limit, offset)
	return jobs, total, err
}

func (r *PostgresBackendRepository) ListDueGitHubRunnerJobs(ctx context.Context) ([]types.GitHubRunnerJob, error) {
	jobs := []types.GitHubRunnerJob{}
	err := r.client.SelectContext(ctx, &jobs, `SELECT j.* FROM github_runner_job j JOIN github_runner_installation i
 ON i.id=j.installation_id AND i.workspace_id=j.workspace_id WHERE j.provision_at<=NOW() ORDER BY j.provision_at LIMIT 100`)
	return jobs, err
}

// ClaimGitHubRunnerJob names a job's next runner before it exists, so a crash
// cannot leave an unreferenced one, and schedules a retry in case it fails.
func (r *PostgresBackendRepository) ClaimGitHubRunnerJob(ctx context.Context, job *types.GitHubRunnerJob, vm string) (bool, error) {
	result, err := r.client.ExecContext(ctx, `UPDATE github_runner_job SET vm=$3, attempts=attempts+1,
 provision_at=NOW()+(attempts+1)*INTERVAL '1 minute' WHERE id=$1 AND attempts=$2 AND provision_at<=NOW()`, job.ID, job.Attempts, vm)
	if err != nil {
		return false, err
	}

	count, err := result.RowsAffected()
	if count == 1 {
		job.VM, job.Attempts = vm, job.Attempts+1
	}
	return count == 1, err
}

// FinishGitHubRunnerJob stops provisioning a job, or keeps the retry its claim
// scheduled.
func (r *PostgresBackendRepository) FinishGitHubRunnerJob(ctx context.Context, id int64, retry bool, message string) error {
	_, err := r.client.ExecContext(ctx, `UPDATE github_runner_job SET provision_at=CASE WHEN $2 THEN provision_at END, error=$3 WHERE id=$1`, id, retry, message)
	return err
}

func (r *PostgresBackendRepository) GetGitHubRunner(ctx context.Context, ws uint, vm string) (*types.GitHubRunner, error) {
	return getRow[types.GitHubRunner](ctx, r, `SELECT j.installation_id, j.repository,
 EXISTS (SELECT 1 FROM github_runner_installation i WHERE i.id=j.installation_id AND i.workspace_id=j.workspace_id) connected,
 COALESCE((SELECT a.status FROM github_runner_job a WHERE a.workspace_id=j.workspace_id AND a.runner_name=j.vm LIMIT 1),'') job_status
 FROM github_runner_job j WHERE j.workspace_id=$1 AND j.vm=$2`, ws, vm)
}

// ReleaseGitHubRunner deletes a runner VM that stopped before GitHub gave it a
// job, and starts another, as a different queued job may have been counting
// on it. One statement does both, so neither is lost.
func (r *PostgresBackendRepository) ReleaseGitHubRunner(ctx context.Context, ws uint, vm, message string) error {
	_, err := r.client.ExecContext(ctx, `WITH deleted AS (UPDATE persistent_vm SET data=jsonb_set(data,'{desired_state}','"deleted"')
 WHERE workspace_id=$1 AND name=$2 AND data->>'desired_state'<>'deleted')
 UPDATE github_runner_job SET provision_at=NOW(), error=$3 WHERE workspace_id=$1 AND vm=$2`, ws, vm, message)
	return err
}

// getRow returns nil, nil when no row matches.
func getRow[T any](ctx context.Context, r *PostgresBackendRepository, query string, args ...any) (*T, error) {
	var row T
	err := r.client.GetContext(ctx, &row, query, args...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}
