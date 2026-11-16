package repository

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Run against a migrated development database only, as the VM integration
// tests are. Every row belongs to a workspace made for the test, and is
// removed on completion.
func TestGitHubRunnerRepositoryIntegration(t *testing.T) {
	repo := newVMIntegrationRepo(t)
	db := repo.client
	ctx := context.Background()
	newWorkspace := func() uint {
		var id uint
		require.NoError(t, db.Get(&id, `INSERT INTO workspace (name) VALUES ($1) RETURNING id`, "github-runner-test-"+uuid.NewString()))
		t.Cleanup(func() {
			_, _ = db.Exec(`DELETE FROM github_runner_job WHERE workspace_id=$1`, id)
			_, _ = db.Exec(`DELETE FROM github_runner_installation WHERE workspace_id=$1`, id)
			_, _ = db.Exec(`DELETE FROM persistent_vm WHERE workspace_id=$1`, id)
			_, _ = db.Exec(`DELETE FROM workspace WHERE id=$1`, id)
		})
		return id
	}
	ws, other := newWorkspace(), newWorkspace()

	// GitHub's IDs are far below the clock's, so these collide with nothing.
	id := time.Now().UnixNano()
	none, err := repo.GetGitHubRunnerInstallation(ctx, ws)
	require.NoError(t, err)
	require.Nil(t, none)
	none, err = repo.GetGitHubRunnerInstallationById(ctx, id)
	require.NoError(t, err)
	require.Nil(t, none)

	// A workspace connects one installation, and an installation one workspace.
	installation := types.GitHubRunnerInstallation{ID: id, WorkspaceID: ws, TokenID: "token", Account: "acme", AccountType: "Organization"}
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &installation))
	installation.TokenID = "new-token"
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &installation), "connecting again refreshes the token")
	moved, second := installation, installation
	moved.WorkspaceID, second.ID = other, id+1
	require.ErrorIs(t, repo.SaveGitHubRunnerInstallation(ctx, &moved), ErrGitHubRunnerConflict)
	require.ErrorIs(t, repo.SaveGitHubRunnerInstallation(ctx, &second), ErrGitHubRunnerConflict)
	saved, err := repo.GetGitHubRunnerInstallation(ctx, ws)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), saved.CreatedAt, time.Minute)
	installation.CreatedAt = saved.CreatedAt
	require.Equal(t, &installation, saved)
	saved, err = repo.GetGitHubRunnerInstallationById(ctx, id)
	require.NoError(t, err)
	require.Equal(t, &installation, saved)

	job := func(id int64, status, runner string) *types.GitHubRunnerJob {
		return &types.GitHubRunnerJob{ID: id, InstallationID: installation.ID, WorkspaceID: ws, Repository: "acme/repo", Workflow: "CI", Name: "test",
			URL: "https://github.com/acme/repo/actions/runs/1/job/1", Label: "beam-4vcpu-ubuntu-2404", Status: status, RunnerName: runner,
			CreatedAt: time.Now().UTC().Truncate(time.Second)}
	}
	stored := func(id int64) types.GitHubRunnerJob {
		jobs, _, err := repo.ListGitHubRunnerJobs(ctx, ws, 100, 0)
		require.NoError(t, err)
		index := slices.IndexFunc(jobs, func(job types.GitHubRunnerJob) bool { return job.ID == id })
		require.GreaterOrEqual(t, index, 0)
		return jobs[index]
	}
	isDue := func(id int64) bool {
		due, err := repo.ListDueGitHubRunnerJobs(ctx)
		require.NoError(t, err)
		return slices.ContainsFunc(due, func(job types.GitHubRunnerJob) bool { return job.ID == id })
	}

	// A claim gives a due job to one caller, and schedules its retry.
	queued := job(id, types.GitHubJobQueued, "")
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, queued))
	require.True(t, isDue(id))
	first := stored(id)
	require.Equal(t, queued.CreatedAt, first.CreatedAt.UTC())
	first.CreatedAt, first.ProvisionAt = queued.CreatedAt, nil
	require.Equal(t, *queued, first)
	stale := first
	claimed, err := repo.ClaimGitHubRunnerJob(ctx, &first, "ghr-a")
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, "ghr-a", first.VM)
	require.Equal(t, 1, first.Attempts)
	claimed, err = repo.ClaimGitHubRunnerJob(ctx, &stale, "ghr-b")
	require.NoError(t, err)
	require.False(t, claimed)
	require.Empty(t, stale.VM)
	require.False(t, isDue(id))
	require.NoError(t, repo.FailGitHubRunnerJob(ctx, id, "boom"))
	failed := stored(id)
	require.Equal(t, "boom", failed.Error)
	require.WithinDuration(t, time.Now().Add(time.Minute), *failed.ProvisionAt, 10*time.Second, "a failed attempt keeps its retry")
	require.NoError(t, repo.FinishGitHubRunnerJob(ctx, id, ""))
	finished := stored(id)
	require.Nil(t, finished.ProvisionAt)
	require.Empty(t, finished.Error)
	require.Equal(t, "ghr-a", finished.VM)

	// GitHub ran another job on this job's runner. That job is due when it
	// is first seen, whatever its status, and a late delivery changes nothing.
	ran := job(id+1, types.GitHubJobInProgress, "ghr-a")
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, ran))
	require.True(t, isDue(id+1))
	require.NoError(t, repo.FinishGitHubRunnerJob(ctx, id+1, ""))
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, job(id+1, types.GitHubJobQueued, "")))
	require.False(t, isDue(id+1))
	require.Equal(t, types.GitHubJobInProgress, stored(id+1).Status, "a job's status never moves backwards")
	require.Equal(t, "ghr-a", stored(id+1).RunnerName)
	stolen := job(id+1, types.GitHubJobCompleted, "ghr-a")
	stolen.WorkspaceID = other
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, stolen))
	require.Equal(t, types.GitHubJobInProgress, stored(id+1).Status, "a job stays in its workspace")

	runner, err := repo.GetGitHubRunner(ctx, ws, "ghr-a")
	require.NoError(t, err)
	require.Equal(t, &types.GitHubRunner{InstallationID: id, Repository: "acme/repo", Connected: true, JobStatus: types.GitHubJobInProgress}, runner)
	for _, missing := range []struct {
		workspace uint
		vm        string
	}{{other, "ghr-a"}, {ws, "ghr-b"}, {ws, ""}} {
		runner, err = repo.GetGitHubRunner(ctx, missing.workspace, missing.vm)
		require.NoError(t, err)
		require.Nil(t, runner)
	}

	// Replacing a runner deletes its VM and makes its job due, at once.
	vm := &types.VM{ID: uuid.NewString(), WorkspaceID: ws, WorkspaceExternalID: "test", TokenID: "test", Name: "ghr-a",
		Handle: uuid.NewString(), DesiredState: types.VMStateRunning, Status: types.VMStateRunning, LastActiveAt: time.Now()}
	require.NoError(t, repo.CreateVM(ctx, vm))
	require.NoError(t, repo.ReplaceGitHubRunner(ctx, ws, "ghr-a", "stopped"))
	require.True(t, isDue(id))
	require.Equal(t, "stopped", stored(id).Error)
	vm, err = repo.GetVM(ctx, ws, "ghr-a")
	require.NoError(t, err)
	require.Equal(t, types.VMStateDeleted, vm.DesiredState)
	require.Equal(t, types.VMStateRunning, vm.Status)

	// History is newest first.
	jobs, total, err := repo.ListGitHubRunnerJobs(ctx, ws, 1, 0)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, jobs, 1)
	require.Equal(t, id+1, jobs[0].ID)
	jobs, _, err = repo.ListGitHubRunnerJobs(ctx, ws, 1, 1)
	require.NoError(t, err)
	require.Equal(t, id, jobs[0].ID)
	for _, page := range [][2]int{{0, 0}, {1, 2}} {
		jobs, total, err = repo.ListGitHubRunnerJobs(ctx, ws, page[0], page[1])
		require.NoError(t, err)
		require.Equal(t, 2, total)
		require.Empty(t, jobs)
	}
	_, total, err = repo.ListGitHubRunnerJobs(ctx, other, 1, 0)
	require.NoError(t, err)
	require.Zero(t, total)

	// Disconnecting starts nothing more. Nothing reports how the jobs it
	// leaves unfinished end, so the history is the ones that finished.
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, job(id+1, types.GitHubJobCompleted, "ghr-a")))
	require.NoError(t, repo.DeleteGitHubRunnerInstallation(ctx, id))
	require.False(t, isDue(id))
	none, err = repo.GetGitHubRunnerInstallation(ctx, ws)
	require.NoError(t, err)
	require.Nil(t, none)
	runner, err = repo.GetGitHubRunner(ctx, ws, "ghr-a")
	require.NoError(t, err)
	require.False(t, runner.Connected, "its runners are still known, to be deleted")
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, job(id+2, types.GitHubJobQueued, "")))
	require.False(t, isDue(id+2), "jobs of a disconnected account are not due")
	jobs, total, err = repo.ListGitHubRunnerJobs(ctx, ws, 100, 0)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, jobs, 1)
	require.Equal(t, id+1, jobs[0].ID)

	// Connecting again shows them, without starting what was given up.
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &installation))
	_, total, err = repo.ListGitHubRunnerJobs(ctx, ws, 0, 0)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Nil(t, stored(id).ProvisionAt)
	require.False(t, isDue(id))
}
