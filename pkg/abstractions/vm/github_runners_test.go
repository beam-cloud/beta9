package vm

import (
	"bytes"
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

var runnerKey = sync.OnceValue(func() *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return key
})

func at(t time.Time) *time.Time { return &t }

// runnerStore adds GitHub runners to the VMs in memory, with one installation.
type runnerStore struct {
	*vmStore
	installation *types.GitHubRunnerInstallation
	jobs         map[int64]*types.GitHubRunnerJob
	// The cache disks of the workspace, and the generations that workers
	// published of them (see github_runner_cache_test.go).
	caches    []*types.GitHubRunnerCache
	published []*publishedCache
	// The most runners that is set for a workspace, where one is.
	limits map[uint]int
}

func (r *runnerStore) GetGitHubRunnerLimit(_ context.Context, ws uint, limit int) (int, error) {
	if set, ok := r.limits[ws]; ok {
		limit = set
	}
	return limit, nil
}

func (r *runnerStore) SetGitHubRunnerLimit(_ context.Context, ws uint, limit *int) error {
	if limit == nil {
		delete(r.limits, ws)
		return nil
	}
	if r.limits == nil {
		r.limits = map[uint]int{}
	}
	r.limits[ws] = *limit
	return nil
}

func (r *runnerStore) connected(id int64) bool {
	return r.installation != nil && r.installation.ID == id
}

func (r *runnerStore) GetGitHubRunnerInstallation(_ context.Context, ws uint) (*types.GitHubRunnerInstallation, error) {
	if r.installation != nil && r.installation.WorkspaceID == ws {
		return r.installation, nil
	}
	return nil, nil
}

func (r *runnerStore) GetGitHubRunnerInstallationById(_ context.Context, id int64) (*types.GitHubRunnerInstallation, error) {
	if r.connected(id) {
		return r.installation, nil
	}
	return nil, nil
}

func (r *runnerStore) SaveGitHubRunnerInstallation(_ context.Context, i *types.GitHubRunnerInstallation) error {
	if r.installation != nil && (r.installation.ID == i.ID) != (r.installation.WorkspaceID == i.WorkspaceID) {
		return repository.ErrGitHubRunnerConflict
	}
	saved := *i
	saved.Paused = r.installation != nil && r.installation.Paused
	// A new connection has the cache, and connecting again leaves it as it is.
	saved.EnhancedCache = r.installation == nil || r.installation.EnhancedCache
	r.installation = &saved
	return nil
}

func (r *runnerStore) PauseGitHubRunnerInstallation(ctx context.Context, ws uint, paused bool) (*types.GitHubRunnerInstallation, error) {
	installation, _ := r.GetGitHubRunnerInstallation(ctx, ws)
	if installation != nil {
		installation.Paused = paused
	}
	return installation, nil
}

func (r *runnerStore) DeleteGitHubRunnerInstallation(_ context.Context, id int64) error {
	if r.connected(id) {
		r.installation = nil
	}
	return nil
}

func (r *runnerStore) SaveGitHubRunnerJob(_ context.Context, job *types.GitHubRunnerJob) error {
	rank := map[string]int{types.GitHubJobQueued: 1, types.GitHubJobInProgress: 2, types.GitHubJobCompleted: 3}
	if old, ok := r.jobs[job.ID]; ok {
		if rank[old.Status] <= rank[job.Status] {
			old.Status, old.Conclusion, old.RunnerName = job.Status, job.Conclusion, job.RunnerName
			old.RunID = cmp.Or(job.RunID, old.RunID)
		}
		return nil
	}
	saved := *job
	saved.ProvisionAt, saved.ReceivedAt = at(time.Now()), at(time.Now())
	r.jobs[job.ID] = &saved
	return nil
}

func (r *runnerStore) due(job *types.GitHubRunnerJob) bool {
	return job.ProvisionAt != nil && !job.ProvisionAt.After(time.Now())
}

// retry is when a job is due again after an attempt at its runner failed.
func (r *runnerStore) retry(job *types.GitHubRunnerJob) *time.Time {
	return at(time.Now().Add(min(time.Minute, 5*time.Second<<(max(min(job.Attempts, 5), 1)-1))))
}

func (r *runnerStore) vm(ws uint, name string) *types.VM {
	for _, v := range r.rows {
		if v.WorkspaceID == ws && v.Name == name {
			return v
		}
	}
	return nil
}

func (r *runnerStore) listed(job *types.GitHubRunnerJob, ws uint) bool {
	return job.WorkspaceID == ws && (job.Status == types.GitHubJobCompleted || r.connected(job.InstallationID))
}

func (r *runnerStore) GetGitHubRunnerJob(_ context.Context, ws uint, id int64) (*types.GitHubRunnerJob, error) {
	if job, ok := r.jobs[id]; ok && r.listed(job, ws) {
		found := *job
		return &found, nil
	}
	return nil, nil
}

func (r *runnerStore) ListActiveGitHubRunnerRuns(_ context.Context, ws uint, repository string) ([]types.GitHubRunnerRun, int, error) {
	runs := []types.GitHubRunnerRun{}
	for _, job := range r.jobs {
		run := types.GitHubRunnerRun{InstallationID: job.InstallationID, Repository: job.Repository, RunID: job.RunID}
		if r.listed(job, ws) && job.Status != types.GitHubJobCompleted && job.RunID != 0 &&
			(repository == "" || job.Repository == repository) && !slices.Contains(runs, run) {
			runs = append(runs, run)
		}
	}
	slices.SortFunc(runs, func(a, b types.GitHubRunnerRun) int { return cmp.Compare(a.RunID, b.RunID) })
	return runs[:min(len(runs), 100)], len(runs), nil
}

func (r *runnerStore) ListGitHubRunnerJobs(_ context.Context, ws uint, filter types.GitHubRunnerJobFilter) (*types.GitHubRunnerJobPage, error) {
	jobs, repositories := []types.GitHubRunnerJob{}, []string{}
	search := strings.ToLower(filter.Search)
	for _, job := range r.jobs {
		if !r.listed(job, ws) {
			continue
		}

		repositories = append(repositories, job.Repository)
		if (filter.Repository == "" || job.Repository == filter.Repository) &&
			strings.Contains(strings.ToLower(job.Name+"\n"+job.Workflow+"\n"+job.Repository), search) &&
			(len(filter.Statuses) == 0 || slices.Contains(filter.Statuses, job.Status)) &&
			(len(filter.Conclusions) == 0 || slices.Contains(filter.Conclusions, job.Conclusion)) {
			jobs = append(jobs, *job)
		}
	}
	slices.SortFunc(jobs, func(a, b types.GitHubRunnerJob) int { return int(b.ID - a.ID) })
	slices.Sort(repositories)
	return &types.GitHubRunnerJobPage{
		Jobs:         jobs[min(filter.Offset, len(jobs)):min(filter.Offset+filter.Limit, len(jobs))],
		Total:        len(jobs),
		Repositories: slices.Compact(repositories),
	}, nil
}

// longerDue orders jobs as they are due: the job due longest first.
func longerDue(a, b types.GitHubRunnerJob) int {
	return cmp.Or(a.ProvisionAt.Compare(*b.ProvisionAt), cmp.Compare(a.ID, b.ID))
}

// pool is the jobs that the runners of one repository and label can take.
type runnerPool struct {
	repository, label string
}

func (r *runnerStore) in(pool runnerPool, job *types.GitHubRunnerJob) bool {
	return job.WorkspaceID == 7 && job.Repository == pool.repository && job.Label == pool.label
}

// alive reports whether the VM of a name exists.
func (r *runnerStore) alive(name string) bool {
	v := r.vm(7, name)
	return name != "" && v != nil && v.DesiredState != types.VMStateDeleted
}

// taken reports whether GitHub gave the runner of a name a job.
func (r *runnerStore) taken(name string) bool {
	for _, job := range r.jobs {
		if name != "" && job.RunnerName == name {
			return true
		}
	}
	return false
}

// busy counts the runners that are running a job of a pool.
func (r *runnerStore) busy(pool runnerPool) int {
	count := 0
	for _, job := range r.jobs {
		if r.in(pool, job) && job.Status == types.GitHubJobInProgress && r.alive(job.RunnerName) {
			count++
		}
	}
	return count
}

// RearmGitHubRunnerJobs makes the queued jobs whose runner is gone due again,
// oldest first, for as many runners as their pool is short of.
func (r *runnerStore) RearmGitHubRunnerJobs(_ context.Context, reserve int, held string) error {
	if r.installation == nil || r.installation.Paused {
		return nil
	}

	orphans := map[runnerPool][]*types.GitHubRunnerJob{}
	for _, job := range r.jobs {
		if r.connected(job.InstallationID) && job.Status == types.GitHubJobQueued && job.RunnerName == "" &&
			job.ProvisionAt == nil && job.Error == "" && job.VM != "" && !r.alive(job.VM) {
			pool := runnerPool{job.Repository, job.Label}
			orphans[pool] = append(orphans[pool], job)
		}
	}

	for pool, jobs := range orphans {
		queued, holds, live, pending := 0, 0, 0, 0
		for _, job := range r.jobs {
			if !r.in(pool, job) {
				continue
			}

			waits := job.Status == types.GitHubJobQueued && job.RunnerName == ""
			if waits && (job.Error == "" || job.ProvisionAt != nil) {
				queued++
			}
			if waits && job.Waiting == held {
				holds++
			}
			if v := r.vm(7, job.VM); r.alive(job.VM) && v.DesiredState == types.VMStateRunning && !r.taken(job.VM) {
				live++
			}
			if job.ProvisionAt != nil && !r.alive(job.VM) && (job.Status == types.GitHubJobQueued || strings.HasPrefix(job.RunnerName, runnerPrefix)) {
				pending++
			}
		}

		wanted := queued
		if holds > 0 {
			wanted = min(queued, max(reserve, r.busy(pool)))
		}

		slices.SortFunc(jobs, func(a, b *types.GitHubRunnerJob) int {
			return cmp.Or(a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
		})
		for _, job := range jobs[:max(min(wanted-live-pending, len(jobs)), 0)] {
			job.ProvisionAt, job.Waiting = at(time.Now()), ""
		}
	}
	return nil
}

func (r *runnerStore) runnerJob(ws uint, vm string) *types.GitHubRunnerJob {
	for _, job := range r.jobs {
		if job.WorkspaceID == ws && job.VM == vm && vm != "" {
			return job
		}
	}
	return nil
}

func (r *runnerStore) GetGitHubRunnerPool(_ context.Context, ws uint, vm string) (*types.GitHubRunnerPool, error) {
	runner := r.runnerJob(ws, vm)
	if runner == nil {
		return nil, nil
	}

	pool := runnerPool{runner.Repository, runner.Label}
	found := &types.GitHubRunnerPool{Busy: r.busy(pool), Ahead: []string{}}
	for _, job := range r.jobs {
		if !r.in(pool, job) {
			continue
		}
		if job.Status == types.GitHubJobQueued {
			found.Queued++
			if job.RunnerName == "" && job.ProvisionAt == nil && job.Error == "" && job.Waiting == "" {
				found.Unheld++
			}
		}
		if job.VM != "" && job.VM < vm && r.idle(job) {
			found.Ahead = append(found.Ahead, job.VM)
		}
	}
	slices.Sort(found.Ahead)
	return found, nil
}

func (r *runnerStore) HoldGitHubRunnerJobs(_ context.Context, ws uint, vm, waiting string, freeSince time.Time) error {
	runner := r.runnerJob(ws, vm)
	if runner == nil {
		return nil
	}

	for _, job := range r.jobs {
		if r.in(runnerPool{runner.Repository, runner.Label}, job) && job.Status == types.GitHubJobQueued && job.RunnerName == "" &&
			job.ProvisionAt == nil && job.Error == "" && job.Waiting == "" {
			job.Waiting = waiting
			if job.RunnerReadyAt == nil {
				job.RunnerReadyAt = at(maxTime(*cmp.Or(job.ReceivedAt, &job.CreatedAt), freeSince))
			}
		}
	}
	return nil
}

func (r *runnerStore) AdoptGitHubRunner(_ context.Context, ws uint, id int64, runner string) error {
	taken, started := r.jobs[id], r.runnerJob(ws, runner)
	if taken == nil || started == nil || started == taken || taken.WorkspaceID != ws || taken.VM == "" {
		return nil
	}

	taken.VM, started.VM = started.VM, taken.VM
	taken.RegisteredAt, started.RegisteredAt = started.RegisteredAt, taken.RegisteredAt
	taken.VMReadyAt, started.VMReadyAt = started.VMReadyAt, taken.VMReadyAt
	if taken.RunnerReadyAt == nil && taken.VMReadyAt != nil {
		taken.RunnerReadyAt = at(maxTime(*cmp.Or(taken.ReceivedAt, &taken.CreatedAt), *taken.VMReadyAt))
	}
	return nil
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (r *runnerStore) ListDueGitHubRunnerJobs(ctx context.Context, limit int) ([]types.GitHubRunnerJob, error) {
	due := []types.GitHubRunnerJob{}
	for _, job := range r.jobs {
		running, _ := r.CountGitHubRunners(ctx, job.WorkspaceID)
		limit, _ := r.GetGitHubRunnerLimit(ctx, job.WorkspaceID, limit)
		if r.due(job) && r.connected(job.InstallationID) && !r.installation.Paused && (limit <= 0 || running < limit) {
			due = append(due, *job)
		}
	}
	slices.SortFunc(due, longerDue)
	return due, nil
}

func (r *runnerStore) ClaimGitHubRunnerJob(ctx context.Context, job *types.GitHubRunnerJob, vm string, limit int) (bool, error) {
	stored := r.jobs[job.ID]
	if stored.Attempts != job.Attempts || !r.due(stored) || r.connected(stored.InstallationID) && r.installation.Paused {
		return false, nil
	}

	// The jobs of the account that have been due longer count as runners.
	places, _ := r.CountGitHubRunners(ctx, stored.WorkspaceID)
	for _, other := range r.jobs {
		if other.WorkspaceID == stored.WorkspaceID && other.InstallationID == stored.InstallationID && r.due(other) && longerDue(*other, *stored) < 0 {
			places++
		}
	}
	if limit, _ := r.GetGitHubRunnerLimit(ctx, stored.WorkspaceID, limit); limit > 0 && places >= limit {
		return false, nil
	}

	stored.VM, stored.Attempts, stored.Waiting = vm, stored.Attempts+1, ""
	stored.ProvisionAt, stored.RegisteredAt, stored.VMReadyAt = at(time.Now().Add(time.Minute)), nil, nil
	job.VM, job.Attempts = vm, stored.Attempts
	return true, nil
}

func (r *runnerStore) FinishGitHubRunnerJob(_ context.Context, id int64, message string) error {
	r.jobs[id].ProvisionAt, r.jobs[id].Error, r.jobs[id].Waiting = nil, message, ""
	return nil
}

func (r *runnerStore) FailGitHubRunnerJob(_ context.Context, id int64, message, waiting string) error {
	if job := r.jobs[id]; job.ProvisionAt != nil {
		job.ProvisionAt, job.Error, job.Waiting = r.retry(job), message, waiting
	}
	return nil
}

func (r *runnerStore) WaitGitHubRunnerJob(_ context.Context, id int64, waiting string, until time.Time) error {
	if job := r.jobs[id]; r.due(job) {
		job.ProvisionAt, job.Waiting = &until, waiting
	}
	return nil
}

func (r *runnerStore) MarkGitHubRunnerRegistered(_ context.Context, id int64) error {
	r.jobs[id].RegisteredAt = at(time.Now())
	return nil
}

// CountGitHubRunners counts the runner VMs that exist, and the runners that
// a claim has named and is starting.
func (r *runnerStore) CountGitHubRunners(_ context.Context, ws uint) (int, error) {
	count := 0
	for _, job := range r.jobs {
		if job.WorkspaceID != ws || job.VM == "" {
			continue
		}
		v := r.vm(ws, job.VM)
		starting := v == nil && job.Waiting == "" && job.ProvisionAt != nil && job.ProvisionAt.After(time.Now())
		if starting || v != nil && v.DesiredState != types.VMStateDeleted {
			count++
		}
	}
	return count, nil
}

func (r *runnerStore) GetGitHubRunner(_ context.Context, ws uint, vm string) (*types.GitHubRunner, error) {
	for _, job := range r.jobs {
		if job.WorkspaceID == ws && job.VM == vm {
			runner := &types.GitHubRunner{InstallationID: job.InstallationID, Repository: job.Repository, Connected: r.connected(job.InstallationID),
				Waiting: job.Waiting, Ready: job.VMReadyAt != nil, Queued: job.Status == types.GitHubJobQueued}
			for _, assigned := range r.jobs {
				if assigned.WorkspaceID == ws && assigned.RunnerName == vm {
					runner.JobStatus = assigned.Status
				}
			}
			return runner, nil
		}
	}
	return nil, nil
}

func (r *runnerStore) ObserveGitHubRunner(_ context.Context, ws uint, vm, waiting string, readyAt *time.Time) error {
	for _, job := range r.jobs {
		if job.WorkspaceID == ws && job.VM == vm {
			job.Waiting, job.VMReadyAt, job.RunnerReadyAt = waiting, cmp.Or(job.VMReadyAt, readyAt), cmp.Or(job.RunnerReadyAt, readyAt)
		}
	}
	return nil
}

// idle reports whether a job's runner is up and GitHub has given it no job.
func (r *runnerStore) idle(job *types.GitHubRunnerJob) bool {
	v := r.vm(job.WorkspaceID, job.VM)
	if v == nil || v.DesiredState == types.VMStateDeleted || v.Status != types.VMStateRunning {
		return false
	}
	for _, other := range r.jobs {
		if other.WorkspaceID == job.WorkspaceID && other.RunnerName == job.VM {
			return false
		}
	}
	return true
}

func (r *runnerStore) IsSurplusGitHubRunner(_ context.Context, ws uint, vm string) (bool, error) {
	var runner *types.GitHubRunnerJob
	for _, job := range r.jobs {
		if job.WorkspaceID == ws && job.VM == vm {
			runner = job
		}
	}
	if runner == nil {
		return false, nil
	}

	queued, idleBefore := 0, 0
	for _, job := range r.jobs {
		if job.WorkspaceID != ws || job.Repository != runner.Repository || job.Label != runner.Label {
			continue
		}
		if job.Status == types.GitHubJobQueued {
			queued++
		}
		if job.VM != "" && job.VM < vm && r.idle(job) {
			idleBefore++
		}
	}
	return queued <= idleBefore, nil
}

func (r *runnerStore) ReplaceGitHubRunner(_ context.Context, ws uint, vm, message, waiting string) error {
	for _, v := range r.rows {
		if v.WorkspaceID == ws && v.Name == vm {
			v.DesiredState = types.VMStateDeleted
		}
	}
	for _, job := range r.jobs {
		if job.WorkspaceID == ws && job.VM == vm {
			job.ProvisionAt, job.Error, job.Waiting, job.RunnerReadyAt = r.retry(job), message, waiting, nil
		}
	}
	return nil
}

func (b *vmBackend) GetWorkspace(_ context.Context, id uint) (*types.Workspace, error) {
	return &types.Workspace{Id: id, ExternalId: "workspace"}, nil
}

var runnerConfig = types.GitHubRunnerConfig{AppID: "1", AppSlug: "beam-runners", ClientID: "client", ClientSecret: "client-secret",
	WebhookSecret: "webhook-secret", ImageID: "runner-image"}

// runnerFixture is a workspace connected to the organization acme, through
// installation 42 of an App whose token for it is cached.
func runnerFixture(t *testing.T) (*githubRunners, *runnerStore, *auth.AuthInfo, *vmRuntime) {
	t.Helper()
	s, v, info, runtime, _ := fixture()
	store := &runnerStore{vmStore: s.repo.(*vmStore), jobs: map[int64]*types.GitHubRunnerJob{}, installation: &types.GitHubRunnerInstallation{
		ID: 42, WorkspaceID: 7, TokenID: info.Token.ExternalId, Account: "acme", AccountType: "Organization", EnhancedCache: true}}
	delete(store.rows, v.ID)
	s.repo = store
	app := &githubApp{id: runnerConfig.AppID, slug: runnerConfig.AppSlug, clientID: runnerConfig.ClientID, clientSecret: runnerConfig.ClientSecret,
		key: runnerKey(), client: http.DefaultClient, apiURL: "https://api.github.invalid", webURL: "https://github.invalid",
		tokens:    map[int64]githubToken{42: {Token: "installation-token", ExpiresAt: time.Now().Add(time.Hour)}},
		notBefore: map[int64]time.Time{}}
	r := &githubRunners{s: s, config: runnerConfig, github: app, wake: make(chan struct{}, 1), redelivered: map[string]int{}}
	s.runners = r
	return r, store, info, runtime
}

// fakeGitHub answers "METHOD /path" with a "status body" response, and
// returns a log of the requests it served, with their bodies.
func fakeGitHub(t *testing.T, r *githubRunners, routes map[string]string) func() []string {
	t.Helper()
	var mu sync.Mutex
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		key := req.Method + " " + req.URL.Path
		body, _ := io.ReadAll(req.Body)
		mu.Lock()
		calls = append(calls, strings.TrimSpace(key+" "+string(body)))
		mu.Unlock()
		response, ok := routes[key]
		if !ok {
			t.Errorf("unexpected GitHub request %s", key)
			response = "404 {}"
		}
		code, payload, _ := strings.Cut(response, " ")
		status, _ := strconv.Atoi(code)
		w.WriteHeader(status)
		fmt.Fprint(w, payload)
	}))
	t.Cleanup(server.Close)
	r.github.apiURL, r.github.webURL = server.URL, server.URL
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(calls)
	}
}

var beamLabels = []string{"beam-4vcpu-ubuntu-2404"}

// A runner whose guest is killed in its job, as when the job takes more
// memory than the VM has, exits as a failed finalization does. It keeps
// nothing to recover, and is deleted as any runner that is done.
func TestGitHubRunnerKilledInItsJobIsDeleted(t *testing.T) {
	r, store, _, runtime := runnerFixture(t)
	ctx := context.Background()
	fakeGitHub(t, r, map[string]string{registersRunner: `201 {"encoded_jit_config":"jit-secret"}`})
	v := startRunner(t, r, store, 1, false)
	running := *store.jobs[1]
	running.Status, running.RunnerName = "in_progress", v.Name
	require.Equal(t, 202, deliver(t, r, "workflow_job", "in_progress", beamLabels, &running, true))

	delete(runtime.containers.states, v.ContainerID)
	runtime.containers.exitCode = int(types.ContainerExitCodeUnknownError)
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.Equal(t, types.VMStateDeleted, v.Status)
	require.Empty(t, v.ContainerID)
}

// A worker that is lost with a runner on it reports no exit for it, ever.
// The runner kept nothing there to wait for, so it is deleted all the same,
// and its place in the workspace's limit with it. Redis being out of reach
// is not that: then nothing is known, and the runner waits.
func TestGitHubRunnerWhoseWorkerIsLostIsDeleted(t *testing.T) {
	for name, known := range map[string]bool{"no exit was reported": true, "nothing can be read": false} {
		t.Run(name, func(t *testing.T) {
			r, store, _, runtime := runnerFixture(t)
			ctx := context.Background()
			fakeGitHub(t, r, map[string]string{registersRunner: `201 {"encoded_jit_config":"jit-secret"}`})
			v := startRunner(t, r, store, 1, false)
			running := *store.jobs[1]
			running.Status, running.RunnerName = "in_progress", v.Name
			require.Equal(t, 202, deliver(t, r, "workflow_job", "in_progress", beamLabels, &running, true))

			delete(runtime.containers.states, v.ContainerID)
			runtime.containers.exitError = redis.Nil
			if !known {
				runtime.containers.exitError = errors.New("redis: connection refused")
			}

			err := r.s.reconcileVM(ctx, v)
			if !known {
				require.ErrorContains(t, err, "unable to confirm final disk sync")
				require.NotEmpty(t, v.ContainerID)
				return
			}
			require.NoError(t, err)
			require.Equal(t, types.VMStateDeleted, v.Status)
			require.Empty(t, v.ContainerID)
			count, err := store.CountGitHubRunners(ctx, v.WorkspaceID)
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
}

func queuedJob(id int64) *types.GitHubRunnerJob {
	return &types.GitHubRunnerJob{ID: id, InstallationID: 42, WorkspaceID: 7, Repository: "acme/repo", Label: beamLabels[0], Status: types.GitHubJobQueued}
}

// deliver sends a webhook the way GitHub does, and returns the response code.
func deliver(t *testing.T, r *githubRunners, event, action string, labels []string, job *types.GitHubRunnerJob, signed bool) int {
	t.Helper()
	// GitHub sends null for what a job does not have yet.
	nullable := func(value string) any {
		if value == "" {
			return nil
		}
		return value
	}
	// A job that is not on the default branch is on a pull request's.
	branch := "feature"
	if job.DefaultBranch {
		branch = "main"
	}
	body, err := json.Marshal(map[string]any{"action": action, "installation": map[string]any{"id": 42},
		"repository": map[string]any{"full_name": "acme/repo", "default_branch": "main"},
		"workflow_job": map[string]any{"id": job.ID, "run_id": job.RunID, "head_branch": branch, "name": "test", "workflow_name": "CI", "html_url": "https://github.com/acme/repo/actions/runs/1/job/1",
			"labels": labels, "runner_name": nullable(job.RunnerName), "conclusion": nullable(job.Conclusion),
			"created_at": "2026-11-16T09:00:00Z", "completed_at": nil}})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/vm/github-runners/webhook", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", event)
	if signed {
		mac := hmac.New(sha256.New, []byte(r.config.WebhookSecret))
		mac.Write(body)
		req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	if err := r.webhook(c); err != nil {
		e.HTTPErrorHandler(err, c)
	}
	return rec.Code
}

func TestGitHubRunnerLabels(t *testing.T) {
	r := &githubRunners{}
	label, ok := r.label([]string{"Beam-8vCPU-Ubuntu-2404"})
	require.True(t, ok)
	size, ok := r.sizeFor(label)
	require.True(t, ok)
	require.Equal(t, runnerSize{"beam-8vcpu-ubuntu-2404", 8000, 32768, "50Gi"}, size)

	for _, labels := range [][]string{nil, {"ubuntu-latest"}, {"self-hosted", "linux"}} {
		_, ok := r.label(labels)
		require.False(t, ok, "%v does not ask for Beam", labels)
	}

	// These ask for Beam, but match no runner.
	for _, labels := range [][]string{{"beam-3vcpu-ubuntu-2404"}, {"self-hosted", "beam-4vcpu-ubuntu-2404"}} {
		label, ok := r.label(labels)
		require.True(t, ok, labels)
		_, ok = r.sizeFor(label)
		require.False(t, ok, labels)
	}
}

func TestGitHubRunnerWebhookIntake(t *testing.T) {
	r, store, _, _ := runnerFixture(t)
	require.Equal(t, 401, deliver(t, r, "workflow_job", "queued", beamLabels, queuedJob(1), false))
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", []string{"ubuntu-latest"}, queuedJob(1), true))
	require.Equal(t, 202, deliver(t, r, "workflow_job", "waiting", beamLabels, queuedJob(1), true))
	require.Empty(t, store.jobs)
	require.Empty(t, r.wake)

	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", []string{"Beam-4vCPU-Ubuntu-2404"}, queuedJob(1), true))
	job := store.jobs[1]
	require.NotNil(t, job.ProvisionAt, "a new job is due for a runner")
	require.NotNil(t, job.ReceivedAt, "a new job says when it was received")
	require.Len(t, r.wake, 1)
	require.Equal(t, types.GitHubRunnerJob{ID: 1, InstallationID: 42, WorkspaceID: 7, Repository: "acme/repo", Workflow: "CI", Name: "test",
		URL: "https://github.com/acme/repo/actions/runs/1/job/1", Label: "beam-4vcpu-ubuntu-2404", Status: "queued",
		CreatedAt: time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC), ProvisionAt: job.ProvisionAt, ReceivedAt: job.ReceivedAt}, *job)

	// Deliveries repeat and arrive out of order.
	finished := queuedJob(1)
	finished.RunnerName, finished.Conclusion = "ghr-other", "success"
	require.Equal(t, 202, deliver(t, r, "workflow_job", "completed", beamLabels, finished, true))
	require.Equal(t, 202, deliver(t, r, "workflow_job", "in_progress", beamLabels, queuedJob(1), true))
	require.Equal(t, "completed", job.Status, "late deliveries cannot regress a job")
	require.Equal(t, "ghr-other", job.RunnerName)
	require.Equal(t, "success", job.Conclusion)

	// A job that no runner will match is kept, to say so.
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", []string{"self-hosted", "beam-4vcpu-ubuntu-2404"}, queuedJob(2), true))
	require.Equal(t, "self-hosted, beam-4vcpu-ubuntu-2404", store.jobs[2].Label)

	require.Equal(t, 202, deliver(t, r, "installation", "suspend", nil, queuedJob(0), true))
	require.Nil(t, store.installation)
	require.Empty(t, r.github.tokens)
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", beamLabels, queuedJob(3), true))
	require.NotContains(t, store.jobs, int64(3), "jobs of accounts that are not connected are ignored")
}

func TestGitHubRunnerProvisionsOnce(t *testing.T) {
	r, store, info, runtime := runnerFixture(t)
	ctx := context.Background()
	calls := fakeGitHub(t, r, map[string]string{
		"POST /repos/acme/repo/actions/runners/generate-jitconfig": `201 {"encoded_jit_config":"jit-secret"}`,
		"GET /repos/acme/repo/actions/runners":                     `200 {"runners":[{"id":9}]}`,
		"DELETE /repos/acme/repo/actions/runners/9":                `204 {}`,
	})
	require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(1)))
	due, err := store.ListDueGitHubRunnerJobs(ctx, 0)
	require.NoError(t, err)
	require.NoError(t, r.provision(ctx, due[0]))
	require.NoError(t, r.provision(ctx, due[0]), "a concurrent attempt loses the claim")

	job := store.jobs[1]
	require.Nil(t, job.ProvisionAt)
	require.Len(t, calls(), 1)
	require.Contains(t, calls()[0], `"labels":["beam-4vcpu-ubuntu-2404"]`)
	require.Contains(t, calls()[0], `"name":"`+job.VM+`"`)
	require.Contains(t, calls()[0], `"work_folder":"/home/runner/work"`, "where GitHub's runners keep a job's files")
	require.Len(t, runtime.requests, 1)
	v, err := r.s.repo.GetVM(ctx, 7, job.VM)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(v.Name, runnerPrefix))
	require.True(t, validName.MatchString(v.Name))
	require.Equal(t, info.Token.ExternalId, v.TokenID)
	require.EqualValues(t, 4000, v.Spec.CPU)
	require.EqualValues(t, 16384, v.Spec.Memory)
	require.Equal(t, "53687091200", v.Spec.DiskSize, "50 GiB, as the VM stores it")
	require.True(t, v.Spec.DockerEnabled)
	require.True(t, v.Spec.Headless)
	require.Empty(t, v.Spec.Ports, "a runner takes no port of its machine")
	require.Contains(t, r.s.gateway.(*vmGateway).stub.Env, "BEAM_VM_HEADLESS=1")
	require.False(t, types.CapturesExitSnapshot(v.Spec.CaptureExitSnapshot), "a runner's disk is not uploaded when it exits")
	root := r.s.gateway.(*vmGateway).stub.Disks[0]
	require.Equal(t, rootDisk(v), root.Name)
	require.NotNil(t, root.CaptureExitSnapshot)
	require.False(t, *root.CaptureExitSnapshot)
	require.True(t, types.NewDurableDiskMountConfigFromProto(root).SkipExitSnapshot)
	require.Empty(t, r.s.response(v).TerminalURL)
	require.Equal(t, "runner-image", v.Spec.ImageID)
	require.Equal(t, map[string]string{"github.repository": "acme/repo"}, v.Metadata)
	require.Contains(t, v.Spec.Env, "BEAM_GITHUB_JIT_CONFIG=jit-secret")
	response, err := json.Marshal(r.s.response(v))
	require.NoError(t, err)
	require.NotContains(t, string(response), "jit-secret")

	// A retry of an attempt that died after it created its runner keeps it.
	job.ProvisionAt = at(time.Now())
	require.NoError(t, r.provision(ctx, *job))
	require.Len(t, calls(), 1)
	require.Nil(t, job.ProvisionAt)

	// A runner that stops before it takes a job is replaced, after the time
	// that a job waits to be tried again, and GitHub is told to forget it.
	delete(runtime.containers.states, v.ContainerID)
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.Equal(t, types.VMStateDeleted, v.Status)
	require.Contains(t, job.Error, "stopped before it took a job")
	require.Equal(t, []string{"GET /repos/acme/repo/actions/runners", "DELETE /repos/acme/repo/actions/runners/9"}, calls()[1:])
	require.WithinDuration(t, time.Now().Add(5*time.Second), *job.ProvisionAt, time.Second)
	require.NoError(t, r.provision(ctx, *job))
	require.Len(t, runtime.requests, 1, "a runner that stops every time is not started without a pause")
	job.ProvisionAt = at(time.Now())
	require.NoError(t, r.provision(ctx, *job))
	require.Len(t, runtime.requests, 2)
	require.NotEqual(t, v.Name, job.VM)
	require.Nil(t, job.ProvisionAt)
	require.Empty(t, job.Error)
}

// Every job that waits for a runner, or took one of ours, starts one: a job
// that runs on another job's runner leaves that job without.
func TestGitHubRunnerProvisionsWhatJobsNeed(t *testing.T) {
	for name, tc := range map[string]struct {
		status, runner, label string
		launched              bool
		err                   string
	}{
		"queued":                            {status: "queued", launched: true},
		"cancelled before it took a runner": {status: "completed"},
		"runs on another job's runner":      {status: "in_progress", runner: "ghr-0123456789ab", launched: true},
		"ran on another job's runner":       {status: "completed", runner: "ghr-0123456789ab", launched: true},
		"runs on a runner that is not ours": {status: "in_progress", runner: "self-hosted-1"},
		"asks for a size there is not":      {status: "queued", label: "beam-3vcpu-ubuntu-2404", err: "runs-on must be exactly one of beam-2vcpu-ubuntu-2404, "},
		"asks for more than a size":         {status: "queued", label: "self-hosted, beam-4vcpu-ubuntu-2404", err: "runs-on must be exactly one of "},
	} {
		t.Run(name, func(t *testing.T) {
			r, store, _, runtime := runnerFixture(t)
			ctx := context.Background()
			fakeGitHub(t, r, map[string]string{
				"POST /repos/acme/repo/actions/runners/generate-jitconfig": `201 {"encoded_jit_config":"jit-secret"}`,
			})
			job := queuedJob(1)
			job.Status, job.RunnerName = tc.status, tc.runner
			if tc.label != "" {
				job.Label = tc.label
			}

			// The job is due however it is first seen.
			require.NoError(t, store.SaveGitHubRunnerJob(ctx, job))
			due, err := store.ListDueGitHubRunnerJobs(ctx, 0)
			require.NoError(t, err)
			require.Len(t, due, 1)
			require.NoError(t, r.provision(ctx, due[0]))

			require.Nil(t, store.jobs[1].ProvisionAt)
			require.Equal(t, tc.launched, len(runtime.requests) == 1)
			if tc.err == "" {
				require.Empty(t, store.jobs[1].Error)
			} else {
				require.Contains(t, store.jobs[1].Error, tc.err)
			}
		})
	}
}

// A job waits out what passes, for as long as GitHub has it queued: it is
// tried again after a time that grows to a minute, and is never given up on.
func TestGitHubRunnerProvisionRetriesWhatPasses(t *testing.T) {
	r, store, info, runtime := runnerFixture(t)
	ctx := context.Background()
	r.s.backend.(*vmBackend).token = &types.Token{Active: false}
	require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(1)))
	job := store.jobs[1]
	waits := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, time.Minute, time.Minute}
	for attempt := 1; attempt <= 4*runnerAttempts; attempt++ {
		require.ErrorContains(t, r.provision(ctx, *job), "inactive")
		require.Equal(t, attempt, job.Attempts)
		require.WithinDuration(t, time.Now().Add(waits[min(attempt, len(waits))-1]), *job.ProvisionAt, time.Second, "retries back off, up to a minute")
		require.Contains(t, job.Error, "inactive")
		require.Equal(t, "Retrying", job.Waiting)
		require.Equal(t, "Retrying", r.waiting(*job, false, false))
		require.NoError(t, r.provision(ctx, *job), "a job is not tried before it is due")
		require.Equal(t, attempt, job.Attempts)
		job.ProvisionAt = at(time.Now())
	}
	require.Empty(t, runtime.requests)

	// The job gets its runner once what was wrong has passed.
	r.s.backend.(*vmBackend).token = &types.Token{ExternalId: info.Token.ExternalId, Active: true}
	fakeGitHub(t, r, map[string]string{
		"POST /repos/acme/repo/actions/runners/generate-jitconfig": `201 {"encoded_jit_config":"jit-secret"}`,
	})
	require.NoError(t, r.provision(ctx, *job))
	require.Nil(t, job.ProvisionAt)
	require.Empty(t, job.Error)
	require.Empty(t, job.Waiting)
	require.Len(t, runtime.requests, 1)

	// So does a job that GitHub fails to register a runner for, or refuses
	// one in a way that passes. A token that GitHub no longer takes is not
	// used again.
	for _, response := range []string{`500 {}`, `502 {"message":"Server Error"}`, `401 {"message":"Bad credentials"}`, `422 {"message":"Validation Failed"}`} {
		r, store, _, runtime := runnerFixture(t)
		fakeGitHub(t, r, map[string]string{"POST /repos/acme/repo/actions/runners/generate-jitconfig": response})
		require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(1)))
		require.ErrorContains(t, r.provision(ctx, *store.jobs[1]), "register the runner with GitHub: GitHub returned HTTP "+response[:3], response)
		require.NotNil(t, store.jobs[1].ProvisionAt, response)
		require.Equal(t, "Retrying", store.jobs[1].Waiting, response)
		require.Equal(t, strings.HasPrefix(response, "401"), len(r.github.tokens) == 0, response)
		require.Empty(t, runtime.requests)
	}
}

// What does not pass ends a job, with the reason: GitHub answers the same
// until the account changes something, and an account that disconnected has
// no runners.
func TestGitHubRunnerProvisionEndsOnWhatStands(t *testing.T) {
	for name, tc := range map[string]struct {
		routes       map[string]string
		disconnected bool
		err          string
	}{
		"the App may not register runners": {
			routes: map[string]string{"POST /repos/acme/repo/actions/runners/generate-jitconfig": `403 {"message":"Resource not accessible by integration"}`},
			err:    "register the runner with GitHub: GitHub returned HTTP 403: Resource not accessible by integration"},
		"the App can't reach the repository": {
			routes: map[string]string{"POST /repos/acme/repo/actions/runners/generate-jitconfig": `404 {"message":"Not Found"}`},
			err:    "register the runner with GitHub: GitHub returned HTTP 404: Not Found"},
		"the App is not installed any more": {
			routes: map[string]string{"POST /app/installations/42/access_tokens": `404 {"message":"Not Found"}`},
			err:    "register the runner with GitHub: GitHub returned HTTP 404: Not Found"},
		"the App is suspended": {
			routes: map[string]string{"POST /app/installations/42/access_tokens": `403 {"message":"This installation has been suspended"}`},
			err:    "register the runner with GitHub: GitHub returned HTTP 403: This installation has been suspended"},
		"the account disconnected": {disconnected: true, err: "the GitHub account is disconnected"},
	} {
		t.Run(name, func(t *testing.T) {
			r, store, _, runtime := runnerFixture(t)
			ctx := context.Background()
			calls := fakeGitHub(t, r, tc.routes)
			if _, ok := tc.routes["POST /app/installations/42/access_tokens"]; ok {
				r.github.forget(42)
			}
			require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(1)))
			job := store.jobs[1]
			listed := *job
			if tc.disconnected {
				store.installation = nil
			}

			require.ErrorContains(t, r.provision(ctx, listed), tc.err)
			require.Nil(t, job.ProvisionAt, "no runner is started for the job any more")
			require.Equal(t, tc.err, job.Error)
			require.Equal(t, 1, job.Attempts)
			require.Empty(t, r.waiting(*job, false, false), "the job waits for nothing, and its error says why")
			require.Len(t, calls(), len(tc.routes))
			require.Empty(t, runtime.requests)
		})
	}
}

func TestGitHubRunnerExplainsGitHubRefusals(t *testing.T) {
	r, store, _, _ := runnerFixture(t)
	ctx := context.Background()
	fakeGitHub(t, r, map[string]string{
		"POST /repos/acme/repo/actions/runners/generate-jitconfig": `403 {"message":"Resource not accessible by integration"}`,
	})
	require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(1)))
	require.Error(t, r.provision(ctx, *store.jobs[1]))
	require.Equal(t, "register the runner with GitHub: GitHub returned HTTP 403: Resource not accessible by integration", store.jobs[1].Error)
}

func TestGitHubRunnerVMLifecycle(t *testing.T) {
	assign := func(status string) func(*types.VM, *runnerStore, *githubRunners) {
		return func(v *types.VM, store *runnerStore, _ *githubRunners) {
			store.jobs[2] = &types.GitHubRunnerJob{ID: 2, WorkspaceID: 7, Status: status, RunnerName: v.Name}
		}
	}
	exit := func(v *types.VM, _ *runnerStore, r *githubRunners) {
		delete(r.s.containers.(*vmContainers).states, v.ContainerID)
	}
	idle := func(v *types.VM, _ *runnerStore, _ *githubRunners) {
		v.UpdatedAt = time.Now().Add(-runnerIdle - time.Minute)
	}
	online := `200 {"runners":[{"id":9,"name":"ghr-test","status":"online"}]}`
	for name, tc := range map[string]struct {
		setup                      func(*types.VM, *runnerStore, *githubRunners)
		deleted, replaced, refresh bool
		lookup, deregister         string
	}{
		"running a job": {setup: assign("in_progress")},
		"job completed": {setup: assign("completed"), deleted: true},
		"exited after a job": {setup: func(v *types.VM, store *runnerStore, r *githubRunners) {
			assign("in_progress")(v, store, r)
			exit(v, store, r)
		}, deleted: true},
		"exited before a job": {setup: exit, deregister: "204", deleted: true, replaced: true},
		"failed to launch": {setup: func(v *types.VM, store *runnerStore, r *githubRunners) {
			exit(v, store, r)
			v.ContainerID, v.Status, v.LaunchAttempts = "", types.VMStateError, 5
		}, deregister: "204", deleted: true, replaced: true},
		"idle and offline": {setup: idle, deregister: "204", deleted: true},
		"idle and missing": {setup: idle, lookup: `200 {"runners":[]}`, deleted: true},
		// GitHub has the runner and gives it no job yet: it is the job's.
		"idle and online":          {setup: idle, lookup: online},
		"idle but busy":            {setup: idle, lookup: `200 {"runners":[{"id":9,"name":"ghr-test","status":"offline","busy":true}]}`, refresh: true},
		"idle with unknown status": {setup: idle, lookup: `200 {"runners":[{"id":9,"name":"ghr-test"}]}`, refresh: true},
		"idle but just busy":       {setup: idle, deregister: "422", refresh: true},
		"offline for a while":      {lookup: `200 {"runners":[{"id":9,"name":"ghr-test","status":"offline"}]}`},
		"just started":             {setup: func(v *types.VM, _ *runnerStore, _ *githubRunners) { v.UpdatedAt = time.Now() }},
		"disconnected":             {setup: func(_ *types.VM, store *runnerStore, _ *githubRunners) { store.installation = nil }, deleted: true},
		"disabled":                 {setup: func(_ *types.VM, _ *runnerStore, r *githubRunners) { r.github = nil }, deleted: true},
		"stopped":                  {setup: func(v *types.VM, _ *runnerStore, _ *githubRunners) { v.DesiredState = types.VMStateStopped }, deleted: true},
		"past its lifetime":        {setup: func(v *types.VM, _ *runnerStore, _ *githubRunners) { v.CreatedAt = time.Now().Add(-runnerLifetime) }, deleted: true},
		"named like a runner":      {setup: func(_ *types.VM, store *runnerStore, _ *githubRunners) { store.jobs[1].VM = "ghr-another" }},
		"idle without GitHub":      {setup: idle, deregister: "500"},
		"running before a job":     {lookup: online},
		"account paused":           {setup: func(_ *types.VM, store *runnerStore, _ *githubRunners) { store.installation.Paused = true }, lookup: online},
		"pending a long time": {setup: func(v *types.VM, store *runnerStore, r *githubRunners) {
			idle(v, store, r)
			v.Status = types.VMStateStarting
			r.s.containers.(*vmContainers).states[v.ContainerID].Status = types.ContainerStatusPending
		}},
	} {
		t.Run(name, func(t *testing.T) {
			r, store, _, runtime := runnerFixture(t)
			routes := map[string]string{}
			if tc.lookup != "" || tc.deregister != "" {
				routes["GET /repos/acme/repo/actions/runners"] = cmp.Or(tc.lookup, `200 {"runners":[{"id":9,"name":"ghr-test","status":"offline"}]}`)
			}
			if tc.deregister != "" {
				routes["DELETE /repos/acme/repo/actions/runners/9"] = tc.deregister + " {}"
			}
			calls := fakeGitHub(t, r, routes)
			store.jobs[1] = queuedJob(1)
			store.jobs[1].VM = "ghr-test"
			v := &types.VM{ID: "vm-runner", Name: "ghr-test", WorkspaceID: 7, TokenID: "token", DesiredState: types.VMStateRunning,
				Status: types.VMStateRunning, EverRunning: true, ContainerID: "runner-container", StubID: "stub",
				CreatedAt: time.Now(), UpdatedAt: time.Now().Add(-2 * time.Minute), LastActiveAt: time.Now()}
			runtime.containers.states[v.ContainerID] = &types.ContainerState{ContainerId: v.ContainerID, Status: types.ContainerStatusRunning}
			runtime.diskName = rootDisk(v)
			store.rows[v.ID] = v
			if tc.setup != nil {
				tc.setup(v, store, r)
			}

			err := r.s.reconcileVM(context.Background(), v)
			if tc.deregister == "500" {
				require.ErrorContains(t, err, "HTTP 500")
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.deleted, v.Status == types.VMStateDeleted, v.Status)
			require.Equal(t, tc.replaced, store.jobs[1].ProvisionAt != nil, "replacement requested")
			require.Empty(t, runtime.requests, "runners never restart")
			require.Equal(t, tc.lookup != "" || tc.deregister != "", slices.Contains(calls(), "GET /repos/acme/repo/actions/runners"), "looked up the runner")
			require.Equal(t, tc.deregister != "", slices.Contains(calls(), "DELETE /repos/acme/repo/actions/runners/9"), "deregistered")
			if tc.refresh || tc.lookup != "" && !tc.deleted {
				next, err := store.GetVM(context.Background(), v.WorkspaceID, v.ID)
				require.NoError(t, err)
				require.Equal(t, tc.refresh, time.Since(next.UpdatedAt) < time.Second, "a runner that GitHub has work for gets more time")
				previousCalls := calls()
				require.NoError(t, r.s.reconcileVM(context.Background(), next))
				require.Equal(t, previousCalls, calls(), "the next pass does not ask GitHub again")
				require.NotEqual(t, types.VMStateDeleted, next.Status)
			}
			if name == "named like a runner" {
				require.Equal(t, 1, runtime.snapshots, "ordinary VMs keep periodic snapshots")
			} else if !tc.deleted {
				require.Zero(t, runtime.snapshots, "runner disks are not snapshotted")
			}
		})
	}
}

// connect posts to the connect endpoint, and returns the response code with
// the step it answered, or with the error message.
func connect(t *testing.T, r *githubRunners, info *auth.AuthInfo, body string) (int, runnerConnection, string) {
	t.Helper()
	c, rec := actionContext(info, "", "", body)
	if err := r.connect(c); err != nil {
		c.Echo().HTTPErrorHandler(err, c)
	}
	var response struct {
		runnerConnection
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), rec.Body.String())
	return rec.Code, response.runnerConnection, response.Message
}

func connectState(t *testing.T, target string) string {
	t.Helper()
	parsed, err := url.Parse(target)
	require.NoError(t, err)
	require.NotEmpty(t, parsed.Query().Get("state"))
	return parsed.Query().Get("state")
}

func TestGitHubRunnerConnect(t *testing.T) {
	r, store, info, _ := runnerFixture(t)
	store.installation = nil
	calls := fakeGitHub(t, r, map[string]string{
		"POST /login/oauth/access_token":   `200 {"access_token":"user-token"}`,
		"GET /user":                        `200 {"login":"alice"}`,
		"GET /user/installations":          `200 {"total_count":0,"installations":[]}`,
		"GET /app/installations/42":        `200 {"account":{"login":"acme","type":"Organization"}}`,
		"GET /orgs/acme/memberships/alice": `200 {"state":"active","role":"admin"}`,
	})

	// To GitHub to authorize the App, and back with a code for the user.
	code, step, _ := connect(t, r, info, `{}`)
	require.Equal(t, 200, code)
	require.True(t, strings.HasPrefix(step.URL, r.github.webURL+"/login/oauth/authorize?client_id=client&state="), step.URL)
	started := connectState(t, step.URL)
	for _, incomplete := range []string{`{"state":%q}`, `{"state":%q,"installation_id":42}`} {
		code, _, message := connect(t, r, info, fmt.Sprintf(incomplete, started))
		require.Equal(t, 400, code, "an account is connected only once the user is known")
		require.Contains(t, message, "connect again")
	}
	require.Empty(t, calls(), "nothing is asked of GitHub before the user returns from it")

	other := *info
	other.Token = &types.Token{ExternalId: "another-token"}
	code, _, _ = connect(t, r, &other, fmt.Sprintf(`{"state":%q,"code":"code"}`, started))
	require.Equal(t, 400, code, "a state is only valid for the token that started connecting")

	// The user has the App nowhere: to GitHub to install it, and back with
	// what was installed.
	code, step, _ = connect(t, r, info, fmt.Sprintf(`{"state":%q,"code":"code"}`, started))
	require.Equal(t, 200, code)
	require.False(t, step.Connected)
	require.Empty(t, step.Accounts)
	require.True(t, strings.HasPrefix(step.URL, r.github.webURL+"/apps/beam-runners/installations/new?state="), step.URL)
	require.Equal(t, step.State, connectState(t, step.URL))
	require.Contains(t, calls(), `POST /login/oauth/access_token {"client_id":"client","client_secret":"client-secret","code":"code"}`)
	authorized := step.State
	code, _, message := connect(t, r, info, fmt.Sprintf(`{"state":%q}`, authorized))
	require.Equal(t, 400, code)
	require.Contains(t, message, "connect again")
	require.Nil(t, store.installation)

	code, step, _ = connect(t, r, info, fmt.Sprintf(`{"state":%q,"installation_id":42}`, authorized))
	require.Equal(t, 200, code)
	require.Equal(t, runnerConnection{Connected: true}, step)
	require.Equal(t, &types.GitHubRunnerInstallation{ID: 42, WorkspaceID: 7, TokenID: info.Token.ExternalId, Account: "acme", AccountType: "Organization",
		EnhancedCache: true}, store.installation, "a new connection has the cache")
}

// GitHub has nothing to install on an account that has the App, so the user
// chooses one of those here.
func TestGitHubRunnerConnectInstalledAccount(t *testing.T) {
	r, store, info, _ := runnerFixture(t)
	store.installation = nil
	token := `201 {"token":"installation-token","expires_at":"2099-01-01T00:00:00Z"}`
	calls := fakeGitHub(t, r, map[string]string{
		"POST /login/oauth/access_token": `200 {"access_token":"user-token"}`,
		"GET /user":                      `200 {"login":"alice"}`,
		"GET /user/installations": `200 {"total_count":5,"installations":[
			{"id":44,"account":{"login":"member-of","type":"Organization"}},
			{"id":43,"account":{"login":"Alice","type":"User"}},
			{"id":45,"account":{"login":"suspended","type":"Organization"},"suspended_at":"2026-11-16T09:00:00Z"},
			{"id":46,"account":{"login":"unreachable","type":"Organization"}},
			{"id":42,"account":{"login":"acme","type":"Organization"}}]}`,
		"GET /orgs/acme/memberships/alice":         `200 {"state":"active","role":"admin"}`,
		"POST /app/installations/44/access_tokens": token,
		"GET /orgs/member-of/memberships/alice":    `200 {"state":"active","role":"member"}`,
		"POST /app/installations/46/access_tokens": `500 {}`,
		"GET /app/installations/43":                `200 {"account":{"login":"Alice","type":"User"}}`,
		"GET /app/installations/44":                `200 {"account":{"login":"member-of","type":"Organization"}}`,
	})

	_, step, _ := connect(t, r, info, `{}`)
	code, step, _ := connect(t, r, info, fmt.Sprintf(`{"state":%q,"code":"code"}`, connectState(t, step.URL)))
	require.Equal(t, 200, code)
	require.False(t, step.Connected)
	require.Equal(t, []runnerAccount{
		{InstallationID: 42, Account: "acme", AccountType: "Organization"},
		{InstallationID: 43, Account: "Alice", AccountType: "User"},
	}, step.Accounts, "the accounts the user owns, by name")
	require.True(t, strings.HasPrefix(step.URL, r.github.webURL+"/apps/beam-runners/installations/new?state="), "another account is a trip to GitHub")
	require.Contains(t, calls(), "GET /user/installations")
	require.Nil(t, store.installation)

	// The state says who chooses, so an account is still theirs to connect.
	code, _, message := connect(t, r, info, fmt.Sprintf(`{"state":%q,"installation_id":44}`, step.State))
	require.Equal(t, 403, code, message)
	require.Nil(t, store.installation)

	code, step, _ = connect(t, r, info, fmt.Sprintf(`{"state":%q,"installation_id":43}`, step.State))
	require.Equal(t, 200, code)
	require.Equal(t, runnerConnection{Connected: true}, step)
	require.Equal(t, &types.GitHubRunnerInstallation{ID: 43, WorkspaceID: 7, TokenID: info.Token.ExternalId, Account: "Alice", AccountType: "User",
		EnhancedCache: true}, store.installation)
}

// An account the user owns can be on any page of their installations.
func TestGitHubRunnerConnectReadsEveryPageOfInstallations(t *testing.T) {
	r, _, _, _ := runnerFixture(t)
	var mu sync.Mutex
	var served []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		served = append(served, req.Method+" "+req.URL.RequestURI()+" "+req.Header.Get("Authorization"))
		mu.Unlock()
		pages := fmt.Sprintf("http://%s/user/installations?per_page=100&page=", req.Host)
		switch req.URL.Query().Get("page") {
		case "":
			w.Header().Set("Link", fmt.Sprintf(`<%s2>; rel="next", <%s2>; rel="last"`, pages, pages))
			fmt.Fprint(w, `{"total_count":2,"installations":[{"id":47,"account":{"login":"bob","type":"User"}}]}`)
		case "2":
			w.Header().Set("Link", fmt.Sprintf(`<%s1>; rel="prev", <%s1>; rel="first"`, pages, pages))
			fmt.Fprint(w, `{"total_count":2,"installations":[{"id":43,"account":{"login":"Alice","type":"User"}}]}`)
		default:
			t.Errorf("read past the last page: %s", req.URL)
		}
	}))
	t.Cleanup(server.Close)
	r.github.apiURL = server.URL

	accounts, err := r.ownedAccounts(context.Background(), "alice", "user-token")
	require.NoError(t, err)
	require.Equal(t, []runnerAccount{{InstallationID: 43, Account: "Alice", AccountType: "User"}}, accounts)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{
		"GET /user/installations?per_page=100 Bearer user-token",
		"GET /user/installations?per_page=100&page=2 Bearer user-token",
	}, served)
}

func TestGitHubRunnerConnectState(t *testing.T) {
	r, _, info, _ := runnerFixture(t)
	for _, login := range []string{"", "alice"} {
		state := r.state(info, login, time.Now().Add(time.Minute).Unix())
		verified, err := r.verify(info, state)
		require.NoError(t, err)
		require.Equal(t, login, verified)
		for _, tampered := range []string{state + "0", "bob" + state[len(login):], strings.Replace(state, ".", ".1", 1), strings.TrimPrefix(state, login)} {
			if tampered != state {
				_, err = r.verify(info, tampered)
				require.Error(t, err, tampered)
			}
		}
		other := *info
		other.Workspace = &types.Workspace{Id: 9}
		_, err = r.verify(&other, state)
		require.Error(t, err)
		other.Workspace, other.Token = info.Workspace, &types.Token{ExternalId: "another-token"}
		_, err = r.verify(&other, state)
		require.Error(t, err)
		_, err = r.verify(info, r.state(info, login, time.Now().Add(-time.Second).Unix()))
		require.Error(t, err)
	}
}

func TestGitHubRunnerConnectRequiresAccountOwner(t *testing.T) {
	for _, tc := range []struct {
		name, installation, membership string
		installedIn                    uint
		code                           int
	}{
		{name: "org owner", membership: `200 {"state":"active","role":"admin"}`, code: 200},
		{name: "org member", membership: `200 {"state":"active","role":"member"}`, code: 403},
		{name: "invited owner", membership: `200 {"state":"pending","role":"admin"}`, code: 403},
		{name: "not in org", membership: `404 {}`, code: 403},
		{name: "user", installation: `200 {"account":{"login":"Alice","type":"User"}}`, code: 200},
		{name: "another user", installation: `200 {"account":{"login":"bob","type":"User"}}`, code: 403},
		{name: "suspended", installation: `200 {"account":{"login":"Alice","type":"User"},"suspended_at":"2026-11-16T09:00:00Z"}`, code: 403},
		{name: "uninstalled", installation: `404 {"message":"Not Found"}`, code: 400},
		{name: "other workspace", membership: `200 {"state":"active","role":"admin"}`, installedIn: 9, code: 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, store, info, _ := runnerFixture(t)
			store.installation = nil
			if tc.installedIn != 0 {
				store.installation = &types.GitHubRunnerInstallation{ID: 42, WorkspaceID: tc.installedIn}
			}
			routes := map[string]string{
				"GET /app/installations/42": cmp.Or(tc.installation, `200 {"account":{"login":"acme","type":"Organization"}}`),
			}
			if tc.membership != "" {
				routes["GET /orgs/acme/memberships/alice"] = tc.membership
			}
			fakeGitHub(t, r, routes)
			state := r.state(info, "alice", time.Now().Add(time.Minute).Unix())
			code, step, message := connect(t, r, info, fmt.Sprintf(`{"installation_id":42,"state":%q}`, state))
			require.Equal(t, tc.code, code, message)
			require.Equal(t, tc.code == 200, step.Connected)
			require.Equal(t, tc.code == 200, store.installation != nil && store.installation.WorkspaceID == 7)
		})
	}
}

func TestGitHubRunnerConnectRejectedCode(t *testing.T) {
	r, _, info, _ := runnerFixture(t)
	fakeGitHub(t, r, map[string]string{"POST /login/oauth/access_token": `200 {"error":"bad_verification_code"}`})
	state := r.state(info, "", time.Now().Add(time.Minute).Unix())
	code, step, message := connect(t, r, info, fmt.Sprintf(`{"code":"code","state":%q}`, state))
	require.Equal(t, 400, code, message)
	require.Equal(t, runnerConnection{}, step)
}

func TestGitHubRunnerRedeliversFailedWebhooks(t *testing.T) {
	r, _, _, _ := runnerFixture(t)
	delivery := func(id int, guid string, age time.Duration, code int) string {
		return fmt.Sprintf(`{"id":%d,"guid":%q,"delivered_at":%q,"status_code":%d}`, id, guid, time.Now().Add(-age).UTC().Format(time.RFC3339), code)
	}
	deliveries := []string{delivery(5, "timed-out", time.Minute, 0), delivery(4, "timed-out", 2*time.Minute, 502),
		delivery(3, "accepted", 3*time.Minute, 202), delivery(2, "rejected", 4*time.Minute, 401), delivery(1, "old", 2*time.Hour, 502)}
	calls := fakeGitHub(t, r, map[string]string{
		"GET /app/hook/deliveries":             "200 [" + strings.Join(deliveries, ",") + "]",
		"POST /app/hook/deliveries/5/attempts": "202 {}",
	})
	r.redelivered["rejected"], r.redelivered["forgotten"] = runnerAttempts, 1
	for range runnerAttempts + 1 {
		require.NoError(t, r.redeliver(context.Background(), time.Now().Add(-time.Hour)))
	}
	posts := slices.DeleteFunc(calls(), func(call string) bool { return !strings.HasPrefix(call, "POST") })
	require.Len(t, posts, runnerAttempts, "only the latest failed delivery is retried, a few times")
	require.Equal(t, map[string]int{"timed-out": runnerAttempts, "rejected": runnerAttempts}, r.redelivered, "events out of the window are forgotten")

	require.Equal(t, "https://api.github.com/app/hook/deliveries?cursor=v1_2", nextPage(
		`<https://api.github.com/app/hook/deliveries?cursor=v1_1>; rel="prev", <https://api.github.com/app/hook/deliveries?cursor=v1_2>; rel="next"`))
	require.Empty(t, nextPage(`<https://api.github.com/app/hook/deliveries?cursor=v1_1>; rel="prev"`))
}

func TestGitHubRunnerRedeliveryReadsEveryPageInTheWindow(t *testing.T) {
	r, _, _, _ := runnerFixture(t)
	var mu sync.Mutex
	var served []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		served = append(served, req.Method+" "+req.URL.RequestURI())
		mu.Unlock()
		recent := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
		switch {
		case req.Method == http.MethodPost:
			w.WriteHeader(http.StatusAccepted)
		case req.URL.Query().Get("cursor") == "":
			w.Header().Set("Link", fmt.Sprintf(`<http://%s/app/hook/deliveries?cursor=2>; rel="next"`, req.Host))
			fmt.Fprintf(w, `[{"id":3,"guid":"a","delivered_at":%q,"status_code":202}]`, recent)
		case req.URL.Query().Get("cursor") == "2":
			w.Header().Set("Link", fmt.Sprintf(`<http://%s/app/hook/deliveries?cursor=3>; rel="next"`, req.Host))
			fmt.Fprintf(w, `[{"id":2,"guid":"b","delivered_at":%q,"status_code":500},{"id":1,"guid":"c","delivered_at":"2020-01-01T00:00:00Z","status_code":500}]`, recent)
		default:
			t.Errorf("read past the window: %s", req.URL)
		}
	}))
	t.Cleanup(server.Close)
	r.github.apiURL = server.URL

	require.NoError(t, r.redeliver(context.Background(), time.Now().Add(-time.Hour)))
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"GET /app/hook/deliveries?per_page=100", "GET /app/hook/deliveries?cursor=2", "POST /app/hook/deliveries/2/attempts"}, served)
}

func TestGitHubRunnerConfiguration(t *testing.T) {
	key := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(runnerKey())}))
	configured := runnerConfig
	configured.PrivateKey = key
	escaped, converted, incomplete, unreadable, public := configured, configured, configured, configured, configured
	escaped.PrivateKey = strings.ReplaceAll(key, "\n", `\n`)
	pkcs8, err := x509.MarshalPKCS8PrivateKey(runnerKey())
	require.NoError(t, err)
	converted.PrivateKey = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}))
	incomplete.ClientSecret, incomplete.ImageID = "", ""
	unreadable.PrivateKey = "secret"
	public.PrivateKey = string(pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&runnerKey().PublicKey)}))

	for name, tc := range map[string]struct {
		config  types.GitHubRunnerConfig
		enabled bool
		err     string
	}{
		"not configured":           {},
		"configured":               {config: configured, enabled: true},
		"key from the environment": {config: escaped, enabled: true},
		"key converted to PKCS #8": {config: converted, enabled: true},
		"missing settings":         {config: incomplete, err: "abstractions.vm.githubRunners has an appID but no clientSecret, imageID"},
		"unreadable key":           {config: unreadable, err: "abstractions.vm.githubRunners.privateKey must be the App's private key"},
		"not a private key":        {config: public, err: "abstractions.vm.githubRunners.privateKey must be the App's private key"},
	} {
		t.Run(name, func(t *testing.T) {
			s, _, info, _, _ := fixture()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.ctx, s.config.GitHubRunners = ctx, tc.config
			s.repo = &runnerStore{vmStore: s.repo.(*vmStore)}
			err := s.initGitHubRunners(echo.New().Group("/api/v1/vm"))
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)

			c, rec := actionContext(info, "", "", "")
			require.NoError(t, s.runners.status(c))
			var status struct {
				Enabled      bool            `json:"enabled"`
				Installation json.RawMessage `json:"installation"`
				Sizes        []runnerSize    `json:"sizes"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &status))
			require.Equal(t, tc.enabled, status.Enabled)
			require.Equal(t, "null", string(status.Installation))
			require.Equal(t, runnerSizes, status.Sizes)
			require.Contains(t, rec.Body.String(), `{"label":"beam-2vcpu-ubuntu-2404","cpu":2000,"memory":8192,"disk_size":"50Gi"}`)

			code, _, message := connect(t, s.runners, info, `{}`)
			if tc.enabled {
				require.Equal(t, 200, code)
			} else {
				require.Equal(t, 503, code)
				require.Equal(t, "GitHub runners are not configured", message)
			}
		})
	}
}

// The dashboard reads these names.
func TestGitHubRunnerJobsResponse(t *testing.T) {
	r, store, info, _ := runnerFixture(t)
	started := time.Date(2026, 11, 16, 9, 1, 0, 0, time.UTC)
	store.jobs[1] = queuedJob(1)
	store.jobs[2] = &types.GitHubRunnerJob{ID: 2, RunID: 1, InstallationID: 42, WorkspaceID: 7, Repository: "acme/repo", Workflow: "CI", Name: "test",
		URL: "https://github.com/acme/repo/actions/runs/1/job/2", Label: beamLabels[0], Status: "in_progress", RunnerName: "ghr-0123456789ab",
		CreatedAt: started.Add(-time.Minute), StartedAt: &started, VM: "ghr-ba9876543210", Attempts: 2, ProvisionAt: &started, Error: "boom"}
	c, rec := actionContext(info, "", "", "")
	c.Request().URL.RawQuery = "limit=1"
	require.NoError(t, r.jobs(c))
	require.JSONEq(t, `{"total":2,"repositories":["acme/repo"],"data":[{"id":2,"run_id":1,"repository":"acme/repo","workflow":"CI","name":"test",
		"url":"https://github.com/acme/repo/actions/runs/1/job/2","label":"beam-4vcpu-ubuntu-2404","status":"in_progress",
		"runner_name":"ghr-0123456789ab","created_at":"2026-11-16T09:00:00Z","started_at":"2026-11-16T09:01:00Z"}]}`, rec.Body.String())

	// A job's error says why it has no runner, which the job above has. It
	// waits for nothing once no more runners are started for it.
	store.jobs[1].Error = "no runner after 5 attempts: boom"
	c, rec = actionContext(info, "", "", "")
	c.Request().URL.RawQuery = "limit=1&offset=1"
	require.NoError(t, r.jobs(c))
	require.Contains(t, rec.Body.String(), `"status":"queued","created_at":"0001-01-01T00:00:00Z","error":"no runner after 5 attempts: boom"}]`)

	// The filters narrow the jobs, and never the repositories to choose from.
	store.jobs[3] = &types.GitHubRunnerJob{ID: 3, InstallationID: 42, WorkspaceID: 7, Repository: "acme/site", Workflow: "Deploy", Name: "build",
		Status: types.GitHubJobCompleted, Conclusion: "failure"}
	store.jobs[4] = &types.GitHubRunnerJob{ID: 4, InstallationID: 42, WorkspaceID: 7, Repository: "acme/site", Workflow: "Deploy", Name: "publish",
		Status: types.GitHubJobCompleted, Conclusion: "success"}
	for query, ids := range map[string][]int64{
		"":                                    {4, 3, 2, 1},
		"repository=acme/site":                {4, 3},
		"repository=acme":                     {},
		"q=DEPLOY":                            {4, 3},
		"q=site&status=completed":             {4, 3},
		"q=test":                              {2},
		"status=queued,in_progress":           {2, 1},
		"conclusion=failure,timed_out":        {3},
		"repository=acme/repo&status=queued":  {1},
		"repository=acme/repo&q=publish":      {},
		"conclusion=success&offset=1":         {},
		"status=completed&conclusion=success": {4},
	} {
		c, rec = actionContext(info, "", "", "")
		c.Request().URL.RawQuery = "limit=10&" + query
		require.NoError(t, r.jobs(c))
		var page struct {
			Total int `json:"total"`
			Data  []struct {
				ID int64 `json:"id"`
			} `json:"data"`
			Repositories []string `json:"repositories"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
		listed := []int64{}
		for _, job := range page.Data {
			listed = append(listed, job.ID)
		}
		require.Equal(t, ids, listed, query)
		require.Equal(t, []string{"acme/repo", "acme/site"}, page.Repositories, query)
		if !strings.Contains(query, "offset") {
			require.Equal(t, len(ids), page.Total, query)
		}
	}

	c, rec = actionContext(info, "", "", "")
	require.NoError(t, r.status(c))
	var status struct {
		Installation map[string]any `json:"installation"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &status))
	require.Equal(t, map[string]any{"id": float64(42), "account": "acme", "account_type": "Organization", "paused": false,
		"created_at": "0001-01-01T00:00:00Z"}, status.Installation)
}

// runnerRequest runs a handler as the workspace, for a job if one is named,
// and returns the response code with the body.
func runnerRequest(t *testing.T, info *auth.AuthInfo, handler echo.HandlerFunc, job, body string) (int, string) {
	t.Helper()
	c, rec := actionContext(info, "", "", body)
	c.SetParamNames("jobId")
	c.SetParamValues(job)
	if err := handler(c); err != nil {
		c.Echo().HTTPErrorHandler(err, c)
	}
	return rec.Code, strings.TrimSpace(rec.Body.String())
}

func TestGitHubRunnerWebhookRecordsTheRun(t *testing.T) {
	r, store, _, _ := runnerFixture(t)
	job := queuedJob(1)
	job.RunID = 77
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", beamLabels, job, true))
	require.EqualValues(t, 77, store.jobs[1].RunID)

	// The deliveries that follow keep the run, also one that does not name it.
	job.RunnerName = "ghr-other"
	require.Equal(t, 202, deliver(t, r, "workflow_job", "in_progress", beamLabels, job, true))
	job.RunID, job.Conclusion = 0, "success"
	require.Equal(t, 202, deliver(t, r, "workflow_job", "completed", beamLabels, job, true))
	require.Equal(t, "completed", store.jobs[1].Status)
	require.EqualValues(t, 77, store.jobs[1].RunID)

	// A job recorded before runs were learns its run from its next delivery.
	store.jobs[2] = queuedJob(2)
	job = queuedJob(2)
	job.RunID, job.RunnerName = 78, "ghr-other"
	require.Equal(t, 202, deliver(t, r, "workflow_job", "in_progress", beamLabels, job, true))
	require.EqualValues(t, 78, store.jobs[2].RunID)
}

func TestGitHubRunnerPause(t *testing.T) {
	r, store, info, runtime := runnerFixture(t)
	ctx := context.Background()
	calls := fakeGitHub(t, r, map[string]string{
		"POST /repos/acme/repo/actions/runners/generate-jitconfig": `201 {"encoded_jit_config":"jit-secret"}`,
	})
	update := func(body string) (int, string) {
		t.Helper()
		return runnerRequest(t, info, r.update, "", body)
	}

	// The loop has listed this job as due when its account pauses.
	require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(1)))
	listed := *store.jobs[1]
	code, body := update(`{"paused":true}`)
	require.Equal(t, 200, code, body)
	c, rec := actionContext(info, "", "", "")
	require.NoError(t, r.status(c))
	require.JSONEq(t, rec.Body.String(), body, "the answer is the status")
	require.Contains(t, body, `"paused":true`)
	require.Empty(t, r.wake)

	// The jobs that arrive are recorded, and none starts a runner.
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", beamLabels, queuedJob(2), true))
	<-r.wake
	due, err := store.ListDueGitHubRunnerJobs(ctx, 0)
	require.NoError(t, err)
	require.Empty(t, due)
	require.NoError(t, r.provision(ctx, listed))
	require.Len(t, store.jobs, 2)
	for _, job := range store.jobs {
		require.NotNil(t, job.ProvisionAt, "the job is still to get a runner")
		require.Zero(t, job.Attempts, "waiting for the account spends no attempt")
		require.Empty(t, job.VM)
		require.Empty(t, job.Error)
	}
	require.Empty(t, calls())
	require.Empty(t, runtime.requests)

	// A request that names no setting changes none.
	code, body = update(`{}`)
	require.Equal(t, 200, code, body)
	require.Contains(t, body, `"paused":true`)
	require.True(t, store.installation.Paused)
	require.Empty(t, r.wake)

	// Resuming has the loop look at the jobs that waited, which are all due.
	code, body = update(`{"paused":false}`)
	require.Equal(t, 200, code, body)
	require.Contains(t, body, `"paused":false`)
	require.Len(t, r.wake, 1)
	due, err = store.ListDueGitHubRunnerJobs(ctx, 0)
	require.NoError(t, err)
	require.Len(t, due, 2)
	for _, job := range due {
		require.NoError(t, r.provision(ctx, job))
	}
	require.Len(t, runtime.requests, 2)
	for _, job := range store.jobs {
		require.Nil(t, job.ProvisionAt)
		require.Equal(t, 1, job.Attempts)
	}

	for _, invalid := range []string{``, `{"paused":"yes"}`, `{"pause":true}`} {
		code, body = update(invalid)
		require.Equal(t, 400, code, body)
	}
	store.installation = nil
	for _, request := range []string{`{"paused":true}`, `{}`} {
		code, body = update(request)
		require.Equal(t, 409, code)
		require.JSONEq(t, `{"message":"no GitHub account is connected"}`, body)
	}
}

func TestGitHubRunnerJobsSayWhatTheyWaitFor(t *testing.T) {
	r, store, info, _ := runnerFixture(t)
	due, later := at(time.Now()), at(time.Now().Add(time.Minute))
	for name, tc := range map[string]struct {
		job     types.GitHubRunnerJob
		paused  bool
		full    bool
		waiting string
	}{
		"due for a runner":                     {job: types.GitHubRunnerJob{ProvisionAt: due}, waiting: "Starting a runner"},
		"its runner starts":                    {job: types.GitHubRunnerJob{VM: "ghr-0123456789ab", Attempts: 1}, waiting: "Starting a runner"},
		"its runner is up":                     {job: types.GitHubRunnerJob{VM: "ghr-0123456789ab", Attempts: 1, RunnerReadyAt: due}, waiting: "Runner ready, waiting for GitHub"},
		"its runner is up, GitHub holds it":    {job: types.GitHubRunnerJob{VM: "ghr-0123456789ab", Attempts: 1, RunnerReadyAt: due, Waiting: waitingHeld}, waiting: "Held by GitHub, runner ready"},
		"another runner starts, one was ready": {job: types.GitHubRunnerJob{VM: "ghr-0123456789ab", Attempts: 2, RunnerReadyAt: due, ProvisionAt: due}, waiting: "Runner ready, waiting for GitHub"},
		"its runner is up, paused":             {job: types.GitHubRunnerJob{VM: "ghr-0123456789ab", Attempts: 1, RunnerReadyAt: due, Waiting: waitingHeld}, paused: true, waiting: "Runners are paused"},
		"an attempt failed":                    {job: types.GitHubRunnerJob{ProvisionAt: due, Attempts: 1, Error: "boom"}, waiting: "Retrying"},
		"its runner stopped before a job":      {job: types.GitHubRunnerJob{ProvisionAt: due, VM: "ghr-0123456789ab", Error: "stopped"}, waiting: "Retrying"},
		"paused":                               {job: types.GitHubRunnerJob{ProvisionAt: due}, paused: true, waiting: "Runners are paused"},
		"paused after an attempt failed":       {job: types.GitHubRunnerJob{ProvisionAt: due, Attempts: 1, Error: "boom"}, paused: true, waiting: "Runners are paused"},
		"paused while its runner starts":       {job: types.GitHubRunnerJob{VM: "ghr-0123456789ab", Attempts: 1}, paused: true, waiting: "Runners are paused"},
		"every attempt failed":                 {job: types.GitHubRunnerJob{Attempts: 5, Error: "no runner after 5 attempts: boom"}},
		"every attempt failed, paused":         {job: types.GitHubRunnerJob{Attempts: 5, Error: "no runner after 5 attempts: boom"}, paused: true},
		"asks for a size there is not":         {job: types.GitHubRunnerJob{Label: "beam-3vcpu-ubuntu-2404", Error: "runs-on must be exactly one of"}},
		"asks for a size there is not, new":    {job: types.GitHubRunnerJob{Label: "beam-3vcpu-ubuntu-2404", ProvisionAt: due}},
		"asks for more than a size, paused":    {job: types.GitHubRunnerJob{Label: "self-hosted, beam-4vcpu-ubuntu-2404", ProvisionAt: due}, paused: true},
		"left by an account that reconnected":  {job: types.GitHubRunnerJob{}},
		"running":                              {job: types.GitHubRunnerJob{Status: "in_progress", RunnerName: "ghr-0123456789ab", ProvisionAt: due}, paused: true},
		"running on a runner that is not ours": {job: types.GitHubRunnerJob{Status: "in_progress", RunnerName: "self-hosted-1"}},
		"cancelled before it took a runner":    {job: types.GitHubRunnerJob{Status: "completed", ProvisionAt: due, Error: "boom"}},

		// What a job's row says it waits for, and a workspace that is full.
		"to be tried again":                 {job: types.GitHubRunnerJob{ProvisionAt: later, Attempts: 1, Error: "boom", Waiting: "Retrying"}, waiting: "Retrying"},
		"its account is limited by GitHub":  {job: types.GitHubRunnerJob{ProvisionAt: later, Waiting: "GitHub is limiting requests"}, waiting: "GitHub is limiting requests"},
		"its runner has no worker yet":      {job: types.GitHubRunnerJob{VM: "ghr-0123456789ab", Attempts: 1, Waiting: "Waiting for capacity"}, waiting: "Waiting for capacity"},
		"no worker for its runner, paused":  {job: types.GitHubRunnerJob{VM: "ghr-0123456789ab", Attempts: 1, Waiting: "Waiting for capacity"}, paused: true, waiting: "Runners are paused"},
		"due in a full workspace":           {job: types.GitHubRunnerJob{ProvisionAt: due}, full: true, waiting: "Waiting for a free runner"},
		"due again in a full workspace":     {job: types.GitHubRunnerJob{ProvisionAt: due, Attempts: 1, Error: "boom", Waiting: "Retrying"}, full: true, waiting: "Waiting for a free runner"},
		"due in a full workspace, paused":   {job: types.GitHubRunnerJob{ProvisionAt: due}, paused: true, full: true, waiting: "Runners are paused"},
		"not due yet in a full workspace":   {job: types.GitHubRunnerJob{ProvisionAt: later, Attempts: 1, Error: "boom", Waiting: "Retrying"}, full: true, waiting: "Retrying"},
		"its runner starts, workspace full": {job: types.GitHubRunnerJob{VM: "ghr-0123456789ab", Attempts: 1}, full: true, waiting: "Starting a runner"},
		"refused by GitHub, workspace full": {job: types.GitHubRunnerJob{Attempts: 1, Error: "GitHub returned HTTP 404: Not Found"}, full: true},
		"running, workspace full":           {job: types.GitHubRunnerJob{Status: "in_progress", RunnerName: "ghr-0123456789ab", ProvisionAt: due}, full: true},
	} {
		// Queued for a size there is, unless it says otherwise.
		tc.job.Status, tc.job.Label = cmp.Or(tc.job.Status, types.GitHubJobQueued), cmp.Or(tc.job.Label, beamLabels[0])
		require.Equal(t, tc.waiting, r.waiting(tc.job, tc.paused, tc.full), name)
	}

	// A job keeps its error, which says why it waits.
	store.jobs[1] = queuedJob(1)
	store.jobs[1].ProvisionAt, store.jobs[1].Error = due, "boom"
	listed := func() types.GitHubRunnerJob {
		c, rec := actionContext(info, "", "", "")
		c.Request().URL.RawQuery = "limit=1"
		require.NoError(t, r.jobs(c))
		var page runnerJobPage
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
		require.Len(t, page.Data, 1)
		return page.Data[0]
	}
	require.Equal(t, "Retrying", listed().Waiting)
	store.installation.Paused = true
	require.Equal(t, "Runners are paused", listed().Waiting)
	require.Equal(t, "boom", listed().Error)
}

func TestGitHubRunnerCancelJob(t *testing.T) {
	running := func(job *types.GitHubRunnerJob) { job.Status, job.RunnerName = types.GitHubJobInProgress, "ghr-test" }
	for name, tc := range map[string]struct {
		job     func(*types.GitHubRunnerJob)
		id      string
		github  string
		code    int
		message string
	}{
		"queued":               {github: `202 {}`, code: 202},
		"running":              {job: running, github: `202 {}`, code: 202},
		"run finished since":   {job: running, github: `409 {"message":"Cannot cancel a workflow run that is completed."}`, code: 202},
		"App may not cancel":   {github: `403 {"message":"Resource not accessible by integration"}`, code: 403, message: "The GitHub App needs the Actions permission set to read and write; accept the updated permissions on GitHub"},
		"GitHub fails":         {github: `500 {}`, code: 500, message: "Internal Server Error"},
		"completed":            {job: func(job *types.GitHubRunnerJob) { job.Status = types.GitHubJobCompleted }, code: 202},
		"of another workspace": {job: func(job *types.GitHubRunnerJob) { job.WorkspaceID = 9 }, code: 404, message: "job not found"},
		"unknown":              {id: "2", code: 404, message: "job not found"},
		"not a job":            {id: "one", code: 404, message: "job not found"},
	} {
		t.Run(name, func(t *testing.T) {
			r, store, info, runtime := runnerFixture(t)
			routes := map[string]string{}
			if tc.github != "" {
				routes["POST /repos/acme/repo/actions/runs/77/cancel"] = tc.github
			}
			calls := fakeGitHub(t, r, routes)
			store.jobs[1] = queuedJob(1)
			store.jobs[1].RunID = 77
			if tc.job != nil {
				tc.job(store.jobs[1])
			}
			v := &types.VM{ID: "vm-runner", Name: "ghr-test", WorkspaceID: 7, DesiredState: types.VMStateRunning, Status: types.VMStateRunning,
				ContainerID: "runner-container"}
			runtime.containers.states[v.ContainerID] = &types.ContainerState{ContainerId: v.ContainerID, Status: types.ContainerStatusRunning}
			store.rows[v.ID] = v

			code, body := runnerRequest(t, info, r.cancelJob, cmp.Or(tc.id, "1"), "")
			require.Equal(t, tc.code, code, body)
			if tc.message == "" {
				require.Empty(t, body)
			} else {
				require.JSONEq(t, fmt.Sprintf(`{"message":%q}`, tc.message), body)
			}
			if tc.github == "" {
				require.Empty(t, calls())
			} else {
				require.Equal(t, []string{"POST /repos/acme/repo/actions/runs/77/cancel"}, calls())
			}
			require.Equal(t, types.VMStateRunning, store.rows[v.ID].DesiredState, "GitHub's report of the job deletes its runner")
			require.Zero(t, r.s.gateway.(*vmGateway).stops)
		})
	}
}

func TestGitHubRunnerStopJob(t *testing.T) {
	const elsewhere = "the job is not running on a runner of this workspace"
	for name, tc := range map[string]struct {
		job        func(*types.GitHubRunnerJob)
		setup      func(*types.VM)
		deregister string
		code       int
		message    string
	}{
		"running":                      {deregister: "422", code: 202},
		"running, GitHub unreachable":  {deregister: "500", code: 202},
		"finished since":               {deregister: "204", code: 202},
		"queued":                       {job: func(job *types.GitHubRunnerJob) { job.Status, job.RunnerName = types.GitHubJobQueued, "" }, code: 409, message: elsewhere},
		"completed":                    {job: func(job *types.GitHubRunnerJob) { job.Status = types.GitHubJobCompleted }, code: 409, message: elsewhere},
		"on a runner that is not ours": {job: func(job *types.GitHubRunnerJob) { job.RunnerName = "self-hosted-1" }, code: 409, message: elsewhere},
		"on a runner named as a VM is": {job: func(job *types.GitHubRunnerJob) { job.RunnerName = "dev" }, code: 409, message: elsewhere},
		"on a runner that is deleted":  {setup: func(v *types.VM) { v.DesiredState = types.VMStateDeleted }, code: 409, message: "the job's runner is already being deleted"},
		"of another workspace":         {job: func(job *types.GitHubRunnerJob) { job.WorkspaceID = 9 }, code: 404, message: "job not found"},
		"unknown":                      {job: func(job *types.GitHubRunnerJob) { job.ID = 3 }, code: 404, message: "job not found"},
	} {
		t.Run(name, func(t *testing.T) {
			r, store, info, runtime := runnerFixture(t)
			routes := map[string]string{}
			if tc.deregister != "" {
				routes["GET /repos/acme/repo/actions/runners"] = `200 {"runners":[{"id":9}]}`
				routes["DELETE /repos/acme/repo/actions/runners/9"] = tc.deregister + " {}"
			}
			calls := fakeGitHub(t, r, routes)

			// The runner was started for one job, and GitHub gave it another.
			store.jobs[1] = queuedJob(1)
			store.jobs[1].VM = "ghr-test"
			job := queuedJob(2)
			job.Status, job.RunnerName = types.GitHubJobInProgress, "ghr-test"
			if tc.job != nil {
				tc.job(job)
			}
			store.jobs[job.ID] = job
			v := &types.VM{ID: "vm-runner", Name: "ghr-test", WorkspaceID: 7, TokenID: "token", DesiredState: types.VMStateRunning,
				Status: types.VMStateRunning, EverRunning: true, ContainerID: "runner-container", StubID: "stub", CreatedAt: time.Now()}
			runtime.containers.states[v.ContainerID] = &types.ContainerState{ContainerId: v.ContainerID, Status: types.ContainerStatusRunning}
			store.rows[v.ID] = v
			store.rows["vm-dev"] = &types.VM{ID: "vm-dev", Name: "dev", WorkspaceID: 7, DesiredState: types.VMStateRunning, Status: types.VMStateRunning}
			if tc.setup != nil {
				tc.setup(v)
			}

			code, body := runnerRequest(t, info, r.stopJob, "2", "")
			require.Equal(t, tc.code, code, body)
			gateway, backend := r.s.gateway.(*vmGateway), r.s.backend.(*vmBackend)
			require.Equal(t, types.VMStateRunning, store.rows["vm-dev"].DesiredState)
			require.Empty(t, runtime.requests)
			if tc.code != 202 {
				require.JSONEq(t, fmt.Sprintf(`{"message":%q}`, tc.message), body)
				require.Equal(t, tc.setup == nil, store.rows[v.ID].DesiredState == types.VMStateRunning)
				require.Equal(t, types.VMStateRunning, store.rows[v.ID].Status)
				require.Zero(t, gateway.stops)
				require.Empty(t, calls())
				return
			}

			require.Empty(t, body)
			require.Equal(t, types.VMStateDeleted, store.rows[v.ID].Status)
			require.NotContains(t, runtime.containers.states, v.ContainerID)
			require.Equal(t, 1, gateway.stops)
			require.Equal(t, 1, backend.deletedDisks)
			require.Equal(t, []string{"GET /repos/acme/repo/actions/runners", "DELETE /repos/acme/repo/actions/runners/9"}, calls(), "unregistered, if GitHub lets go of it")
			require.Nil(t, store.jobs[1].ProvisionAt, "a runner that took a job is not replaced")

			code, body = runnerRequest(t, info, r.stopJob, "2", "")
			require.Equal(t, 409, code)
			require.JSONEq(t, `{"message":"the job's runner is already being deleted"}`, body)
			require.Equal(t, 1, gateway.stops)
		})
	}
}

func TestGitHubRunnerCancelAll(t *testing.T) {
	for name, tc := range map[string]struct {
		body, response string
		cancelled      []string
	}{
		"every repository": {body: `{}`, response: `{"cancelled":3,"failed":1}`, cancelled: []string{
			"POST /repos/acme/repo/actions/runs/70/cancel", "POST /repos/acme/repo/actions/runs/73/cancel",
			"POST /repos/acme/site/actions/runs/71/cancel", "POST /repos/acme/site/actions/runs/75/cancel"}},
		"one repository": {body: `{"repository":"acme/site"}`, response: `{"cancelled":2,"failed":0}`, cancelled: []string{
			"POST /repos/acme/site/actions/runs/71/cancel", "POST /repos/acme/site/actions/runs/75/cancel"}},
		"a repository without jobs": {body: `{"repository":"acme"}`, response: `{"cancelled":0,"failed":0}`},
	} {
		t.Run(name, func(t *testing.T) {
			r, store, info, _ := runnerFixture(t)
			// Any other request fails the test: of a run twice, of one that
			// has finished or is not known, or of another workspace's.
			calls := fakeGitHub(t, r, map[string]string{
				"POST /repos/acme/repo/actions/runs/70/cancel": `202 {}`,
				"POST /repos/acme/site/actions/runs/71/cancel": `202 {}`,
				"POST /repos/acme/repo/actions/runs/73/cancel": `403 {"message":"Resource not accessible by integration"}`,
				"POST /repos/acme/site/actions/runs/75/cancel": `409 {"message":"Cannot cancel a workflow run that is completed."}`,
			})
			for id, job := range map[int64]struct {
				run                int64
				repository, status string
				workspace          uint
			}{
				1: {run: 70, repository: "acme/repo", status: types.GitHubJobQueued},
				2: {run: 70, repository: "acme/repo", status: types.GitHubJobInProgress},
				3: {run: 71, repository: "acme/site", status: types.GitHubJobQueued},
				4: {run: 72, repository: "acme/repo", status: types.GitHubJobCompleted},
				5: {run: 0, repository: "acme/repo", status: types.GitHubJobQueued},
				6: {run: 73, repository: "acme/repo", status: types.GitHubJobQueued},
				7: {run: 74, repository: "acme/repo", status: types.GitHubJobQueued, workspace: 9},
				8: {run: 75, repository: "acme/site", status: types.GitHubJobInProgress},
			} {
				store.jobs[id] = queuedJob(id)
				store.jobs[id].RunID, store.jobs[id].Repository, store.jobs[id].Status = job.run, job.repository, job.status
				store.jobs[id].WorkspaceID = cmp.Or(job.workspace, store.jobs[id].WorkspaceID)
			}

			code, body := runnerRequest(t, info, r.cancelAll, "", tc.body)
			require.Equal(t, 200, code, body)
			require.JSONEq(t, tc.response, body)
			cancelled := calls()
			slices.Sort(cancelled)
			require.Equal(t, tc.cancelled, cancelled)
		})
	}

	// A request cancels the oldest hundred runs, and says that there are more.
	r, store, info, _ := runnerFixture(t)
	routes := map[string]string{}
	for id := int64(1); id <= 102; id++ {
		store.jobs[id] = queuedJob(id)
		store.jobs[id].RunID = 1000 - id
		if id > 2 {
			routes[fmt.Sprintf("POST /repos/acme/repo/actions/runs/%d/cancel", 1000-id)] = `202 {}`
		}
	}
	calls := fakeGitHub(t, r, routes)
	code, body := runnerRequest(t, info, r.cancelAll, "", `{}`)
	require.Equal(t, 200, code, body)
	require.JSONEq(t, `{"cancelled":100,"failed":2}`, body)
	require.Len(t, calls(), 100)

	code, _ = runnerRequest(t, info, r.cancelAll, "", ``)
	require.Equal(t, 400, code)
	r.github = nil
	for _, handler := range []echo.HandlerFunc{r.cancelAll, r.cancelJob, r.stopJob} {
		code, body := runnerRequest(t, info, handler, "1", `{}`)
		require.Equal(t, 503, code)
		require.JSONEq(t, `{"message":"GitHub runners are not configured"}`, body)
	}
}

// The stop controls are the workspace's, as the other routes are, and none
// of them is taken for an action on a VM.
func TestGitHubRunnerStopRoutes(t *testing.T) {
	r, store, info, _ := runnerFixture(t)
	r, request := runnerRoutes(t, r)
	fakeGitHub(t, r, map[string]string{"POST /repos/acme/repo/actions/runs/77/cancel": `202 {}`})
	store.jobs[1] = queuedJob(1)
	store.jobs[1].RunID = 77

	other, restricted := *info, *info
	other.Workspace = &types.Workspace{Id: 9, ExternalId: "another"}
	restricted.Token = &types.Token{ExternalId: info.Token.ExternalId, TokenType: types.TokenTypeWorkspaceRestricted}
	workspace := info.Workspace.ExternalId
	for _, route := range []struct {
		method, path, body string
		code               int
	}{
		{http.MethodPatch, workspace, `{"paused":true}`, 200},
		{http.MethodPost, workspace + "/jobs/1/cancel", ``, 202},
		{http.MethodPost, workspace + "/jobs/1/stop", ``, 409},
		{http.MethodPost, workspace + "/cancel", `{}`, 200},
		{http.MethodPost, "self/cancel", `{}`, 200},
	} {
		for _, as := range []struct {
			info *auth.AuthInfo
			code int
		}{{info, route.code}, {&other, 401}, {&restricted, 401}} {
			if as.info == &other && strings.HasPrefix(route.path, "self") {
				continue
			}
			code, body := request(as.info, route.method, route.path, route.body)
			require.Equal(t, as.code, code, "%s %s: %s", route.method, route.path, body)
		}
	}
	require.True(t, store.installation.Paused)
}

// runnerRoutes serves the routes of a fixture's runners as the gateway does.
// It returns the runners that answer them, and what they answer a caller.
func runnerRoutes(t *testing.T, r *githubRunners) (*githubRunners, func(as *auth.AuthInfo, method, path, body string) (int, string)) {
	t.Helper()
	var caller *auth.AuthInfo
	e := echo.New()
	api := e.Group("/vm", func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error { return next(&auth.HttpAuthContext{Context: c, AuthInfo: caller}) }
	})
	registerVMRoutes(api, r.s)
	require.NoError(t, r.s.initGitHubRunners(api))
	r.s.runners.github, r.s.runners.config = r.github, r.config
	return r.s.runners, func(as *auth.AuthInfo, method, path, body string) (int, string) {
		t.Helper()
		caller = as
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(method, "/vm/github-runners/"+path, strings.NewReader(body)))
		return rec.Code, strings.TrimSpace(rec.Body.String())
	}
}

// listedJobs returns what the jobs API says of each job, by its ID.
func listedJobs(t *testing.T, r *githubRunners, info *auth.AuthInfo) map[int64]map[string]json.RawMessage {
	t.Helper()
	c, rec := actionContext(info, "", "", "")
	c.Request().URL.RawQuery = "limit=100"
	require.NoError(t, r.jobs(c))
	var page struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
	jobs := map[int64]map[string]json.RawMessage{}
	for _, job := range page.Data {
		var id int64
		require.NoError(t, json.Unmarshal(job["id"], &id))
		jobs[id] = job
	}
	return jobs
}

// A workspace has no more runners than its limit. The jobs past it wait as
// they are, and get a runner as others go, the job due longest first.
func TestGitHubRunnerLimit(t *testing.T) {
	r, store, info, runtime := runnerFixture(t)
	ctx := context.Background()
	r.config.RunnerLimit = 2
	fakeGitHub(t, r, map[string]string{
		"POST /repos/acme/repo/actions/runners/generate-jitconfig": `201 {"encoded_jit_config":"jit-secret"}`,
	})
	for id := int64(1); id <= 4; id++ {
		require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(id)))
		store.jobs[id].ProvisionAt = at(time.Now().Add(time.Duration(id-10) * time.Second))
	}
	due := func() []int64 {
		t.Helper()
		jobs, err := store.ListDueGitHubRunnerJobs(ctx, r.limit())
		require.NoError(t, err)
		ids := []int64{}
		for _, job := range jobs {
			ids = append(ids, job.ID)
		}
		return ids
	}
	waiting := func() map[int64]string {
		t.Helper()
		waiting := map[int64]string{}
		for id, job := range listedJobs(t, r, info) {
			waiting[id] = strings.Trim(string(job["waiting"]), `"`)
		}
		return waiting
	}
	status := func() string {
		t.Helper()
		c, rec := actionContext(info, "", "", "")
		require.NoError(t, r.status(c))
		return rec.Body.String()
	}
	require.Equal(t, []int64{1, 2, 3, 4}, due())
	require.Contains(t, status(), `"runners":{"running":0,"limit":2}`)

	// In whatever order the gateways get to them, the two jobs that have
	// been due longest get the runners, and the others spend no attempt.
	for _, id := range []int64{4, 3, 2, 1} {
		require.NoError(t, r.provision(ctx, *store.jobs[id]))
	}
	require.Len(t, runtime.requests, 2)
	for id, job := range store.jobs {
		require.Equal(t, id <= 2, job.ProvisionAt == nil, id)
		require.Equal(t, id <= 2, job.VM != "", id)
		require.Equal(t, id <= 2, job.Attempts == 1, id)
		require.Empty(t, job.Error, id)
	}
	require.Empty(t, due(), "a full workspace has no job to start a runner for")
	require.Contains(t, status(), `"runners":{"running":2,"limit":2}`)
	require.Equal(t, map[int64]string{1: "Starting a runner", 2: "Starting a runner", 3: "Waiting for a free runner", 4: "Waiting for a free runner"}, waiting())

	// A runner goes, and the job that has waited longest gets its place.
	store.vm(7, store.jobs[1].VM).DesiredState = types.VMStateDeleted
	require.Equal(t, []int64{3, 4}, due())
	require.Contains(t, status(), `"runners":{"running":1,"limit":2}`)
	require.NoError(t, r.provision(ctx, *store.jobs[4]))
	require.Zero(t, store.jobs[4].Attempts, "the job before it is to get the runner")
	require.NoError(t, r.provision(ctx, *store.jobs[3]))
	require.Len(t, runtime.requests, 3)
	require.Empty(t, due())
	require.Equal(t, "Waiting for a free runner", waiting()[4])

	// A runner that a claim has named counts before its VM exists.
	store.vm(7, store.jobs[2].VM).DesiredState = types.VMStateDeleted
	claimed := *store.jobs[4]
	ok, err := store.ClaimGitHubRunnerJob(ctx, &claimed, "ghr-claimed", r.limit())
	require.NoError(t, err)
	require.True(t, ok)
	require.Contains(t, status(), `"runners":{"running":2,"limit":2}`)

	// Without a limit every job gets its runner, and the status says so.
	r.config.RunnerLimit = -1
	for id := int64(5); id <= 8; id++ {
		require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(id)))
		require.NoError(t, r.provision(ctx, *store.jobs[id]))
	}
	require.Len(t, runtime.requests, 7)
	require.Contains(t, status(), `"runners":{"running":6,"limit":0}`)
	require.Equal(t, "Starting a runner", waiting()[8])
}

// A workspace may be allowed more runners than every workspace is, or
// fewer, which only an administrator of the cluster sets. Its jobs past
// what every workspace is allowed then get runners too.
func TestGitHubRunnerLimitOfAWorkspace(t *testing.T) {
	r, store, info, runtime := runnerFixture(t)
	ctx := context.Background()
	r, request := runnerRoutes(t, r)
	r.config.RunnerLimit = 2
	fakeGitHub(t, r, map[string]string{
		"POST /repos/acme/repo/actions/runners/generate-jitconfig": `201 {"encoded_jit_config":"jit-secret"}`,
	})
	for id := int64(1); id <= 5; id++ {
		require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(id)))
		store.jobs[id].ProvisionAt = at(time.Now().Add(time.Duration(id-10) * time.Second))
	}
	// provision starts a runner for each job that is due one, as a pass
	// does, and returns how many runners were started in all.
	provision := func() int {
		t.Helper()
		due, err := store.ListDueGitHubRunnerJobs(ctx, r.limit())
		require.NoError(t, err)
		for _, job := range due {
			require.NoError(t, r.provision(ctx, job))
		}
		return len(runtime.requests)
	}
	// status is what the workspace is told of its runners.
	status := func() string {
		t.Helper()
		code, body := request(info, http.MethodGet, info.Workspace.ExternalId, "")
		require.Equal(t, http.StatusOK, code, body)
		return body
	}
	// set asks for a limit of the workspace's, or with DELETE for none.
	set := func(as *auth.AuthInfo, method, body string) (int, string) {
		t.Helper()
		return request(as, method, info.Workspace.ExternalId+"/limit", body)
	}
	admin := &auth.AuthInfo{Workspace: &types.Workspace{Id: 1, ExternalId: "operator"}, Token: &types.Token{TokenType: types.TokenTypeClusterAdmin}}
	restricted := *info
	restricted.Token = &types.Token{ExternalId: info.Token.ExternalId, TokenType: types.TokenTypeWorkspaceRestricted}

	require.Equal(t, 2, provision())
	require.Contains(t, status(), `"runners":{"running":2,"limit":2}`)

	// Nobody else sets it, the workspace's own tokens included, and a limit
	// is a number of runners, of a workspace that there is.
	for _, as := range []*auth.AuthInfo{info, &restricted} {
		for _, method := range []string{http.MethodPut, http.MethodDelete} {
			code, body := set(as, method, `{"limit":4}`)
			require.Equal(t, http.StatusUnauthorized, code, body)
		}
	}
	for _, body := range []string{``, `{}`, `{"limit":-1}`, `{"limit":2147483648}`, `{"limit":"4"}`, `{"limit":4.5}`, `{"limit":4,"workspace":7}`} {
		code, answer := set(admin, http.MethodPut, body)
		require.Equal(t, http.StatusBadRequest, code, "%s: %s", body, answer)
	}
	r.s.backend.(*vmBackend).unknown = "gone"
	code, body := request(admin, http.MethodPut, "gone/limit", `{"limit":4}`)
	require.Equal(t, http.StatusNotFound, code, body)
	require.Empty(t, store.limits)
	require.Equal(t, 2, provision())
	require.Contains(t, status(), `"runners":{"running":2,"limit":2}`)

	// With four allowed, the two jobs that have waited longest get runners
	// without waiting for the next pass, and the fifth still waits.
	code, body = set(admin, http.MethodPut, `{"limit":4}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, `{"running":2,"limit":4}`, body)
	require.Len(t, r.wake, 1, "the jobs that waited are looked at now")
	require.Equal(t, 4, provision())
	require.Contains(t, status(), `"runners":{"running":4,"limit":4}`)
	require.Zero(t, store.jobs[5].Attempts)
	require.Equal(t, "Waiting for a free runner", strings.Trim(string(listedJobs(t, r, info)[5]["waiting"]), `"`))

	// Fewer than it has: its runners stay, and it gets no other.
	code, body = set(admin, http.MethodPut, `{"limit":1}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, `{"running":4,"limit":1}`, body)
	require.Equal(t, 4, provision())
	for id := int64(1); id <= 4; id++ {
		require.Equal(t, types.VMStateRunning, store.vm(7, store.jobs[id].VM).DesiredState, id)
	}

	// Any number.
	code, body = set(admin, http.MethodPut, `{"limit":0}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, `{"running":4,"limit":0}`, body)
	require.Equal(t, 5, provision())

	// What every workspace is allowed, again.
	code, body = set(admin, http.MethodDelete, "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, `{"running":5,"limit":2}`, body)
	require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(6)))
	require.Equal(t, 5, provision())
	require.Contains(t, status(), `"runners":{"running":5,"limit":2}`)
}

// A runner that no queued job is left for goes at once, and not when its
// time to be idle is up. GitHub refuses to let go of one it has given a job,
// which is no error.
func TestGitHubRunnerSurplusGoesAtOnce(t *testing.T) {
	r, store, _, runtime := runnerFixture(t)
	ctx := context.Background()
	routes := map[string]string{
		"GET /repos/acme/repo/actions/runners":      `200 {"runners":[{"id":9,"status":"online"}]}`,
		"DELETE /repos/acme/repo/actions/runners/9": `204 {}`,
	}
	calls := fakeGitHub(t, r, routes)
	runner := func(id int64, name string) *types.VM {
		store.jobs[id] = queuedJob(id)
		store.jobs[id].VM = name
		v := &types.VM{ID: "vm-" + name, Name: name, WorkspaceID: 7, TokenID: "token", DesiredState: types.VMStateRunning,
			Status: types.VMStateRunning, EverRunning: true, ContainerID: name + "-container", StubID: "stub",
			CreatedAt: time.Now(), UpdatedAt: time.Now(), LastActiveAt: time.Now()}
		runtime.containers.states[v.ContainerID] = &types.ContainerState{ContainerId: v.ContainerID, Status: types.ContainerStatusRunning}
		store.rows[v.ID] = v
		return v
	}
	first, second, third := runner(1, "ghr-a"), runner(2, "ghr-b"), runner(3, "ghr-c")
	starting := runner(4, "ghr-d")
	starting.Status = types.VMStateStarting
	runtime.containers.states[starting.ContainerID].Status = types.ContainerStatusPending
	store.jobs[4].Status = types.GitHubJobCompleted
	reconcile := func(removed ...*types.VM) {
		t.Helper()
		for _, v := range []*types.VM{first, second, third, starting} {
			if v.Status != types.VMStateDeleted {
				require.NoError(t, r.s.reconcileVM(ctx, v))
				require.Equal(t, slices.Contains(removed, v), v.Status == types.VMStateDeleted, v.Name)
			}
		}
	}

	// Each queued job has a runner, and a runner that is not up yet is left
	// to start, whatever became of its job.
	reconcile()
	require.Empty(t, calls())

	// Runners are interchangeable: the job that was cancelled leaves its
	// runner to another job, and the last runner is the one too many.
	store.jobs[1].Status, store.jobs[1].Conclusion = types.GitHubJobCompleted, "cancelled"
	reconcile(third)
	require.Equal(t, []string{"GET /repos/acme/repo/actions/runners", "DELETE /repos/acme/repo/actions/runners/9"}, calls())
	require.Nil(t, store.jobs[3].ProvisionAt, "a runner that is one too many is not replaced")

	// GitHub has given the runner a job that the gateway has yet to hear of.
	store.jobs[2].Status = types.GitHubJobCompleted
	routes["DELETE /repos/acme/repo/actions/runners/9"] = `422 {"message":"Runner is busy"}`
	saved := store.rows[second.ID].UpdatedAt
	reconcile()
	require.Len(t, calls(), 4)
	require.True(t, store.rows[second.ID].UpdatedAt.After(saved), "a busy runner gets more time")
	store.jobs[3].Status, store.jobs[3].RunnerName = types.GitHubJobInProgress, second.Name

	// No job of its repository and label is queued, so the last one goes
	// too. A job of another repository is not one that it could take.
	store.jobs[5] = queuedJob(5)
	store.jobs[5].Repository = "acme/site"
	store.jobs[6] = queuedJob(6)
	store.jobs[6].Label = "beam-8vcpu-ubuntu-2404"
	routes["DELETE /repos/acme/repo/actions/runners/9"] = `204 {}`
	reconcile(first)
	require.Len(t, calls(), 6)
	require.Equal(t, types.VMStateRunning, store.rows[second.ID].Status)
}

// GitHub says when an account that asked too much of it may ask again. Its
// jobs wait until then without being tried, which spends no attempt.
func TestGitHubRunnerWaitsWhileGitHubLimitsRequests(t *testing.T) {
	r, store, info, runtime := runnerFixture(t)
	ctx := context.Background()
	var mu sync.Mutex
	limited, served := true, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		served++
		if limited {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"message":"You have exceeded a secondary rate limit."}`)
			return
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"encoded_jit_config":"jit-secret"}`)
	}))
	t.Cleanup(server.Close)
	r.github.apiURL = server.URL
	asked := func() int {
		mu.Lock()
		defer mu.Unlock()
		return served
	}
	for id := int64(1); id <= 2; id++ {
		require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(id)))
		store.jobs[id].RunID = 77
	}

	// The attempt that GitHub refuses is not one to give up after.
	var limit *githubLimit
	require.ErrorAs(t, r.provision(ctx, *store.jobs[1]), &limit)
	require.WithinDuration(t, time.Now().Add(2*time.Minute), limit.Until, 5*time.Second)
	first := store.jobs[1]
	require.Equal(t, 1, first.Attempts)
	require.NotNil(t, first.ProvisionAt)
	require.Equal(t, "register the runner with GitHub: "+limit.Error(), first.Error)
	require.Equal(t, "GitHub is limiting requests", first.Waiting)

	// Until GitHub takes its calls again the account's jobs are due then,
	// and nothing is asked of GitHub for them or for anything else.
	first.ProvisionAt = at(time.Now())
	for id, attempts := range map[int64]int{1: 1, 2: 0} {
		job := store.jobs[id]
		require.NoError(t, r.provision(ctx, *job))
		require.Equal(t, attempts, job.Attempts, id)
		require.Equal(t, limit.Until, *job.ProvisionAt, id)
		require.Equal(t, "GitHub is limiting requests", r.waiting(*job, false, false), id)
	}
	code, body := runnerRequest(t, info, r.cancelJob, "1", "")
	require.Equal(t, 429, code)
	require.JSONEq(t, fmt.Sprintf(`{"message":%q}`, limit.Error()), body)
	idle := &types.VM{ID: "vm-runner", Name: "ghr-test", WorkspaceID: 7, TokenID: "token", DesiredState: types.VMStateRunning,
		Status: types.VMStateRunning, EverRunning: true, ContainerID: "runner-container", StubID: "stub",
		CreatedAt: time.Now(), UpdatedAt: time.Now().Add(-runnerIdle - time.Minute), LastActiveAt: time.Now()}
	runtime.containers.states[idle.ContainerID] = &types.ContainerState{ContainerId: idle.ContainerID, Status: types.ContainerStatusRunning}
	store.rows[idle.ID], store.jobs[3] = idle, queuedJob(3)
	store.jobs[3].VM = idle.Name
	require.NoError(t, r.s.reconcileVM(ctx, idle), "a runner that can't be let go of yet is no worse for it")
	require.Equal(t, types.VMStateRunning, idle.Status)
	require.Equal(t, 1, asked())
	require.Empty(t, runtime.requests)

	// The jobs get their runners once GitHub takes the account's calls.
	mu.Lock()
	limited = false
	mu.Unlock()
	r.github.notBefore[42] = time.Now().Add(-time.Second)
	for _, id := range []int64{1, 2} {
		store.jobs[id].ProvisionAt = at(time.Now())
		require.NoError(t, r.provision(ctx, *store.jobs[id]))
		require.Nil(t, store.jobs[id].ProvisionAt)
		require.Empty(t, store.jobs[id].Waiting)
	}
	require.Len(t, runtime.requests, 2)
	require.Equal(t, 2, store.jobs[1].Attempts)
	require.Equal(t, 1, store.jobs[2].Attempts)
}

// GitHub says to wait in more than one way, and refuses with the same status
// for reasons that do not pass.
func TestGitHubLimitsAreReadFromResponses(t *testing.T) {
	reset := time.Now().Add(10 * time.Minute).Truncate(time.Second)
	remaining := map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": strconv.FormatInt(reset.Unix(), 10)}
	for name, tc := range map[string]struct {
		status  int
		header  map[string]string
		message string
		until   time.Time
		refused int
	}{
		"told how long to wait":           {status: 403, header: map[string]string{"Retry-After": "30"}, until: time.Now().Add(30 * time.Second)},
		"unavailable for a while":         {status: 503, header: map[string]string{"Retry-After": "5"}, until: time.Now().Add(5 * time.Second)},
		"out of requests until a time":    {status: 403, header: remaining, message: "API rate limit exceeded for installation ID 42.", until: reset},
		"too many requests":               {status: 429, until: time.Now().Add(time.Minute)},
		"limited, and not told how long":  {status: 403, message: "You have exceeded a secondary rate limit.", until: time.Now().Add(time.Minute)},
		"the last request it takes":       {status: 202, header: remaining, until: reset},
		"requests left":                   {status: 202, header: map[string]string{"X-RateLimit-Remaining": "4999", "X-RateLimit-Reset": remaining["X-RateLimit-Reset"]}},
		"refused for what does not pass":  {status: 403, message: "Resource not accessible by integration", refused: 403},
		"refused with requests left":      {status: 404, header: map[string]string{"X-RateLimit-Remaining": "12"}, message: "Not Found", refused: 404},
		"failed without a time to wait":   {status: 502, refused: 502},
		"a time to wait that is no count": {status: 500, header: map[string]string{"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}, refused: 500},
	} {
		t.Run(name, func(t *testing.T) {
			r, _, _, _ := runnerFixture(t)
			var served atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				served.Add(1)
				for key, value := range tc.header {
					w.Header().Set(key, value)
				}
				w.WriteHeader(tc.status)
				fmt.Fprintf(w, `{"message":%q}`, tc.message)
			}))
			t.Cleanup(server.Close)
			r.github.apiURL = server.URL

			// A run that has finished is as cancelled as it can be, so this
			// call fails only if GitHub refuses it.
			err := r.github.cancelRun(context.Background(), 42, "acme/repo", 77)
			until, limited := r.github.limited(42)
			require.Equal(t, !tc.until.IsZero(), limited)
			require.Equal(t, tc.refused, githubStatus(err))
			var limit *githubLimit
			require.Equal(t, tc.status >= 300 && limited, errors.As(err, &limit), "%v", err)
			if !limited {
				require.Equal(t, tc.status < 300, err == nil, "%v", err)
				return
			}

			require.WithinDuration(t, tc.until, until, 5*time.Second)
			if limit != nil {
				require.Equal(t, until, limit.Until)
			}

			// GitHub is not asked again for the installation, and is for
			// another one.
			require.ErrorAs(t, r.github.cancelRun(context.Background(), 42, "acme/repo", 77), &limit)
			require.Equal(t, until, limit.Until)
			require.EqualValues(t, 1, served.Load())
			r.github.tokens[43] = r.github.tokens[42]
			_ = r.github.cancelRun(context.Background(), 43, "acme/repo", 77)
			require.EqualValues(t, 2, served.Load())
			r.github.forget(42)
			_, limited = r.github.limited(42)
			require.False(t, limited, "an installation that is gone is forgotten")
		})
	}
}

// A job says when each step that got it its runner was done.
func TestGitHubRunnerJobTimeline(t *testing.T) {
	r, store, info, runtime := runnerFixture(t)
	ctx := context.Background()
	fakeGitHub(t, r, map[string]string{
		"POST /repos/acme/repo/actions/runners/generate-jitconfig": `201 {"encoded_jit_config":"jit-secret"}`,
		"GET /repos/acme/repo/actions/runners":                     `200 {"runners":[{"id":9}]}`,
		"DELETE /repos/acme/repo/actions/runners/9":                `204 {}`,
	})
	timeline := func(id int64) map[string]string {
		t.Helper()
		steps := map[string]string{}
		if listed, ok := listedJobs(t, r, info)[id]["timeline"]; ok {
			require.NoError(t, json.Unmarshal(listed, &steps))
		}
		for step, at := range steps {
			require.Regexp(t, `^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$`, at, step)
		}
		return steps
	}
	format := func(at *time.Time) string { return at.UTC().Format("2006-01-02T15:04:05.000Z") }

	// A job recorded before its steps were timed has no timeline.
	store.jobs[9] = queuedJob(9)
	require.Empty(t, timeline(9))
	require.NotContains(t, listedJobs(t, r, info)[9], "timeline")

	started := time.Now()
	require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(1)))
	job := store.jobs[1]
	require.WithinDuration(t, started, *job.ReceivedAt, time.Second)
	require.Equal(t, map[string]string{"received_at": format(job.ReceivedAt)}, timeline(1))

	require.NoError(t, r.provision(ctx, *job))
	require.NotNil(t, job.RegisteredAt)
	require.False(t, job.RegisteredAt.Before(*job.ReceivedAt))
	require.WithinDuration(t, started, *job.RegisteredAt, time.Second)
	require.Nil(t, job.VMReadyAt, "the runner's VM has not been found ready yet")
	require.Equal(t, map[string]string{"received_at": format(job.ReceivedAt), "registered_at": format(job.RegisteredAt)}, timeline(1))

	// The worker says when the VM started, which is nearer than when the
	// gateway next looks at it. The first time counts.
	v, err := r.s.repo.GetVM(ctx, 7, job.VM)
	require.NoError(t, err)
	ready := time.Now().Add(-3 * time.Second).Truncate(time.Second)
	runtime.containers.states[v.ContainerID].StartedAt = ready.Unix()
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.NotNil(t, job.VMReadyAt)
	require.True(t, ready.Equal(*job.VMReadyAt), job.VMReadyAt)
	runtime.containers.states[v.ContainerID].StartedAt++
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.True(t, ready.Equal(*job.VMReadyAt), job.VMReadyAt)
	require.Equal(t, map[string]string{"received_at": format(job.ReceivedAt), "registered_at": format(job.RegisteredAt),
		"vm_ready_at": format(&ready), "runner_ready_at": format(&ready)}, timeline(1), "from here, the job waits for GitHub")
	require.Regexp(t, `\.000Z$`, timeline(1)["vm_ready_at"])

	// The times are of the runner started for the job, so those of a runner
	// that is replaced go with it. When the job was received stays.
	received, registered := *job.ReceivedAt, *job.RegisteredAt
	delete(runtime.containers.states, v.ContainerID)
	require.NoError(t, r.s.reconcileVM(ctx, v))
	job.ProvisionAt = at(time.Now())
	require.NoError(t, r.provision(ctx, *job))
	require.Equal(t, received, *job.ReceivedAt)
	require.False(t, job.RegisteredAt.Before(registered))
	require.Nil(t, job.VMReadyAt)
	require.Nil(t, job.RunnerReadyAt, "a runner that stopped was not one that GitHub could give the job")
	require.Len(t, timeline(1), 2)

	// A VM that the worker gave no time for was ready when it was found so.
	v, err = r.s.repo.GetVM(ctx, 7, job.VM)
	require.NoError(t, err)
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.NotNil(t, job.VMReadyAt)
	require.WithinDuration(t, time.Now(), *job.VMReadyAt, time.Second)
}

// A job is told that it waits for capacity while its runner's VM has no
// worker to start on, and when the scheduler gave up on finding it one.
func TestGitHubRunnerJobWaitsForCapacity(t *testing.T) {
	r, store, _, runtime := runnerFixture(t)
	ctx := context.Background()
	fakeGitHub(t, r, map[string]string{
		"GET /repos/acme/repo/actions/runners":      `200 {"runners":[{"id":9}]}`,
		"DELETE /repos/acme/repo/actions/runners/9": `204 {}`,
	})
	store.jobs[1] = queuedJob(1)
	store.jobs[1].VM, store.jobs[1].Attempts = "ghr-test", 1
	job := store.jobs[1]
	v := &types.VM{ID: "vm-runner", Name: "ghr-test", WorkspaceID: 7, TokenID: "token", DesiredState: types.VMStateRunning,
		Status: types.VMStateStarting, ContainerID: "runner-container", StubID: "stub",
		CreatedAt: time.Now(), UpdatedAt: time.Now(), LastActiveAt: time.Now()}
	state := &types.ContainerState{ContainerId: v.ContainerID, Status: types.ContainerStatusPending}
	runtime.containers.states[v.ContainerID] = state
	store.rows[v.ID] = v
	waits := func() string {
		t.Helper()
		require.NoError(t, r.s.reconcileVM(ctx, v))
		return r.waiting(*job, false, false)
	}

	require.Equal(t, "Starting a runner", waits(), "a runner takes a moment to be given a worker")
	v.CreatedAt = time.Now().Add(-runnerPlacement - time.Second)
	require.Equal(t, "Waiting for capacity", waits())
	state.WorkerId = "worker"
	require.Equal(t, "Starting a runner", waits(), "the runner has a worker to start on")
	state.WorkerId = ""
	require.Equal(t, "Waiting for capacity", waits())
	store.jobs[2] = &types.GitHubRunnerJob{ID: 2, WorkspaceID: 7, Status: types.GitHubJobInProgress, RunnerName: "ghr-other"}
	require.Equal(t, "Waiting for capacity", waits(), "another runner's job is not this runner's")

	// The scheduler gives up, and the job is tried again later.
	delete(runtime.containers.states, v.ContainerID)
	runtime.containers.requestStatus = types.ContainerRequestStatusFailed
	require.Equal(t, "Waiting for capacity", waits())
	require.Equal(t, types.VMStateDeleted, v.Status)
	require.Equal(t, "no worker had capacity for the runner", job.Error)
	require.WithinDuration(t, time.Now().Add(5*time.Second), *job.ProvisionAt, time.Second)
	require.Empty(t, runtime.requests, "runners never restart")
}

// queuedRunner provisions the runner of job 1 in a workspace that refuses to
// start it, and returns its VM and a log of each status the VM is saved
// with from then on, which is what anyone who lists the VMs can find.
func queuedRunner(t *testing.T, r *githubRunners, store *runnerStore) (*types.VM, *types.GitHubRunnerJob, func() []string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(1)))
	job := store.jobs[1]
	require.NoError(t, r.provision(ctx, *job))
	require.Nil(t, job.ProvisionAt, "the job has its runner")
	v, err := store.GetVM(ctx, 7, job.VM)
	require.NoError(t, err)

	var statuses []string
	store.onSave = func(saved *types.VM) { statuses = append(statuses, saved.Status) }
	return v, job, func() []string { return statuses }
}

// A runner that the workspace's concurrency limit has no room for is queued
// until there is room, and its job says so. It is never in error, which a
// runner that keeps crashing is, and it is not replaced: another would be
// refused too, at the price of one more runner registered with GitHub.
func TestGitHubRunnerWaitsForRoomInTheWorkspaceLimit(t *testing.T) {
	r, store, _, runtime := runnerFixture(t)
	ctx := context.Background()
	calls := fakeGitHub(t, r, map[string]string{
		"POST /repos/acme/repo/actions/runners/generate-jitconfig": `201 {"encoded_jit_config":"jit-secret"}`,
		"GET /repos/acme/repo/actions/runners":                     `200 {"runners":[{"id":9}]}`,
		"DELETE /repos/acme/repo/actions/runners/9":                `204 {}`,
	})
	runtime.refuse = &types.ThrottledByConcurrencyLimitError{Reason: "cpu quota exceeded"}
	v, job, statuses := queuedRunner(t, r, store)
	require.Equal(t, types.VMStateQueued, v.Status)
	require.Equal(t, "Workspace CPU limit reached", v.Waiting)
	require.Empty(t, v.Error)
	require.Empty(t, v.ContainerID)
	response, err := json.Marshal(r.s.response(v))
	require.NoError(t, err)
	require.Contains(t, string(response), `"status":"queued"`)
	require.Contains(t, string(response), `"waiting":"Workspace CPU limit reached"`)
	require.NotContains(t, string(response), "concurrency_limit_reached")

	// However often it is refused, it stays as it is and asks again at
	// once, so that it starts as soon as there is room.
	for range 7 {
		require.NoError(t, r.s.reconcileVM(ctx, v))
	}
	require.Equal(t, 8, runtime.refusals)
	require.Equal(t, types.VMStateQueued, v.Status)
	require.Empty(t, v.Error)
	require.NotEmpty(t, statuses())
	for _, status := range statuses() {
		require.Equal(t, types.VMStateQueued, status, "it is never seen starting or in error between two refusals")
	}
	require.Equal(t, types.VMStateRunning, v.DesiredState)
	require.Equal(t, "Workspace CPU limit reached", r.waiting(*job, false, false))
	require.Nil(t, job.ProvisionAt, "no runner is started in its place")
	require.Equal(t, 1, job.Attempts)
	require.Empty(t, job.Error)
	require.Len(t, calls(), 1, "GitHub keeps the runner it registered")

	// There is room: it starts, and is then a VM like any that starts.
	runtime.refuse = nil
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.Len(t, runtime.requests, 1)
	require.Equal(t, runtime.requests[0], v.ContainerID)
	require.Equal(t, types.VMStateStarting, v.Status)
	require.Empty(t, v.Waiting)

	// A gateway that stopped before it could say so left it queued with its
	// runtime, which the next pass, of any gateway, finds starting.
	v.Status, v.Waiting = types.VMStateQueued, "Workspace CPU limit reached"
	require.NoError(t, store.SaveVM(ctx, v))
	runtime.containers.states[v.ContainerID] = &types.ContainerState{ContainerId: v.ContainerID, Status: types.ContainerStatusPending, WorkerId: "worker"}
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.Equal(t, types.VMStateStarting, store.rows[v.ID].Status)
	require.Empty(t, store.rows[v.ID].Waiting)
	require.Len(t, runtime.requests, 1, "it is not started again")
	require.Equal(t, "Starting a runner", r.waiting(*job, false, false))
	require.Equal(t, types.VMStateRunning, v.DesiredState)

	// A runner that fails to start for any other reason is given up on.
	delete(runtime.containers.states, v.ContainerID)
	v.ContainerID, v.Status, v.Error, v.LaunchAttempts = "", types.VMStateError, "no image", 5
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.Equal(t, types.VMStateDeleted, v.Status)
	require.NotNil(t, job.ProvisionAt)
}

// A runner of a workspace that has run out of credit is queued as well, and
// its job says what it waits for. Billing is asked for it no more often than
// for a VM in error, and it starts once there is credit.
func TestGitHubRunnerWaitsForCredit(t *testing.T) {
	r, store, _, runtime := runnerFixture(t)
	ctx := context.Background()
	calls := fakeGitHub(t, r, map[string]string{
		"POST /repos/acme/repo/actions/runners/generate-jitconfig": `201 {"encoded_jit_config":"jit-secret"}`,
	})
	gateway := r.s.gateway.(*vmGateway)
	stubs := 0
	asked := func() int {
		if gateway.stub != nil {
			stubs, gateway.stub = stubs+1, nil
		}
		return stubs
	}

	// The gateway asks billing before it makes the VM's stub.
	gateway.refuse = "insufficient_credits: Out of credits. Add credits at https://platform.beam.cloud/settings/credits to keep running workloads."
	v, job, statuses := queuedRunner(t, r, store)
	require.Equal(t, types.VMStateQueued, v.Status)
	require.Equal(t, "Workspace is out of credits", v.Waiting)
	require.Empty(t, v.Error)
	require.Equal(t, 1, asked())

	// It was refused a moment ago, and is not started again yet.
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.Equal(t, 1, asked())
	require.Equal(t, "Workspace is out of credits", r.waiting(*job, false, false))

	// The scheduler refuses a VM that has its stub in the same words.
	gateway.refuse = ""
	runtime.refuse = &types.InsufficientCreditsError{Code: "insufficient_credits", Reason: "Out of credits."}
	for range 6 {
		v.UpdatedAt = time.Now().Add(-time.Minute)
		require.NoError(t, r.s.reconcileVM(ctx, v))
	}
	require.Equal(t, 2, asked(), "the stub is made once")
	require.Equal(t, 6, runtime.refusals)
	require.Equal(t, types.VMStateQueued, v.Status)
	for _, status := range statuses() {
		require.Equal(t, types.VMStateQueued, status)
	}
	require.Nil(t, job.ProvisionAt, "no runner is started in its place")
	require.Equal(t, 1, job.Attempts)
	require.Len(t, calls(), 1, "GitHub keeps the runner it registered")

	// Credit was added.
	runtime.refuse = nil
	v.UpdatedAt = time.Now().Add(-time.Minute)
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.Len(t, runtime.requests, 1)
	require.Equal(t, types.VMStateStarting, v.Status)
	require.Empty(t, v.Waiting)
}

// What billing refuses for another reason than credit does not pass on its
// own, and neither does any other failure to start: the runner is in error,
// and says why.
func TestGitHubRunnerIsInErrorWhenItCannotStart(t *testing.T) {
	for name, refuse := range map[string]func(*vmGateway, *vmRuntime) string{
		"blocked workspace": func(gateway *vmGateway, _ *vmRuntime) string {
			gateway.refuse = "workspace_blocked: This workspace has been blocked."
			return gateway.refuse
		},
		"credit could not be read": func(_ *vmGateway, runtime *vmRuntime) string {
			runtime.refuse = &types.InsufficientCreditsError{Code: "billing_unavailable", Reason: "unable to verify credit balance; try again shortly"}
			return runtime.refuse.Error()
		},
		"an image that is not a credit": func(gateway *vmGateway, _ *vmRuntime) string {
			gateway.refuse = "insufficient_credits_image is not a VM image"
			return gateway.refuse
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, store, _, runtime := runnerFixture(t)
			fakeGitHub(t, r, map[string]string{
				"POST /repos/acme/repo/actions/runners/generate-jitconfig": `201 {"encoded_jit_config":"jit-secret"}`,
			})
			message := refuse(r.s.gateway.(*vmGateway), runtime)
			v, _, _ := queuedRunner(t, r, store)
			require.Equal(t, types.VMStateError, v.Status)
			require.Equal(t, message, v.Error)
			require.Empty(t, v.Waiting)
		})
	}
}

// Only a runner is queued. A VM that someone starts is refused in so many
// words, as before, so that whoever waits for it is told at once.
func TestVMRefusedByTheWorkspaceLimitIsInError(t *testing.T) {
	r, _, info, runtime := runnerFixture(t)
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	runtime.refuse = &types.ThrottledByConcurrencyLimitError{Reason: "cpu quota exceeded"}
	v, err := r.s.createVM(ctx, info, types.VMCreateOptions{Name: "ghr-mine", Spec: types.VMSpec{ImageID: "image"}})
	require.NoError(t, err)
	require.Equal(t, types.VMStateError, v.Status)
	require.Equal(t, "concurrency_limit_reached: cpu quota exceeded", v.Error)

	// Its name makes it no runner, as no job was given it, and the pass that
	// tries it again is told that it failed.
	v.UpdatedAt = time.Now().Add(-time.Minute)
	require.ErrorIs(t, r.s.reconcileVM(ctx, v), runtime.refuse)
	require.Equal(t, 2, runtime.refusals)
	require.NotEqual(t, types.VMStateQueued, v.Status)
	require.Empty(t, v.Waiting)
}

// A gateway from before runners were queued left one that the workspace's
// limit had no room for in error. It is read as queued, and is at its next
// pass.
func TestGitHubRunnerLeftInErrorByAnOlderGatewayIsQueued(t *testing.T) {
	r, store, _, runtime := runnerFixture(t)
	ctx := context.Background()
	calls := fakeGitHub(t, r, map[string]string{})
	store.jobs[1] = queuedJob(1)
	store.jobs[1].VM, store.jobs[1].Attempts = "ghr-test", 1
	job := store.jobs[1]
	refused := &types.ThrottledByConcurrencyLimitError{Reason: "cpu quota exceeded"}
	v := &types.VM{ID: "vm-runner", Name: "ghr-test", WorkspaceID: 7, TokenID: "token", DesiredState: types.VMStateRunning,
		Status: types.VMStateError, Error: refused.Error(), LaunchAttempts: 5, StubID: "stub",
		CreatedAt: time.Now(), UpdatedAt: time.Now(), LastActiveAt: time.Now()}
	runtime.diskName = rootDisk(v)
	store.rows[v.ID] = v

	runtime.refuse = refused
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.Equal(t, 1, runtime.refusals, "it asks at once, however often it was refused")
	require.Equal(t, types.VMStateQueued, v.Status)
	require.Equal(t, "Workspace CPU limit reached", v.Waiting)
	require.Empty(t, v.Error)
	require.Equal(t, types.VMStateRunning, v.DesiredState)
	require.Equal(t, "Workspace CPU limit reached", r.waiting(*job, false, false))
	require.Nil(t, job.ProvisionAt, "no runner is started in its place")
	require.Empty(t, calls(), "GitHub keeps the runner")

	runtime.refuse = nil
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.Len(t, runtime.requests, 1)
	require.Equal(t, types.VMStateStarting, v.Status)
}

// A runner that waits for room in the workspace's limit is removed without
// starting once its job no longer waits for it, so that the room goes to a
// job that does.
func TestGitHubRunnerWaitingForRoomGoesWithItsJob(t *testing.T) {
	for name, change := range map[string]func(*types.GitHubRunnerJob){
		"cancelled": func(job *types.GitHubRunnerJob) {
			job.Status, job.Conclusion = types.GitHubJobCompleted, "cancelled"
		},
		"run by another runner": func(job *types.GitHubRunnerJob) {
			job.Status, job.RunnerName = types.GitHubJobInProgress, "elsewhere"
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, store, _, runtime := runnerFixture(t)
			ctx := context.Background()
			calls := fakeGitHub(t, r, map[string]string{
				"GET /repos/acme/repo/actions/runners":      `200 {"runners":[{"id":9}]}`,
				"DELETE /repos/acme/repo/actions/runners/9": `204 {}`,
			})
			store.jobs[1] = queuedJob(1)
			store.jobs[1].VM, store.jobs[1].Attempts = "ghr-test", 1
			v := &types.VM{ID: "vm-runner", Name: "ghr-test", WorkspaceID: 7, TokenID: "token", DesiredState: types.VMStateRunning,
				Status: types.VMStateQueued, Waiting: "Workspace CPU limit reached", LaunchAttempts: 1, StubID: "stub",
				CreatedAt: time.Now(), UpdatedAt: time.Now(), LastActiveAt: time.Now()}
			runtime.diskName = rootDisk(v)
			store.rows[v.ID] = v

			change(store.jobs[1])
			require.NoError(t, r.s.reconcileVM(ctx, v))
			require.Equal(t, types.VMStateDeleted, v.DesiredState)
			require.Empty(t, runtime.requests, "it never starts")
			require.Nil(t, store.jobs[1].ProvisionAt, "no runner is started in its place")
			require.Equal(t, []string{"GET /repos/acme/repo/actions/runners", "DELETE /repos/acme/repo/actions/runners/9"}, calls())

			// It is gone at the next pass, and waits for nothing.
			require.NoError(t, r.s.reconcileVM(ctx, v))
			require.Equal(t, types.VMStateDeleted, v.Status)
			require.Empty(t, v.Waiting)
		})
	}
}

// The gateways see the same webhook deliveries, so one of them sweeps at a
// time, and not again while a sweep is recent.
func TestGitHubRunnerSweepsOnOneGatewayAtATime(t *testing.T) {
	redis, stateRepo := redisRepositoryFixture(t)
	first, _, _, _ := runnerFixture(t)
	second, _, _, _ := runnerFixture(t)
	first.s.stateRepo, second.s.stateRepo = stateRepo, NewVMRedisRepository(stateRepo.(*VMRedisRepository).rdb)
	calls := fakeGitHub(t, first, map[string]string{"GET /app/hook/deliveries": "200 []"})
	second.github.apiURL = first.github.apiURL
	sweep := func(r *githubRunners) bool {
		t.Helper()
		swept, err := r.sweep(context.Background(), time.Now().Add(-time.Hour))
		require.NoError(t, err)
		return swept
	}

	require.True(t, sweep(first))
	require.False(t, sweep(second), "another gateway swept a moment ago")
	require.False(t, sweep(first))
	require.Len(t, calls(), 1)
	redis.FastForward(runnerRecent)
	require.True(t, sweep(second), "the gateway that asks next takes the sweep")
	require.False(t, sweep(first))
	require.Len(t, calls(), 2)

	// A gateway without Redis has no other to take turns with.
	first.s.stateRepo = nil
	require.True(t, sweep(first))
	require.True(t, sweep(first))
	require.Len(t, calls(), 4)
}
