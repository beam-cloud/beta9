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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
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
	r.installation = i
	return nil
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
		}
		return nil
	}
	saved := *job
	saved.ProvisionAt = at(time.Now())
	r.jobs[job.ID] = &saved
	return nil
}

func (r *runnerStore) ListGitHubRunnerJobs(_ context.Context, ws uint, filter types.GitHubRunnerJobFilter) (*types.GitHubRunnerJobPage, error) {
	jobs, repositories := []types.GitHubRunnerJob{}, []string{}
	search := strings.ToLower(filter.Search)
	for _, job := range r.jobs {
		if job.WorkspaceID != ws || job.Status != types.GitHubJobCompleted && !r.connected(job.InstallationID) {
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

func (r *runnerStore) ListDueGitHubRunnerJobs(context.Context) ([]types.GitHubRunnerJob, error) {
	due := []types.GitHubRunnerJob{}
	for _, job := range r.jobs {
		if job.ProvisionAt != nil && !job.ProvisionAt.After(time.Now()) && r.connected(job.InstallationID) {
			due = append(due, *job)
		}
	}
	return due, nil
}

func (r *runnerStore) ClaimGitHubRunnerJob(_ context.Context, job *types.GitHubRunnerJob, vm string) (bool, error) {
	stored := r.jobs[job.ID]
	if stored.Attempts != job.Attempts || stored.ProvisionAt == nil || stored.ProvisionAt.After(time.Now()) {
		return false, nil
	}
	stored.VM, stored.Attempts = vm, stored.Attempts+1
	stored.ProvisionAt = at(time.Now().Add(time.Duration(stored.Attempts) * time.Minute))
	job.VM, job.Attempts = vm, stored.Attempts
	return true, nil
}

func (r *runnerStore) FinishGitHubRunnerJob(_ context.Context, id int64, message string) error {
	r.jobs[id].ProvisionAt, r.jobs[id].Error = nil, message
	return nil
}

func (r *runnerStore) FailGitHubRunnerJob(_ context.Context, id int64, message string) error {
	r.jobs[id].Error = message
	return nil
}

func (r *runnerStore) GetGitHubRunner(_ context.Context, ws uint, vm string) (*types.GitHubRunner, error) {
	for _, job := range r.jobs {
		if job.WorkspaceID == ws && job.VM == vm {
			runner := &types.GitHubRunner{InstallationID: job.InstallationID, Repository: job.Repository, Connected: r.connected(job.InstallationID)}
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

func (r *runnerStore) ReplaceGitHubRunner(_ context.Context, ws uint, vm, message string) error {
	for _, v := range r.rows {
		if v.WorkspaceID == ws && v.Name == vm {
			v.DesiredState = types.VMStateDeleted
		}
	}
	for _, job := range r.jobs {
		if job.WorkspaceID == ws && job.VM == vm {
			job.ProvisionAt, job.Error = at(time.Now()), message
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
		ID: 42, WorkspaceID: 7, TokenID: info.Token.ExternalId, Account: "acme", AccountType: "Organization"}}
	delete(store.rows, v.ID)
	s.repo = store
	app := &githubApp{id: runnerConfig.AppID, slug: runnerConfig.AppSlug, clientID: runnerConfig.ClientID, clientSecret: runnerConfig.ClientSecret,
		key: runnerKey(), client: http.DefaultClient, apiURL: "https://api.github.invalid", webURL: "https://github.invalid",
		tokens: map[int64]githubToken{42: {Token: "installation-token", ExpiresAt: time.Now().Add(time.Hour)}}}
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
	body, err := json.Marshal(map[string]any{"action": action, "installation": map[string]any{"id": 42},
		"repository": map[string]any{"full_name": "acme/repo"},
		"workflow_job": map[string]any{"id": job.ID, "name": "test", "workflow_name": "CI", "html_url": "https://github.com/acme/repo/actions/runs/1/job/1",
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
	label, ok := runnerLabel([]string{"Beam-8vCPU-Ubuntu-2404"})
	require.True(t, ok)
	size, ok := runnerSizeFor(label)
	require.True(t, ok)
	require.Equal(t, runnerSize{"beam-8vcpu-ubuntu-2404", 8000, 32768, "50Gi"}, size)

	for _, labels := range [][]string{nil, {"ubuntu-latest"}, {"self-hosted", "linux"}} {
		_, ok := runnerLabel(labels)
		require.False(t, ok, "%v does not ask for Beam", labels)
	}

	// These ask for Beam, but match no runner.
	for _, labels := range [][]string{{"beam-3vcpu-ubuntu-2404"}, {"self-hosted", "beam-4vcpu-ubuntu-2404"}} {
		label, ok := runnerLabel(labels)
		require.True(t, ok, labels)
		_, ok = runnerSizeFor(label)
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
	require.Len(t, r.wake, 1)
	require.Equal(t, types.GitHubRunnerJob{ID: 1, InstallationID: 42, WorkspaceID: 7, Repository: "acme/repo", Workflow: "CI", Name: "test",
		URL: "https://github.com/acme/repo/actions/runs/1/job/1", Label: "beam-4vcpu-ubuntu-2404", Status: "queued",
		CreatedAt: time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC), ProvisionAt: job.ProvisionAt}, *job)

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
	})
	require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(1)))
	due, err := store.ListDueGitHubRunnerJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, r.provision(ctx, due[0]))
	require.NoError(t, r.provision(ctx, due[0]), "a concurrent attempt loses the claim")

	job := store.jobs[1]
	require.Nil(t, job.ProvisionAt)
	require.Len(t, calls(), 1)
	require.Contains(t, calls()[0], `"labels":["beam-4vcpu-ubuntu-2404"]`)
	require.Contains(t, calls()[0], `"name":"`+job.VM+`"`)
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

	// A runner that stops before it takes a job is replaced.
	delete(runtime.containers.states, v.ContainerID)
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.Equal(t, types.VMStateDeleted, v.Status)
	require.Contains(t, job.Error, "stopped before it took a job")
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
			due, err := store.ListDueGitHubRunnerJobs(ctx)
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

func TestGitHubRunnerProvisionRetriesThenGivesUp(t *testing.T) {
	r, store, _, runtime := runnerFixture(t)
	ctx := context.Background()
	r.s.backend.(*vmBackend).token = &types.Token{Active: false}
	require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(1)))
	job := store.jobs[1]
	for attempt := 1; attempt <= runnerAttempts; attempt++ {
		require.ErrorContains(t, r.provision(ctx, *job), "inactive")
		require.Equal(t, attempt, job.Attempts)
		require.True(t, job.ProvisionAt.After(time.Now()), "retries back off")
		require.Contains(t, job.Error, "inactive")
		job.ProvisionAt = at(time.Now())
	}
	require.NoError(t, r.provision(ctx, *job))
	require.Nil(t, job.ProvisionAt)
	require.Contains(t, job.Error, "no runner after 5 attempts")
	require.Empty(t, runtime.requests)
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
	for name, tc := range map[string]struct {
		setup             func(*types.VM, *runnerStore, *githubRunners)
		deleted, replaced bool
		deregister        string
	}{
		"running a job": {setup: assign("in_progress")},
		"job completed": {setup: assign("completed"), deleted: true},
		"exited after a job": {setup: func(v *types.VM, store *runnerStore, r *githubRunners) {
			assign("in_progress")(v, store, r)
			exit(v, store, r)
		}, deleted: true},
		"exited before a job": {setup: exit, deleted: true, replaced: true},
		"failed to launch": {setup: func(v *types.VM, store *runnerStore, r *githubRunners) {
			exit(v, store, r)
			v.ContainerID, v.Status, v.LaunchAttempts = "", types.VMStateError, 5
		}, deleted: true, replaced: true},
		"idle":                 {setup: idle, deregister: "204", deleted: true},
		"idle but just busy":   {setup: idle, deregister: "422"},
		"disconnected":         {setup: func(_ *types.VM, store *runnerStore, _ *githubRunners) { store.installation = nil }, deleted: true},
		"disabled":             {setup: func(_ *types.VM, _ *runnerStore, r *githubRunners) { r.github = nil }, deleted: true},
		"stopped":              {setup: func(v *types.VM, _ *runnerStore, _ *githubRunners) { v.DesiredState = types.VMStateStopped }, deleted: true},
		"past its lifetime":    {setup: func(v *types.VM, _ *runnerStore, _ *githubRunners) { v.CreatedAt = time.Now().Add(-runnerLifetime) }, deleted: true},
		"named like a runner":  {setup: func(_ *types.VM, store *runnerStore, _ *githubRunners) { store.jobs[1].VM = "ghr-another" }},
		"idle without GitHub":  {setup: idle, deregister: "500"},
		"running before a job": {},
		"pending a long time": {setup: func(v *types.VM, store *runnerStore, r *githubRunners) {
			idle(v, store, r)
			v.Status = types.VMStateStarting
			r.s.containers.(*vmContainers).states[v.ContainerID].Status = types.ContainerStatusPending
		}},
	} {
		t.Run(name, func(t *testing.T) {
			r, store, _, runtime := runnerFixture(t)
			routes := map[string]string{}
			if tc.deregister != "" {
				routes["GET /repos/acme/repo/actions/runners"] = `200 {"runners":[{"id":9}]}`
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
			require.Equal(t, tc.deregister != "", slices.Contains(calls(), "GET /repos/acme/repo/actions/runners"), "deregistered")
			if tc.deregister == "422" {
				require.WithinDuration(t, time.Now(), store.rows[v.ID].UpdatedAt, time.Second, "a busy runner gets more time")
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
	require.Equal(t, &types.GitHubRunnerInstallation{ID: 42, WorkspaceID: 7, TokenID: info.Token.ExternalId, Account: "acme", AccountType: "Organization"}, store.installation)
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
	require.Equal(t, &types.GitHubRunnerInstallation{ID: 43, WorkspaceID: 7, TokenID: info.Token.ExternalId, Account: "Alice", AccountType: "User"}, store.installation)
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
	store.jobs[2] = &types.GitHubRunnerJob{ID: 2, InstallationID: 42, WorkspaceID: 7, Repository: "acme/repo", Workflow: "CI", Name: "test",
		URL: "https://github.com/acme/repo/actions/runs/1/job/2", Label: beamLabels[0], Status: "in_progress", RunnerName: "ghr-0123456789ab",
		CreatedAt: started.Add(-time.Minute), StartedAt: &started, VM: "ghr-ba9876543210", Attempts: 2, ProvisionAt: &started, Error: "boom"}
	c, rec := actionContext(info, "", "", "")
	c.Request().URL.RawQuery = "limit=1"
	require.NoError(t, r.jobs(c))
	require.JSONEq(t, `{"total":2,"repositories":["acme/repo"],"data":[{"id":2,"repository":"acme/repo","workflow":"CI","name":"test",
		"url":"https://github.com/acme/repo/actions/runs/1/job/2","label":"beam-4vcpu-ubuntu-2404","status":"in_progress",
		"runner_name":"ghr-0123456789ab","created_at":"2026-11-16T09:00:00Z","started_at":"2026-11-16T09:01:00Z"}]}`, rec.Body.String())

	// A job's error says why it has no runner, which the job above has.
	store.jobs[1].Error = "no runner after 5 attempts: boom"
	c, rec = actionContext(info, "", "", "")
	c.Request().URL.RawQuery = "limit=1&offset=1"
	require.NoError(t, r.jobs(c))
	require.Contains(t, rec.Body.String(), `"status":"queued","created_at":"0001-01-01T00:00:00Z","error":"no runner after 5 attempts: boom"`)

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
	require.Equal(t, map[string]any{"id": float64(42), "account": "acme", "account_type": "Organization", "created_at": "0001-01-01T00:00:00Z"}, status.Installation)
}
