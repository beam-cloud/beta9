package vm

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
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

// runnerStore is an in-memory GitHubRunnerRepository with one installation.
type runnerStore struct {
	installation *types.GitHubRunnerInstallation
	jobs         map[int64]*types.GitHubRunnerJob
	vms          *vmStore
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
func (r *runnerStore) GetGitHubRunnerInstallationByID(_ context.Context, id int64) (*types.GitHubRunnerInstallation, error) {
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
	rank := map[string]int{"queued": 1, "in_progress": 2, "completed": 3}
	if old, ok := r.jobs[job.ID]; ok {
		if rank[old.Status] <= rank[job.Status] {
			old.Status, old.Conclusion, old.RunnerName = job.Status, job.Conclusion, job.RunnerName
		}
		return nil
	}
	saved := *job
	if job.Status == "queued" {
		saved.ProvisionAt = at(time.Now())
	}
	r.jobs[job.ID] = &saved
	return nil
}
func (r *runnerStore) ListGitHubRunnerJobs(_ context.Context, ws uint, limit, offset int) ([]types.GitHubRunnerJob, int, error) {
	jobs := []types.GitHubRunnerJob{}
	for _, job := range r.jobs {
		if job.WorkspaceID == ws {
			jobs = append(jobs, *job)
		}
	}
	slices.SortFunc(jobs, func(a, b types.GitHubRunnerJob) int { return int(b.ID - a.ID) })
	return jobs[min(offset, len(jobs)):min(offset+limit, len(jobs))], len(jobs), nil
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
func (r *runnerStore) FinishGitHubRunnerJob(_ context.Context, id int64, retry bool, message string) error {
	if !retry {
		r.jobs[id].ProvisionAt = nil
	}
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
func (r *runnerStore) ReleaseGitHubRunner(_ context.Context, ws uint, vm, message string) error {
	for _, v := range r.vms.rows {
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

func runnerFixture(t *testing.T) (*githubRunners, *runnerStore, *auth.AuthInfo, *vmRuntime) {
	t.Helper()
	s, v, info, runtime, _ := fixture()
	delete(s.repo.(*vmStore).rows, v.ID)
	store := &runnerStore{jobs: map[int64]*types.GitHubRunnerJob{}, vms: s.repo.(*vmStore), installation: &types.GitHubRunnerInstallation{
		ID: 42, WorkspaceID: 7, TokenID: info.Token.ExternalId, Account: "acme", AccountType: "Organization"}}
	r := &githubRunners{s: s, repo: store, key: runnerKey(), client: http.DefaultClient, wake: make(chan struct{}, 1),
		redelivered: map[string]int{}, tokens: map[int64]installationToken{42: {Token: "installation-token", ExpiresAt: time.Now().Add(time.Hour)}},
		config: types.GitHubRunnerConfig{AppID: "1", AppSlug: "beam-runners", ClientID: "client", ClientSecret: "client-secret",
			WebhookSecret: "webhook-secret", ImageID: "runner-image"}}
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
	r.apiURL, r.webURL = server.URL, server.URL
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(calls)
	}
}

var beamLabels = []string{"beam-4vcpu-ubuntu-2404"}

func queuedJob(id int64) *types.GitHubRunnerJob {
	return &types.GitHubRunnerJob{ID: id, InstallationID: 42, WorkspaceID: 7, Repository: "acme/repo", Label: beamLabels[0], Status: "queued"}
}

func deliver(t *testing.T, r *githubRunners, event, action string, labels []string, job *types.GitHubRunnerJob, signed bool) int {
	t.Helper()
	body, err := json.Marshal(map[string]any{"action": action, "installation": map[string]any{"id": 42},
		"repository":   map[string]any{"full_name": "acme/repo"},
		"workflow_job": map[string]any{"id": job.ID, "name": "test", "labels": labels, "runner_name": job.RunnerName}})
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
	size, ok := runnerSizeFor([]string{"Beam-8vcpu-Ubuntu-2404"})
	require.True(t, ok)
	require.Equal(t, runnerSize{"beam-8vcpu-ubuntu-2404", 8000, 32768}, size)
	for _, labels := range [][]string{nil, {"ubuntu-latest"}, {"beam-3vcpu-ubuntu-2404"}, {"self-hosted", "beam-4vcpu-ubuntu-2404"}} {
		_, ok := runnerSizeFor(labels)
		require.False(t, ok, labels)
	}
}

func TestGitHubRunnerWebhookIntake(t *testing.T) {
	r, store, _, _ := runnerFixture(t)
	require.Equal(t, 401, deliver(t, r, "workflow_job", "queued", beamLabels, queuedJob(1), false))
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", []string{"ubuntu-latest"}, queuedJob(1), true))
	require.Empty(t, store.jobs)

	finished := queuedJob(1)
	finished.RunnerName = "ghr-other"
	require.Equal(t, 202, deliver(t, r, "workflow_job", "completed", beamLabels, finished, true))
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", beamLabels, queuedJob(1), true))
	require.Equal(t, "completed", store.jobs[1].Status, "late deliveries cannot regress a job")
	require.Equal(t, "ghr-other", store.jobs[1].RunnerName)
	require.Nil(t, store.jobs[1].ProvisionAt)

	<-r.wake
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", beamLabels, queuedJob(2), true))
	require.Equal(t, uint(7), store.jobs[2].WorkspaceID)
	require.NotNil(t, store.jobs[2].ProvisionAt)
	require.Len(t, r.wake, 1)

	require.Equal(t, 202, deliver(t, r, "installation", "suspend", nil, queuedJob(0), true))
	require.Nil(t, store.installation)
	require.Empty(t, r.tokens)
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", beamLabels, queuedJob(3), true))
	require.NotContains(t, store.jobs, int64(3))
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
	require.Equal(t, info.Token.ExternalId, v.TokenID)
	require.EqualValues(t, 4000, v.Spec.CPU)
	require.EqualValues(t, 16384, v.Spec.Memory)
	require.True(t, v.Spec.DockerEnabled)
	require.Contains(t, v.Spec.Env, "BEAM_GITHUB_JIT_CONFIG=jit-secret")
	response, err := json.Marshal(r.s.response(v))
	require.NoError(t, err)
	require.NotContains(t, string(response), "jit-secret")

	// A retry of an attempt which died after creating its runner reuses it.
	job.ProvisionAt = at(time.Now())
	require.NoError(t, r.provision(ctx, *job))
	require.Len(t, calls(), 1)
	require.Nil(t, job.ProvisionAt)

	// A runner which stops before taking a job is replaced.
	delete(runtime.containers.states, v.ContainerID)
	require.NoError(t, r.s.reconcileVM(ctx, v))
	require.Equal(t, types.VMStateDeleted, v.Status)
	require.NoError(t, r.provision(ctx, *job))
	require.Len(t, runtime.requests, 2)
	require.NotEqual(t, v.Name, job.VM)
	require.Nil(t, job.ProvisionAt)

	// A job cancelled before taking a runner needs none, but one that took
	// another job's runner starts one for that job.
	require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(2)))
	store.jobs[2].Status = "completed"
	require.NoError(t, r.provision(ctx, *store.jobs[2]))
	require.Len(t, runtime.requests, 2)
	require.Nil(t, store.jobs[2].ProvisionAt)
	require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(3)))
	store.jobs[3].Status, store.jobs[3].RunnerName = "in_progress", job.VM
	require.NoError(t, r.provision(ctx, *store.jobs[3]))
	require.Len(t, runtime.requests, 3)
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
		"disabled":             {setup: func(_ *types.VM, _ *runnerStore, r *githubRunners) { r.key = nil }, deleted: true},
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
			r.s.repo.(*vmStore).rows[v.ID] = v
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
				require.WithinDuration(t, time.Now(), r.s.repo.(*vmStore).rows[v.ID].UpdatedAt, time.Second, "a busy runner gets more time")
			}
			if name == "named like a runner" {
				require.Equal(t, 1, runtime.snapshots, "ordinary VMs keep periodic snapshots")
			} else if !tc.deleted {
				require.Zero(t, runtime.snapshots, "runner disks are not snapshotted")
			}
		})
	}
}

func TestGitHubRunnerConnectState(t *testing.T) {
	r, _, info, _ := runnerFixture(t)
	state := r.state(info, 42, time.Now().Add(time.Minute).Unix())
	installation, err := r.verify(info, state)
	require.NoError(t, err)
	require.EqualValues(t, 42, installation)
	for _, tampered := range []string{state + "0", "43" + state[2:], strings.Replace(state, ".", ".1", 1)} {
		_, err = r.verify(info, tampered)
		require.Error(t, err, tampered)
	}
	other := *info
	other.Workspace = &types.Workspace{Id: 9}
	_, err = r.verify(&other, state)
	require.Error(t, err)
	other.Workspace, other.Token = info.Workspace, &types.Token{ExternalId: "another-token"}
	_, err = r.verify(&other, state)
	require.Error(t, err)
	_, err = r.verify(info, r.state(info, 42, time.Now().Add(-time.Second).Unix()))
	require.Error(t, err)
}

func TestGitHubRunnerConnectRequiresAccountOwner(t *testing.T) {
	for _, tc := range []struct {
		name, account, membership string
		installedIn               uint
		code                      int
	}{
		{"org owner", `{"login":"acme","type":"Organization"}`, `200 {"state":"active","role":"admin"}`, 0, 204},
		{"org member", `{"login":"acme","type":"Organization"}`, `200 {"state":"active","role":"member"}`, 0, 403},
		{"not in org", `{"login":"acme","type":"Organization"}`, `404 {}`, 0, 403},
		{"user", `{"login":"Alice","type":"User"}`, "", 0, 204},
		{"another user", `{"login":"bob","type":"User"}`, "", 0, 403},
		{"other workspace", `{"login":"acme","type":"Organization"}`, `200 {"state":"active","role":"admin"}`, 9, 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, store, info, _ := runnerFixture(t)
			store.installation = nil
			if tc.installedIn != 0 {
				store.installation = &types.GitHubRunnerInstallation{ID: 42, WorkspaceID: tc.installedIn}
			}
			routes := map[string]string{
				"POST /login/oauth/access_token": `200 {"access_token":"user-token"}`,
				"GET /user":                      `200 {"login":"alice"}`,
				"GET /app/installations/42":      `200 {"account":` + tc.account + `}`,
			}
			if tc.membership != "" {
				routes["GET /orgs/acme/memberships/alice"] = tc.membership
			}
			fakeGitHub(t, r, routes)
			state := r.state(info, 42, time.Now().Add(time.Minute).Unix())
			c, rec := actionContext(info, "", "", fmt.Sprintf(`{"code":"code","state":%q}`, state))
			if err := r.complete(c); err != nil {
				c.Echo().HTTPErrorHandler(err, c)
			}
			require.Equal(t, tc.code, rec.Code, rec.Body.String())
			require.Equal(t, tc.code == 204, store.installation != nil && store.installation.WorkspaceID == 7)
		})
	}
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

// Set BETA9_GITHUB_RUNNER_TEST_DATABASE to the JSON PostgresConfig of a
// disposable database to run the repository's SQL.
func TestGitHubRunnerPostgres(t *testing.T) {
	config := os.Getenv("BETA9_GITHUB_RUNNER_TEST_DATABASE")
	if config == "" {
		t.Skip("BETA9_GITHUB_RUNNER_TEST_DATABASE is not set")
	}
	var postgres types.PostgresConfig
	require.NoError(t, json.Unmarshal([]byte(config), &postgres))
	repo, err := repository.NewBackendPostgresRepository(postgres, nil)
	require.NoError(t, err)
	require.NoError(t, repo.Migrate())
	ctx := context.Background()
	workspace, err := repo.CreateWorkspace(ctx)
	require.NoError(t, err)
	other, err := repo.CreateWorkspace(ctx)
	require.NoError(t, err)

	id := time.Now().UnixNano()
	installation := types.GitHubRunnerInstallation{ID: id, WorkspaceID: workspace.Id, TokenID: "token", Account: "acme", AccountType: "Organization"}
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &installation))
	installation.TokenID = "new-token"
	require.NoError(t, repo.SaveGitHubRunnerInstallation(ctx, &installation))
	moved, second := installation, installation
	moved.WorkspaceID, second.ID = other.Id, id+1
	require.ErrorIs(t, repo.SaveGitHubRunnerInstallation(ctx, &moved), repository.ErrGitHubRunnerConflict)
	require.ErrorIs(t, repo.SaveGitHubRunnerInstallation(ctx, &second), repository.ErrGitHubRunnerConflict)
	saved, err := repo.GetGitHubRunnerInstallation(ctx, workspace.Id)
	require.NoError(t, err)
	require.Equal(t, "new-token", saved.TokenID)

	isDue := func(id int64) bool {
		due, err := repo.ListDueGitHubRunnerJobs(ctx)
		require.NoError(t, err)
		return slices.ContainsFunc(due, func(job types.GitHubRunnerJob) bool { return job.ID == id })
	}
	job := types.GitHubRunnerJob{ID: id, InstallationID: id, WorkspaceID: workspace.Id, Repository: "acme/repo",
		Label: beamLabels[0], Status: "queued", CreatedAt: time.Now()}
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, &job))
	require.True(t, isDue(id))
	stale := job
	claimed, err := repo.ClaimGitHubRunnerJob(ctx, &job, "ghr-a")
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = repo.ClaimGitHubRunnerJob(ctx, &stale, "ghr-b")
	require.NoError(t, err)
	require.False(t, claimed)
	require.False(t, isDue(id), "a claim schedules its retry")
	require.NoError(t, repo.FinishGitHubRunnerJob(ctx, id, false, ""))

	// GitHub ran another job on this job's runner.
	ran := types.GitHubRunnerJob{ID: id + 1, InstallationID: id, WorkspaceID: workspace.Id, Repository: "acme/repo",
		Label: beamLabels[0], Status: "in_progress", RunnerName: "ghr-a", CreatedAt: time.Now()}
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, &ran))
	ran.Status, ran.RunnerName = "queued", ""
	require.NoError(t, repo.SaveGitHubRunnerJob(ctx, &ran))
	require.False(t, isDue(id+1))
	runner, err := repo.GetGitHubRunner(ctx, workspace.Id, "ghr-a")
	require.NoError(t, err)
	require.Equal(t, &types.GitHubRunner{InstallationID: id, Repository: "acme/repo", Connected: true, JobStatus: "in_progress"}, runner)
	runner, err = repo.GetGitHubRunner(ctx, other.Id, "ghr-a")
	require.NoError(t, err)
	require.Nil(t, runner)

	vm := &types.VM{ID: uuid.NewString(), WorkspaceID: workspace.Id, WorkspaceExternalID: workspace.ExternalId, Name: "ghr-a",
		Handle: uuid.NewString(), DesiredState: types.VMStateRunning, LastActiveAt: time.Now()}
	require.NoError(t, repo.CreateVM(ctx, vm))
	require.NoError(t, repo.ReleaseGitHubRunner(ctx, workspace.Id, "ghr-a", "boom"))
	require.True(t, isDue(id))
	vm, err = repo.GetVM(ctx, workspace.Id, "ghr-a")
	require.NoError(t, err)
	require.Equal(t, types.VMStateDeleted, vm.DesiredState)
	jobs, total, err := repo.ListGitHubRunnerJobs(ctx, workspace.Id, 1, 0)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Equal(t, id+1, jobs[0].ID)
	jobs, _, err = repo.ListGitHubRunnerJobs(ctx, workspace.Id, 1, 1)
	require.NoError(t, err)
	require.Equal(t, "boom", jobs[0].Error)

	require.NoError(t, repo.DeleteGitHubRunnerInstallation(ctx, id))
	require.False(t, isDue(id))
	runner, err = repo.GetGitHubRunner(ctx, workspace.Id, "ghr-a")
	require.NoError(t, err)
	require.False(t, runner.Connected)
}
