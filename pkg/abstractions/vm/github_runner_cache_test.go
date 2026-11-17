package vm

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// publishedCache is a generation of a cache disk that a worker published
// for the VM of a runner, and why it was withdrawn if it was.
type publishedCache struct {
	types.GitHubRunnerCacheCandidate
	withdrawn string
}

func (r *runnerStore) SetGitHubRunnerEnhancedCache(ctx context.Context, ws uint, enabled bool) (*types.GitHubRunnerInstallation, error) {
	installation, _ := r.GetGitHubRunnerInstallation(ctx, ws)
	if installation != nil {
		installation.EnhancedCache = enabled
	}
	return installation, nil
}

// current is the cache disk of a workspace that is not retired.
func (r *runnerStore) current(ws uint) *types.GitHubRunnerCache {
	for _, cache := range r.caches {
		if cache.WorkspaceID == ws && cache.RetiredAt == nil {
			return cache
		}
	}
	return nil
}

func (r *runnerStore) CurrentGitHubRunnerCache(_ context.Context, ws uint, name string) (*types.GitHubRunnerCache, error) {
	cache := r.current(ws)
	if cache == nil {
		cache = &types.GitHubRunnerCache{WorkspaceID: ws, DiskName: name, CreatedAt: time.Now()}
		r.caches = append(r.caches, cache)
	}
	found := *cache
	return &found, nil
}

func (r *runnerStore) ClaimGitHubRunnerCachePublisher(_ context.Context, ws uint, disk string, wait time.Duration) (bool, error) {
	cache := r.current(ws)
	if cache == nil || cache.DiskName != disk || cache.PublisherAt != nil && time.Since(*cache.PublisherAt) < wait {
		return false, nil
	}
	cache.PublisherAt = at(time.Now())
	return true, nil
}

func (r *runnerStore) RetireGitHubRunnerCache(_ context.Context, ws uint, age time.Duration) error {
	if cache := r.current(ws); cache != nil && time.Since(cache.CreatedAt) >= age {
		cache.RetiredAt = at(time.Now())
	}
	return nil
}

// ListGitHubRunnerCacheCandidates finds the runner of a generation as the
// query does: by the VM it was published for, which says what it started on.
func (r *runnerStore) ListGitHubRunnerCacheCandidates(context.Context) ([]types.GitHubRunnerCacheCandidate, error) {
	candidates := []types.GitHubRunnerCacheCandidate{}
	for _, published := range r.published {
		cache, v := r.current(published.WorkspaceID), r.vm(published.WorkspaceID, published.Runner)
		if cache == nil || v == nil || v.Spec.RootLayer == nil || published.withdrawn != "" || time.Since(published.CreatedAt) > time.Hour {
			continue
		}
		layer := v.Spec.RootLayer
		if published.DiskName != cache.DiskName || layer.DiskName != cache.DiskName || layer.SnapshotID != cache.SnapshotID || published.SnapshotID == cache.SnapshotID {
			continue
		}
		candidate := published.GitHubRunnerCacheCandidate
		candidate.Parent = cache.SnapshotID
		candidates = append(candidates, candidate)
	}
	slices.SortFunc(candidates, func(a, b types.GitHubRunnerCacheCandidate) int { return a.CreatedAt.Compare(b.CreatedAt) })
	return candidates, nil
}

func (r *runnerStore) PromoteGitHubRunnerCache(_ context.Context, candidate types.GitHubRunnerCacheCandidate) (bool, error) {
	cache := r.current(candidate.WorkspaceID)
	if cache == nil || cache.DiskName != candidate.DiskName || cache.SnapshotID != candidate.Parent {
		return false, nil
	}
	cache.SnapshotID, cache.PublisherAt = candidate.SnapshotID, nil
	return true, nil
}

func (r *runnerStore) RejectGitHubRunnerCacheCandidate(_ context.Context, candidate types.GitHubRunnerCacheCandidate, reason string) error {
	for _, published := range r.published {
		if published.WorkspaceID != candidate.WorkspaceID || published.DiskName != candidate.DiskName || published.SnapshotID != candidate.SnapshotID {
			continue
		}
		published.withdrawn = reason
		for _, cache := range r.caches {
			if cache.WorkspaceID == candidate.WorkspaceID && cache.DiskName == candidate.DiskName {
				cache.PublisherAt = nil
			}
		}
	}
	return nil
}

func (r *runnerStore) ListRetiredGitHubRunnerCaches(_ context.Context, age time.Duration) ([]types.GitHubRunnerCache, error) {
	retired := []types.GitHubRunnerCache{}
	for _, cache := range r.caches {
		if cache.RetiredAt != nil && time.Since(*cache.RetiredAt) >= age {
			retired = append(retired, *cache)
		}
	}
	return retired, nil
}

func (r *runnerStore) DeleteGitHubRunnerCache(_ context.Context, ws uint, disk string) error {
	gone := func(workspace uint, name string) bool { return workspace == ws && name == disk }
	r.caches = slices.DeleteFunc(r.caches, func(cache *types.GitHubRunnerCache) bool {
		return cache.RetiredAt != nil && gone(cache.WorkspaceID, cache.DiskName)
	})
	r.published = slices.DeleteFunc(r.published, func(published *publishedCache) bool {
		return gone(published.WorkspaceID, published.DiskName)
	})
	return nil
}

func (r *runnerStore) GetGitHubRunnerJobByRunner(_ context.Context, ws uint, runner string) (*types.GitHubRunnerJob, error) {
	for _, job := range r.jobs {
		if job.WorkspaceID == ws && job.RunnerName == runner && runner != "" {
			found := *job
			return &found, nil
		}
	}
	return nil, nil
}

const registersRunner = "POST /repos/acme/repo/actions/runners/generate-jitconfig"

// startRunner delivers a queued job, on its repository's default branch or
// on another, and returns the runner VM that is started for it.
func startRunner(t *testing.T, r *githubRunners, store *runnerStore, id int64, defaultBranch bool) *types.VM {
	t.Helper()
	job := queuedJob(id)
	job.RunID, job.DefaultBranch = 100+id, defaultBranch
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", beamLabels, job, true))
	require.NoError(t, r.provision(context.Background(), *store.jobs[id]))
	v := store.vm(7, store.jobs[id].VM)
	require.NotNil(t, v)
	return v
}

// Every runner of an account that has the cache starts on its workspace's
// cache disk, and one at a time, of those started for a job on a default
// branch, keeps what its job adds to it.
func TestGitHubRunnerStartsOnItsWorkspacesCache(t *testing.T) {
	r, store, _, _ := runnerFixture(t)
	fakeGitHub(t, r, map[string]string{registersRunner: `201 {"encoded_jit_config":"jit-secret"}`})

	layer := startRunner(t, r, store, 1, false).Spec.RootLayer
	require.NotNil(t, layer)
	require.True(t, strings.HasPrefix(layer.DiskName, types.RootLayerDiskPrefix), layer.DiskName)
	require.Equal(t, types.RootLayer{DiskName: layer.DiskName, Size: "64Gi", Paths: runnerCachePaths}, *layer,
		"a cache that has no generation yet is started on as it is, and a pull request's job adds nothing to it")
	require.Equal(t, layer, r.s.gateway.(*vmGateway).layer, "the stub of the VM is given the layer")
	require.Len(t, store.caches, 1)

	first := startRunner(t, r, store, 2, true).Spec.RootLayer
	require.Equal(t, layer.DiskName, first.DiskName, "a workspace has one cache")
	require.True(t, first.Publish)
	require.False(t, startRunner(t, r, store, 3, true).Spec.RootLayer.Publish, "one runner at a time adds to the cache")

	// The runner that was to add to the cache is given up on in the end.
	store.caches[0].PublisherAt = at(time.Now().Add(-runnerCachePublisher))
	require.True(t, startRunner(t, r, store, 4, true).Spec.RootLayer.Publish)

	// The next runners start on the generation the cache has by then.
	store.caches[0].SnapshotID = "generation-1"
	require.Equal(t, "generation-1", startRunner(t, r, store, 5, false).Spec.RootLayer.SnapshotID)
	require.Len(t, store.caches, 1)
}

// A cache starts empty again when it is a week old, and the old one is
// deleted once no runner that started on it can be left.
func TestGitHubRunnerCacheStartsAgainWhenItIsOld(t *testing.T) {
	r, store, _, _ := runnerFixture(t)
	ctx := context.Background()
	fakeGitHub(t, r, map[string]string{registersRunner: `201 {"encoded_jit_config":"jit-secret"}`})
	var deleted []string
	r.deleteStored = func(_ context.Context, workspace *types.Workspace, prefix string) error {
		deleted = append(deleted, fmt.Sprintf("%d %s", workspace.Id, prefix))
		return nil
	}

	old := startRunner(t, r, store, 1, true).Spec.RootLayer.DiskName
	store.caches[0].SnapshotID = "generation-9"
	store.published = append(store.published, &publishedCache{GitHubRunnerCacheCandidate: types.GitHubRunnerCacheCandidate{
		WorkspaceID: 7, DiskName: old, SnapshotID: "generation-9", CreatedAt: time.Now()}})
	store.caches[0].CreatedAt = time.Now().Add(-runnerCacheLifetime + time.Minute)
	require.Equal(t, old, startRunner(t, r, store, 2, false).Spec.RootLayer.DiskName)

	store.caches[0].CreatedAt = time.Now().Add(-runnerCacheLifetime)
	fresh := startRunner(t, r, store, 3, true).Spec.RootLayer
	require.NotEqual(t, old, fresh.DiskName)
	require.Empty(t, fresh.SnapshotID, "the new cache is empty")
	require.True(t, fresh.Publish, "and the first job on a default branch adds to it")
	require.Len(t, store.caches, 2)
	require.NotNil(t, store.caches[0].RetiredAt)

	// A runner that started on the old cache still reads it.
	r.deleteCaches(ctx)
	require.Empty(t, deleted)
	require.Len(t, store.caches, 2)

	store.caches[0].RetiredAt = at(time.Now().Add(-runnerCacheRetired))
	r.deleteCaches(ctx)
	require.Equal(t, []string{"7 durable-disks/" + old + "/"}, deleted, "only what is stored for that disk")
	require.Len(t, store.caches, 1)
	require.Equal(t, fresh.DiskName, store.caches[0].DiskName)
	require.Empty(t, store.published)

	// What could not be deleted is still there to delete the next time.
	store.caches[0].RetiredAt = at(time.Now().Add(-runnerCacheRetired))
	r.deleteStored = func(context.Context, *types.Workspace, string) error { return fmt.Errorf("storage is down") }
	r.deleteCaches(ctx)
	require.Len(t, store.caches, 1)

	// Nothing but a cache disk is deleted, whatever a record says.
	for _, name := range []string{"vm-0123456789abcdef", "data", "", "root-layer-a/../vm-0123456789abcdef"} {
		require.Error(t, r.deleteCache(ctx, types.GitHubRunnerCache{WorkspaceID: 7, DiskName: name}), name)
	}
}

// The cache of a workspace moves to what a runner left only when the job
// that ran on that runner is one that GitHub started for a push to its
// repository's default branch, or on its schedule, and that succeeded.
func TestGitHubRunnerCacheKeepsOnlyWhatADefaultBranchJobLeft(t *testing.T) {
	const getsRun, getsRepository = "GET /repos/acme/repo/actions/runs/101", "GET /repos/acme/repo"
	run := func(event, branch, repository string) string {
		return fmt.Sprintf(`200 {"event":%q,"head_branch":%q,"head_repository":{"full_name":%q}}`, event, branch, repository)
	}
	const repository = `200 {"default_branch":"main"}`
	const notDefault = "the run of the job was not started by a push to the default branch of its repository, or on its schedule"

	for name, tc := range map[string]struct {
		// How the job that ran on the runner ended, and what GitHub says
		// of its run. A job that has no status was not reported.
		status, conclusion string
		pullRequest        bool
		run                string
		age                time.Duration
		promoted           bool
		withdrawn          string
		asksGitHub         bool
	}{
		"a push to the default branch":       {status: "completed", conclusion: "success", run: run("push", "main", "acme/repo"), promoted: true, asksGitHub: true},
		"the schedule of the default branch": {status: "completed", conclusion: "success", run: run("schedule", "main", "Acme/Repo"), promoted: true, asksGitHub: true},
		"a pull request from a branch named as the default branch": {status: "completed", conclusion: "success", run: run("pull_request", "main", "acme/repo"),
			withdrawn: notDefault, asksGitHub: true},
		"a pull request that runs with the base repository's rights": {status: "completed", conclusion: "success", run: run("pull_request_target", "main", "acme/repo"),
			withdrawn: notDefault, asksGitHub: true},
		"a run that someone started by hand": {status: "completed", conclusion: "success", run: run("workflow_dispatch", "main", "acme/repo"),
			withdrawn: notDefault, asksGitHub: true},
		"a push to a fork's branch of the same name": {status: "completed", conclusion: "success", run: run("push", "main", "mallory/repo"),
			withdrawn: notDefault, asksGitHub: true},
		"a push to another branch": {status: "completed", conclusion: "success", run: run("push", "release", "acme/repo"),
			withdrawn: notDefault, asksGitHub: true},
		"a job that the webhook said is on another branch": {status: "completed", conclusion: "success", pullRequest: true,
			withdrawn: "the job of the runner was not on a default branch"},
		"a job that failed":                  {status: "completed", conclusion: "failure", withdrawn: "the job of the runner did not succeed"},
		"a job that was cancelled":           {status: "completed", conclusion: "cancelled", withdrawn: "the job of the runner did not succeed"},
		"a job that still runs":              {status: "in_progress"},
		"a job that GitHub has not reported": {},
		"a job that GitHub never reported as finished": {status: "in_progress", age: runnerCacheVerdict + time.Second,
			withdrawn: "GitHub did not say that the job of the runner finished"},
		"a runner that no job is known to have run on": {age: runnerCacheVerdict + time.Second,
			withdrawn: "GitHub did not say that the job of the runner finished"},
		"a run that GitHub does not have":       {status: "completed", conclusion: "success", run: "404 {}", withdrawn: "GitHub does not have the run of the job", asksGitHub: true},
		"a run that GitHub cannot show for now": {status: "completed", conclusion: "success", run: "500 {}", asksGitHub: true},
		"a run that GitHub never showed": {status: "completed", conclusion: "success", run: "500 {}", age: runnerCacheVerdict + time.Second,
			withdrawn: "GitHub did not say what started the run of the job", asksGitHub: true},
	} {
		t.Run(name, func(t *testing.T) {
			r, store, _, _ := runnerFixture(t)
			ctx := context.Background()
			calls := fakeGitHub(t, r, map[string]string{registersRunner: `201 {"encoded_jit_config":"jit-secret"}`, getsRun: tc.run, getsRepository: repository})

			// The runner was started for a job on the default branch, and
			// GitHub gave it the job that the case is about.
			v := startRunner(t, r, store, 1, !tc.pullRequest)
			if tc.pullRequest {
				store.caches[0].PublisherAt = at(time.Now())
			}
			require.NotNil(t, store.caches[0].PublisherAt)
			if tc.status != "" {
				ran := *store.jobs[1]
				ran.Status, ran.Conclusion, ran.RunnerName = tc.status, tc.conclusion, v.Name
				require.Equal(t, 202, deliver(t, r, "workflow_job", tc.status, beamLabels, &ran, true))
			}
			store.published = append(store.published, &publishedCache{GitHubRunnerCacheCandidate: types.GitHubRunnerCacheCandidate{
				WorkspaceID: 7, DiskName: store.caches[0].DiskName, SnapshotID: "generation-1", Runner: v.Name, CreatedAt: time.Now().Add(-tc.age)}})
			before := len(calls())

			r.promoteCaches(ctx)
			r.promoteCaches(ctx)

			cache := store.caches[0]
			if tc.promoted {
				require.Equal(t, "generation-1", cache.SnapshotID)
			} else {
				require.Empty(t, cache.SnapshotID, "runners get nothing that the job left")
			}
			require.Equal(t, tc.withdrawn, store.published[0].withdrawn)

			// Another runner is started to add to the cache as soon as the
			// last is done with, and not while its job may yet be the kind.
			require.Equal(t, tc.promoted || tc.withdrawn != "", cache.PublisherAt == nil)

			asked := len(calls()) - before
			switch {
			case !tc.asksGitHub:
				require.Zero(t, asked, "GitHub is asked about no run that the job rules out")
			case tc.promoted || tc.withdrawn != "":
				require.LessOrEqual(t, asked, 2, "a candidate that is settled is not looked at again")
			}
		})
	}
}

// A runner takes any job with its label. What it leaves is judged by the job
// that ran on it, not by the one it was started for.
func TestGitHubRunnerCacheJudgesTheJobThatRan(t *testing.T) {
	r, store, _, _ := runnerFixture(t)
	ctx := context.Background()
	fakeGitHub(t, r, map[string]string{
		registersRunner:                         `201 {"encoded_jit_config":"jit-secret"}`,
		"GET /repos/acme/repo/actions/runs/101": `200 {"event":"push","head_branch":"main","head_repository":{"full_name":"acme/repo"}}`,
		"GET /repos/acme/repo":                  `200 {"default_branch":"main"}`,
	})

	// The runner started for the push takes the pull request's job.
	publisher := startRunner(t, r, store, 1, true)
	other := startRunner(t, r, store, 2, false)
	finish := func(id int64, runner string) {
		t.Helper()
		ran := *store.jobs[id]
		ran.Status, ran.Conclusion, ran.RunnerName = "completed", "success", runner
		require.Equal(t, 202, deliver(t, r, "workflow_job", "completed", beamLabels, &ran, true))
	}
	finish(2, publisher.Name)
	finish(1, other.Name)
	disk := store.caches[0].DiskName
	store.published = append(store.published, &publishedCache{GitHubRunnerCacheCandidate: types.GitHubRunnerCacheCandidate{
		WorkspaceID: 7, DiskName: disk, SnapshotID: "from-the-pull-request", Runner: publisher.Name, CreatedAt: time.Now()}})

	r.promoteCaches(ctx)
	require.Empty(t, store.caches[0].SnapshotID)
	require.Equal(t, "the job of the runner was not on a default branch", store.published[0].withdrawn)

	// Two runners left a generation of the same one. The first is kept, and
	// the second, which lacks what the first added, is not put in its place.
	first, second := startRunner(t, r, store, 3, true), startRunner(t, r, store, 4, true)
	for i, runner := range []*types.VM{first, second} {
		id := int64(3 + i)
		store.jobs[id].RunID = 101
		finish(id, runner.Name)
		store.published = append(store.published, &publishedCache{GitHubRunnerCacheCandidate: types.GitHubRunnerCacheCandidate{
			WorkspaceID: 7, DiskName: disk, SnapshotID: fmt.Sprintf("generation-%d", i+1), Runner: runner.Name, CreatedAt: time.Now().Add(time.Duration(i) * time.Second)}})
	}
	r.promoteCaches(ctx)
	r.promoteCaches(ctx)
	require.Equal(t, "generation-1", store.caches[0].SnapshotID)
	require.Empty(t, store.published[2].withdrawn, "it is left to be deleted with its disk")

	// A generation of a disk that is not the workspace's cache any more is
	// no candidate.
	require.NoError(t, store.RetireGitHubRunnerCache(ctx, 7, 0))
	store.published[1].SnapshotID = "late"
	r.promoteCaches(ctx)
	require.Equal(t, "generation-1", store.caches[0].SnapshotID)
}

// One switch turns the cache of a workspace's runners off, which deletes it,
// and on again, which starts a new one.
func TestGitHubRunnerCacheSwitch(t *testing.T) {
	r, store, info, _ := runnerFixture(t)
	fakeGitHub(t, r, map[string]string{registersRunner: `201 {"encoded_jit_config":"jit-secret"}`})
	update := func(body string) string {
		t.Helper()
		code, response := runnerRequest(t, info, r.update, "", body)
		require.Equal(t, http.StatusOK, code, response)
		return response
	}

	require.Contains(t, update(`{}`), `"enhanced_cache":true`, "an account has the cache unless it is turned off")
	old := startRunner(t, r, store, 1, true).Spec.RootLayer.DiskName
	require.Contains(t, update(`{"paused":true}`), `"enhanced_cache":true`)
	require.Nil(t, store.caches[0].RetiredAt, "another setting leaves the cache as it is")
	update(`{"paused":false}`)

	require.Contains(t, update(`{"enhanced_cache":false}`), `"enhanced_cache":false`)
	require.NotNil(t, store.caches[0].RetiredAt, "a cache that is turned off is deleted")
	require.Nil(t, startRunner(t, r, store, 2, true).Spec.RootLayer)
	require.Nil(t, r.s.gateway.(*vmGateway).layer)
	require.Len(t, store.caches, 1)

	require.Contains(t, update(`{"enhanced_cache":true}`), `"enhanced_cache":true`)
	fresh := startRunner(t, r, store, 3, true).Spec.RootLayer
	require.NotNil(t, fresh)
	require.NotEqual(t, old, fresh.DiskName)
	require.True(t, fresh.Publish)

	code, response := runnerRequest(t, info, r.update, "", `{"enhanced_cache":"yes"}`)
	require.Equal(t, http.StatusBadRequest, code, response)

	// An account that disconnects leaves no cache for the next one.
	code, response = runnerRequest(t, info, r.disconnect, "", "")
	require.Equal(t, http.StatusNoContent, code, response)
	require.Nil(t, store.current(7))
	c, rec := actionContext(info, "", "", "")
	require.NoError(t, r.status(c))
	require.NotContains(t, rec.Body.String(), "enhanced_cache", "no account is connected to have the setting")

	// As does an account that removes the App.
	store.installation = &types.GitHubRunnerInstallation{ID: 42, WorkspaceID: 7, TokenID: info.Token.ExternalId, Account: "acme", EnhancedCache: true}
	startRunner(t, r, store, 4, false)
	require.NotNil(t, store.current(7))
	require.Equal(t, 202, deliver(t, r, "installation", "deleted", nil, queuedJob(0), true))
	require.Nil(t, store.current(7))
}

// The webhook says which branch a job is on, which is kept from when the
// job is first heard of.
func TestGitHubRunnerWebhookRecordsTheDefaultBranch(t *testing.T) {
	r, store, _, _ := runnerFixture(t)
	onDefault, onOther := queuedJob(1), queuedJob(2)
	onDefault.DefaultBranch = true
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", beamLabels, onDefault, true))
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", beamLabels, onOther, true))
	require.True(t, store.jobs[1].DefaultBranch)
	require.False(t, store.jobs[2].DefaultBranch)
}

// Only the service gives a VM a root layer, for GitHub runners. A request
// can ask for none, and can name no disk as a layer's is named.
func TestRootLayerIsNotAnAPIOption(t *testing.T) {
	for _, spec := range []string{
		`{"image_id":"base","root_layer":{"disk_name":"root-layer-0123456789abcdef","size":"1Gi"}}`,
		`{"image_id":"base","root_layer":{}}`,
		`{"image_id":"base","disks":[{"name":"root-layer-0123456789abcdef","size":"1Gi","mount_path":"/data"}]}`,
	} {
		s, _, info, runtime, gateway := fixture()
		rec := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId, fmt.Sprintf(`{"name":"scratch","spec":%s}`, spec))
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Empty(t, runtime.requests)
		require.Nil(t, gateway.stub)
	}

	s, _, info, _, gateway := fixture()
	rec := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId, `{"name":"ordinary","spec":{"image_id":"base"}}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "root_layer")
	require.Nil(t, gateway.layer, "the stub of an ordinary VM is the one it has today")
}
