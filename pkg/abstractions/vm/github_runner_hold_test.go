package vm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// heldRun is a workflow run on GitHub that runs only parallel of its jobs at
// once, as one whose matrix sets max-parallel does. GitHub queues every job
// of it at once, and gives the next one to a free runner as soon as one of
// those that run ends.
type heldRun struct {
	t        *testing.T
	r        *githubRunners
	store    *runnerStore
	info     *auth.AuthInfo
	parallel int

	mu       sync.Mutex
	ids      map[string]int64 // the runners GitHub has, by name
	deleted  []string
	assigned map[string]int64 // the job GitHub gave each runner
	// Times that GitHub let a job go and had no free runner to give it.
	missed int
}

func newHeldRun(t *testing.T, parallel int) *heldRun {
	r, store, info, _ := runnerFixture(t)
	h := &heldRun{t: t, r: r, store: store, info: info, parallel: parallel, ids: map[string]int64{}, assigned: map[string]int64{}}
	server := httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(server.Close)
	r.github.apiURL, r.github.webURL = server.URL, server.URL
	return h
}

// serve is GitHub's API for the runners of acme/repo. A runner is online
// once its VM runs.
func (h *heldRun) serve(w http.ResponseWriter, req *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	const runners = "/repos/acme/repo/actions/runners"
	switch {
	case req.Method == http.MethodPost && req.URL.Path == runners+"/generate-jitconfig":
		var body struct {
			Name string `json:"name"`
		}
		_ = json.NewDecoder(req.Body).Decode(&body)
		h.ids[body.Name] = int64(len(h.ids) + len(h.deleted) + 1)
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"encoded_jit_config":"jit"}`)
	case req.Method == http.MethodGet && req.URL.Path == runners:
		listed := []githubRunner{}
		for name, id := range h.ids {
			if only := req.URL.Query().Get("name"); only != "" && only != name {
				continue
			}
			status := "offline"
			if v := h.store.vm(7, name); v != nil && v.Status == types.VMStateRunning {
				status = "online"
			}
			_, busy := h.assigned[name]
			listed = append(listed, githubRunner{ID: id, Name: name, Status: status, Busy: busy})
		}
		json.NewEncoder(w).Encode(map[string]any{"total_count": len(listed), "runners": listed})
	case req.Method == http.MethodDelete && strings.HasPrefix(req.URL.Path, runners+"/"):
		id, _ := strconv.ParseInt(strings.TrimPrefix(req.URL.Path, runners+"/"), 10, 64)
		for name, known := range h.ids {
			if known != id {
				continue
			}
			if _, busy := h.assigned[name]; busy {
				w.WriteHeader(http.StatusUnprocessableEntity)
				fmt.Fprint(w, `{"message":"Runner is busy"}`)
				return
			}
			delete(h.ids, name)
			h.deleted = append(h.deleted, name)
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		h.t.Errorf("unexpected GitHub request %s %s", req.Method, req.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

// free lists the runners that GitHub could give a job now, last name first,
// which is not the order that the gateway keeps them in.
func (h *heldRun) free() []string {
	names := []string{}
	for name := range h.ids {
		v := h.store.vm(7, name)
		if _, busy := h.assigned[name]; !busy && v != nil && v.Status == types.VMStateRunning && v.DesiredState != types.VMStateDeleted {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	slices.Reverse(names)
	return names
}

// release has GitHub start as many queued jobs as the run may run, oldest
// first, each on a free runner, and tell the gateway.
func (h *heldRun) release() {
	h.t.Helper()
	for {
		h.mu.Lock()
		if len(h.assigned) >= h.parallel {
			h.mu.Unlock()
			return
		}
		var next *types.GitHubRunnerJob
		for _, job := range h.store.jobs {
			if job.Status == types.GitHubJobQueued && (next == nil || job.ID < next.ID) {
				next = job
			}
		}
		if next == nil {
			h.mu.Unlock()
			return
		}
		free := h.free()
		if len(free) == 0 {
			h.missed++
			h.mu.Unlock()
			return
		}
		h.assigned[free[0]] = next.ID
		event := *next
		event.RunnerName = free[0]
		h.mu.Unlock()
		require.Equal(h.t, 202, deliver(h.t, h.r, "workflow_job", "in_progress", beamLabels, &event, true))
	}
}

// finish ends the jobs that run, as GitHub tells the gateway.
func (h *heldRun) finish(count int) {
	h.t.Helper()
	h.mu.Lock()
	names := []string{}
	for name := range h.assigned {
		names = append(names, name)
	}
	slices.Sort(names)
	names = names[:min(count, len(names))]
	events := []types.GitHubRunnerJob{}
	for _, name := range names {
		events = append(events, types.GitHubRunnerJob{ID: h.assigned[name], RunnerName: name, Conclusion: "success"})
		delete(h.assigned, name)
	}
	h.mu.Unlock()
	for _, event := range events {
		require.Equal(h.t, 202, deliver(h.t, h.r, "workflow_job", "completed", beamLabels, &event, true))
	}
}

// pass is what the gateway does in a few seconds: it makes the jobs that are
// short of a runner due, starts their runners, and looks at every runner.
func (h *heldRun) pass() {
	h.t.Helper()
	ctx := context.Background()
	require.NoError(h.t, h.store.RearmGitHubRunnerJobs(ctx, runnerReserve, waitingHeld))
	// As provisionDue does, one job at a time, as the store is not safe for
	// more.
	due, err := h.store.ListDueGitHubRunnerJobs(ctx, h.r.limit())
	require.NoError(h.t, err)
	for _, job := range due {
		require.NoError(h.t, h.r.provision(ctx, job))
	}
	names := []string{}
	for _, v := range h.store.rows {
		if v.DesiredState != types.VMStateDeleted && strings.HasPrefix(v.Name, runnerPrefix) {
			names = append(names, v.ID)
		}
	}
	slices.Sort(names)
	for _, id := range names {
		v, err := h.store.GetVM(ctx, 7, id)
		require.NoError(h.t, err)
		require.NoError(h.t, h.r.s.reconcileVM(ctx, v))
	}
}

// later moves the runners, and what GitHub was last found to say of them,
// back in time, as if that much time had passed.
func (h *heldRun) later(d time.Duration) {
	for _, v := range h.store.rows {
		v.CreatedAt, v.UpdatedAt = v.CreatedAt.Add(-d), v.UpdatedAt.Add(-d)
	}
	h.r.github.lock.Lock()
	for key, list := range h.r.github.listed {
		list.at = list.at.Add(-d)
		for name, runner := range list.runners {
			if !runner.FreeSince.IsZero() {
				runner.FreeSince = runner.FreeSince.Add(-d)
			}
			list.runners[name] = runner
		}
		h.r.github.listed[key] = list
	}
	h.r.github.lock.Unlock()
}

// idle counts the runner VMs that exist and that GitHub gave no job.
func (h *heldRun) idle() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for _, v := range h.store.rows {
		if _, busy := h.assigned[v.Name]; v.DesiredState != types.VMStateDeleted && strings.HasPrefix(v.Name, runnerPrefix) && !busy {
			count++
		}
	}
	return count
}

// queue is what the API tells a user of each queued job: what it waits for,
// and when a runner was ready for it.
func (h *heldRun) queue() map[int64][2]string {
	queued := map[int64][2]string{}
	for id, job := range listedJobs(h.t, h.r, h.info) {
		var status, waiting string
		var timeline types.GitHubRunnerTimeline
		_ = json.Unmarshal(job["status"], &status)
		_ = json.Unmarshal(job["waiting"], &waiting)
		_ = json.Unmarshal(job["timeline"], &timeline)
		if status == types.GitHubJobQueued {
			queued[id] = [2]string{waiting, timeline.RunnerReadyAt}
		}
	}
	return queued
}

// A run whose workflow lets GitHub run 6 of its 45 jobs at once gets a runner
// for each job at first, as GitHub queues them all and says nothing of how
// many it will run. Once the runners have been free for a while, the jobs are
// told that GitHub holds them, and a few spare runners are kept for them, not
// one each. Each job that GitHub lets go finds a runner ready, and none is
// left waiting for one.
func TestGitHubRunnerHeldJobsShareAFewRunners(t *testing.T) {
	const jobs, parallel = 45, 6
	h := newHeldRun(t, parallel)
	for id := int64(1); id <= jobs; id++ {
		job := queuedJob(id)
		job.RunID = 100
		require.Equal(t, 202, deliver(t, h.r, "workflow_job", "queued", beamLabels, job, true))
	}

	h.pass()
	h.pass()
	require.Equal(t, jobs, h.idle(), "every job gets a runner, as GitHub may run them all")
	for id, queued := range h.queue() {
		require.Equal(t, "Runner ready, waiting for GitHub", queued[0], id)
		require.NotEmpty(t, queued[1], id)
	}

	h.release()
	require.Zero(t, h.missed)

	// GitHub gives the other runners nothing.
	h.later(runnerHold)
	h.pass()
	h.later(runnerHold)
	h.pass()
	spares := max(runnerReserve, parallel) + 1
	require.LessOrEqual(t, h.idle(), spares, "the runners that only wait are removed")
	require.GreaterOrEqual(t, h.idle(), max(runnerReserve, parallel), "enough stay for the next jobs")
	for id, queued := range h.queue() {
		require.Equal(t, "Held by GitHub, runner ready", queued[0], id)
		require.NotEmpty(t, queued[1], "the job's time in the queue is GitHub's from when a runner was ready")
	}

	// The jobs end, a few at a time, and GitHub lets as many go.
	most := 0
	for range 200 {
		if len(h.queue()) == 0 {
			break
		}
		h.finish(1 + len(h.assigned)%3)
		h.release()
		h.pass()
		h.later(runnerHold)
		h.pass()
		most = max(most, h.idle())
		for id, queued := range h.queue() {
			require.Contains(t, []string{"Held by GitHub, runner ready", "Runner ready, waiting for GitHub"}, queued[0], id)
			require.NotEmpty(t, queued[1], id)
		}
	}
	require.Empty(t, h.queue(), "every job ran")
	require.Zero(t, h.missed, "GitHub always had a free runner for the next job")
	require.LessOrEqual(t, most, spares+parallel/2, "spare runners stay few")

	// The last jobs end, and with them the runners.
	h.finish(parallel)
	h.pass()
	h.pass()
	require.Zero(t, h.idle())
	for _, job := range h.store.jobs {
		require.Equal(t, types.GitHubJobCompleted, job.Status, job.ID)
	}
}

// A run that GitHub lets run every job at once is never held: each job gets
// its own runner at once, and no runner is removed while a job waits for it.
func TestGitHubRunnerUnheldJobsEachGetARunner(t *testing.T) {
	const jobs = 20
	h := newHeldRun(t, jobs)
	for id := int64(1); id <= jobs; id++ {
		require.Equal(t, 202, deliver(t, h.r, "workflow_job", "queued", beamLabels, queuedJob(id), true))
	}
	h.pass()
	h.release()
	require.Zero(t, h.missed)
	require.Empty(t, h.queue())
	require.Zero(t, h.idle())

	// GitHub is slow to give the runners their jobs: none is removed for it.
	h2 := newHeldRun(t, 0)
	for id := int64(1); id <= 4; id++ {
		require.Equal(t, 202, deliver(t, h2.r, "workflow_job", "queued", beamLabels, queuedJob(id), true))
	}
	h2.pass()
	h2.pass()
	require.Equal(t, 4, h2.idle())
	h2.parallel = 4
	h2.release()
	require.Zero(t, h2.missed)
	require.Empty(t, h2.queue())
}
