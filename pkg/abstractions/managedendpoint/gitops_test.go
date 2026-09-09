package managedendpoint

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

func newGitOpsForTest(t *testing.T) (*Service, *gitops) {
	t.Helper()
	s := newServiceForTest(t)
	s.config.Repo = types.ManagedEndpointsRepoConfig{URL: "https://example.invalid/endpoints.git", Ref: "main"}
	s.config.Webhook.Secret = "hook-secret"
	g := &gitops{s: s, lock: common.NewRedisLock(s.rdb), pending: make(chan gitopsRequest, 1)}
	s.gitops = g
	return s, g
}

func TestGitOpsTriggerValidatesAndCoalesces(t *testing.T) {
	_, g := newGitOpsForTest(t)

	_, err := g.Trigger("not a sha")
	require.Error(t, err)

	started, err := g.Trigger("")
	require.NoError(t, err)
	assert.True(t, started)

	started, err = g.Trigger("abcdef1")
	require.NoError(t, err)
	assert.False(t, started, "a second trigger while one is queued is coalesced")
}

func TestGitOpsApplyReportRecordsVersionsAndRetires(t *testing.T) {
	s, g := newGitOpsForTest(t)
	ctx := context.Background()

	// Two endpoints exist from an earlier commit.
	seedEndpoint(t, s)
	require.NoError(t, s.repo.SaveEndpoint(ctx, &types.ManagedEndpoint{
		Spec: types.ManagedEndpointSpec{ID: "acme/old", Kind: types.EndpointKindLLM, Engine: "vllm", Port: 8000}, StubID: "stub-old", Version: 1, Status: types.EndpointStatusActive,
	}))
	require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{
		LastSHA: "aaaaaaaa", Running: true, RunID: "run-1", TargetSHA: "bbbbbbbb", StartedAt: time.Now(),
		PerEndpoint: map[string]types.GitOpsEndpointState{
			"acme/model": {Path: "acme/model", ID: "acme/model", Status: types.GitOpsStatusApplied, AppliedSHA: "aaaaaaaa"},
			"acme/old":   {Path: "acme/old", ID: "acme/old", Status: types.GitOpsStatusApplied, AppliedSHA: "aaaaaaaa"},
		},
	}))

	// A report for an unknown run is rejected.
	err := g.applyReport(ctx, &types.GitOpsReport{RunID: "stale", SHA: "bbbbbbbb"})
	require.Error(t, err)

	// acme/model redeployed, acme/old removed from the repo, one directory
	// failed to import, and fleet.yaml places acme/model on H100.
	report := &types.GitOpsReport{
		RunID: "run-1", SHA: "bbbbbbbb",
		Results: []types.GitOpsDeployResult{
			{Path: "acme/model", ID: "acme/model", OK: true, StubID: "stub-2", Version: 2},
			{Path: "acme/broken", OK: false, Error: "import failed: boom"},
		},
		FleetYAML: "h100:\n  acme/model:\n    share: 0.5\n    min: 1\n",
	}
	require.NoError(t, g.applyReport(ctx, report))

	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.False(t, state.Running)
	assert.Empty(t, state.RunID)
	assert.Equal(t, "bbbbbbbb", state.LastSHA, "LastSHA advances; the failed stub is retried on its own")
	assert.Equal(t, "bbbbbbbb", state.TargetSHA)
	assert.Contains(t, state.LastError, "1 stub(s) failed")
	assert.Empty(t, state.FleetError)

	model := state.PerEndpoint["acme/model"]
	assert.Equal(t, types.GitOpsStatusApplied, model.Status)
	assert.Equal(t, "bbbbbbbb", model.AppliedSHA)
	assert.Equal(t, "stub-2", model.StubID)
	assert.Equal(t, uint(2), model.Version)

	old := state.PerEndpoint["acme/old"]
	assert.Equal(t, types.GitOpsStatusRetired, old.Status)
	retired, err := s.repo.GetEndpoint(ctx, "acme/old")
	require.NoError(t, err)
	assert.False(t, retired.Enabled())
	assert.Equal(t, types.EndpointStatusRetired, retired.Status)

	broken := state.PerEndpoint["path:acme/broken"]
	assert.Equal(t, types.GitOpsStatusFailed, broken.Status)
	assert.Contains(t, broken.Error, "boom")

	fleet, err := s.repo.GetFleet(ctx)
	require.NoError(t, err)
	assert.Equal(t, "bbbbbbbb", fleet.GitSHA)
	assert.Equal(t, []types.FleetTarget{{GPU: "H100", Placement: types.Placement{Share: 0.5, Min: 1, Count: 1}}}, fleet.Placements("acme/model"), "fleet keys are normalized")

	// A duplicate report for the same run is rejected once the run closed.
	require.Error(t, g.applyReport(ctx, report))

	// Next run: everything applies, the broken path is gone; LastSHA advances
	// and the stale failure entry is dropped. A repo without fleet.yaml
	// places nothing.
	state.Running, state.RunID = true, "run-2"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-2", SHA: "cccccccc",
		Results: []types.GitOpsDeployResult{{Path: "acme/model", ID: "acme/model", OK: true, Skipped: true}},
	}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cccccccc", state.LastSHA)
	assert.Empty(t, state.LastError)
	_, hasBroken := state.PerEndpoint["path:acme/broken"]
	assert.False(t, hasBroken)
	assert.Equal(t, types.GitOpsStatusRetired, state.PerEndpoint["acme/old"].Status, "retired entries are kept for visibility")
	assert.Equal(t, "cccccccc", state.PerEndpoint["acme/model"].AppliedSHA, "unchanged stubs follow the repo head")
	fleet, err = s.repo.GetFleet(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cccccccc", fleet.GitSHA)
	assert.Empty(t, fleet.Placements("acme/model"))
}

func TestGitOpsApplyReportInvalidFleetKeepsPrevious(t *testing.T) {
	s, g := newGitOpsForTest(t)
	ctx := context.Background()
	seedEndpoint(t, s) // places acme/model on H100
	require.NoError(t, s.repo.SaveEndpoint(ctx, &types.ManagedEndpoint{
		Spec: types.ManagedEndpointSpec{ID: "acme/retired", Gpu: map[string]types.GpuSpec{"H100": {}}}, StubID: "stub-r", Version: 1, Status: types.EndpointStatusRetired,
	}))
	require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{
		LastSHA: "aaaaaaaa", Running: true, RunID: "run-1", TargetSHA: "bbbbbbbb", StartedAt: time.Now(),
		PerEndpoint: map[string]types.GitOpsEndpointState{"acme/model": {Path: "acme/model", ID: "acme/model", Status: types.GitOpsStatusApplied, AppliedSHA: "aaaaaaaa"}},
	}))

	// fleet.yaml is structurally broken (shares over-subscribed, unknown GPU
	// type): every stub applied, but the fleet is rejected as a whole.
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-1", SHA: "bbbbbbbb",
		Results:   []types.GitOpsDeployResult{{Path: "acme/model", ID: "acme/model", OK: true, Skipped: true}},
		FleetYAML: "H100:\n  acme/model: {share: 0.7}\n  acme/other: {share: 0.6}\nNOTAGPU:\n  acme/model: {share: 1}\n",
	}))
	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "bbbbbbbb", state.LastSHA, "the stubs applied; only the fleet is held back")
	assert.Contains(t, state.LastError, "1 stub(s) failed")
	for _, want := range []string{"fleet.yaml", "shares sum to 1.30", "NOTAGPU: unknown GPU type"} {
		assert.Contains(t, state.FleetError, want)
	}
	assert.Equal(t, types.GitOpsStatusApplied, state.PerEndpoint["acme/model"].Status)

	fleet, err := s.repo.GetFleet(ctx)
	require.NoError(t, err)
	assert.Equal(t, "fleet-sha", fleet.GitSHA, "the previous placement stays in force")
	assert.Len(t, fleet.Placements("acme/model"), 1)

	// Unparseable yaml is rejected the same way.
	state.Running, state.RunID = true, "run-1b"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-1b", SHA: "bbbbbbb1",
		Results:   []types.GitOpsDeployResult{{Path: "acme/model", ID: "acme/model", OK: true, Skipped: true}},
		FleetYAML: "H100: [not, a, map]\n",
	}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Contains(t, state.FleetError, "fleet.yaml")
	fleet, err = s.repo.GetFleet(ctx)
	require.NoError(t, err)
	assert.Equal(t, "fleet-sha", fleet.GitSHA)

	// A fleet naming a retired endpoint, a typo and a GPU the app does not
	// declare is applied without those placements; the drops are surfaced as
	// skipped rather than failing the run.
	state.Running, state.RunID = true, "run-2"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-2", SHA: "cccccccc",
		Results:   []types.GitOpsDeployResult{{Path: "acme/model", ID: "acme/model", OK: true, Skipped: true}},
		FleetYAML: "H100:\n  acme/model: {share: 0.5, min: 1}\n  acme/retired: {share: 0.1}\n  acme/typo: {share: 0.1}\nA10G:\n  acme/model: {share: 1}\n",
	}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cccccccc", state.LastSHA)
	assert.Empty(t, state.LastError, "pruned placements are not a failure")
	for _, want := range []string{"skipped:", "acme/retired is not a deployed endpoint", "acme/typo is not a deployed endpoint", `does not declare gpu "A10G"`} {
		assert.Contains(t, state.FleetError, want)
	}
	fleet, err = s.repo.GetFleet(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cccccccc", fleet.GitSHA)
	assert.Equal(t, []types.FleetTarget{{GPU: "H100", Placement: types.Placement{Share: 0.5, Min: 1, Count: 1}}}, fleet.Placements("acme/model"))
	assert.Empty(t, fleet.Targets["H100"]["acme/typo"])
	assert.Empty(t, fleet.Targets["A10G"])

	// The fix lands: the fleet applies cleanly and the error clears.
	state.Running, state.RunID = true, "run-3"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-3", SHA: "dddddddd",
		Results:   []types.GitOpsDeployResult{{Path: "acme/model", ID: "acme/model", OK: true, Skipped: true}},
		FleetYAML: "H100:\n  acme/model: {share: 1, min: 2, count: 2}\n",
	}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "dddddddd", state.LastSHA)
	assert.Empty(t, state.FleetError)
	fleet, err = s.repo.GetFleet(ctx)
	require.NoError(t, err)
	assert.Equal(t, "dddddddd", fleet.GitSHA)
	assert.Equal(t, types.Placement{Share: 1, Min: 2, Count: 2}, fleet.Targets["H100"]["acme/model"])
}

func TestGitOpsApplyReportImportFailureKeepsPriorEndpoint(t *testing.T) {
	s, g := newGitOpsForTest(t)
	ctx := context.Background()

	require.NoError(t, s.repo.SaveEndpoint(ctx, &types.ManagedEndpoint{
		Spec: types.ManagedEndpointSpec{ID: "acme/model", Kind: types.EndpointKindLLM, Engine: "vllm", Port: 8000}, StubID: "stub-1", Version: 3, Status: types.EndpointStatusActive,
	}))
	require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{
		LastSHA: "aaaaaaaa", Running: true, RunID: "run-1", TargetSHA: "bbbbbbbb", StartedAt: time.Now(),
		PerEndpoint: map[string]types.GitOpsEndpointState{
			"acme/model": {Path: "acme/model", ID: "acme/model", Status: types.GitOpsStatusApplied, AppliedSHA: "aaaaaaaa", StubID: "stub-1", Version: 3},
		},
	}))

	// The new commit breaks acme/model/app.py: it is not discovered, only an
	// import failure for its path is reported.
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-1", SHA: "bbbbbbbb",
		Results: []types.GitOpsDeployResult{{Path: "acme/model", OK: false, Error: "import failed: SyntaxError"}},
	}))

	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "bbbbbbbb", state.LastSHA, "LastSHA follows the repo head; the broken stub is retried on its own")
	assert.Contains(t, state.LastError, "1 stub(s) failed")

	model := state.PerEndpoint["acme/model"]
	assert.Equal(t, types.GitOpsStatusFailed, model.Status, "a broken directory marks the stub failed instead of retiring it")
	assert.Contains(t, model.Error, "SyntaxError")
	assert.Equal(t, "aaaaaaaa", model.AppliedSHA, "the last applied version is kept")
	assert.Equal(t, "stub-1", model.StubID)
	_, hasPathEntry := state.PerEndpoint["path:acme/model"]
	assert.False(t, hasPathEntry, "the failure is attributed to the known stub, not duplicated by path")

	endpoint, err := s.repo.GetEndpoint(ctx, "acme/model")
	require.NoError(t, err)
	assert.True(t, endpoint.Enabled(), "the previously deployed endpoint keeps serving")
	assert.Equal(t, types.EndpointStatusActive, endpoint.Status)

	// The failed path is queued for redeploy; the same SHA is not suppressed.
	state.Running, state.RunID = true, "run-2"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))

	// The fix lands: the directory imports again and is redeployed.
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-2", SHA: "cccccccc",
		Results: []types.GitOpsDeployResult{{Path: "acme/model", ID: "acme/model", OK: true, StubID: "stub-2", Version: 4}},
	}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cccccccc", state.LastSHA)
	assert.Empty(t, state.LastError)
	model = state.PerEndpoint["acme/model"]
	assert.Equal(t, types.GitOpsStatusApplied, model.Status)
	assert.Empty(t, model.Error)
	assert.Equal(t, uint(4), model.Version)

	// The directory is really deleted afterwards: now it is retired.
	state.Running, state.RunID = true, "run-3"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{RunID: "run-3", SHA: "dddddddd"}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, types.GitOpsStatusRetired, state.PerEndpoint["acme/model"].Status)
	endpoint, err = s.repo.GetEndpoint(ctx, "acme/model")
	require.NoError(t, err)
	assert.False(t, endpoint.Enabled())
}

// failingEndpointRepo cannot read endpoint records.
type failingEndpointRepo struct {
	repository.ManagedEndpointRepository
}

func (failingEndpointRepo) GetEndpoint(context.Context, string) (*types.ManagedEndpoint, error) {
	return nil, errors.New("redis: connection refused")
}

func TestGitOpsApplyReportRetireFailureIsRetried(t *testing.T) {
	s, g := newGitOpsForTest(t)
	ctx := context.Background()

	// The registry is unreachable while the run is applied, so retire fails.
	repo := s.repo
	s.repo = failingEndpointRepo{repo}
	require.NoError(t, repo.SaveEndpoint(ctx, &types.ManagedEndpoint{
		Spec: types.ManagedEndpointSpec{ID: "acme/old", Kind: types.EndpointKindLLM, Engine: "vllm", Port: 8000}, StubID: "stub-old", Version: 1, Status: types.EndpointStatusActive,
	}))
	require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{
		LastSHA: "aaaaaaaa", Running: true, RunID: "run-1", TargetSHA: "bbbbbbbb", StartedAt: time.Now(),
		PerEndpoint: map[string]types.GitOpsEndpointState{
			"acme/old": {Path: "acme/old", ID: "acme/old", Status: types.GitOpsStatusApplied, AppliedSHA: "aaaaaaaa"},
		},
	}))

	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{RunID: "run-1", SHA: "bbbbbbbb"}))
	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	old := state.PerEndpoint["acme/old"]
	assert.Equal(t, types.GitOpsStatusFailed, old.Status, "a failed retirement is recorded, not silently skipped")
	assert.Contains(t, old.Error, "retire:")
	assert.Equal(t, "bbbbbbbb", state.LastSHA, "LastSHA advances; the failed entry alone drives the retry")
	assert.Equal(t, "bbbbbbbb", state.TargetSHA)
	assert.Contains(t, state.LastError, "1 stub(s) failed")

	// sync would relaunch at the same SHA: the entry is on the retry list, so
	// the same-SHA short-circuit does not apply.
	var retry []string
	for _, e := range state.PerEndpoint {
		if e.Status == types.GitOpsStatusFailed && e.Path != "" {
			retry = append(retry, e.Path)
		}
	}
	assert.Equal(t, []string{"acme/old"}, retry)
	assert.False(t, "bbbbbbbb" == state.LastSHA && len(retry) == 0)

	// The registry is back; the retried run retires it and LastSHA advances.
	s.repo = repo
	state.Running, state.RunID = true, "run-2"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{RunID: "run-2", SHA: "bbbbbbbb"}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, types.GitOpsStatusRetired, state.PerEndpoint["acme/old"].Status)
	assert.Equal(t, "bbbbbbbb", state.LastSHA)
	assert.Empty(t, state.LastError)
	retired, err := s.repo.GetEndpoint(ctx, "acme/old")
	require.NoError(t, err)
	assert.False(t, retired.Enabled())
}

func TestGitOpsApplyReportDeployerError(t *testing.T) {
	s, g := newGitOpsForTest(t)
	ctx := context.Background()
	require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{LastSHA: "aaaaaaaa", Running: true, RunID: "run-1", TargetSHA: "bbbbbbbb", StartedAt: time.Now()}))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{RunID: "run-1", SHA: "bbbbbbbb", Error: "clone failed"}))
	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.False(t, state.Running)
	assert.Equal(t, "aaaaaaaa", state.LastSHA)
	assert.Equal(t, "clone failed", state.LastError)
}

func TestGitOpsSyncExpiresStuckRun(t *testing.T) {
	s, g := newGitOpsForTest(t)
	ctx := context.Background()
	require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{Running: true, RunID: "run-1", TargetSHA: "bbbbbbbb", StartedAt: time.Now().Add(-2 * gitopsRunTimeout)}))

	// The stuck run is failed; the subsequent head resolution fails against
	// the invalid URL, which is reported on the state rather than swallowed.
	err := g.sync(ctx, gitopsRequest{})
	require.Error(t, err)
	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.False(t, state.Running)
	assert.Contains(t, state.LastError, "resolve head")
}

func TestGitOpsResolveHeadFromLocalRepo(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	run := func(args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = append(cmd.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		return strings.TrimSpace(string(out))
	}
	run("init", "-q", "-b", "main")
	require.NoError(t, writeFile(filepath.Join(dir, "README"), "x"))
	run("add", ".")
	run("commit", "-q", "-m", "init")
	head := run("rev-parse", "HEAD")
	run("tag", "v1")
	run("tag", "-a", "v2", "-m", "release v2")
	tagObject := run("rev-parse", "v2")
	require.NotEqual(t, head, tagObject, "annotated tag is its own object")

	s, g := newGitOpsForTest(t)
	s.config.Repo.URL = dir

	sha, err := g.resolveHead(context.Background())
	require.NoError(t, err)
	assert.Equal(t, head, sha)

	s.config.Repo.Ref = "v1"
	sha, err = g.resolveHead(context.Background())
	require.NoError(t, err)
	assert.Equal(t, head, sha, "lightweight tags resolve to the commit")

	s.config.Repo.Ref = "v2"
	sha, err = g.resolveHead(context.Background())
	require.NoError(t, err)
	assert.Equal(t, head, sha, "annotated tags resolve to the peeled commit, not the tag object")

	s.config.Repo.Ref = "refs/tags/v2"
	sha, err = g.resolveHead(context.Background())
	require.NoError(t, err)
	assert.Equal(t, head, sha)

	s.config.Repo.Ref = "missing"
	_, err = g.resolveHead(context.Background())
	require.Error(t, err)

	// A full commit id is accepted as-is without contacting the remote.
	s.config.Repo.URL = "https://example.invalid/endpoints.git"
	s.config.Repo.Ref = strings.ToUpper(head)
	sha, err = g.resolveHead(context.Background())
	require.NoError(t, err)
	assert.Equal(t, head, sha)
}

func TestPickRemoteSHA(t *testing.T) {
	const (
		branchSHA = "1111111111111111111111111111111111111111"
		tagObjSHA = "2222222222222222222222222222222222222222"
		peeledSHA = "3333333333333333333333333333333333333333"
		lightSHA  = "4444444444444444444444444444444444444444"
	)
	output := strings.Join([]string{
		branchSHA + "\trefs/heads/main",
		tagObjSHA + "\trefs/tags/main",
		peeledSHA + "\trefs/tags/main^{}",
		tagObjSHA + "\trefs/tags/v2",
		peeledSHA + "\trefs/tags/v2^{}",
		lightSHA + "\trefs/tags/v1",
		"not a sha\trefs/heads/garbage",
		"",
	}, "\n")

	cases := []struct {
		ref  string
		want string
		ok   bool
	}{
		{"main", branchSHA, true},            // branch beats same-named tag
		{"refs/heads/main", branchSHA, true}, // fully qualified branch
		{"refs/tags/main", peeledSHA, true},  // fully qualified tag ignores the branch
		{"v2", peeledSHA, true},              // annotated tag: peeled commit, not the tag object
		{"v1", lightSHA, true},               // lightweight tag: only the un-peeled line exists
		{"v1^{}", "", false},                 // peel suffix is not a ref
		{"garbage", "", false},               // malformed sha is skipped
		{"nope", "", false},                  // absent
		{"", "", false},                      // empty ref never matches
		{"refs/heads/v2", "", false},         // qualified branch does not fall back to the tag
		{"mai", "", false},                   // exact match only
		{"refs/tags/v2", peeledSHA, true},    // qualified annotated tag
		{"  main  ", branchSHA, true},        // whitespace is trimmed
	}
	for _, tc := range cases {
		got, ok := pickRemoteSHA(output, tc.ref)
		assert.Equal(t, tc.ok, ok, "ref %q", tc.ref)
		assert.Equal(t, tc.want, got, "ref %q", tc.ref)
	}
}

func TestGitOpsWebhook(t *testing.T) {
	s, g := newGitOpsForTest(t)
	e := echo.New()
	sign := func(body []byte) string {
		mac := hmac.New(sha256.New, []byte(s.config.Webhook.Secret))
		mac.Write(body)
		return "sha256=" + hex.EncodeToString(mac.Sum(nil))
	}
	post := func(body []byte, headers map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/endpoints/gitops/webhook", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, g.handleWebhook(c))
		return rec
	}
	push := func(ref, after string) []byte {
		b, _ := json.Marshal(map[string]any{"ref": ref, "after": after, "deleted": false})
		return b
	}

	// Bad signature.
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(push("refs/heads/main", "abc")))
	req.Header.Set("X-Hub-Signature-256", "sha256=deadbeef")
	err := g.handleWebhook(e.NewContext(req, httptest.NewRecorder()))
	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, http.StatusUnauthorized, httpErr.Code)

	// Push to another branch is ignored.
	body := push("refs/heads/feature", "abc")
	rec := post(body, map[string]string{"X-Hub-Signature-256": sign(body)})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"ignored":true`)
	assert.Empty(t, g.pending)

	// Push to main triggers a head-resolving sync.
	body = push("refs/heads/main", "abc")
	rec = post(body, map[string]string{"X-Hub-Signature-256": sign(body)})
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Contains(t, rec.Body.String(), `"started":true`)
	require.Len(t, g.pending, 1)
	got := <-g.pending
	assert.Equal(t, gitopsRequest{}, got)

	// GitLab-style shared token, tag push to the configured ref.
	s.config.Repo.Ref = "release"
	body, _ = json.Marshal(map[string]any{"object_kind": "tag_push", "ref": "refs/tags/release", "after": "abc"})
	rec = post(body, map[string]string{"X-Gitlab-Token": s.config.Webhook.Secret})
	assert.Equal(t, http.StatusAccepted, rec.Code)
	<-g.pending

	// Branch deletion is ignored; ping is acknowledged.
	body, _ = json.Marshal(map[string]any{"ref": "refs/tags/release", "after": "0000000000000000000000000000000000000000", "deleted": true})
	rec = post(body, map[string]string{"X-Gitlab-Token": s.config.Webhook.Secret})
	assert.Contains(t, rec.Body.String(), `"deleted":true`)
	body, _ = json.Marshal(map[string]any{"zen": "keep it simple", "hook_id": 1})
	rec = post(body, map[string]string{"X-Hub-Signature-256": sign(body)})
	assert.Contains(t, rec.Body.String(), `"ignored":true`)
	assert.Empty(t, g.pending)
}

func TestGitOpsReportRouteAuth(t *testing.T) {
	s, g := newGitOpsForTest(t)
	ctx := context.Background()
	require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{Running: true, RunID: "run-1", TokenID: "run-token", TargetSHA: "bbbbbbbb", StartedAt: time.Now()}))
	e := echo.New()
	body, _ := json.Marshal(types.GitOpsReport{RunID: "run-1", SHA: "bbbbbbbb"})
	runToken := func(externalID string) context.Context {
		return auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{
			Workspace: &types.Workspace{Id: 1, ExternalId: "admin-ws"},
			Token:     &types.Token{TokenType: types.TokenTypeWorkspace, ExternalId: externalID},
		})
	}
	post := func(authCtx context.Context, body []byte) (*httptest.ResponseRecorder, error) {
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		if authCtx != nil {
			c = authedEchoContext(c, authCtx)
		}
		return rec, g.handleReport(c)
	}
	httpCode := func(err error) int {
		var httpErr *echo.HTTPError
		require.ErrorAs(t, err, &httpErr)
		return httpErr.Code
	}

	// Unauthenticated echo context: rejected.
	_, err := post(nil, body)
	assert.Equal(t, http.StatusUnauthorized, httpCode(err))

	// Any other admin-workspace token is not the run's token.
	_, err = post(runToken("someone-else"), body)
	assert.Equal(t, http.StatusForbidden, httpCode(err))

	// The run token may only report on its own run.
	other, _ := json.Marshal(types.GitOpsReport{RunID: "run-other", SHA: "bbbbbbbb"})
	_, err = post(runToken("run-token"), other)
	assert.Equal(t, http.StatusForbidden, httpCode(err))

	rec, err := post(runToken("run-token"), body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.False(t, state.Running)
	assert.Equal(t, "bbbbbbbb", state.LastSHA)

	// Once the run closed the token is no longer accepted ...
	_, err = post(runToken("run-token"), body)
	assert.Equal(t, http.StatusForbidden, httpCode(err))

	// ... while a cluster admin bypasses the token check (the report still
	// has to match a run in flight).
	_, err = post(adminCtx(), body)
	assert.Equal(t, http.StatusConflict, httpCode(err))
	state.Running, state.RunID, state.TokenID = true, "run-2", "run-token-2"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	second, _ := json.Marshal(types.GitOpsReport{RunID: "run-2", SHA: "cccccccc"})
	rec, err = post(adminCtx(), second)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cccccccc", state.LastSHA)
}

func authedEchoContext(c echo.Context, ctx context.Context) echo.Context {
	info, _ := auth.AuthInfoFromContext(ctx)
	return &auth.HttpAuthContext{Context: c, AuthInfo: info}
}

func writeFile(path, contents string) error {
	return os.WriteFile(path, []byte(contents), 0o644)
}

func TestParsePushAndVerify(t *testing.T) {
	ev, ok := parsePush([]byte(`{"ref":"refs/heads/main","after":"abc","deleted":false}`))
	require.True(t, ok)
	assert.Equal(t, "refs/heads/main", ev.Ref)
	assert.False(t, ev.Deleted)

	_, ok = parsePush([]byte(`{"object_kind":"merge_request","ref":"refs/heads/main"}`))
	assert.False(t, ok)
	_, ok = parsePush([]byte(`not json`))
	assert.False(t, ok)

	h := http.Header{}
	h.Set("Authorization", "Bearer s3cret")
	assert.True(t, verifyWebhook(h, nil, "s3cret"))
	assert.False(t, verifyWebhook(h, nil, "other"))
	assert.False(t, verifyWebhook(http.Header{}, nil, "s3cret"), "no credential is never accepted")
}
