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
	s.config.Repo = types.ManagedEndpointsRepoConfig{URL: "https://example.invalid/endpoints.git", Branch: "main"}
	s.config.Webhook.Secret = "hook-secret"
	g := &gitops{s: s, lock: common.NewRedisLock(s.rdb), pending: make(chan gitopsRequest, 1)}
	s.gitops = g
	return s, g
}

func TestConfigReplicaPolicy(t *testing.T) {
	fleet, err := parseFleet(`acme/model:
  enabled: true
  gpus:
    h100: {priority: 2, minReplicas: 1, maxReplicas: 2, preemption: false}
    a100: {priority: 1, minReplicas: 0, maxReplicas: 1}
acme/other:
  enabled: true
  gpus:
    h100: {priority: 1, minReplicas: 2, maxReplicas: 0, preemption: true}
`)
	require.NoError(t, err)
	require.Equal(t, []types.FleetEntry{
		{EndpointID: "acme/other", Priority: 1, MinReplicas: 2},
		{EndpointID: "acme/model", Priority: 2, MinReplicas: 1, MaxReplicas: 2, ProtectMinimum: true},
	}, fleet.Entries("H100"))
	require.Equal(t, []types.FleetEntry{{EndpointID: "acme/model", Priority: 1, MaxReplicas: 1}}, fleet.Entries("A100"))
	for _, value := range []string{
		"minReplicas: 3, maxReplicas: 2", "minReplicas: 65", "maxReplicas: 65",
		"minReplicas: -1", "minReplicas: true", "minReplicas: 1.5", "minReplicas: 1.0", "minReplicas: '1'", "minReplicas: null",
		"maxReplicas: false", "maxReplicas: 1.5", "maxReplicas: '1'",
		"priority: true", "priority: 1.5", "priority: '1'", "priority: 0",
		"preemption: 'false'", "preemption: 0", "preemption: null", "preemptible: false",
		"minReplicas: 1, minReplicas: 2", "min_replicas: 1",
	} {
		t.Run(value, func(t *testing.T) {
			prefix := "priority: 1, "
			if strings.HasPrefix(value, "priority:") {
				prefix = ""
			}
			_, err := parseFleet("acme/model: {enabled: true, gpus: {H100: {" + prefix + value + "}}}")
			require.Error(t, err)
		})
	}
	for _, value := range []string{"", "  \n", "null", "~", "[]", "{}\n---\n{}", "acme/model: {unexpected: true}", "acme/model: {}\nacme/model: {}", "acme/model: {}\nACME/model: {}", "' ': {}", "acme/model: {gpus: {H100: {priority: 1}, h100: {priority: 2}}}"} {
		_, err := parseFleet(value)
		require.Error(t, err, "invalid config %q", value)
	}
	fleet, err = parseFleet("{}")
	require.NoError(t, err)
	require.Empty(t, fleet.Endpoints)
}

func TestGitOpsMissingConfigPreservesAppliedPlacement(t *testing.T) {
	s, g := newGitOpsForTest(t)
	ctx := context.Background()
	seedEndpoint(t, s)
	for _, config := range []string{"", "null", "acme/model: {enabled: true, gpus: {H100: {priority: 1, minReplicas: 1.5}}}"} {
		require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{Running: true, RunID: "r", TargetSHA: "new"}))
		require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
			RunID: "r", SHA: "new", FleetYAML: config,
			Results: []types.GitOpsDeployResult{{ID: "acme/model", Path: "acme/model", OK: true, Skipped: true}},
		}))
		state, err := s.repo.GetGitOpsState(ctx)
		require.NoError(t, err)
		require.Contains(t, state.FleetError, "config.yaml")
		fleet, err := s.repo.GetFleet(ctx)
		require.NoError(t, err)
		require.Equal(t, "fleet-sha", fleet.GitSHA)
		require.Len(t, fleet.Placements("acme/model"), 1)
	}
}

func TestGitOpsUsesOnlyConfiguredDeployerSecrets(t *testing.T) {
	s, g := newGitOpsForTest(t)
	s.backend = nil // Secret resolution must not consult the admin workspace.
	s.config.DeployerSecrets = map[string]string{"GITHUB_TOKEN": "package-token", "HF_TOKEN": "model-token"}
	env, err := g.deployerSecrets()
	require.NoError(t, err)
	assert.Equal(t, []string{"GITHUB_TOKEN=package-token", "HF_TOKEN=model-token"}, env)

	for _, name := range []string{"BAD=NAME", "BETA9_TOKEN", "ENDPOINTS_REPO_URL", "STUB_ID", "STUB_TYPE"} {
		t.Run(name, func(t *testing.T) {
			s.config.DeployerSecrets = map[string]string{name: "must-not-leak"}
			_, err := g.deployerSecrets()
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "must-not-leak")
		})
	}
}

func TestGitOpsResolveHeadUsesConfigDeployKey(t *testing.T) {
	s, g := newGitOpsForTest(t)
	s.backend = nil
	s.config.Repo.URL = "git@example.invalid:models.git"
	s.config.Repo.DeployKey = "-----BEGIN TEST KEY-----\n"
	bin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, "git"), []byte(`#!/bin/sh
key_path=${GIT_SSH_COMMAND#ssh -i }
key_path=${key_path%% -o *}
[ "$(cat "$key_path")" = "-----BEGIN TEST KEY-----" ] || exit 1
printf 'abcdef1234567890abcdef1234567890abcdef12 refs/heads/main\n'
`), 0o700))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	sha, err := g.resolveHead(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "abcdef1234567890abcdef1234567890abcdef12", sha)
}

func TestGitOpsResolveHeadUsesConfiguredBranch(t *testing.T) {
	for _, branch := range []string{"main", "staging", "release/models"} {
		t.Run(branch, func(t *testing.T) {
			s, g := newGitOpsForTest(t)
			s.config.Repo.Branch = branch
			bin := t.TempDir()
			script := "#!/bin/sh\n[ \"$4\" = \"refs/heads/" + branch + "\" ] || exit 1\nprintf 'abcdef1234567890abcdef1234567890abcdef12 refs/heads/" + branch + "\\n'\n"
			require.NoError(t, os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o700))
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			sha, err := g.resolveHead(context.Background())
			require.NoError(t, err)
			assert.Equal(t, "abcdef1234567890abcdef1234567890abcdef12", sha)
		})
	}
}

func TestGitOpsResolveHeadRedactsConfigTokenOnFailure(t *testing.T) {
	s, g := newGitOpsForTest(t)
	s.backend = nil
	s.config.Repo.DeployKey = "private-config-token"
	bin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\necho \"$@\" >&2\nexit 1\n"), 0o700))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	_, err := g.resolveHead(context.Background())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), s.config.Repo.DeployKey)
	assert.Contains(t, err.Error(), "[redacted]")
}

func TestGitOpsPollRecoversAtUnchangedCommit(t *testing.T) {
	sha := strings.Repeat("a", 40)
	bin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\nprintf '"+sha+" refs/heads/main\\n'\n"), 0o700))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, tc := range []struct {
		name, requestedSHA, previousError, wantError string
	}{
		{name: "recovered poll", previousError: "resolve head: permission denied"},
		{name: "explicit SHA does not verify access", requestedSHA: sha, previousError: "resolve head: permission denied", wantError: "resolve head: permission denied"},
		{name: "deployment error remains visible", previousError: "launch deployer: unavailable", wantError: "launch deployer: unavailable"},
		{name: "healthy poll"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, g := newGitOpsForTest(t)
			s.backend = nil // An unchanged commit must not launch a deployer.
			ctx := context.Background()
			lastRun := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
			require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{
				LastSHA: sha, TargetSHA: sha, FleetSHA: sha, LastError: tc.previousError,
				LastRunAt: lastRun, PerEndpoint: map[string]types.GitOpsEndpointState{},
			}))
			require.NoError(t, g.sync(ctx, gitopsRequest{sha: tc.requestedSHA}))
			state, err := s.repo.GetGitOpsState(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.wantError, state.LastError)
			assert.Equal(t, sha, state.LastSHA)
			assert.Equal(t, sha, state.TargetSHA)
			assert.Equal(t, lastRun, state.LastRunAt, "a lookup recovery must not reset deployment retry backoff")
			assert.False(t, state.Running)
			assert.Empty(t, state.RunID)
		})
	}
}

func TestGitOpsTriggerValidatesAndCoalesces(t *testing.T) {
	_, g := newGitOpsForTest(t)

	_, err := g.Trigger("not a sha")
	require.Error(t, err)
	_, err = g.Trigger("abcdef1")
	require.ErrorContains(t, err, "full 40- or 64-character")
	assert.Empty(t, g.pending, "abbreviated commits must not enqueue a second identity for the same revision")

	started, err := g.Trigger("")
	require.NoError(t, err)
	assert.True(t, started)

	started, err = g.Trigger(strings.Repeat("a", 40))
	require.NoError(t, err)
	assert.False(t, started, "a second trigger while one is queued is coalesced")
	<-g.pending
	started, err = g.Trigger(strings.Repeat("b", 64))
	require.NoError(t, err)
	assert.True(t, started, "SHA-256 repositories are supported")
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
	// failed to import, and config.yaml places acme/model on H100.
	report := &types.GitOpsReport{
		RunID: "run-1", SHA: "bbbbbbbb",
		Results: []types.GitOpsDeployResult{
			{Path: "acme/model", ID: "acme/model", OK: true, StubID: "stub-2", Version: 2},
			{Path: "acme/broken", OK: false, Error: "import failed: boom"},
		},
		FleetYAML: "acme/model:\n  enabled: true\n  gpus:\n    h100: {priority: 1, maxReplicas: 1}\n",
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

	broken := state.PerEndpoint["acme/broken"]
	assert.Equal(t, types.GitOpsStatusFailed, broken.Status)
	assert.Contains(t, broken.Error, "boom")

	fleet, err := s.repo.GetFleet(ctx)
	require.NoError(t, err)
	assert.Equal(t, "bbbbbbbb", fleet.GitSHA)
	assert.Equal(t, map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1}}, fleet.Placements("acme/model"), "fleet keys are normalized")

	// A duplicate report for the same run is rejected once the run closed.
	require.Error(t, g.applyReport(ctx, report))

	// Next run: everything applies, the broken path is gone; LastSHA advances
	// and the stale failure entry is dropped. An explicitly empty config.yaml
	// places nothing.
	state.Running, state.RunID = true, "run-2"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-2", SHA: "cccccccc", FleetYAML: "{}",
		Results: []types.GitOpsDeployResult{{Path: "acme/model", ID: "acme/model", OK: true, Skipped: true}},
	}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cccccccc", state.LastSHA)
	assert.Empty(t, state.LastError)
	_, hasBroken := state.PerEndpoint["acme/broken"]
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

	// config.yaml is structurally broken (unknown GPU type, absurd count):
	// every stub applied, but the fleet is rejected as a whole.
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-1", SHA: "bbbbbbbb",
		Results:   []types.GitOpsDeployResult{{Path: "acme/model", ID: "acme/model", OK: true, Skipped: true}},
		FleetYAML: "acme/model:\n  enabled: true\n  gpus:\n    H100: {priority: 1, maxReplicas: 65}\n    NOTAGPU: {priority: 1}\n",
	}))
	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "bbbbbbbb", state.LastSHA, "the stubs applied; only the fleet is held back")
	assert.Contains(t, state.LastError, "1 stub(s) failed")
	for _, want := range []string{"config.yaml", "maxReplicas 65 exceeds 64", "NOTAGPU is not a known GPU type"} {
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
		FleetYAML: "acme/model: 3\n",
	}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Contains(t, state.FleetError, "config.yaml")
	fleet, err = s.repo.GetFleet(ctx)
	require.NoError(t, err)
	assert.Equal(t, "fleet-sha", fleet.GitSHA)

	// A fleet naming a retired endpoint, a typo and a GPU the app does not
	// declare is applied without those entries; the drops are surfaced as
	// skipped rather than failing the run.
	state.Running, state.RunID = true, "run-2"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-2", SHA: "cccccccc",
		Results:   []types.GitOpsDeployResult{{Path: "acme/model", ID: "acme/model", OK: true, Skipped: true}},
		FleetYAML: "acme/model:\n  enabled: true\n  gpus:\n    H100: {priority: 1, maxReplicas: 1}\n    A10G: {priority: 2, maxReplicas: 2}\nacme/retired:\n  enabled: true\n  gpus: {H100: {priority: 2}}\nacme/typo:\n  enabled: true\n  gpus: {H100: {priority: 3}}\n",
	}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cccccccc", state.LastSHA)
	assert.Empty(t, state.LastError, "pruned entries are not a failure")
	for _, want := range []string{"skipped:", "acme/retired is not a deployed endpoint", "acme/typo is not a deployed endpoint", `does not declare gpu "A10G"`} {
		assert.Contains(t, state.FleetError, want)
	}
	fleet, err = s.repo.GetFleet(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cccccccc", fleet.GitSHA)
	assert.Equal(t, map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 1}}, fleet.Placements("acme/model"))
	assert.Empty(t, fleet.Placements("acme/typo"))
	assert.Empty(t, fleet.Placements("acme/retired"))

	// The fix lands: the fleet applies cleanly and the error clears.
	state.Running, state.RunID = true, "run-3"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-3", SHA: "dddddddd",
		Results:   []types.GitOpsDeployResult{{Path: "acme/model", ID: "acme/model", OK: true, Skipped: true}},
		FleetYAML: "acme/model:\n  enabled: true\n  gpus:\n    H100: {priority: 1, maxReplicas: 4}\n",
	}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "dddddddd", state.LastSHA)
	assert.Empty(t, state.FleetError)
	fleet, err = s.repo.GetFleet(ctx)
	require.NoError(t, err)
	assert.Equal(t, "dddddddd", fleet.GitSHA)
	assert.Equal(t, map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 4}}, fleet.Placements("acme/model"))
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
		RunID: "run-1", SHA: "bbbbbbbb", FleetYAML: "{}",
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

	endpoint, err := s.repo.GetEndpoint(ctx, "acme/model")
	require.NoError(t, err)
	assert.True(t, endpoint.Enabled(), "the previously deployed endpoint keeps serving")
	assert.Equal(t, types.EndpointStatusActive, endpoint.Status)

	// The failed path is queued for redeploy; the same SHA is not suppressed.
	state.Running, state.RunID = true, "run-2"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))

	// The fix lands: the directory imports again and is redeployed.
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-2", SHA: "cccccccc", FleetYAML: "{}",
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
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{RunID: "run-3", SHA: "dddddddd", FleetYAML: "{}"}))
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

	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{RunID: "run-1", SHA: "bbbbbbbb", FleetYAML: "{}"}))
	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	old := state.PerEndpoint["acme/old"]
	assert.Equal(t, types.GitOpsStatusFailed, old.Status, "a failed retirement is recorded, not silently skipped")
	assert.Contains(t, old.Error, "retire:")
	assert.Equal(t, "bbbbbbbb", state.LastSHA, "LastSHA advances; the failed entry alone drives the retry")
	assert.Equal(t, "bbbbbbbb", state.TargetSHA)
	assert.Contains(t, state.LastError, "1 stub(s) failed")

	// sync relaunches at the same SHA once the retry backoff has passed, with
	// the failed entry on the redeploy list.
	retry, run := needsRun(state, "bbbbbbbb", false, state.LastRunAt.Add(gitopsRetryBackoff))
	assert.True(t, run)
	assert.Equal(t, []string{"acme/old"}, retry)

	// The registry is back; the retried run retires it and LastSHA advances.
	s.repo = repo
	state.Running, state.RunID = true, "run-2"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{RunID: "run-2", SHA: "bbbbbbbb", FleetYAML: "{}"}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, types.GitOpsStatusRetired, state.PerEndpoint["acme/old"].Status)
	assert.Equal(t, "bbbbbbbb", state.LastSHA)
	assert.Empty(t, state.LastError)
	retired, err := s.repo.GetEndpoint(ctx, "acme/old")
	require.NoError(t, err)
	assert.False(t, retired.Enabled())
}

type failOnceRepo struct {
	repository.ManagedEndpointRepository
	fleetFailures int
	stateFailures int
}

func (r *failOnceRepo) SaveFleet(ctx context.Context, f *types.Fleet) error {
	if r.fleetFailures > 0 {
		r.fleetFailures--
		return errors.New("synthetic fleet persistence outage")
	}
	return r.ManagedEndpointRepository.SaveFleet(ctx, f)
}

func (r *failOnceRepo) SaveGitOpsState(ctx context.Context, state *types.GitOpsState) error {
	if r.stateFailures > 0 {
		r.stateFailures--
		return errors.New("synthetic state persistence outage")
	}
	return r.ManagedEndpointRepository.SaveGitOpsState(ctx, state)
}

// A fleet write that fails is retried at the same SHA even when every app
// applied; an invalid fleet is not (nothing changes until the next commit).
// A stub that fails at a commit (e.g. its image does not exist) must not be
// rebuilt on every poll; only a new commit or a forced trigger runs at once.
func TestNeedsRunBacksOffRetriesAtUnchangedCommit(t *testing.T) {
	now := time.Now()
	state := &types.GitOpsState{
		LastSHA: "aaaaaaaa", FleetSHA: "aaaaaaaa", LastRunAt: now,
		PerEndpoint: map[string]types.GitOpsEndpointState{
			"qwen/qwen3-8b": {Path: "qwen/qwen3-8b", ID: "qwen/qwen3-8b", Status: types.GitOpsStatusFailed, Error: "Image build failed"},
			"acme/ok":       {Path: "acme/ok", ID: "acme/ok", Status: types.GitOpsStatusApplied},
		},
	}

	_, run := needsRun(state, "aaaaaaaa", false, now.Add(2*time.Minute))
	assert.False(t, run, "the next poll does not rebuild a stub that just failed")

	retry, run := needsRun(state, "aaaaaaaa", false, now.Add(gitopsRetryBackoff))
	assert.True(t, run)
	assert.Equal(t, []string{"qwen/qwen3-8b"}, retry)

	retry, run = needsRun(state, "bbbbbbbb", false, now.Add(time.Second))
	assert.True(t, run, "a new commit runs immediately")
	assert.Equal(t, []string{"qwen/qwen3-8b"}, retry, "and still redeploys the failed stub")

	_, run = needsRun(state, "aaaaaaaa", true, now.Add(time.Second))
	assert.True(t, run, "a forced trigger runs immediately")

	clean := &types.GitOpsState{LastSHA: "aaaaaaaa", FleetSHA: "aaaaaaaa", LastRunAt: now}
	_, run = needsRun(clean, "aaaaaaaa", false, now.Add(time.Hour))
	assert.False(t, run, "nothing to do at an unchanged commit")

	fleetPending := &types.GitOpsState{LastSHA: "aaaaaaaa", FleetSHA: "", LastRunAt: now}
	_, run = needsRun(fleetPending, "aaaaaaaa", false, now.Add(time.Minute))
	assert.False(t, run, "a failed fleet write is a retry too and backs off")
	_, run = needsRun(fleetPending, "aaaaaaaa", false, now.Add(gitopsRetryBackoff))
	assert.True(t, run)
}

func TestGitOpsFailedFleetWriteIsRetried(t *testing.T) {
	s, g := newGitOpsForTest(t)
	endpoint := seedEndpoint(t, s)
	ctx := context.Background()
	repo := &failOnceRepo{ManagedEndpointRepository: s.repo, fleetFailures: 1}
	s.repo = repo
	require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{Running: true, RunID: "r", PerEndpoint: map[string]types.GitOpsEndpointState{}}))
	report := &types.GitOpsReport{RunID: "r", SHA: "abcdef1", Results: []types.GitOpsDeployResult{{ID: endpoint.Spec.ID, Path: "model", OK: true}}, FleetYAML: "acme/model:\n  enabled: true\n  gpus:\n    H100: {priority: 1, maxReplicas: 3}\n"}
	require.NoError(t, g.applyReport(ctx, report))
	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "abcdef1", state.LastSHA)
	assert.Empty(t, state.FleetSHA, "the fleet was not applied")
	assert.Contains(t, state.FleetError, "synthetic")
	needsRun := func() bool { return state.LastSHA != state.FleetSHA }
	assert.True(t, needsRun(), "sync must relaunch at the same SHA")

	state.Running, state.RunID = true, "r2"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	report.RunID = "r2"
	report.Results[0].Skipped = true
	require.NoError(t, g.applyReport(ctx, report))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "abcdef1", state.FleetSHA)
	assert.Empty(t, state.FleetError)
	assert.False(t, needsRun())
	fleet, err := s.repo.GetFleet(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 3}}, fleet.Placements("acme/model"))

	// Invalid fleet: surfaced, but checkpointed so the poller does not loop.
	state.Running, state.RunID = true, "r3"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	report.RunID, report.SHA, report.FleetYAML = "r3", "abcdef2", "acme/model:\n  enabled: true\n  gpus: {NOTAGPU: {priority: 1}}\n"
	require.NoError(t, g.applyReport(ctx, report))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "abcdef2", state.FleetSHA)
	assert.Contains(t, state.FleetError, "not a known GPU type")
	fleet, _ = s.repo.GetFleet(ctx)
	assert.Equal(t, map[string]types.FleetPlacement{"H100": {Priority: 1, MaxReplicas: 3}}, fleet.Placements("acme/model"), "previous fleet stays in force")
}

// The run's outcome is saved before its token/container are cleaned up and a
// report for any other run is fenced out, so a report cannot be lost to a
// failed save or clobber a newer run.
func TestGitOpsReportIsFencedAndAcceptedBeforeCleanup(t *testing.T) {
	s, g := newGitOpsForTest(t)
	endpoint := seedEndpoint(t, s)
	ctx := context.Background()
	require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{Running: true, RunID: "r", TokenID: "tok-r", PerEndpoint: map[string]types.GitOpsEndpointState{}}))
	s.repo = &failOnceRepo{ManagedEndpointRepository: s.repo, stateFailures: 1}
	report := &types.GitOpsReport{RunID: "r", SHA: "abcdef1", Results: []types.GitOpsDeployResult{{ID: endpoint.Spec.ID, Path: "model", OK: true, Version: 2}}}

	require.Error(t, g.applyReport(ctx, report), "the save failed")
	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.True(t, state.Running, "the run is still in flight: its token stays valid for the retry")
	assert.Equal(t, "tok-r", state.TokenID)

	require.NoError(t, g.applyReport(ctx, report), "the deployer's retry lands")
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.False(t, state.Running)
	assert.Empty(t, state.TokenID)
	assert.Equal(t, "abcdef1", state.LastSHA)

	// Duplicate and stale reports are rejected once the run is closed.
	require.ErrorContains(t, g.applyReport(ctx, report), "not in flight")
	state.Running, state.RunID = true, "newer"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.ErrorContains(t, g.applyReport(ctx, &types.GitOpsReport{RunID: "r", SHA: "stale"}), "not in flight")
	state, _ = s.repo.GetGitOpsState(ctx)
	assert.Equal(t, "newer", state.RunID)
	assert.Equal(t, "abcdef1", state.LastSHA)
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

	// Staging only accepts its branch, including when a tag has the same name.
	s.config.Repo.Branch = "staging"
	for _, ref := range []string{"refs/heads/main", "refs/tags/staging", "staging"} {
		body = push(ref, "abc")
		rec = post(body, map[string]string{"X-Hub-Signature-256": sign(body)})
		assert.Contains(t, rec.Body.String(), `"ignored":true`)
		assert.Empty(t, g.pending)
	}
	body = push("refs/heads/staging", "abc")
	rec = post(body, map[string]string{"X-Hub-Signature-256": sign(body)})
	assert.Equal(t, http.StatusAccepted, rec.Code)
	require.Len(t, g.pending, 1)
	<-g.pending

	// Branch deletion is ignored; ping is acknowledged.
	body, _ = json.Marshal(map[string]any{"ref": "refs/heads/main", "after": "0000000000000000000000000000000000000000", "deleted": true})
	rec = post(body, map[string]string{"X-Hub-Signature-256": sign(body)})
	assert.Contains(t, rec.Body.String(), `"ignored":true`)
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
	body, _ := json.Marshal(types.GitOpsReport{RunID: "run-1", SHA: "bbbbbbbb", FleetYAML: "{}"})
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

func TestGitOpsLaunchFailureIsVisibleBeforeContainerCreation(t *testing.T) {
	s, g := newGitOpsForTest(t)
	ctx := context.Background()
	require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{LastSHA: "aaaaaaaa", FleetSHA: "aaaaaaaa"}))
	err := g.sync(ctx, gitopsRequest{sha: "bbbbbbbb"})
	require.ErrorContains(t, err, "scheduler unavailable")
	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	require.False(t, state.Running)
	require.Equal(t, "aaaaaaaa", state.LastSHA)
	require.Equal(t, "bbbbbbbb", state.TargetSHA)
	require.Contains(t, state.LastError, "launch deployer: scheduler unavailable")
	require.False(t, state.LastRunAt.IsZero())
}
