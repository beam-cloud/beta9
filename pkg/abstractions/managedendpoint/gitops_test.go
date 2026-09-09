package managedendpoint

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

	// Two endpoints and a service exist from an earlier commit.
	seedEndpoint(t, s)
	require.NoError(t, s.repo.SaveEndpoint(ctx, &types.ManagedEndpoint{
		Spec: types.ManagedEndpointSpec{ID: "acme/old", Kind: types.EndpointKindLLM, Engine: "vllm", Port: 8000}, StubID: "stub-old", Version: 1, Enabled: true, Status: types.EndpointStatusActive,
	}))
	require.NoError(t, s.repo.SaveService(ctx, &types.ManagedService{
		Spec: types.ManagedServiceSpec{Name: "mooncake", Port: 9000}, StubID: "stub-svc", Version: 1, Enabled: true, Status: types.EndpointStatusActive,
	}))
	require.NoError(t, s.repo.SaveGitOpsState(ctx, &types.GitOpsState{
		LastSHA: "aaaaaaaa", Running: true, RunID: "run-1", TargetSHA: "bbbbbbbb", StartedAt: time.Now(),
		PerEndpoint: map[string]types.GitOpsEndpointState{
			"endpoint:acme/model": {Path: "acme/model", ID: "acme/model", Kind: "endpoint", Status: types.GitOpsStatusApplied, AppliedSHA: "aaaaaaaa"},
			"endpoint:acme/old":   {Path: "acme/old", ID: "acme/old", Kind: "endpoint", Status: types.GitOpsStatusApplied, AppliedSHA: "aaaaaaaa"},
			"service:mooncake":    {Path: "services/mooncake", ID: "mooncake", Kind: "service", Status: types.GitOpsStatusApplied, AppliedSHA: "aaaaaaaa"},
		},
	}))

	// A report for an unknown run is rejected.
	err := g.applyReport(ctx, &types.GitOpsReport{RunID: "stale", SHA: "bbbbbbbb"})
	require.Error(t, err)

	// acme/model redeployed, mooncake unchanged, acme/old removed from the repo,
	// and one directory failed to import.
	report := &types.GitOpsReport{
		RunID: "run-1", SHA: "bbbbbbbb",
		Discovered: []types.GitOpsDiscovered{
			{Path: "acme/model", ID: "acme/model", Kind: "endpoint"},
			{Path: "services/mooncake", ID: "mooncake", Kind: "service"},
		},
		Results: []types.GitOpsDeployResult{
			{Path: "acme/model", ID: "acme/model", Kind: "endpoint", OK: true, StubID: "stub-2", Version: 2},
			{Path: "services/mooncake", ID: "mooncake", Kind: "service", OK: true, Skipped: true},
			{Path: "acme/broken", OK: false, Error: "import failed: boom"},
		},
	}
	require.NoError(t, g.applyReport(ctx, report))

	state, err := s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.False(t, state.Running)
	assert.Empty(t, state.RunID)
	assert.Equal(t, "aaaaaaaa", state.LastSHA, "a failed stub keeps LastSHA so the run is retried")
	assert.Equal(t, "bbbbbbbb", state.TargetSHA)
	assert.Contains(t, state.LastError, "1 stub(s) failed")

	model := state.PerEndpoint["endpoint:acme/model"]
	assert.Equal(t, types.GitOpsStatusApplied, model.Status)
	assert.Equal(t, "bbbbbbbb", model.AppliedSHA)
	assert.Equal(t, "stub-2", model.StubID)
	assert.Equal(t, uint(2), model.Version)

	svc := state.PerEndpoint["service:mooncake"]
	assert.Equal(t, types.GitOpsStatusApplied, svc.Status)
	assert.Equal(t, "bbbbbbbb", svc.AppliedSHA, "unchanged stubs follow the repo head")

	old := state.PerEndpoint["endpoint:acme/old"]
	assert.Equal(t, types.GitOpsStatusRetired, old.Status)
	retired, err := s.repo.GetEndpoint(ctx, "acme/old")
	require.NoError(t, err)
	assert.False(t, retired.Enabled)
	assert.Equal(t, types.EndpointStatusRetired, retired.Status)

	broken := state.PerEndpoint["path:acme/broken"]
	assert.Equal(t, types.GitOpsStatusFailed, broken.Status)
	assert.Contains(t, broken.Error, "boom")

	// A duplicate report for the same run is rejected once the run closed.
	require.Error(t, g.applyReport(ctx, report))

	// Next run: everything applies, the broken path is gone; LastSHA advances
	// and the stale failure entry is dropped.
	state.Running, state.RunID = true, "run-2"
	require.NoError(t, s.repo.SaveGitOpsState(ctx, state))
	require.NoError(t, g.applyReport(ctx, &types.GitOpsReport{
		RunID: "run-2", SHA: "cccccccc",
		Discovered: []types.GitOpsDiscovered{{Path: "acme/model", ID: "acme/model", Kind: "endpoint"}, {Path: "services/mooncake", ID: "mooncake", Kind: "service"}},
		Results: []types.GitOpsDeployResult{
			{Path: "acme/model", ID: "acme/model", Kind: "endpoint", OK: true, Skipped: true},
			{Path: "services/mooncake", ID: "mooncake", Kind: "service", OK: true, Skipped: true},
		},
	}))
	state, err = s.repo.GetGitOpsState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "cccccccc", state.LastSHA)
	assert.Empty(t, state.LastError)
	_, hasBroken := state.PerEndpoint["path:acme/broken"]
	assert.False(t, hasBroken)
	assert.Equal(t, types.GitOpsStatusRetired, state.PerEndpoint["endpoint:acme/old"].Status, "retired entries are kept for visibility")
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

	s, g := newGitOpsForTest(t)
	s.config.Repo.URL = dir

	sha, err := g.resolveHead(context.Background())
	require.NoError(t, err)
	assert.Equal(t, head, sha)

	s.config.Repo.Ref = "v1"
	sha, err = g.resolveHead(context.Background())
	require.NoError(t, err)
	assert.Equal(t, head, sha, "tags resolve to the commit, not the tag object")

	s.config.Repo.Ref = "missing"
	_, err = g.resolveHead(context.Background())
	require.Error(t, err)
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
	require.NoError(t, s.repo.SaveGitOpsState(context.Background(), &types.GitOpsState{Running: true, RunID: "run-1", TargetSHA: "bbbbbbbb", StartedAt: time.Now()}))
	e := echo.New()
	body, _ := json.Marshal(types.GitOpsReport{RunID: "run-1", SHA: "bbbbbbbb"})

	// Unauthenticated echo context: rejected.
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	err := g.handleReport(e.NewContext(req, httptest.NewRecorder()))
	var httpErr *echo.HTTPError
	require.ErrorAs(t, err, &httpErr)
	assert.Equal(t, http.StatusUnauthorized, httpErr.Code)

	// Admin-workspace token (the run token) is accepted.
	req = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, g.handleReport(authedEchoContext(c, harnessCtx())))
	assert.Equal(t, http.StatusOK, rec.Code)

	state, err := s.repo.GetGitOpsState(context.Background())
	require.NoError(t, err)
	assert.False(t, state.Running)
	assert.Equal(t, "bbbbbbbb", state.LastSHA)
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
