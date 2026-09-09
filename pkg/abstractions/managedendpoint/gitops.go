package managedendpoint

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

// GitOps keeps the endpoint registry in sync with the endpoints repository.
//
// The gateway itself never imports Python: on a new commit it launches a
// one-shot deployer container (SDK + git) in the admin workspace that checks
// out the SHA, discovers every app.py exporting a ManagedEndpoint or
// ManagedService, runs the SDK deploy for the changed ones and posts a report
// back to /api/v1/endpoints/gitops/report. The report drives version records,
// per-endpoint status and retirement of directories that disappeared.

//go:embed gitops_deployer.py
var gitopsDeployerScript string

const (
	gitopsLockKey = "managed_endpoint:gitops:lock"
	gitopsLockTTL = 30 * time.Second
	// gitopsRunTimeout bounds one deployer run, including image builds.
	gitopsRunTimeout     = 45 * time.Minute
	gitopsStubName       = "managed-endpoints-deployer"
	gitopsContainerPfx   = "me-deployer"
	gitopsDeployerCPU    = int64(2000)
	gitopsDeployerMemory = int64(4096)
	gitopsResolveTimeout = 30 * time.Second
)

var shaPattern = regexp.MustCompile(`^[0-9a-f]{7,64}$`)

type gitops struct {
	s       *Service
	lock    *common.RedisLock
	pending chan gitopsRequest
}

type gitopsRequest struct {
	sha   string
	force bool
}

func newGitOps(s *Service) *gitops {
	if s == nil || strings.TrimSpace(s.config.Repo.URL) == "" || s.rdb == nil {
		return nil
	}
	return &gitops{s: s, lock: common.NewRedisLock(s.rdb), pending: make(chan gitopsRequest, 1)}
}

// Trigger requests a sync. With an empty sha the remote head of the
// configured ref is resolved; a non-empty sha forces a redeploy of every stub
// at that commit. It returns false when a sync is already queued.
func (g *gitops) Trigger(sha string) (bool, error) {
	sha = strings.ToLower(strings.TrimSpace(sha))
	if sha != "" && !shaPattern.MatchString(sha) {
		return false, errors.New("sha must be a hex commit id")
	}
	select {
	case g.pending <- gitopsRequest{sha: sha, force: sha != ""}:
		return true, nil
	default:
		return false, nil
	}
}

// mount registers the unauthenticated webhook and the token-authenticated
// deployer report route.
func (g *gitops) mount(public *echo.Group, authed *echo.Group) {
	if public != nil {
		public.POST("/gitops/webhook", g.handleWebhook)
	}
	if authed != nil {
		authed.POST("/gitops/report", g.handleReport)
	}
}

// run polls the remote ref and services triggers. Like the controller it is
// leader-elected so only one gateway launches deployers.
func (g *gitops) run(ctx context.Context) {
	ticker := time.NewTicker(g.s.config.Repo.PollInterval)
	defer ticker.Stop()
	for {
		var req gitopsRequest
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case req = <-g.pending:
		}
		if err := g.lock.Acquire(ctx, gitopsLockKey, common.RedisLockOptions{TtlS: int(gitopsLockTTL.Seconds()), Retries: 0}); err != nil {
			continue
		}
		if err := g.sync(ctx, req); err != nil {
			log.Error().Err(err).Msg("managed endpoints: gitops sync failed")
		}
		_ = g.lock.Release(gitopsLockKey)
	}
}

// sync is one reconciler pass: expire a stuck run, then launch a deployer if
// the target commit differs from the last applied one.
func (g *gitops) sync(ctx context.Context, req gitopsRequest) error {
	state, err := g.state(ctx)
	if err != nil {
		return err
	}
	if state.Running {
		switch {
		case g.exitedWithoutReport(state):
			g.failRun(ctx, state, "deployer exited without reporting")
		case time.Since(state.StartedAt) > gitopsRunTimeout:
			g.failRun(ctx, state, "deployer timed out")
		default:
			return nil
		}
	}
	sha := req.sha
	if sha == "" {
		if sha, err = g.resolveHead(ctx); err != nil {
			state.LastError = "resolve head: " + err.Error()
			state.LastRunAt = time.Now()
			_ = g.s.repo.SaveGitOpsState(ctx, state)
			return err
		}
	}
	var retry []string
	for _, e := range state.PerEndpoint {
		if e.Status == types.GitOpsStatusFailed && e.Path != "" {
			retry = append(retry, e.Path)
		}
	}
	if !req.force && sha == state.LastSHA && len(retry) == 0 {
		return nil
	}
	return g.launch(ctx, state, sha, req.force, retry)
}

func (g *gitops) state(ctx context.Context) (*types.GitOpsState, error) {
	state, err := g.s.repo.GetGitOpsState(ctx)
	if err != nil {
		return nil, err
	}
	if state == nil {
		state = &types.GitOpsState{PerEndpoint: map[string]types.GitOpsEndpointState{}}
	}
	state.RepoURL = g.s.config.Repo.URL
	state.Ref = g.s.config.Repo.Ref
	return state, nil
}

// exitedWithoutReport is true when the deployer container is gone but no
// report arrived; the SDK deploy may have partially applied. The scheduler
// gets a couple of minutes to create the container before a missing state
// counts as an exit.
func (g *gitops) exitedWithoutReport(state *types.GitOpsState) bool {
	if state.ContainerID == "" || g.s.containers == nil || time.Since(state.StartedAt) < 2*time.Minute {
		return false
	}
	_, err := g.s.containers.GetContainerState(state.ContainerID)
	var notFound *types.ErrContainerStateNotFound
	return errors.As(err, &notFound)
}

// deployKey returns the decrypted deploy key secret (SSH private key or https
// token) from the admin workspace, or "" when none is configured.
func (g *gitops) deployKey(ctx context.Context) (string, error) {
	name := strings.TrimSpace(g.s.config.Repo.DeployKeySecret)
	if name == "" {
		return "", nil
	}
	workspace, err := g.s.AdminWorkspace(ctx)
	if err != nil {
		return "", err
	}
	secret, err := g.s.backend.GetSecretByName(ctx, workspace, name)
	if err != nil {
		return "", fmt.Errorf("deploy key secret %q: %w", name, err)
	}
	if secret == nil || workspace.SigningKey == nil {
		return "", fmt.Errorf("deploy key secret %q not found in admin workspace", name)
	}
	signingKey, err := common.ParseSecretKey(*workspace.SigningKey)
	if err != nil {
		return "", err
	}
	return common.Decrypt(signingKey, secret.Value)
}

// resolveHead runs `git ls-remote` for the configured ref.
func (g *gitops) resolveHead(ctx context.Context) (string, error) {
	key, err := g.deployKey(ctx)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, gitopsResolveTimeout)
	defer cancel()

	url := g.s.config.Repo.URL
	env := append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	switch {
	case strings.HasPrefix(key, "-----BEGIN"):
		dir, err := os.MkdirTemp("", "endpoints-key-")
		if err != nil {
			return "", err
		}
		defer os.RemoveAll(dir)
		keyPath := filepath.Join(dir, "deploy_key")
		if err := os.WriteFile(keyPath, []byte(strings.TrimRight(key, "\n")+"\n"), 0o600); err != nil {
			return "", err
		}
		env = append(env, "GIT_SSH_COMMAND=ssh -i "+keyPath+" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new")
	case key != "" && strings.HasPrefix(url, "https://"):
		url = "https://x-access-token:" + key + "@" + strings.TrimPrefix(url, "https://")
	}

	ref := g.s.config.Repo.Ref
	cmd := exec.CommandContext(ctx, "git", "ls-remote", "--heads", "--tags", url, ref, "refs/heads/"+ref, "refs/tags/"+ref)
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git ls-remote: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	// Prefer the branch over a same-named tag; the first matching line wins.
	for _, line := range strings.Split(stdout.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && shaPattern.MatchString(fields[0]) && !strings.HasSuffix(fields[1], "^{}") {
			return fields[0], nil
		}
	}
	return "", fmt.Errorf("ref %q not found on %s", ref, g.s.config.Repo.URL)
}

// launch starts the deployer container for sha and marks the run in flight.
func (g *gitops) launch(ctx context.Context, state *types.GitOpsState, sha string, force bool, retry []string) error {
	if g.s.scheduler == nil {
		return errors.New("scheduler unavailable")
	}
	image := strings.TrimSpace(g.s.config.DeployerImage)
	if image == "" {
		return errors.New("managedEndpoints.deployerImage is required for gitops")
	}
	workspace, err := g.s.AdminWorkspace(ctx)
	if err != nil {
		return err
	}
	key, err := g.deployKey(ctx)
	if err != nil {
		return err
	}
	stub, err := g.deployerStub(ctx, workspace, image)
	if err != nil {
		return err
	}
	token, err := g.s.backend.CreateToken(ctx, workspace.Id, types.TokenTypeWorkspace, true)
	if err != nil {
		return fmt.Errorf("mint deployer token: %w", err)
	}

	runID := uuid.New().String()
	containerID := fmt.Sprintf("%s-%s", gitopsContainerPfx, runID[:8])
	env := []string{
		"BETA9_TOKEN=" + token.Key,
		"STUB_ID=" + stub.ExternalId,
		"STUB_TYPE=" + string(stub.Type),
		"ENDPOINTS_REPO_URL=" + g.s.config.Repo.URL,
		"ENDPOINTS_REPO_SHA=" + sha,
		"ENDPOINTS_LAST_SHA=" + state.LastSHA,
		"ENDPOINTS_REPO_REF=" + g.s.config.Repo.Ref,
		"ENDPOINTS_REPO_PATH=" + strings.Trim(g.s.config.Repo.Path, "/"),
		"ENDPOINTS_RUN_ID=" + runID,
		"ENDPOINTS_REDEPLOY=" + strings.Join(retry, ","),
		"ENDPOINTS_DEPLOYER=" + gitopsDeployerScript,
	}
	if force {
		env = append(env, "ENDPOINTS_FORCE=1")
	}
	if key != "" {
		env = append(env, "ENDPOINTS_DEPLOY_KEY="+key)
	}
	request := &types.ContainerRequest{
		ContainerId: containerID,
		EntryPoint:  []string{"sh", "-c", `printf '%s' "$ENDPOINTS_DEPLOYER" > /tmp/deployer.py && exec python3 /tmp/deployer.py`},
		Env:         env,
		Cpu:         gitopsDeployerCPU,
		Memory:      gitopsDeployerMemory,
		ImageId:     image,
		StubId:      stub.ExternalId,
		AppId:       stub.App.ExternalId,
		WorkspaceId: workspace.ExternalId,
		Workspace:   *workspace,
		Stub:        *stub,
		Timestamp:   time.Now(),
	}

	now := time.Now()
	state.Running = true
	state.TargetSHA = sha
	state.RunID = runID
	state.ContainerID = containerID
	state.TokenID = token.ExternalId
	state.StartedAt = now
	state.LastRunAt = now
	state.LastError = ""
	if err := g.s.repo.SaveGitOpsState(ctx, state); err != nil {
		_ = g.s.backend.DeleteToken(ctx, workspace.Id, token.ExternalId)
		return err
	}
	if err := g.s.scheduler.Run(request); err != nil {
		g.failRun(ctx, state, "schedule deployer: "+err.Error())
		return err
	}
	g.s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{
		Action: "gitops.started", ContainerID: containerID, Message: sha,
		Data: map[string]any{"sha": sha, "last_sha": state.LastSHA, "force": force, "retry": retry},
	})
	log.Info().Str("sha", sha).Str("container_id", containerID).Msg("managed endpoints: gitops deployer launched")
	return nil
}

// deployerStub returns the pod stub the deployer runs under, creating it on
// first use. The stub is recreated when the configured image changes.
func (g *gitops) deployerStub(ctx context.Context, workspace *types.Workspace, image string) (*types.StubWithRelated, error) {
	config := types.StubConfigV1{Runtime: types.Runtime{Cpu: gitopsDeployerCPU, Memory: gitopsDeployerMemory, ImageId: image}}
	app, err := g.s.backend.GetOrCreateApp(ctx, workspace.Id, gitopsStubName)
	if err != nil {
		return nil, fmt.Errorf("deployer app: %w", err)
	}
	object, err := abstractions.EnsureEmptyStubObject(ctx, g.s.backend, workspace)
	if err != nil {
		return nil, fmt.Errorf("deployer object: %w", err)
	}
	stub, err := g.s.backend.GetOrCreateStub(ctx, gitopsStubName, types.StubTypePod, config, object.Id, workspace.Id, false, app.Id)
	if err != nil {
		return nil, fmt.Errorf("deployer stub: %w", err)
	}
	return &types.StubWithRelated{Stub: stub, Workspace: *workspace, App: app, Object: object}, nil
}

// failRun closes an in-flight run as failed and releases its token.
func (g *gitops) failRun(ctx context.Context, state *types.GitOpsState, reason string) {
	g.finishRun(ctx, state)
	state.LastError = reason
	state.LastRunAt = time.Now()
	_ = g.s.repo.SaveGitOpsState(ctx, state)
	g.s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{Action: "gitops.failed", Message: reason, Data: map[string]any{"sha": state.TargetSHA}})
	log.Warn().Str("sha", state.TargetSHA).Str("reason", reason).Msg("managed endpoints: gitops run failed")
}

// finishRun revokes the run token, stops a lingering container and clears
// the in-flight markers.
func (g *gitops) finishRun(ctx context.Context, state *types.GitOpsState) {
	if state.TokenID != "" && g.s.backend != nil {
		if workspace, err := g.s.AdminWorkspace(ctx); err == nil {
			_ = g.s.backend.DeleteToken(ctx, workspace.Id, state.TokenID)
		}
	}
	if state.ContainerID != "" && g.s.scheduler != nil && g.s.containers != nil {
		if _, err := g.s.containers.GetContainerState(state.ContainerID); err == nil {
			_ = g.s.scheduler.Stop(&types.StopContainerArgs{ContainerId: state.ContainerID, Force: true, Reason: types.StopContainerReasonScheduler})
		}
	}
	state.Running = false
	state.RunID, state.ContainerID, state.TokenID = "", "", ""
}

// applyReport folds the deployer's report into the registry: records
// per-endpoint status, advances LastSHA when everything applied, and retires
// endpoints and services whose directories disappeared.
func (g *gitops) applyReport(ctx context.Context, report *types.GitOpsReport) error {
	if report == nil || report.RunID == "" {
		return errors.New("run_id is required")
	}
	state, err := g.state(ctx)
	if err != nil {
		return err
	}
	if !state.Running || state.RunID != report.RunID {
		return fmt.Errorf("run %s is not in flight", report.RunID)
	}
	now := time.Now()
	g.finishRun(ctx, state)
	state.LastRunAt = now

	if report.Error != "" {
		state.LastError = report.Error
		_ = g.s.repo.SaveGitOpsState(ctx, state)
		g.s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{Action: "gitops.failed", Message: report.Error, Data: map[string]any{"sha": report.SHA}})
		return nil
	}

	failed := 0
	failedPaths := map[string]bool{}
	for _, r := range report.Results {
		if r.ID == "" {
			// Import failure: keyed by path so it stays visible.
			state.PerEndpoint["path:"+r.Path] = types.GitOpsEndpointState{Path: r.Path, Status: types.GitOpsStatusFailed, Error: r.Error, UpdatedAt: now}
			failedPaths[r.Path] = true
			failed++
			continue
		}
		key := r.Kind + ":" + r.ID
		entry := state.PerEndpoint[key]
		entry.Path, entry.ID, entry.Kind, entry.UpdatedAt = r.Path, r.ID, r.Kind, now
		if r.OK {
			// Unchanged (skipped) directories move forward with the repo head too.
			entry.Status, entry.Error, entry.AppliedSHA = types.GitOpsStatusApplied, "", report.SHA
			if !r.Skipped {
				entry.StubID, entry.Version = r.StubID, r.Version
			}
		} else {
			entry.Status, entry.Error = types.GitOpsStatusFailed, r.Error
			failed++
		}
		state.PerEndpoint[key] = entry
	}

	// Anything previously applied that is no longer discovered is retired.
	present := map[string]bool{}
	for _, d := range report.Discovered {
		present[d.Kind+":"+d.ID] = true
	}
	for key, entry := range state.PerEndpoint {
		switch {
		case strings.HasPrefix(key, "path:"):
			if !failedPaths[entry.Path] {
				delete(state.PerEndpoint, key)
			}
		case !present[key] && entry.Status != types.GitOpsStatusRetired:
			if err := g.retire(ctx, entry.Kind, entry.ID, report.SHA); err != nil {
				log.Warn().Err(err).Str("id", entry.ID).Msg("managed endpoints: gitops retire failed")
				continue
			}
			entry.Status, entry.Error, entry.UpdatedAt = types.GitOpsStatusRetired, "", now
			state.PerEndpoint[key] = entry
		}
	}

	state.TargetSHA = report.SHA
	if failed == 0 {
		state.LastSHA, state.LastError = report.SHA, ""
	} else {
		state.LastError = fmt.Sprintf("%d stub(s) failed to deploy at %.8s", failed, report.SHA)
	}
	if err := g.s.repo.SaveGitOpsState(ctx, state); err != nil {
		return err
	}
	g.s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{
		Action: "gitops.applied", Message: report.SHA,
		Data: map[string]any{"sha": report.SHA, "results": len(report.Results), "failed": failed, "discovered": len(report.Discovered)},
	})
	log.Info().Str("sha", report.SHA).Int("results", len(report.Results)).Int("failed", failed).Msg("managed endpoints: gitops report applied")
	return nil
}

// retire disables an endpoint or service removed from the repo. Replicas are
// drained by the controller; usage history and versions are kept.
func (g *gitops) retire(ctx context.Context, kind, id, sha string) error {
	now := time.Now()
	if kind == "service" {
		service, err := g.s.repo.GetService(ctx, id)
		if err != nil || service == nil {
			return err
		}
		service.Enabled, service.Status, service.UpdatedAt = false, types.EndpointStatusRetired, now
		if err := g.s.repo.SaveService(ctx, service); err != nil {
			return err
		}
	} else {
		endpoint, err := g.s.repo.GetEndpoint(ctx, id)
		if err != nil || endpoint == nil {
			return err
		}
		endpoint.Enabled, endpoint.Status, endpoint.UpdatedAt = false, types.EndpointStatusRetired, now
		if err := g.s.repo.SaveEndpoint(ctx, endpoint); err != nil {
			return err
		}
	}
	g.s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{
		EndpointID: id, Action: "gitops.retired", Message: "removed from repo", Data: map[string]any{"kind": kind, "sha": sha},
	})
	return nil
}

// --- HTTP ----------------------------------------------------------------------

// handleWebhook accepts GitHub/GitLab push notifications. The body is
// verified with Webhook.Secret; pushes to other refs are ignored.
func (g *gitops) handleWebhook(ctx echo.Context) error {
	secret := strings.TrimSpace(g.s.config.Webhook.Secret)
	if secret == "" {
		return echo.NewHTTPError(http.StatusNotFound, "webhook is not configured")
	}
	body, err := io.ReadAll(io.LimitReader(ctx.Request().Body, 1<<20))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "read body")
	}
	if !verifyWebhook(ctx.Request().Header, body, secret) {
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid signature")
	}
	push, ok := parsePush(body)
	want := g.s.config.Repo.Ref
	switch {
	case !ok:
		// Ping / non-push events are acknowledged and ignored.
		return ctx.JSON(http.StatusOK, map[string]any{"ok": true, "ignored": true})
	case push.Ref != want && push.Ref != "refs/heads/"+want && push.Ref != "refs/tags/"+want:
		return ctx.JSON(http.StatusOK, map[string]any{"ok": true, "ignored": true, "ref": push.Ref})
	case push.Deleted:
		return ctx.JSON(http.StatusOK, map[string]any{"ok": true, "ignored": true, "deleted": true})
	}
	// Let the poller resolve the head rather than trusting the payload sha,
	// so a burst of pushes collapses into one run at the latest commit.
	started, _ := g.Trigger("")
	return ctx.JSON(http.StatusAccepted, map[string]any{"ok": true, "started": started, "after": push.After})
}

// handleReport receives the deployer's result. It accepts any active token of
// the admin workspace (the run token) or a cluster admin token.
func (g *gitops) handleReport(ctx echo.Context) error {
	reqCtx := ctx.Request().Context()
	if cc, ok := ctx.(*auth.HttpAuthContext); ok && cc.AuthInfo != nil {
		reqCtx = auth.ContextWithAuthInfo(reqCtx, cc.AuthInfo)
	}
	if err := g.s.authorizeHarness(reqCtx); err != nil {
		return httpError(err)
	}
	var report types.GitOpsReport
	if err := json.NewDecoder(io.LimitReader(ctx.Request().Body, 4<<20)).Decode(&report); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err := g.applyReport(reqCtx, &report); err != nil {
		return echo.NewHTTPError(http.StatusConflict, err.Error())
	}
	return ctx.JSON(http.StatusOK, map[string]any{"ok": true})
}

// pushEvent is the subset of a GitHub/GitLab push payload the reconciler needs.
type pushEvent struct {
	Ref     string
	After   string
	Deleted bool
}

// parsePush extracts ref/after from GitHub and GitLab push payloads. It
// returns false for payloads that are not pushes (pings, PR events, ...).
func parsePush(body []byte) (pushEvent, bool) {
	var raw struct {
		Ref        string `json:"ref"`
		After      string `json:"after"`
		Deleted    bool   `json:"deleted"`
		ObjectKind string `json:"object_kind"` // gitlab
		Zen        string `json:"zen"`         // github ping
	}
	if err := json.Unmarshal(body, &raw); err != nil || raw.Ref == "" || raw.Zen != "" {
		return pushEvent{}, false
	}
	if raw.ObjectKind != "" && raw.ObjectKind != "push" && raw.ObjectKind != "tag_push" {
		return pushEvent{}, false
	}
	return pushEvent{Ref: raw.Ref, After: raw.After, Deleted: raw.Deleted || strings.Trim(raw.After, "0") == ""}, true
}

// verifyWebhook accepts GitHub's HMAC-SHA256 signature
// (X-Hub-Signature-256: sha256=<hex>), GitLab's shared token
// (X-Gitlab-Token) and a generic bearer/plain token in X-Webhook-Token.
func verifyWebhook(header http.Header, body []byte, secret string) bool {
	if sig := strings.TrimSpace(header.Get("X-Hub-Signature-256")); sig != "" {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(body)
		return hmac.Equal([]byte(strings.ToLower(sig)), []byte("sha256="+hex.EncodeToString(mac.Sum(nil))))
	}
	for _, name := range []string{"X-Gitlab-Token", "X-Webhook-Token"} {
		if token := strings.TrimSpace(header.Get(name)); token != "" {
			return subtle.ConstantTimeCompare([]byte(token), []byte(secret)) == 1
		}
	}
	if authz := strings.TrimSpace(header.Get("Authorization")); strings.HasPrefix(authz, "Bearer ") {
		return subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(authz, "Bearer ")), []byte(secret)) == 1
	}
	return false
}
