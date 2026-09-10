package managedendpoint

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
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
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v2"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

// gitops keeps the registry in sync with the endpoints repo. On a new commit
// a one-shot deployer container (SDK + git) deploys the changed apps and
// posts a report to /api/v1/endpoints/gitops/report.

//go:embed gitops_deployer.py
var gitopsDeployerScript string

const (
	gitopsLockKey        = "managed_endpoint:gitops:lock"
	gitopsLockTTL        = 30 * time.Second
	gitopsRunTimeout     = 45 * time.Minute // one deployer run, including image builds
	gitopsRetryBackoff   = 15 * time.Minute // between runs that only retry failures at an unchanged commit
	gitopsSyncWarnAfter  = 2 * time.Minute
	gitopsStubName       = "managed-endpoints-deployer"
	gitopsContainerPfx   = "me-deployer"
	gitopsDeployerCPU    = int64(2000)
	gitopsDeployerMemory = int64(4096)
	gitopsResolveTimeout = 30 * time.Second
)

var shaPattern = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)
var deployerSecretNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type gitops struct {
	s       *Service
	lock    *common.RedisLock
	pending chan gitopsRequest
	stub    *types.StubWithRelated // deployer stub, resolved once per process
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

// Trigger requests a sync; a non-empty sha forces a redeploy of every stub at
// that commit. It returns false when a sync is already queued.
func (g *gitops) Trigger(sha string) (bool, error) {
	sha = strings.ToLower(strings.TrimSpace(sha))
	if sha != "" && !shaPattern.MatchString(sha) {
		return false, errors.New("sha must be a full 40- or 64-character hex commit id; omit it to sync the configured ref")
	}
	select {
	case g.pending <- gitopsRequest{sha: sha, force: sha != ""}:
		return true, nil
	default:
		return false, nil
	}
}

func (g *gitops) mount(public *echo.Group, authed *echo.Group) {
	if public != nil {
		public.POST("/gitops/webhook", g.handleWebhook)
	}
	if authed != nil {
		authed.POST("/gitops/report", g.handleReport)
	}
}

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
		err := g.lock.WithLease(ctx, gitopsLockKey, common.RedisLockOptions{TtlS: int(gitopsLockTTL.Seconds()), Retries: 0}, func(ctx context.Context) error {
			stop := warnIfStuck(gitopsSyncWarnAfter, "managed endpoints: gitops sync has not returned; reconciliation is stalled")
			defer stop()
			return g.sync(ctx, req)
		})
		if err != nil && !common.IsRedisLockNotObtained(err) {
			log.Error().Err(err).Msg("managed endpoints: gitops sync failed")
		}
	}
}

// warnIfStuck logs msg every interval until the returned stop is called.
func warnIfStuck(interval time.Duration, msg string) (stop func()) {
	done := make(chan struct{})
	started := time.Now()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				log.Error().Dur("running_for", time.Since(started)).Msg(msg)
			}
		}
	}()
	return func() { close(done) }
}

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
	retry, run := needsRun(state, sha, req.force, time.Now())
	if !run {
		// A recovered repository can still point at the already applied commit.
		// Clear only the lookup failure after actually resolving the ref; other
		// deployment errors must survive a poll that does not run a deployer.
		if req.sha == "" && strings.HasPrefix(state.LastError, "resolve head: ") {
			state.LastError = ""
			return g.s.repo.SaveGitOpsState(ctx, state)
		}
		return nil
	}
	if err := g.launch(ctx, state, sha, req.force, retry); err != nil {
		state.TargetSHA = sha
		g.failRun(ctx, state, "launch deployer: "+err.Error())
		return err
	}
	return nil
}

// needsRun decides whether to launch a deployer for sha and which failed
// endpoints to redeploy. A run that would only retry failures at an unchanged
// commit waits gitopsRetryBackoff.
func needsRun(state *types.GitOpsState, sha string, force bool, now time.Time) (retry []string, run bool) {
	for _, e := range state.PerEndpoint {
		if e.Status == types.GitOpsStatusFailed && e.Path != "" {
			retry = append(retry, e.Path)
		}
	}
	if force || sha != state.LastSHA {
		return retry, true
	}
	if len(retry) == 0 && state.FleetSHA == state.LastSHA {
		return nil, false
	}
	return retry, now.Sub(state.LastRunAt) >= gitopsRetryBackoff
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
	state.Ref = g.s.config.Repo.Branch
	return state, nil
}

// exitedWithoutReport is true when the deployer container is gone but no report arrived.
func (g *gitops) exitedWithoutReport(state *types.GitOpsState) bool {
	if state.ContainerID == "" || g.s.containers == nil || time.Since(state.StartedAt) < 2*time.Minute {
		return false
	}
	_, err := g.s.containers.GetContainerState(state.ContainerID)
	var notFound *types.ErrContainerStateNotFound
	return errors.As(err, &notFound)
}

// deployerSecrets never reads workspace secrets. The cluster config explicitly
// names which credentials the repository's build code may access.
func (g *gitops) deployerSecrets() ([]string, error) {
	env := make([]string, 0, len(g.s.config.DeployerSecrets))
	for name, value := range g.s.config.DeployerSecrets {
		if !deployerSecretNamePattern.MatchString(name) || strings.ContainsRune(value, '\x00') {
			return nil, fmt.Errorf("invalid managedEndpoints.deployerSecrets entry %q", name)
		}
		if strings.HasPrefix(name, "BETA9_") || strings.HasPrefix(name, "ENDPOINTS_") || name == "STUB_ID" || name == "STUB_TYPE" {
			return nil, fmt.Errorf("managedEndpoints.deployerSecrets entry %q is reserved", name)
		}
		env = append(env, name+"="+value)
	}
	sort.Strings(env)
	return env, nil
}

// resolveHead resolves only the configured branch, never a same-named tag.
func (g *gitops) resolveHead(ctx context.Context) (string, error) {
	ref := "refs/heads/" + strings.TrimSpace(g.s.config.Repo.Branch)
	key := strings.TrimSpace(g.s.config.Repo.DeployKey)
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

	cmd := exec.CommandContext(ctx, "git", "ls-remote", "--exit-code", url, ref)
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if key != "" {
			detail = strings.ReplaceAll(detail, key, "[redacted]")
		}
		return "", fmt.Errorf("git ls-remote %s: %v: %s", ref, err, detail)
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == ref && shaPattern.MatchString(fields[0]) {
			return fields[0], nil
		}
	}
	return "", fmt.Errorf("branch %q not found on %s", g.s.config.Repo.Branch, g.s.config.Repo.URL)
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
	key := strings.TrimSpace(g.s.config.Repo.DeployKey)
	stub, err := g.deployerStub(ctx, workspace, image)
	if err != nil {
		return err
	}
	secrets, err := g.deployerSecrets()
	if err != nil {
		return err
	}
	token, err := g.s.backend.CreateToken(ctx, workspace.Id, types.TokenTypePlatformDeployer, true)
	if err != nil {
		return fmt.Errorf("mint deployer token: %w", err)
	}

	runID := uuid.New().String()
	containerID := fmt.Sprintf("%s-%s", gitopsContainerPfx, runID[:8])
	env := append(secrets,
		"BETA9_TOKEN="+token.Key,
		"STUB_ID="+stub.ExternalId,
		"STUB_TYPE="+string(stub.Type),
		"ENDPOINTS_REPO_URL="+g.s.config.Repo.URL,
		"ENDPOINTS_REPO_SHA="+sha,
		"ENDPOINTS_LAST_SHA="+state.LastSHA,
		"ENDPOINTS_REPO_BRANCH="+g.s.config.Repo.Branch,
		"ENDPOINTS_REPO_PATH="+strings.Trim(g.s.config.Repo.Path, "/"),
		"ENDPOINTS_RUN_ID="+runID,
		"ENDPOINTS_REDEPLOY="+strings.Join(retry, ","),
		"ENDPOINTS_DEPLOYER="+gitopsDeployerScript,
	)
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
		return err
	}
	if err := g.s.scheduler.Run(request); err != nil {
		return fmt.Errorf("schedule deployer: %w", err)
	}
	g.s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{
		Action: "gitops.started", ContainerID: containerID, Message: sha,
		Data: map[string]any{"sha": sha, "last_sha": state.LastSHA, "force": force, "retry": retry},
	})
	log.Info().Str("sha", sha).Str("container_id", containerID).Msg("managed endpoints: gitops deployer launched")
	return nil
}

// deployerStub gives GitOps a service-owned billing identity, resolved once per process.
func (g *gitops) deployerStub(ctx context.Context, workspace *types.Workspace, image string) (*types.StubWithRelated, error) {
	if g.stub != nil {
		return g.stub, nil
	}
	config := types.StubConfigV1{Runtime: types.Runtime{Cpu: gitopsDeployerCPU, Memory: gitopsDeployerMemory, ImageId: image}}
	app, err := g.s.backend.GetOrCreateApp(ctx, workspace.Id, gitopsStubName)
	if err != nil {
		return nil, fmt.Errorf("deployer app: %w", err)
	}
	object, err := abstractions.EnsureEmptyStubObject(ctx, g.s.backend, workspace)
	if err != nil {
		return nil, fmt.Errorf("deployer object: %w", err)
	}
	stub, err := g.s.backend.GetOrCreateStub(ctx, gitopsStubName, types.StubTypePlatformDeployer, config, object.Id, workspace.Id, false, app.Id)
	if err != nil {
		return nil, fmt.Errorf("deployer stub: %w", err)
	}
	g.stub = &types.StubWithRelated{Stub: stub, Workspace: *workspace, App: app, Object: object}
	return g.stub, nil
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

// applyReport folds the deployer's report into the registry. It is fenced by
// the run id and saves state before cleaning up the run, so a failed save
// leaves the deployer's token valid for a retry.
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
	run := *state // for cleanup once the outcome is saved
	state.Running = false
	state.RunID, state.ContainerID, state.TokenID = "", "", ""
	state.LastRunAt = now

	if report.Error != "" {
		state.LastError = report.Error
		if err := g.s.repo.SaveGitOpsState(ctx, state); err != nil {
			return err
		}
		g.finishRun(ctx, &run)
		g.s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{Action: "gitops.failed", Message: report.Error, Data: map[string]any{"sha": report.SHA}})
		return nil
	}

	failed := g.applyResults(ctx, state, report, now)
	if !g.applyFleet(ctx, state, report) {
		failed++
	}

	// LastSHA advances even when some stubs failed; they are retried on their own.
	state.TargetSHA, state.LastSHA = report.SHA, report.SHA
	state.LastError = ""
	if failed > 0 {
		state.LastError = fmt.Sprintf("%d stub(s) failed to deploy at %.8s", failed, report.SHA)
	}
	if err := g.s.repo.SaveGitOpsState(ctx, state); err != nil {
		return err
	}
	g.finishRun(ctx, &run)
	g.s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{
		Action: "gitops.applied", Message: report.SHA,
		Data: map[string]any{"sha": report.SHA, "results": len(report.Results), "failed": failed},
	})
	log.Info().Str("sha", report.SHA).Int("results", len(report.Results)).Int("failed", failed).Msg("managed endpoints: gitops report applied")
	return nil
}

// applyResults records each app directory's outcome (PerEndpoint is keyed by
// path) and retires endpoints whose directory is gone. A directory that is
// still there but failed to import keeps its endpoint: a bad commit never
// tears down the previous one. It returns how many failed.
func (g *gitops) applyResults(ctx context.Context, state *types.GitOpsState, report *types.GitOpsReport, now time.Time) int {
	failed := 0
	seen := map[string]bool{}
	for _, r := range report.Results {
		seen[r.Path] = true
		entry := state.PerEndpoint[r.Path]
		entry.Path, entry.UpdatedAt = r.Path, now
		if r.ID != "" {
			entry.ID = r.ID
		}
		if r.OK {
			entry.Status, entry.Error, entry.AppliedSHA = types.GitOpsStatusApplied, "", report.SHA
			if !r.Skipped {
				entry.StubID, entry.Version = r.StubID, r.Version
			}
		} else {
			entry.Status, entry.Error = types.GitOpsStatusFailed, r.Error
			failed++
		}
		state.PerEndpoint[r.Path] = entry
	}
	for path, entry := range state.PerEndpoint {
		if seen[path] || entry.Status == types.GitOpsStatusRetired {
			continue
		}
		if path != entry.Path || entry.ID == "" {
			delete(state.PerEndpoint, path) // legacy key, or an import failure whose directory is gone
			continue
		}
		entry.Status, entry.Error, entry.UpdatedAt = types.GitOpsStatusRetired, "", now
		if err := g.retire(ctx, entry.ID, report.SHA); err != nil {
			log.Warn().Err(err).Str("id", entry.ID).Msg("managed endpoints: gitops retire failed")
			entry.Status, entry.Error = types.GitOpsStatusFailed, "retire: "+err.Error()
			failed++
		}
		state.PerEndpoint[path] = entry
	}
	return failed
}

// retire disables an endpoint removed from the repo; the controller drains its replicas.
func (g *gitops) retire(ctx context.Context, id, sha string) error {
	endpoint, err := g.s.repo.GetEndpoint(ctx, id)
	if err != nil || endpoint == nil {
		return err
	}
	endpoint.Status, endpoint.UpdatedAt = types.EndpointStatusRetired, time.Now()
	if err := g.s.repo.SaveEndpoint(ctx, endpoint); err != nil {
		return err
	}
	g.s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{
		EndpointID: id, Action: "gitops.retired", Message: "removed from repo", Data: map[string]any{"sha": sha},
	})
	return nil
}

// parseFleet reads config.yaml: endpoint id -> {enabled, gpus: {<gpu>: placement}}.
func parseFleet(text string) (*types.Fleet, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("an endpoint mapping is required (use {} to disable all endpoints)")
	}
	fleet := &types.Fleet{Endpoints: map[string]types.FleetEndpoint{}}
	decoder := yaml.NewDecoder(strings.NewReader(text))
	decoder.SetStrict(true)
	if err := decoder.Decode(&fleet.Endpoints); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("exactly one YAML document is required")
	}
	if fleet.Endpoints == nil {
		return nil, fmt.Errorf("an endpoint mapping is required (use {} to disable all endpoints)")
	}
	ids := map[string]bool{}
	for id, endpoint := range fleet.Endpoints {
		key := strings.ToLower(strings.TrimSpace(id))
		if key == "" || ids[key] {
			return nil, fmt.Errorf("empty or duplicate endpoint %q", id)
		}
		ids[key] = true
		gpus := map[string]bool{}
		for gpu := range endpoint.GPUs {
			key := types.GPUKey(gpu)
			if gpus[key] {
				return nil, fmt.Errorf("%s: duplicate GPU %q", id, gpu)
			}
			gpus[key] = true
		}
	}
	fleet.Normalize()
	return fleet, fleet.Validate()
}

// applyFleet validates and saves the report's config.yaml, dropping entries for
// endpoints that are not deployed (reported as skipped). An invalid fleet is
// not applied and not retried; a failed write leaves FleetSHA behind so sync
// relaunches at the same SHA.
func (g *gitops) applyFleet(ctx context.Context, state *types.GitOpsState, report *types.GitOpsReport) bool {
	fleet, err := parseFleet(report.FleetYAML)
	if err != nil {
		state.FleetSHA, state.FleetError = report.SHA, "config.yaml: "+err.Error()
		return false
	}
	endpoints, err := g.s.repo.ListEndpoints(ctx)
	if err != nil {
		state.FleetError = err.Error()
		return false
	}
	known := map[string]*types.ManagedEndpointSpec{}
	for _, e := range endpoints {
		if e.Enabled() {
			known[e.Spec.ID] = &e.Spec
		}
	}
	for id, entry := range fleet.Endpoints {
		if spec := known[id]; spec != nil && entry.OpenRouter != nil {
			if err := entry.OpenRouter.ValidateFor(spec); err != nil {
				state.FleetSHA, state.FleetError = report.SHA, fmt.Sprintf("config.yaml: %s: openrouter: %v", id, err)
				return false
			}
		}
	}
	fleet.GitSHA = report.SHA
	dropped := fleet.Prune(known)
	if err := g.s.repo.SaveFleet(ctx, fleet); err != nil {
		state.FleetError = err.Error()
		return false
	}
	g.s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{Action: "gitops.fleet", Message: report.SHA, Data: map[string]any{"fleet": fleet.Endpoints, "skipped": dropped}})
	state.FleetSHA, state.FleetError = report.SHA, ""
	if len(dropped) > 0 {
		state.FleetError = "skipped: " + strings.Join(dropped, "; ")
	}
	return true
}

// handleWebhook accepts GitHub push events (X-Hub-Signature-256) for the configured ref.
func (g *gitops) handleWebhook(ctx echo.Context) error {
	secret := strings.TrimSpace(g.s.config.Webhook.Secret)
	if secret == "" {
		return echo.NewHTTPError(http.StatusNotFound, "webhook is not configured")
	}
	body, err := io.ReadAll(io.LimitReader(ctx.Request().Body, 1<<20))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "read body")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(strings.ToLower(strings.TrimSpace(ctx.Request().Header.Get("X-Hub-Signature-256")))), []byte(want)) {
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid signature")
	}
	var push struct {
		Ref     string `json:"ref"`
		After   string `json:"after"`
		Deleted bool   `json:"deleted"`
	}
	ref := g.s.config.Repo.Branch
	if json.Unmarshal(body, &push) != nil || push.Deleted || push.Ref != "refs/heads/"+ref {
		// Pings, other refs and deletions are acknowledged and ignored.
		return ctx.JSON(http.StatusOK, map[string]any{"ok": true, "ignored": true, "ref": push.Ref})
	}
	// The poller resolves the head, so a burst of pushes collapses into one run.
	started, _ := g.Trigger("")
	return ctx.JSON(http.StatusAccepted, map[string]any{"ok": true, "started": started, "after": push.After})
}

// handleReport receives the deployer's result from the run's token or a cluster admin.
func (g *gitops) handleReport(ctx echo.Context) error {
	reqCtx := ctx.Request().Context()
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok || cc.AuthInfo == nil || cc.AuthInfo.Token == nil {
		return echo.NewHTTPError(http.StatusUnauthorized, "token required")
	}
	reqCtx = auth.ContextWithAuthInfo(reqCtx, cc.AuthInfo)
	var report types.GitOpsReport
	if err := json.NewDecoder(io.LimitReader(ctx.Request().Body, 4<<20)).Decode(&report); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		state, err := g.s.repo.GetGitOpsState(reqCtx)
		if err != nil {
			return httpError(err)
		}
		if state == nil || !state.Running || report.RunID != state.RunID || cc.AuthInfo.Token.ExternalId != state.TokenID {
			return echo.NewHTTPError(http.StatusForbidden, "not the token of the run in flight")
		}
	}
	// A busy lease is a 503 (the deployer retries); a fenced-out report a 409.
	var applyErr error
	err := g.lock.WithLease(reqCtx, gitopsLockKey, common.RedisLockOptions{TtlS: int(gitopsLockTTL.Seconds()), Retries: 10, RetryInterval: 500 * time.Millisecond}, func(ctx context.Context) error {
		applyErr = g.applyReport(ctx, &report)
		return nil
	})
	switch {
	case err != nil:
		return echo.NewHTTPError(http.StatusServiceUnavailable, err.Error())
	case applyErr != nil && strings.Contains(applyErr.Error(), "not in flight"):
		return echo.NewHTTPError(http.StatusConflict, applyErr.Error())
	case applyErr != nil:
		return echo.NewHTTPError(http.StatusServiceUnavailable, applyErr.Error())
	}
	return ctx.JSON(http.StatusOK, map[string]any{"ok": true})
}
