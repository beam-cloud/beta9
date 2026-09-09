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
	"gopkg.in/yaml.v2"

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
	gitopsRunTimeout = 45 * time.Minute
	// gitopsRetryBackoff spaces out runs that only retry failed endpoints at
	// an unchanged commit.
	gitopsRetryBackoff   = 15 * time.Minute
	gitopsStubName       = "managed-endpoints-deployer"
	gitopsContainerPfx   = "me-deployer"
	gitopsDeployerCPU    = int64(2000)
	gitopsDeployerMemory = int64(4096)
	gitopsResolveTimeout = 30 * time.Second
)

var (
	shaPattern     = regexp.MustCompile(`^[0-9a-f]{7,64}$`)
	fullSHAPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

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
		// The lease is renewed for as long as sync runs (resolving the remote
		// head alone can take a while) and sync is cancelled if it is lost.
		err := g.lock.WithLease(ctx, gitopsLockKey, common.RedisLockOptions{TtlS: int(gitopsLockTTL.Seconds()), Retries: 0}, func(ctx context.Context) error {
			return g.sync(ctx, req)
		})
		if err != nil && !common.IsRedisLockNotObtained(err) {
			log.Error().Err(err).Msg("managed endpoints: gitops sync failed")
		}
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
	retry, run := needsRun(state, sha, req.force, time.Now())
	if !run {
		return nil
	}
	return g.launch(ctx, state, sha, req.force, retry)
}

// needsRun decides whether a deployer should be launched for sha and which
// failed endpoints it should redeploy. A new commit or a forced trigger runs
// immediately. When nothing changed and the run would only retry failures
// (broken apps, a failed retirement or fleet write), it waits
// gitopsRetryBackoff since the last run: those failures rarely fix themselves
// between polls (a missing image, a bad app.py) and rerunning them every poll
// only rebuilds and re-fails.
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

// resolveHead runs `git ls-remote` for the configured ref. A ref that is
// already a full commit id is returned as-is.
func (g *gitops) resolveHead(ctx context.Context) (string, error) {
	ref := strings.TrimSpace(g.s.config.Repo.Ref)
	if pinned := strings.ToLower(ref); fullSHAPattern.MatchString(pinned) {
		return pinned, nil
	}
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

	// The peeled `<tag>^{}` line is only printed when a pattern matches it, so
	// ask for it explicitly; otherwise an annotated tag yields its tag object.
	cmd := exec.CommandContext(ctx, "git", "ls-remote", "--heads", "--tags", url,
		ref, ref+"^{}", "refs/heads/"+ref, "refs/tags/"+ref, "refs/tags/"+ref+"^{}")
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git ls-remote: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	sha, ok := pickRemoteSHA(stdout.String(), ref)
	if !ok {
		return "", fmt.Errorf("ref %q not found on %s", ref, g.s.config.Repo.URL)
	}
	return sha, nil
}

// pickRemoteSHA chooses the commit id for ref from `git ls-remote` output. A
// branch wins over a same-named tag. For tags the peeled `refs/tags/<ref>^{}`
// line (the commit an annotated tag points at) is preferred; the un-peeled
// line is only used when no peeled line exists, i.e. for lightweight tags.
// ref may be a short name or a fully qualified refs/heads/… or refs/tags/…
// name, in which case only that namespace is considered.
func pickRemoteSHA(output, ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	wantHeads, wantTags := true, true
	switch {
	case strings.HasPrefix(ref, "refs/heads/"):
		ref, wantTags = strings.TrimPrefix(ref, "refs/heads/"), false
	case strings.HasPrefix(ref, "refs/tags/"):
		ref, wantHeads = strings.TrimPrefix(ref, "refs/tags/"), false
	}
	if ref == "" {
		return "", false
	}
	var branch, peeledTag, tag string
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || !shaPattern.MatchString(fields[0]) {
			continue
		}
		sha, name := fields[0], fields[1]
		switch {
		case wantHeads && name == "refs/heads/"+ref:
			branch = sha
		case wantTags && name == "refs/tags/"+ref+"^{}":
			peeledTag = sha
		case wantTags && name == "refs/tags/"+ref:
			tag = sha
		}
	}
	for _, sha := range []string{branch, peeledTag, tag} {
		if sha != "" {
			return sha, true
		}
	}
	return "", false
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
// per-endpoint status, advances LastSHA, retires endpoints whose directories
// disappeared and applies fleet.yaml. It runs under the gitops lease and is
// fenced by the run id, so a stale or duplicate report cannot touch a newer
// run. The state is saved (the run durably accepted) before the run's token
// and container are cleaned up: if the save fails the deployer's token is
// still valid and its retry re-applies the same report idempotently.
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

	// fleet.yaml is applied against the endpoints that exist after this run.
	// An invalid fleet is not applied (the previous placement stays in force,
	// the error is surfaced) and there is nothing to retry at this commit; a
	// failed write leaves FleetSHA behind so sync relaunches at the same SHA.
	skipped, err := g.applyFleet(ctx, report)
	state.FleetError = skipped
	var invalid *fleetInvalidError
	switch {
	case err == nil:
		state.FleetSHA = report.SHA
	case errors.As(err, &invalid):
		state.FleetSHA, state.FleetError = report.SHA, err.Error()
		failed++
	default:
		state.FleetError = err.Error()
		failed++
	}

	// LastSHA advances even when some stubs failed: they are tracked per
	// endpoint and retried on their own, so a broken app never redeploys the
	// healthy ones.
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

// applyResults records each app's outcome and retires endpoints that
// disappeared from the repo. It returns how many failed.
func (g *gitops) applyResults(ctx context.Context, state *types.GitOpsState, report *types.GitOpsReport, now time.Time) int {
	failed := 0
	// importErrors holds directories whose app.py failed to import (no ID is
	// known for them), keyed by path.
	importErrors := map[string]string{}
	for _, r := range report.Results {
		if r.ID == "" {
			importErrors[r.Path] = r.Error
			failed++
			continue
		}
		key := r.ID
		entry := state.PerEndpoint[key]
		entry.Path, entry.ID, entry.UpdatedAt = r.Path, r.ID, now
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

	// Anything previously applied that is no longer discovered is retired,
	// unless its directory is still there but failed to import: that stub keeps
	// its last applied state and is flagged failed, so a bad commit never
	// tears down what the previous commit deployed.
	present := map[string]bool{}
	for _, r := range report.Results {
		if r.ID != "" {
			present[r.ID] = true
		}
	}
	claimed := map[string]bool{} // import failures attributed to a known stub
	for key, entry := range state.PerEndpoint {
		if strings.HasPrefix(key, "path:") || present[key] || entry.Status == types.GitOpsStatusRetired {
			continue
		}
		if msg, broken := importErrors[entry.Path]; broken {
			claimed[entry.Path] = true
			entry.Status, entry.Error, entry.UpdatedAt = types.GitOpsStatusFailed, msg, now
			state.PerEndpoint[key] = entry
			continue
		}
		if err := g.retire(ctx, entry.ID, report.SHA); err != nil {
			// Flagged failed with its path, so the next sync relaunches and,
			// finding the directory still gone, retries the retirement.
			log.Warn().Err(err).Str("id", entry.ID).Msg("managed endpoints: gitops retire failed")
			entry.Status, entry.Error, entry.UpdatedAt = types.GitOpsStatusFailed, "retire: "+err.Error(), now
			state.PerEndpoint[key] = entry
			failed++
			continue
		}
		entry.Status, entry.Error, entry.UpdatedAt = types.GitOpsStatusRetired, "", now
		state.PerEndpoint[key] = entry
	}
	// Import failures in directories with no known stub are keyed by path so
	// they stay visible; stale path entries from earlier runs are dropped.
	for key, entry := range state.PerEndpoint {
		if _, broken := importErrors[entry.Path]; strings.HasPrefix(key, "path:") && (!broken || claimed[entry.Path]) {
			delete(state.PerEndpoint, key)
		}
	}
	for path, msg := range importErrors {
		if !claimed[path] {
			state.PerEndpoint["path:"+path] = types.GitOpsEndpointState{Path: path, Status: types.GitOpsStatusFailed, Error: msg, UpdatedAt: now}
		}
	}
	return failed
}

// retire disables an endpoint removed from the repo. Replicas are drained
// by the controller; usage history is kept.
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

// fleetInvalidError is a fleet.yaml that cannot be applied at this commit.
type fleetInvalidError struct{ err error }

func (e *fleetInvalidError) Error() string { return "fleet.yaml: " + e.err.Error() }
func (e *fleetInvalidError) Unwrap() error { return e.err }

// applyFleet parses and validates the report's fleet.yaml, drops entries for
// endpoints that are not deployed (reported as skipped, so a failed deploy
// never blocks the rest of the fleet) and saves the result. A fleet that fails
// validation is not applied: the previous one stays in force.
func (g *gitops) applyFleet(ctx context.Context, report *types.GitOpsReport) (skipped string, err error) {
	endpoints, err := g.s.repo.ListEndpoints(ctx)
	if err != nil {
		return "", err
	}
	known := map[string]*types.ManagedEndpointSpec{}
	for _, e := range endpoints {
		if e.Enabled() {
			known[e.Spec.ID] = &e.Spec
		}
	}
	fleet := &types.Fleet{GitSHA: report.SHA}
	if err := yaml.Unmarshal([]byte(report.FleetYAML), &fleet.Replicas); err != nil {
		return "", &fleetInvalidError{err}
	}
	fleet.Normalize()
	if err := fleet.Validate(); err != nil {
		return "", &fleetInvalidError{err}
	}
	dropped := fleet.Prune(known)
	if err := g.s.repo.SaveFleet(ctx, fleet); err != nil {
		return "", err
	}
	g.s.emit(types.EventEndpointGitOps, types.EventEndpointSchema{Action: "gitops.fleet", Message: report.SHA, Data: map[string]any{"fleet": fleet.Replicas, "skipped": dropped}})
	if len(dropped) > 0 {
		return "skipped: " + strings.Join(dropped, "; "), nil
	}
	return "", nil
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

// handleReport receives the deployer's result. Only the token minted for the
// run in flight (or a cluster admin) may report on it.
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
	// Report application shares the sync lease so the poller cannot fail or
	// relaunch the run while its report is being applied. A busy lease is a
	// 503, which the deployer retries; a fenced-out report is a 409, which it
	// does not.
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
