package vm

import (
	"bytes"
	"cmp"
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// Runners are interchangeable, as GitHub gives an idle one to any queued job
// with its label. So each queued job starts one, one that stops before taking
// a job is replaced, and an idle one is removed.
const (
	runnerPrefix   = "ghr-"
	runnerAttempts = 5
	runnerIdle     = 5 * time.Minute
	runnerLifetime = 24 * time.Hour
)

type runnerSize struct {
	Label  string `json:"label"`
	CPU    int64  `json:"cpu"`
	Memory int64  `json:"memory"`
}

var runnerSizes = []runnerSize{
	{"beam-2vcpu-ubuntu-2404", 2000, 8192},
	{"beam-4vcpu-ubuntu-2404", 4000, 16384},
	{"beam-8vcpu-ubuntu-2404", 8000, 32768},
	{"beam-16vcpu-ubuntu-2404", 16000, 65536},
	{"beam-32vcpu-ubuntu-2404", 32000, 131072},
}

// Jobs ask for exactly one size, as in runs-on: beam-4vcpu-ubuntu-2404.
func runnerSizeFor(labels []string) (runnerSize, bool) {
	for _, size := range runnerSizes {
		if len(labels) == 1 && strings.EqualFold(labels[0], size.Label) {
			return size, true
		}
	}
	return runnerSize{}, false
}

type githubRunners struct {
	s      *Service
	repo   repository.GitHubRunnerRepository
	config types.GitHubRunnerConfig
	key    *rsa.PrivateKey
	client *http.Client
	apiURL string
	webURL string
	wake   chan struct{}
	// Redeliveries by webhook GUID, used only by run.
	redelivered map[string]int

	mu     sync.Mutex
	tokens map[int64]installationToken
}

func (s *Service) initGitHubRunners(api *echo.Group) error {
	repo, ok := s.backend.(repository.GitHubRunnerRepository)
	if !ok {
		return nil
	}
	r := &githubRunners{s: s, repo: repo, config: s.config.GitHubRunners, client: &http.Client{Timeout: 20 * time.Second},
		apiURL: "https://api.github.com", webURL: "https://github.com", wake: make(chan struct{}, 1),
		redelivered: map[string]int{}, tokens: map[int64]installationToken{}}
	s.runners = r

	group := api.Group("/github-runners")
	group.POST("/webhook", r.webhook)
	group.GET("/:workspaceId", auth.WithStrictWorkspaceAuth(r.status))
	group.DELETE("/:workspaceId", auth.WithStrictWorkspaceAuth(r.disconnect))
	group.POST("/:workspaceId/connect", auth.WithStrictWorkspaceAuth(r.connect))
	group.POST("/:workspaceId/complete", auth.WithStrictWorkspaceAuth(r.complete))
	group.GET("/:workspaceId/jobs", auth.WithStrictWorkspaceAuth(r.jobs))
	if r.config.AppID == "" {
		return nil
	}

	c := r.config
	if c.AppSlug == "" || c.ClientID == "" || c.ClientSecret == "" || c.WebhookSecret == "" || c.ImageID == "" {
		return errors.New("GitHub runners need an App slug, client ID and secret, webhook secret and runner image ID")
	}
	block, _ := pem.Decode([]byte(c.PrivateKey))
	if block == nil {
		return errors.New("GitHub runner private key must be PEM encoded")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("GitHub runner private key: %w", err)
	}
	r.key = key
	go r.run(s.ctx)
	return nil
}

func (r *githubRunners) ready() error {
	if r.key == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "GitHub runners are not configured")
	}
	return nil
}

func (r *githubRunners) status(c echo.Context) error {
	ctx, info := requestContext(c)
	installation, err := r.repo.GetGitHubRunnerInstallation(ctx, info.Workspace.Id)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]any{"configured": r.key != nil, "installation": installation, "sizes": runnerSizes})
}

type runnerJobPage struct {
	Data  []types.GitHubRunnerJob `json:"data"`
	Total int                     `json:"total"`
}

func (r *githubRunners) jobs(c echo.Context) error {
	ctx, info := requestContext(c)
	limit, offset, err := pageParams(c)
	if err != nil {
		return err
	}
	jobs, total, err := r.repo.ListGitHubRunnerJobs(ctx, info.Workspace.Id, limit, offset)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, runnerJobPage{Data: jobs, Total: total})
}

func (r *githubRunners) disconnect(c echo.Context) error {
	ctx, info := requestContext(c)
	installation, err := r.repo.GetGitHubRunnerInstallation(ctx, info.Workspace.Id)
	if err == nil && installation != nil {
		err = r.repo.DeleteGitHubRunnerInstallation(ctx, installation.ID)
	}
	if err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// A connect state carries an installation through GitHub's redirects. It is
// valid for ten minutes, and only for the token that started connecting.
func (r *githubRunners) state(info *auth.AuthInfo, installation, expires int64) string {
	mac := hmac.New(sha256.New, []byte(r.config.ClientSecret))
	fmt.Fprintf(mac, "%d.%s.%d.%d", info.Workspace.Id, info.Token.ExternalId, installation, expires)
	return fmt.Sprintf("%d.%d.%x", installation, expires, mac.Sum(nil))
}

func (r *githubRunners) verify(info *auth.AuthInfo, state string) (int64, error) {
	var installation, expires int64
	_, err := fmt.Sscanf(state, "%d.%d.", &installation, &expires)
	if err != nil || !hmac.Equal([]byte(state), []byte(r.state(info, installation, expires))) || time.Now().Unix() > expires {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "GitHub connection expired; connect again")
	}
	return installation, nil
}

// Connecting takes two GitHub redirects: one to install the App, then one to
// authorize it, which identifies the user so their ownership can be checked.
func (r *githubRunners) connect(c echo.Context) error {
	if err := r.ready(); err != nil {
		return err
	}
	_, info := requestContext(c)
	var req struct {
		State          string `json:"state"`
		InstallationID int64  `json:"installation_id"`
	}
	if err := decode(c, &req); err != nil {
		return err
	}
	target := r.webURL + "/apps/" + url.PathEscape(r.config.AppSlug) + "/installations/new?"
	if req.InstallationID != 0 {
		if _, err := r.verify(info, req.State); err != nil {
			return err
		}
		target = r.webURL + "/login/oauth/authorize?client_id=" + url.QueryEscape(r.config.ClientID) + "&"
	}
	state := r.state(info, req.InstallationID, time.Now().Add(10*time.Minute).Unix())
	return c.JSON(http.StatusOK, map[string]string{"url": target + "state=" + state})
}

func (r *githubRunners) complete(c echo.Context) error {
	if err := r.ready(); err != nil {
		return err
	}
	ctx, info := requestContext(c)
	var req struct {
		Code  string `json:"code"`
		State string `json:"state"`
	}
	if err := decode(c, &req); err != nil {
		return err
	}
	installation, err := r.verify(info, req.State)
	if err != nil {
		return err
	}
	var oauth struct {
		AccessToken string `json:"access_token"`
	}
	if installation != 0 {
		_, err = r.call(ctx, http.MethodPost, r.webURL+"/login/oauth/access_token", "",
			map[string]string{"client_id": r.config.ClientID, "client_secret": r.config.ClientSecret, "code": req.Code}, &oauth)
	}
	if err != nil || oauth.AccessToken == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "GitHub authorization failed; connect again")
	}
	account, err := r.ownedAccount(ctx, installation, oauth.AccessToken)
	if err != nil {
		return err
	}

	err = r.repo.SaveGitHubRunnerInstallation(ctx, &types.GitHubRunnerInstallation{ID: installation, WorkspaceID: info.Workspace.Id,
		TokenID: info.Token.ExternalId, Account: account.Login, AccountType: account.Type})
	if errors.Is(err, repository.ErrGitHubRunnerConflict) {
		return echo.NewHTTPError(http.StatusConflict, "this workspace or GitHub account is already connected; disconnect it first")
	}
	if err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

type githubAccount struct {
	Login string `json:"login"`
	Type  string `json:"type"`
}

// ownedAccount returns the account an installation belongs to, if the user
// owns it: runners bill the workspace and run with its token, so only the user
// themselves or an organization admin may connect it.
func (r *githubRunners) ownedAccount(ctx context.Context, installation int64, userToken string) (*githubAccount, error) {
	var user githubAccount
	if _, err := r.call(ctx, http.MethodGet, r.apiURL+"/user", userToken, nil, &user); err != nil {
		return nil, err
	}
	jwt, err := r.jwt()
	if err != nil {
		return nil, err
	}
	var app struct {
		Account     githubAccount `json:"account"`
		SuspendedAt *time.Time    `json:"suspended_at"`
	}
	if _, err := r.call(ctx, http.MethodGet, fmt.Sprintf("%s/app/installations/%d", r.apiURL, installation), jwt, nil, &app); err != nil {
		return nil, err
	}
	account := app.Account
	if app.SuspendedAt != nil {
		return nil, echo.NewHTTPError(http.StatusForbidden, "the GitHub App installation is suspended")
	}

	owner := account.Type == "User" && strings.EqualFold(account.Login, user.Login)
	if account.Type == "Organization" {
		token, err := r.installationToken(ctx, installation)
		if err != nil {
			return nil, err
		}
		var membership struct {
			State string `json:"state"`
			Role  string `json:"role"`
		}
		_, err = r.call(ctx, http.MethodGet, r.apiURL+"/orgs/"+url.PathEscape(account.Login)+"/memberships/"+url.PathEscape(user.Login), token, nil, &membership)
		if err != nil && githubStatus(err) != http.StatusNotFound {
			return nil, err
		}
		owner = membership.State == "active" && membership.Role == "admin"
	}
	if !owner {
		return nil, echo.NewHTTPError(http.StatusForbidden, "only an owner of "+account.Login+" can connect it")
	}
	return &account, nil
}

func (r *githubRunners) webhook(c echo.Context) error {
	if err := r.ready(); err != nil {
		return err
	}
	body, err := io.ReadAll(io.LimitReader(c.Request().Body, 25<<20))
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, []byte(r.config.WebhookSecret))
	mac.Write(body)
	signature, _ := hex.DecodeString(strings.TrimPrefix(c.Request().Header.Get("X-Hub-Signature-256"), "sha256="))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid webhook signature")
	}

	var event struct {
		Action       string `json:"action"`
		Installation struct {
			ID int64 `json:"id"`
		} `json:"installation"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
		Job struct {
			types.GitHubRunnerJob
			Labels []string `json:"labels"`
		} `json:"workflow_job"`
	}
	if err := json.Unmarshal(body, &event); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid webhook payload")
	}
	ctx := c.Request().Context()
	switch c.Request().Header.Get("X-GitHub-Event") {
	case "installation":
		if event.Action == "deleted" || event.Action == "suspend" {
			if err := r.repo.DeleteGitHubRunnerInstallation(ctx, event.Installation.ID); err != nil {
				return err
			}
			r.mu.Lock()
			delete(r.tokens, event.Installation.ID)
			r.mu.Unlock()
		}
	case "workflow_job":
		size, ok := runnerSizeFor(event.Job.Labels)
		if !ok || !slices.Contains([]string{"queued", "in_progress", "completed"}, event.Action) {
			break
		}
		installation, err := r.repo.GetGitHubRunnerInstallationByID(ctx, event.Installation.ID)
		if err != nil {
			return err
		}
		if installation == nil {
			break
		}
		job := event.Job.GitHubRunnerJob
		job.InstallationID, job.WorkspaceID, job.Repository = installation.ID, installation.WorkspaceID, event.Repository.FullName
		job.Label, job.Status = size.Label, event.Action
		if err := r.repo.SaveGitHubRunnerJob(ctx, &job); err != nil {
			return err
		}
		if job.Status == "queued" {
			select {
			case r.wake <- struct{}{}:
			default:
			}
		}
	}
	return c.NoContent(http.StatusAccepted)
}

func (r *githubRunners) run(ctx context.Context) {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	// The first sweep looks back over any downtime.
	swept, lookback := time.Time{}, time.Hour
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-r.wake:
		}

		jobs, err := r.repo.ListDueGitHubRunnerJobs(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("list GitHub runner jobs")
		}
		var provisions sync.WaitGroup
		slots := make(chan struct{}, 16)
		for _, job := range jobs {
			slots <- struct{}{}
			provisions.Go(func() {
				defer func() { <-slots }()
				// Shorter than a claim's first retry, so that two gateways
				// never provision one job at once.
				op, cancel := context.WithTimeout(ctx, 45*time.Second)
				defer cancel()
				if err := r.provision(op, job); err != nil {
					log.Warn().Err(err).Int64("github_job_id", job.ID).Msg("provision GitHub runner")
				}
			})
		}
		provisions.Wait()

		if time.Since(swept) >= time.Minute {
			swept = time.Now()
			if err := r.redeliver(ctx, swept.Add(-lookback)); err != nil {
				log.Warn().Err(err).Msg("redeliver GitHub webhooks")
			} else {
				lookback = 5 * time.Minute
			}
		}
	}
}

func (r *githubRunners) provision(ctx context.Context, job types.GitHubRunnerJob) error {
	if job.VM != "" {
		// The last attempt may have created its runner before it could finish.
		v, err := r.s.repo.GetVM(ctx, job.WorkspaceID, job.VM)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && v.DesiredState != types.VMStateDeleted {
			return r.repo.FinishGitHubRunnerJob(ctx, job.ID, false, "")
		}
	}
	// A job cancelled before taking a runner needs none. One that took another
	// job's runner still starts one, for that job.
	if job.Status == "completed" && job.RunnerName == "" {
		return r.repo.FinishGitHubRunnerJob(ctx, job.ID, false, "")
	}
	if job.Attempts >= runnerAttempts {
		return r.repo.FinishGitHubRunnerJob(ctx, job.ID, false, fmt.Sprintf("no runner after %d attempts: %s", job.Attempts, job.Error))
	}
	if claimed, err := r.repo.ClaimGitHubRunnerJob(ctx, &job, runnerPrefix+randomHexID()[:12]); err != nil || !claimed {
		return err
	}
	if err := r.launch(ctx, job); err != nil {
		return errors.Join(err, r.repo.FinishGitHubRunnerJob(ctx, job.ID, true, err.Error()))
	}
	return r.repo.FinishGitHubRunnerJob(ctx, job.ID, false, "")
}

func (r *githubRunners) launch(ctx context.Context, job types.GitHubRunnerJob) error {
	installation, err := r.repo.GetGitHubRunnerInstallationByID(ctx, job.InstallationID)
	if err != nil || installation == nil {
		return cmp.Or(err, errors.New("GitHub account is disconnected"))
	}
	workspace, err := r.s.backend.GetWorkspace(ctx, job.WorkspaceID)
	if err != nil {
		return err
	}
	token, err := r.s.backend.GetTokenByExternalId(ctx, workspace.Id, installation.TokenID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if token == nil || !token.Active || token.DisabledByClusterAdmin {
		return errors.New("the Beam token that connected GitHub is inactive; connect again")
	}
	github, err := r.installationToken(ctx, installation.ID)
	if err != nil {
		return fmt.Errorf("authenticate with GitHub: %w", err)
	}

	var jit struct {
		Config string `json:"encoded_jit_config"`
	}
	_, err = r.call(ctx, http.MethodPost, r.apiURL+"/repos/"+job.Repository+"/actions/runners/generate-jitconfig", github,
		map[string]any{"name": job.VM, "runner_group_id": 1, "labels": []string{job.Label}, "work_folder": "_work"}, &jit)
	if err != nil {
		return fmt.Errorf("register runner: %w", err)
	}
	size, _ := runnerSizeFor([]string{job.Label})
	info := &auth.AuthInfo{Workspace: workspace, Token: token}
	_, err = r.s.createVM(auth.ContextWithAuthInfo(ctx, info), info, types.VMCreateOptions{
		Name: job.VM,
		Spec: types.VMSpec{ImageID: r.config.ImageID, CPU: size.CPU, Memory: size.Memory, DiskSize: "50Gi",
			DockerEnabled: true, Env: []string{"BEAM_GITHUB_JIT_CONFIG=" + jit.Config}},
		Metadata: map[string]string{"github.repository": job.Repository},
	})
	return err
}

// reconcileRunner deletes a runner VM once it is done, as runners take one job
// and never restart. It reports whether v is a runner.
func (s *Service) reconcileRunner(ctx context.Context, v *types.VM) (bool, error) {
	r := s.runners
	if r == nil || v.DesiredState == types.VMStateDeleted || !strings.HasPrefix(v.Name, runnerPrefix) {
		return false, nil
	}
	runner, err := r.repo.GetGitHubRunner(ctx, v.WorkspaceID, v.Name)
	if err != nil || runner == nil {
		return false, err
	}
	state, err := s.runtimeState(v)
	if err != nil {
		return true, err
	}

	exited := state == nil && v.ContainerID != "" || v.Status == types.VMStateError && v.LaunchAttempts >= 5
	if exited && runner.JobStatus == "" {
		err := r.repo.ReleaseGitHubRunner(ctx, v.WorkspaceID, v.Name, cmp.Or(v.Error, "runner stopped before taking a job"))
		if err == nil {
			v.DesiredState = types.VMStateDeleted
		}
		return true, err
	}

	done := exited || r.key == nil || !runner.Connected || v.DesiredState != types.VMStateRunning ||
		runner.JobStatus == "completed" || time.Since(v.CreatedAt) > runnerLifetime
	// Nothing else saves a running runner, so UpdatedAt is when it started or
	// was last found busy.
	if !done && runner.JobStatus == "" && v.Status == types.VMStateRunning && time.Since(v.UpdatedAt) > runnerIdle {
		removed, err := r.deregister(ctx, runner, v.Name)
		if err == nil && !removed {
			err = s.repo.SaveVM(ctx, v)
		}
		if err != nil || !removed {
			return true, err
		}
		done = true
	}
	if !done {
		return true, nil
	}
	v.DesiredState = types.VMStateDeleted
	return true, s.repo.SaveVM(ctx, v)
}

// deregister removes an idle runner from GitHub. It reports false if GitHub
// has meanwhile given the runner a job, as GitHub then refuses.
func (r *githubRunners) deregister(ctx context.Context, runner *types.GitHubRunner, name string) (bool, error) {
	token, err := r.installationToken(ctx, runner.InstallationID)
	if err != nil {
		return false, err
	}
	endpoint := r.apiURL + "/repos/" + runner.Repository + "/actions/runners"
	var registered struct {
		Runners []struct {
			ID int64 `json:"id"`
		} `json:"runners"`
	}
	_, err = r.call(ctx, http.MethodGet, endpoint+"?name="+url.QueryEscape(name), token, nil, &registered)
	for _, registration := range registered.Runners {
		if _, err = r.call(ctx, http.MethodDelete, fmt.Sprintf("%s/%d", endpoint, registration.ID), token, nil, nil); err != nil {
			break
		}
	}
	switch githubStatus(err) {
	case http.StatusUnprocessableEntity:
		return false, nil
	case http.StatusNotFound:
		return true, nil
	}
	return err == nil, err
}

// GitHub doesn't retry failed webhook deliveries, so a gateway restart could
// strand a queued job. Redeliver each event whose latest delivery failed, a few
// times at most. GitHub's failure filter misses timeouts, which have no status
// code, so every delivery is read.
func (r *githubRunners) redeliver(ctx context.Context, since time.Time) error {
	jwt, err := r.jwt()
	if err != nil {
		return err
	}
	latest := map[string]bool{}
	for next := r.apiURL + "/app/hook/deliveries?per_page=100"; next != ""; {
		var deliveries []struct {
			ID          int64     `json:"id"`
			GUID        string    `json:"guid"`
			DeliveredAt time.Time `json:"delivered_at"`
			StatusCode  int       `json:"status_code"`
		}
		header, err := r.call(ctx, http.MethodGet, next, jwt, nil, &deliveries)
		if err != nil {
			return err
		}
		next = nextPage(header.Get("Link"))
		for _, d := range deliveries {
			if d.DeliveredAt.Before(since) {
				next = ""
				break
			}
			// Deliveries are newest first.
			seen := latest[d.GUID]
			latest[d.GUID] = true
			if seen || d.StatusCode >= 200 && d.StatusCode < 400 || r.redelivered[d.GUID] >= runnerAttempts {
				continue
			}
			r.redelivered[d.GUID]++
			if _, err := r.call(ctx, http.MethodPost, fmt.Sprintf("%s/app/hook/deliveries/%d/attempts", r.apiURL, d.ID), jwt, nil, nil); err != nil {
				return err
			}
		}
	}
	maps.DeleteFunc(r.redelivered, func(guid string, _ int) bool { return !latest[guid] })
	return nil
}

func nextPage(link string) string {
	for _, part := range strings.Split(link, ",") {
		if target, ok := strings.CutSuffix(strings.TrimSpace(part), `>; rel="next"`); ok {
			return strings.TrimPrefix(target, "<")
		}
	}
	return ""
}

func (r *githubRunners) jwt() (string, error) {
	now := time.Now()
	claims, _ := json.Marshal(map[string]any{"iss": r.config.AppID, "iat": now.Add(-time.Minute).Unix(), "exp": now.Add(9 * time.Minute).Unix()})
	unsigned := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + base64.RawURLEncoding.EncodeToString(claims)
	hash := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, r.key, crypto.SHA256, hash[:])
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), err
}

type installationToken struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (r *githubRunners) installationToken(ctx context.Context, id int64) (string, error) {
	r.mu.Lock()
	cached := r.tokens[id]
	r.mu.Unlock()
	if time.Until(cached.ExpiresAt) > 5*time.Minute {
		return cached.Token, nil
	}
	jwt, err := r.jwt()
	if err != nil {
		return "", err
	}
	var token installationToken
	if _, err := r.call(ctx, http.MethodPost, fmt.Sprintf("%s/app/installations/%d/access_tokens", r.apiURL, id), jwt, nil, &token); err != nil {
		return "", err
	}
	r.mu.Lock()
	r.tokens[id] = token
	r.mu.Unlock()
	return token.Token, nil
}

type githubError struct{ Status int }

func (e *githubError) Error() string { return fmt.Sprintf("GitHub API returned HTTP %d", e.Status) }

func githubStatus(err error) int {
	var e *githubError
	if errors.As(err, &e) {
		return e.Status
	}
	return 0
}

func (r *githubRunners) call(ctx context.Context, method, endpoint, token string, input, output any) (http.Header, error) {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, &githubError{resp.StatusCode}
	}
	if output != nil {
		err = json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(output)
	}
	return resp.Header, err
}
