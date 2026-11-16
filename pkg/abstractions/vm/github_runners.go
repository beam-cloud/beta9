package vm

import (
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
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

// A workspace connects a GitHub account, and a job of that account which asks
// for a Beam runner gets a VM that runs it and is then deleted.
//
// GitHub gives a job to any idle runner with the job's label, so runners are
// interchangeable, and only their number matters: every job starts one, a
// runner that stops before it takes a job is replaced, and a runner that no
// job takes is removed.
const (
	runnerPrefix      = "ghr-"
	runnerLabelPrefix = "beam-"
	runnerDisk        = "50Gi"
	runnerAttempts    = 5
	runnerIdle        = 5 * time.Minute
	runnerLifetime    = 24 * time.Hour
	runnerConnectTime = 10 * time.Minute
)

// runnerSize is a size of runner, which a job asks for by its label, as in
// runs-on: beam-4vcpu-ubuntu-2404.
type runnerSize struct {
	Label    string `json:"label"`
	CPU      int64  `json:"cpu"`       // millicores
	Memory   int64  `json:"memory"`    // MiB
	DiskSize string `json:"disk_size"` // as in a VM's spec
}

var runnerSizes = []runnerSize{
	{"beam-2vcpu-ubuntu-2404", 2000, 8192, runnerDisk},
	{"beam-4vcpu-ubuntu-2404", 4000, 16384, runnerDisk},
	{"beam-8vcpu-ubuntu-2404", 8000, 32768, runnerDisk},
	{"beam-16vcpu-ubuntu-2404", 16000, 65536, runnerDisk},
	{"beam-32vcpu-ubuntu-2404", 32000, 131072, runnerDisk},
}

func runnerSizeFor(label string) (runnerSize, bool) {
	for _, size := range runnerSizes {
		if size.Label == label {
			return size, true
		}
	}

	return runnerSize{}, false
}

// runnerLabel is what the runs-on of a job that asks for a Beam runner says,
// in lower case, as GitHub ignores the case of labels. A job gets a runner
// only when that is the label of one size and nothing else, as a runner has
// no other label. Any other job stays queued on GitHub, so it is recorded as
// well, to say why.
func runnerLabel(labels []string) (string, bool) {
	asksForBeam := slices.ContainsFunc(labels, func(label string) bool {
		return strings.HasPrefix(strings.ToLower(label), runnerLabelPrefix)
	})
	return strings.ToLower(strings.Join(labels, ", ")), asksForBeam
}

type githubRunners struct {
	s      *Service
	config types.GitHubRunnerConfig
	// Nil unless the gateway is configured with a GitHub App.
	github *githubApp
	wake   chan struct{}
	// How often each failed webhook was redelivered, by GUID. Only run uses it.
	redelivered map[string]int
}

func (s *Service) initGitHubRunners(api *echo.Group) error {
	r := &githubRunners{s: s, config: s.config.GitHubRunners, wake: make(chan struct{}, 1), redelivered: map[string]int{}}
	s.runners = r

	group := api.Group("/github-runners")
	group.POST("/webhook", r.webhook)
	group.GET("/:workspaceId", auth.WithStrictWorkspaceAuth(r.status))
	group.DELETE("/:workspaceId", auth.WithStrictWorkspaceAuth(r.disconnect))
	group.POST("/:workspaceId/connect", auth.WithStrictWorkspaceAuth(r.connect))
	group.GET("/:workspaceId/jobs", auth.WithStrictWorkspaceAuth(r.jobs))
	if r.config.AppID == "" {
		return nil
	}

	const section = "abstractions.vm.githubRunners"
	var missing []string
	for _, setting := range []struct{ key, value string }{
		{"appSlug", r.config.AppSlug}, {"clientID", r.config.ClientID}, {"clientSecret", r.config.ClientSecret},
		{"privateKey", r.config.PrivateKey}, {"webhookSecret", r.config.WebhookSecret}, {"imageID", r.config.ImageID},
	} {
		if setting.value == "" {
			missing = append(missing, setting.key)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s has an appID but no %s", section, strings.Join(missing, ", "))
	}

	app, err := newGitHubApp(r.config)
	if err != nil {
		return fmt.Errorf("%s.%w", section, err)
	}

	r.github = app
	go r.run(s.ctx)
	return nil
}

func (r *githubRunners) ready() error {
	if r.github == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "GitHub runners are not configured")
	}

	return nil
}

type runnerStatus struct {
	// Whether the gateway has a GitHub App for workspaces to connect with.
	Enabled      bool                            `json:"enabled"`
	Installation *types.GitHubRunnerInstallation `json:"installation"`
	Sizes        []runnerSize                    `json:"sizes"`
}

func (r *githubRunners) status(c echo.Context) error {
	ctx, info := requestContext(c)
	installation, err := r.s.repo.GetGitHubRunnerInstallation(ctx, info.Workspace.Id)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, runnerStatus{Enabled: r.github != nil, Installation: installation, Sizes: runnerSizes})
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

	jobs, total, err := r.s.repo.ListGitHubRunnerJobs(ctx, info.Workspace.Id, limit, offset)
	if err != nil {
		return err
	}

	// A job's error says why it has no runner. Once it runs on one, what is
	// left is about an earlier attempt, or about replacing the runner it took.
	for i := range jobs {
		if jobs[i].RunnerName != "" {
			jobs[i].Error = ""
		}
	}

	return c.JSON(http.StatusOK, runnerJobPage{Data: jobs, Total: total})
}

// disconnect stops running the account's jobs. Its runners are deleted as
// they are next reconciled; the App stays installed on GitHub.
func (r *githubRunners) disconnect(c echo.Context) error {
	ctx, info := requestContext(c)
	installation, err := r.s.repo.GetGitHubRunnerInstallation(ctx, info.Workspace.Id)
	if err != nil {
		return err
	}

	if installation != nil {
		if err := r.s.repo.DeleteGitHubRunnerInstallation(ctx, installation.ID); err != nil {
			return err
		}
	}

	return c.NoContent(http.StatusNoContent)
}

// runnerConnection is a step in connecting a GitHub account: where the user
// goes next, or that the account is connected.
type runnerConnection struct {
	URL       string `json:"url,omitempty"`
	Connected bool   `json:"connected"`
}

// connect takes a workspace through connecting a GitHub account, which needs
// two trips to GitHub: one to install the App on the account, and one to
// authorize it, which says who the user is, so that their ownership of the
// account can be checked. The dashboard posts the query parameters each trip
// comes back with, and sends the user to the URL it is given, until it is
// told that the account is connected.
func (r *githubRunners) connect(c echo.Context) error {
	if err := r.ready(); err != nil {
		return err
	}

	ctx, info := requestContext(c)
	var req struct {
		State          string `json:"state"`
		InstallationID int64  `json:"installation_id"`
		Code           string `json:"code"`
	}
	if err := decode(c, &req); err != nil {
		return err
	}

	expires := time.Now().Add(runnerConnectTime).Unix()
	if req.State == "" {
		return c.JSON(http.StatusOK, runnerConnection{URL: r.github.installURL(r.state(info, 0, expires))})
	}

	installation, err := r.verify(info, req.State)
	if err != nil {
		return err
	}

	// Back from installing the App: the state does not name an installation.
	if installation == 0 {
		if req.InstallationID == 0 {
			return echo.NewHTTPError(http.StatusBadRequest, "GitHub did not say what was installed; connect again")
		}

		return c.JSON(http.StatusOK, runnerConnection{URL: r.github.authorizeURL(r.state(info, req.InstallationID, expires))})
	}

	// Back from authorizing it.
	login, err := r.github.authorizedUser(ctx, req.Code)
	if err != nil || login == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "GitHub authorization failed; connect again")
	}

	account, err := r.ownedAccount(ctx, installation, login)
	if err != nil {
		return err
	}

	err = r.s.repo.SaveGitHubRunnerInstallation(ctx, &types.GitHubRunnerInstallation{
		ID: installation, WorkspaceID: info.Workspace.Id, TokenID: info.Token.ExternalId,
		Account: account.Login, AccountType: account.Type,
	})
	if errors.Is(err, repository.ErrGitHubRunnerConflict) {
		return echo.NewHTTPError(http.StatusConflict, "this workspace or GitHub account is already connected; disconnect it first")
	}
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, runnerConnection{Connected: true})
}

// A connect state carries the installation being connected through GitHub's
// redirects. It is valid for a few minutes, and only for the token that
// started connecting.
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

// ownedAccount returns the account an installation is on, if the user owns
// it. A workspace pays for the jobs of the account it connects, and its
// runners see their code and secrets, so only the user themselves or an owner
// of the organization may connect one.
func (r *githubRunners) ownedAccount(ctx context.Context, id int64, login string) (*githubAccount, error) {
	installation, err := r.github.installation(ctx, id)
	if githubStatus(err) == http.StatusNotFound {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "the GitHub App is no longer installed; connect again")
	}
	if err != nil {
		return nil, err
	}

	if installation.SuspendedAt != nil {
		return nil, echo.NewHTTPError(http.StatusForbidden, "the GitHub App installation is suspended")
	}

	account := installation.Account
	owner := account.Type == "User" && strings.EqualFold(account.Login, login)
	if account.Type == "Organization" {
		role, err := r.github.organizationRole(ctx, id, account.Login, login)
		if err != nil {
			return nil, err
		}

		owner = role == "admin"
	}

	if !owner {
		return nil, echo.NewHTTPError(http.StatusForbidden, "only an owner of "+account.Login+" can connect it")
	}

	return &account, nil
}

// runnerEvent is a webhook delivery, as far as runners read one.
type runnerEvent struct {
	Action       string `json:"action"`
	Installation struct {
		ID int64 `json:"id"`
	} `json:"installation"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Job struct {
		ID          int64      `json:"id"`
		Name        string     `json:"name"`
		Workflow    string     `json:"workflow_name"`
		URL         string     `json:"html_url"`
		Labels      []string   `json:"labels"`
		Conclusion  string     `json:"conclusion"`
		RunnerName  string     `json:"runner_name"`
		CreatedAt   time.Time  `json:"created_at"`
		StartedAt   *time.Time `json:"started_at"`
		CompletedAt *time.Time `json:"completed_at"`
	} `json:"workflow_job"`
}

func (r *githubRunners) webhook(c echo.Context) error {
	if err := r.ready(); err != nil {
		return err
	}

	// GitHub sends at most 25 MB.
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

	var event runnerEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid webhook payload")
	}

	ctx := c.Request().Context()
	switch c.Request().Header.Get("X-GitHub-Event") {
	case "installation":
		err = r.installationChanged(ctx, event)
	case "workflow_job":
		err = r.jobChanged(ctx, event)
	}
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusAccepted)
}

// installationChanged disconnects an account that removed or suspended the
// App, which can no longer register runners for it.
func (r *githubRunners) installationChanged(ctx context.Context, event runnerEvent) error {
	if event.Action != "deleted" && event.Action != "suspend" {
		return nil
	}

	if err := r.s.repo.DeleteGitHubRunnerInstallation(ctx, event.Installation.ID); err != nil {
		return err
	}

	r.github.forget(event.Installation.ID)
	return nil
}

// jobChanged records a job of a connected account that asks for a Beam
// runner, which makes it due for one when it is new.
func (r *githubRunners) jobChanged(ctx context.Context, event runnerEvent) error {
	label, asksForBeam := runnerLabel(event.Job.Labels)
	status := event.Action
	if !asksForBeam || !slices.Contains([]string{types.GitHubJobQueued, types.GitHubJobInProgress, types.GitHubJobCompleted}, status) {
		return nil
	}

	installation, err := r.s.repo.GetGitHubRunnerInstallationById(ctx, event.Installation.ID)
	if err != nil || installation == nil {
		return err
	}

	err = r.s.repo.SaveGitHubRunnerJob(ctx, &types.GitHubRunnerJob{
		ID:             event.Job.ID,
		InstallationID: installation.ID,
		WorkspaceID:    installation.WorkspaceID,
		Repository:     event.Repository.FullName,
		Workflow:       event.Job.Workflow,
		Name:           event.Job.Name,
		URL:            event.Job.URL,
		Label:          label,
		Status:         status,
		Conclusion:     event.Job.Conclusion,
		RunnerName:     event.Job.RunnerName,
		CreatedAt:      event.Job.CreatedAt,
		StartedAt:      event.Job.StartedAt,
		CompletedAt:    event.Job.CompletedAt,
	})
	if err != nil {
		return err
	}

	if status == types.GitHubJobQueued {
		select {
		case r.wake <- struct{}{}:
		default:
		}
	}

	return nil
}

// run starts the runners that jobs are due, as jobs are queued and every few
// seconds, and has GitHub redeliver the webhooks that the gateway missed.
func (r *githubRunners) run(ctx context.Context) {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

	// The first sweep looks back over the time the gateway may have been down.
	swept, lookback := time.Time{}, time.Hour
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-r.wake:
		}

		r.provisionDue(ctx)
		if time.Since(swept) < time.Minute {
			continue
		}

		swept = time.Now()
		if err := r.redeliver(ctx, swept.Add(-lookback)); err != nil {
			log.Warn().Err(err).Msg("redeliver GitHub webhooks")
			continue
		}

		lookback = 5 * time.Minute
	}
}

func (r *githubRunners) provisionDue(ctx context.Context) {
	jobs, err := r.s.repo.ListDueGitHubRunnerJobs(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("list GitHub runner jobs")
		return
	}

	var provisions sync.WaitGroup
	slots := make(chan struct{}, 16)
	for _, job := range jobs {
		slots <- struct{}{}
		provisions.Go(func() {
			defer func() { <-slots }()

			// Shorter than the first retry of a claim, so that two gateways
			// never start a runner for one job at once.
			op, cancel := context.WithTimeout(ctx, 45*time.Second)
			defer cancel()
			if err := r.provision(op, job); err != nil {
				log.Warn().Err(err).Int64("github_job_id", job.ID).Msg("provision GitHub runner")
			}
		})
	}

	provisions.Wait()
}

// provision starts the runner that a due job needs, if it needs one.
func (r *githubRunners) provision(ctx context.Context, job types.GitHubRunnerJob) error {
	if job.VM != "" {
		// The last attempt may have started its runner without finishing.
		v, err := r.s.repo.GetVM(ctx, job.WorkspaceID, job.VM)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		if err == nil && v.DesiredState != types.VMStateDeleted {
			return r.s.repo.FinishGitHubRunnerJob(ctx, job.ID, "")
		}
	}

	size, ok := runnerSizeFor(job.Label)
	switch {
	case !ok:
		labels := make([]string, len(runnerSizes))
		for i, size := range runnerSizes {
			labels[i] = size.Label
		}

		return r.s.repo.FinishGitHubRunnerJob(ctx, job.ID, "runs-on must be exactly one of "+strings.Join(labels, ", "))
	case job.Status != types.GitHubJobQueued && !strings.HasPrefix(job.RunnerName, runnerPrefix):
		// A job that is not waiting for a runner needs one only if it took
		// one, which another job is then left without.
		return r.s.repo.FinishGitHubRunnerJob(ctx, job.ID, "")
	case job.Attempts >= runnerAttempts:
		return r.s.repo.FinishGitHubRunnerJob(ctx, job.ID, fmt.Sprintf("no runner after %d attempts: %s", job.Attempts, job.Error))
	}

	claimed, err := r.s.repo.ClaimGitHubRunnerJob(ctx, &job, runnerPrefix+randomHexID()[:12])
	if err != nil || !claimed {
		return err
	}

	if err := r.launch(ctx, job, size); err != nil {
		return errors.Join(err, r.s.repo.FailGitHubRunnerJob(ctx, job.ID, err.Error()))
	}

	return r.s.repo.FinishGitHubRunnerJob(ctx, job.ID, "")
}

// launch registers the runner a job has claimed with GitHub, and creates its
// VM, which runs as the Beam token that connected the account.
func (r *githubRunners) launch(ctx context.Context, job types.GitHubRunnerJob, size runnerSize) error {
	installation, err := r.s.repo.GetGitHubRunnerInstallationById(ctx, job.InstallationID)
	if err != nil {
		return err
	}

	if installation == nil {
		return errors.New("the GitHub account is disconnected")
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

	config, err := r.github.registerRunner(ctx, installation.ID, job.Repository, job.VM, size.Label)
	if err != nil {
		return fmt.Errorf("register the runner with GitHub: %w", err)
	}

	info := &auth.AuthInfo{Workspace: workspace, Token: token}
	_, err = r.s.createVM(auth.ContextWithAuthInfo(ctx, info), info, types.VMCreateOptions{
		Name: job.VM,
		Spec: types.VMSpec{
			ImageID:       r.config.ImageID,
			CPU:           size.CPU,
			Memory:        size.Memory,
			DiskSize:      size.DiskSize,
			DockerEnabled: true,
			Env:           []string{"BEAM_GITHUB_JIT_CONFIG=" + config},
		},
		Metadata: map[string]string{"github.repository": job.Repository},
	})
	return err
}

// reconcileRunner deletes a runner VM that has nothing left to do, as a
// runner takes one job and never restarts. It reports whether v is a runner,
// which the reconciler then leaves alone otherwise.
func (s *Service) reconcileRunner(ctx context.Context, v *types.VM) (bool, error) {
	r := s.runners
	if r == nil || v.DesiredState == types.VMStateDeleted || !strings.HasPrefix(v.Name, runnerPrefix) {
		return false, nil
	}

	runner, err := r.s.repo.GetGitHubRunner(ctx, v.WorkspaceID, v.Name)
	if err != nil || runner == nil {
		return false, err
	}

	state, err := s.runtimeState(v)
	if err != nil {
		return true, err
	}

	waiting := runner.JobStatus == ""
	exited := state == nil && v.ContainerID != "" || v.Status == types.VMStateError && v.LaunchAttempts >= 5
	if exited && waiting {
		// Some queued job still counts on this runner.
		reason := cmp.Or(v.Error, "the runner stopped before it took a job")
		if err := r.s.repo.ReplaceGitHubRunner(ctx, v.WorkspaceID, v.Name, reason); err != nil {
			return true, err
		}

		v.DesiredState = types.VMStateDeleted
		return true, nil
	}

	done := exited || r.github == nil || !runner.Connected || v.DesiredState != types.VMStateRunning ||
		runner.JobStatus == types.GitHubJobCompleted || time.Since(v.CreatedAt) > runnerLifetime

	// Nothing else saves a running runner, so UpdatedAt is when it started or
	// was last found to be busy.
	if !done && waiting && v.Status == types.VMStateRunning && time.Since(v.UpdatedAt) > runnerIdle {
		removed, err := r.github.removeRunner(ctx, runner.InstallationID, runner.Repository, v.Name)
		if err != nil {
			return true, err
		}

		// GitHub has given it a job in the meantime, so it gets more time.
		if !removed {
			return true, s.repo.SaveVM(ctx, v)
		}

		done = true
	}

	if !done {
		return true, nil
	}

	v.DesiredState = types.VMStateDeleted
	return true, s.repo.SaveVM(ctx, v)
}

// redeliver has GitHub send again each event whose latest delivery since a
// time failed, a few times at most. GitHub does not retry a delivery itself,
// so one that arrives while the gateway restarts would otherwise leave its
// job without a runner. Every delivery is read, as GitHub's filter for failed
// ones misses those that timed out, which have no status code.
func (r *githubRunners) redeliver(ctx context.Context, since time.Time) error {
	latest := map[string]bool{}
	for page := ""; ; {
		deliveries, next, err := r.github.deliveries(ctx, page)
		if err != nil {
			return err
		}

		for _, delivery := range deliveries {
			if delivery.DeliveredAt.Before(since) {
				next = ""
				break
			}

			// Deliveries are newest first, so the first of an event is its latest.
			seen := latest[delivery.GUID]
			latest[delivery.GUID] = true
			succeeded := delivery.StatusCode >= 200 && delivery.StatusCode < 400
			if seen || succeeded || r.redelivered[delivery.GUID] >= runnerAttempts {
				continue
			}

			r.redelivered[delivery.GUID]++
			if err := r.github.redeliver(ctx, delivery.ID); err != nil {
				return err
			}
		}

		if next == "" {
			break
		}

		page = next
	}

	// Forget the events that are out of the window.
	maps.DeleteFunc(r.redelivered, func(guid string, _ int) bool { return !latest[guid] })
	return nil
}
