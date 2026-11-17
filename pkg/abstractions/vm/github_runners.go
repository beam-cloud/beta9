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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	runnerCancelTime  = 30 * time.Second
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
	group.PATCH("/:workspaceId", auth.WithStrictWorkspaceAuth(r.update))
	group.DELETE("/:workspaceId", auth.WithStrictWorkspaceAuth(r.disconnect))
	group.POST("/:workspaceId/connect", auth.WithStrictWorkspaceAuth(r.connect))
	group.POST("/:workspaceId/cancel", auth.WithStrictWorkspaceAuth(r.cancelAll))
	group.GET("/:workspaceId/jobs", auth.WithStrictWorkspaceAuth(r.jobs))
	group.POST("/:workspaceId/jobs/:jobId/cancel", auth.WithStrictWorkspaceAuth(r.cancelJob))
	group.POST("/:workspaceId/jobs/:jobId/stop", auth.WithStrictWorkspaceAuth(r.stopJob))
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

// update changes the settings of the workspace's account that a request
// names, and answers as status does. An account that is paused starts no
// runner, which leaves its jobs queued on GitHub until it resumes. The jobs
// that run and the runners that exist are left alone.
func (r *githubRunners) update(c echo.Context) error {
	ctx, info := requestContext(c)
	var req struct {
		Paused *bool `json:"paused"`
	}
	if err := decode(c, &req); err != nil {
		return err
	}

	var installation *types.GitHubRunnerInstallation
	var err error
	if req.Paused == nil {
		installation, err = r.s.repo.GetGitHubRunnerInstallation(ctx, info.Workspace.Id)
	} else {
		installation, err = r.s.repo.PauseGitHubRunnerInstallation(ctx, info.Workspace.Id, *req.Paused)
	}
	if err != nil {
		return err
	}

	if installation == nil {
		return echo.NewHTTPError(http.StatusConflict, "no GitHub account is connected")
	}

	// The jobs that waited are due, so the loop need not wait for its tick.
	if req.Paused != nil && !installation.Paused {
		select {
		case r.wake <- struct{}{}:
		default:
		}
	}

	return c.JSON(http.StatusOK, runnerStatus{Enabled: r.github != nil, Installation: installation, Sizes: runnerSizes})
}

type runnerJobPage struct {
	Data  []types.GitHubRunnerJob `json:"data"`
	Total int                     `json:"total"`
	// Every repository with jobs, whatever the filters.
	Repositories []string `json:"repositories"`
}

// jobs returns a page of the workspace's jobs, newest first. They can be
// narrowed to a repository, to statuses and conclusions, which are lists
// separated by commas, and with ?q= to those whose name, workflow or
// repository contains some text.
func (r *githubRunners) jobs(c echo.Context) error {
	ctx, info := requestContext(c)
	limit, offset, err := pageParams(c)
	if err != nil {
		return err
	}

	filter := types.GitHubRunnerJobFilter{Limit: limit, Offset: offset, Search: c.QueryParam("q"), Repository: c.QueryParam("repository")}
	if status := c.QueryParam("status"); status != "" {
		filter.Statuses = strings.Split(status, ",")
	}
	if conclusion := c.QueryParam("conclusion"); conclusion != "" {
		filter.Conclusions = strings.Split(conclusion, ",")
	}

	page, err := r.s.repo.ListGitHubRunnerJobs(ctx, info.Workspace.Id, filter)
	if err != nil {
		return err
	}

	installation, err := r.s.repo.GetGitHubRunnerInstallation(ctx, info.Workspace.Id)
	if err != nil {
		return err
	}

	// A job's error says why it has no runner. Once it runs on one, what is
	// left is about an earlier attempt, or about replacing the runner it took.
	paused := installation != nil && installation.Paused
	for i := range page.Jobs {
		if page.Jobs[i].RunnerName != "" {
			page.Jobs[i].Error = ""
		}

		page.Jobs[i].Waiting = runnerWaiting(page.Jobs[i], paused)
	}

	return c.JSON(http.StatusOK, runnerJobPage{Data: page.Jobs, Total: page.Total, Repositories: page.Repositories})
}

// runnerWaiting says what a queued job that no runner has taken waits for,
// in words that a user is shown as they are. It says nothing of a job that
// no runner is started for any more, as its error says why: its runs-on
// matches no size, or every attempt failed. Nor does it of a job that had
// no runner yet when its account disconnected, which gets none.
func runnerWaiting(job types.GitHubRunnerJob, paused bool) string {
	_, sized := runnerSizeFor(job.Label)
	switch {
	case job.Status != types.GitHubJobQueued || job.RunnerName != "":
		return ""
	case !sized || job.ProvisionAt == nil && (job.Error != "" || job.VM == ""):
		return ""
	case paused:
		return "Runners are paused"
	case job.Error != "":
		return "Retrying"
	}

	return "Starting a runner"
}

// job returns the job of the workspace that a request names.
func (r *githubRunners) job(c echo.Context) (*types.GitHubRunnerJob, error) {
	ctx, info := requestContext(c)
	id, err := strconv.ParseInt(c.Param("jobId"), 10, 64)
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusNotFound, "job not found")
	}

	job, err := r.s.repo.GetGitHubRunnerJob(ctx, info.Workspace.Id, id)
	if err != nil {
		return nil, err
	}

	if job == nil {
		return nil, echo.NewHTTPError(http.StatusNotFound, "job not found")
	}

	return job, nil
}

// cancelJob cancels the workflow run of a job, and so the run's other jobs
// too, as GitHub cancels no job on its own. The runner of a job that ran is
// deleted as every runner is, once GitHub reports the job completed.
func (r *githubRunners) cancelJob(c echo.Context) error {
	if err := r.ready(); err != nil {
		return err
	}

	job, err := r.job(c)
	if err != nil {
		return err
	}

	switch {
	case job.Status == types.GitHubJobCompleted:
		return echo.NewHTTPError(http.StatusConflict, "the job has already completed")
	case job.RunID == 0:
		return echo.NewHTTPError(http.StatusConflict, "the job was recorded without its workflow run; cancel the run on GitHub")
	}

	ctx, _ := requestContext(c)
	err = r.github.cancelRun(ctx, job.InstallationID, job.Repository, job.RunID)
	if githubStatus(err) == http.StatusForbidden {
		return echo.NewHTTPError(http.StatusForbidden, "The GitHub App needs the Actions permission set to read and write; accept the updated permissions on GitHub")
	}
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusAccepted)
}

// stopJob deletes the runner that a job runs on, for a job that hangs, or
// one of an account that has not let the App cancel its runs. GitHub reports
// the job failed once it finds the runner gone.
func (r *githubRunners) stopJob(c echo.Context) error {
	if err := r.ready(); err != nil {
		return err
	}

	job, err := r.job(c)
	if err != nil {
		return err
	}

	ctx, info := requestContext(c)
	elsewhere := echo.NewHTTPError(http.StatusConflict, "the job is not running on a runner of this workspace")
	if job.Status != types.GitHubJobInProgress || job.RunnerName == "" {
		return elsewhere
	}

	// GitHub says which runner a job runs on by its name, which anyone can
	// give a runner of their own, so it is ours only if it started as one.
	runner, err := r.s.repo.GetGitHubRunner(ctx, info.Workspace.Id, job.RunnerName)
	if err != nil {
		return err
	}

	if runner == nil {
		return elsewhere
	}

	v, unlock, err := r.s.lockedVM(ctx, info.Workspace.Id, job.RunnerName)
	if errors.Is(err, sql.ErrNoRows) {
		return elsewhere
	}
	if err != nil {
		return err
	}

	defer unlock()
	if v.DesiredState == types.VMStateDeleted {
		return echo.NewHTTPError(http.StatusConflict, "the job's runner is already being deleted")
	}

	// GitHub refuses to unregister a runner that it gave a job, which spares
	// the runner in reconcileRunner. Here its VM is deleted whatever GitHub
	// answers, and GitHub fails the job.
	if _, err := r.github.removeRunner(ctx, runner.InstallationID, runner.Repository, v.Name); err != nil {
		log.Warn().Err(err).Str("vm_id", v.ID).Msg("unregister GitHub runner")
	}

	v.DesiredState = types.VMStateDeleted
	if err := r.s.repo.SaveVM(ctx, v); err != nil {
		return err
	}

	if err := r.s.stopOrDelete(ctx, v); err != nil {
		return err
	}

	return c.NoContent(http.StatusAccepted)
}

// runnerCancellation counts the workflow runs that a request cancelled, and
// those that could not be.
type runnerCancellation struct {
	Cancelled int `json:"cancelled"`
	Failed    int `json:"failed"`
}

// cancelAll cancels every workflow run of the workspace that has a queued or
// running job, or those of one repository, each of them once however many
// of its jobs asked for a runner. A run that can't be cancelled does not
// stop the others.
//
// A request cancels the oldest hundred runs, a few at a time and for no
// longer than runnerCancelTime, which keeps it within GitHub's rate limits.
// The runs it leaves count as not cancelled, and asking again cancels them.
func (r *githubRunners) cancelAll(c echo.Context) error {
	if err := r.ready(); err != nil {
		return err
	}

	ctx, info := requestContext(c)
	var req struct {
		Repository string `json:"repository"`
	}
	if err := decode(c, &req); err != nil {
		return err
	}

	runs, total, err := r.s.repo.ListActiveGitHubRunnerRuns(ctx, info.Workspace.Id, req.Repository)
	if err != nil {
		return err
	}

	op, cancel := context.WithTimeout(ctx, runnerCancelTime)
	defer cancel()
	var cancelled atomic.Int64
	var cancellations sync.WaitGroup
	slots := make(chan struct{}, 4)
	for _, run := range runs {
		slots <- struct{}{}
		cancellations.Go(func() {
			defer func() { <-slots }()

			if err := r.github.cancelRun(op, run.InstallationID, run.Repository, run.RunID); err != nil {
				log.Warn().Err(err).Str("github_repository", run.Repository).Int64("github_run_id", run.RunID).Msg("cancel GitHub workflow run")
				return
			}

			cancelled.Add(1)
		})
	}

	cancellations.Wait()
	if left := total - len(runs); left > 0 {
		log.Warn().Int64("github_installation_id", runs[0].InstallationID).Int("runs", left).Msg("GitHub workflow runs left to cancel")
	}

	return c.JSON(http.StatusOK, runnerCancellation{Cancelled: int(cancelled.Load()), Failed: total - int(cancelled.Load())})
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

// runnerAccount is a GitHub account that has the App installed.
type runnerAccount struct {
	InstallationID int64  `json:"installation_id"`
	Account        string `json:"account"`
	AccountType    string `json:"account_type"`
}

// runnerConnection is a step in connecting a GitHub account: where the user
// goes next, or that the account is connected.
type runnerConnection struct {
	URL string `json:"url,omitempty"`
	// The accounts the user can connect without going to GitHub again, and
	// the state to connect one with. URL then installs the App on another.
	Accounts  []runnerAccount `json:"accounts,omitempty"`
	State     string          `json:"state,omitempty"`
	Connected bool            `json:"connected"`
}

// connect takes a workspace through connecting a GitHub account. The user
// first authorizes the App, which says who they are, so that their ownership
// of an account can be checked. They then choose an account of theirs that
// has the App installed, or go to GitHub once more to install it on one.
// GitHub would have them change an installation to come back from it, so one
// that is already there is connected from here.
//
// The dashboard posts the query parameters each trip to GitHub comes back
// with, or the installation the user chose, and sends the user to the URL it
// is given, until it is told that the account is connected.
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
		return c.JSON(http.StatusOK, runnerConnection{URL: r.github.authorizeURL(r.state(info, "", expires))})
	}

	login, err := r.verify(info, req.State)
	if err != nil {
		return err
	}

	// Back from authorizing the App: the state does not name a user.
	if login == "" {
		if req.Code == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "GitHub did not say who authorized it; connect again")
		}

		login, token, err := r.github.authorizedUser(ctx, req.Code)
		if err != nil || login == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "GitHub authorization failed; connect again")
		}

		accounts, err := r.ownedAccounts(ctx, login, token)
		if err != nil {
			return err
		}

		state := r.state(info, login, expires)
		return c.JSON(http.StatusOK, runnerConnection{URL: r.github.installURL(state), Accounts: accounts, State: state})
	}

	// The user chose an account, or is back from installing the App on one.
	if req.InstallationID == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "GitHub did not say what was installed; connect again")
	}

	account, err := r.ownedAccount(ctx, req.InstallationID, login)
	if err != nil {
		return err
	}

	err = r.s.repo.SaveGitHubRunnerInstallation(ctx, &types.GitHubRunnerInstallation{
		ID: req.InstallationID, WorkspaceID: info.Workspace.Id, TokenID: info.Token.ExternalId,
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

// A connect state carries the GitHub user who is connecting, once they have
// authorized the App, through GitHub's redirects. It is valid for a few
// minutes, and only for the token that started connecting.
func (r *githubRunners) state(info *auth.AuthInfo, login string, expires int64) string {
	mac := hmac.New(sha256.New, []byte(r.config.ClientSecret))
	fmt.Fprintf(mac, "%d\n%s\n%s\n%d", info.Workspace.Id, info.Token.ExternalId, login, expires)
	return fmt.Sprintf("%s.%d.%x", login, expires, mac.Sum(nil))
}

func (r *githubRunners) verify(info *auth.AuthInfo, state string) (string, error) {
	login, rest, _ := strings.Cut(state, ".")
	var expires int64
	_, err := fmt.Sscanf(rest, "%d.", &expires)
	if err != nil || !hmac.Equal([]byte(state), []byte(r.state(info, login, expires))) || time.Now().Unix() > expires {
		return "", echo.NewHTTPError(http.StatusBadRequest, "GitHub connection expired; connect again")
	}

	return login, nil
}

// owns reports whether a user owns the account an installation is on. A
// workspace pays for the jobs of the account it connects, and its runners see
// their code and secrets, so only the user themselves or an owner of the
// organization may connect one.
func (r *githubRunners) owns(ctx context.Context, installation *githubInstallation, login string) (bool, error) {
	account := installation.Account
	if account.Type != "Organization" {
		return account.Type == "User" && strings.EqualFold(account.Login, login), nil
	}

	role, err := r.github.organizationRole(ctx, installation.ID, account.Login, login)
	return role == "admin", err
}

// ownedAccount returns the account an installation is on, if the user owns
// it.
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

	installation.ID = id
	owner, err := r.owns(ctx, installation, login)
	if err != nil {
		return nil, err
	}

	if !owner {
		return nil, echo.NewHTTPError(http.StatusForbidden, "only an owner of "+installation.Account.Login+" can connect it")
	}

	return &installation.Account, nil
}

// ownedAccounts returns the accounts with the App installed that a user
// owns, by name. An account whose owner can't be checked is left out rather
// than failing the rest. Installing the App on it again still connects it,
// or says why it can't be.
func (r *githubRunners) ownedAccounts(ctx context.Context, login, token string) ([]runnerAccount, error) {
	installations, err := r.github.userInstallations(ctx, token)
	if err != nil {
		return nil, err
	}

	accounts := []runnerAccount{}
	for i := range installations {
		installation := &installations[i]
		if installation.SuspendedAt != nil {
			continue
		}

		owner, err := r.owns(ctx, installation, login)
		if err != nil {
			log.Warn().Err(err).Int64("github_installation_id", installation.ID).Msg("check GitHub account owner")
			continue
		}

		if owner {
			accounts = append(accounts, runnerAccount{InstallationID: installation.ID, Account: installation.Account.Login, AccountType: installation.Account.Type})
		}
	}

	slices.SortFunc(accounts, func(a, b runnerAccount) int {
		return strings.Compare(strings.ToLower(a.Account), strings.ToLower(b.Account))
	})
	return accounts, nil
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
		RunID       int64      `json:"run_id"`
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
		RunID:          event.Job.RunID,
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
			// A job finds no port of its machine taken, as on GitHub's.
			Headless: true,
			Env:      []string{"BEAM_GITHUB_JIT_CONFIG=" + config},
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
