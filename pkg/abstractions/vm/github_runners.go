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
	// How many times GitHub is asked to deliver a webhook that failed.
	runnerAttempts = 5
	runnerIdle     = 5 * time.Minute
	// How long GitHub is given to hand a runner one of the jobs that are
	// queued for it, from when it lists the runner as free. GitHub does so
	// within seconds, so it holds the jobs back from one that is still free
	// after this long: their workflow runs only so many at a time.
	runnerHold = 30 * time.Second
	// How many free runners are kept at least for jobs that GitHub holds
	// back, so that the next one it lets go starts at once.
	runnerReserve     = 2
	runnerLifetime    = 24 * time.Hour
	runnerConnectTime = 10 * time.Minute
	runnerCancelTime  = 30 * time.Second
	// How long a runner's VM may be without a worker to start on before the
	// job it was started for is told that it waits for one.
	runnerPlacement = 15 * time.Second
	// How often a gateway asks to sweep GitHub's webhook deliveries, and for
	// how long a sweep counts as recent: a little less, so that the gateway
	// that swept is let again when it next asks.
	runnerSweep  = time.Minute
	runnerRecent = 50 * time.Second
	// RFC 3339 with milliseconds, which a job's timeline is returned in.
	runnerTimeFormat = "2006-01-02T15:04:05.000Z07:00"
)

// What a queued job waits for, in words that a user is shown as they are.
const (
	waitingPaused   = "Runners are paused"
	waitingLimit    = "Waiting for a free runner"
	waitingGitHub   = "GitHub is limiting requests"
	waitingCapacity = "Waiting for capacity"
	waitingQuota    = "Workspace CPU limit reached"
	waitingRetry    = "Retrying"
	waitingStart    = "Starting a runner"
	// A runner that could take the job is up, and the job is GitHub's to
	// give it.
	waitingAssign = "Runner ready, waiting for GitHub"
	// A runner has been up and free for a while, and GitHub gives it none of
	// the jobs, as it does when their workflow limits how many run at once.
	waitingHeld = "Held by GitHub, runner ready"
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
	// Set by tests, which have no object storage.
	deleteStored func(ctx context.Context, workspace *types.Workspace, prefix string) error
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
	Runners      runnerCount                     `json:"runners"`
	// Whether the runners of the connected account get the workspace's
	// cache, which no account says of itself until one is connected.
	EnhancedCache *bool `json:"enhanced_cache,omitempty"`
}

// runnerCount is how many runners a workspace has, those being started among
// them, and how many it may have at once, which is zero for any number.
type runnerCount struct {
	Running int `json:"running"`
	Limit   int `json:"limit"`
}

func (r *githubRunners) status(c echo.Context) error {
	ctx, info := requestContext(c)
	installation, err := r.s.repo.GetGitHubRunnerInstallation(ctx, info.Workspace.Id)
	if err != nil {
		return err
	}

	return r.respond(c, installation)
}

// respond answers a request with the status of its workspace's runners.
func (r *githubRunners) respond(c echo.Context, installation *types.GitHubRunnerInstallation) error {
	ctx, info := requestContext(c)
	running, err := r.s.repo.CountGitHubRunners(ctx, info.Workspace.Id)
	if err != nil {
		return err
	}

	status := runnerStatus{
		Enabled:      r.github != nil,
		Installation: installation,
		Sizes:        runnerSizes,
		Runners:      runnerCount{Running: running, Limit: r.limit()},
	}
	if installation != nil {
		status.EnhancedCache = &installation.EnhancedCache
	}

	return c.JSON(http.StatusOK, status)
}

// limit is the most runners a workspace has at once, or zero for any number.
func (r *githubRunners) limit() int {
	return max(r.config.RunnerLimit, 0)
}

// update changes the settings of the workspace's account that a request
// names, and answers as status does. An account that is paused starts no
// runner, which leaves its jobs queued on GitHub until it resumes. The jobs
// that run and the runners that exist are left alone. An account whose
// enhanced cache is turned off has it deleted, and starts a new one when it
// is turned on again.
func (r *githubRunners) update(c echo.Context) error {
	ctx, info := requestContext(c)
	var req struct {
		Paused        *bool `json:"paused"`
		EnhancedCache *bool `json:"enhanced_cache"`
	}
	if err := decode(c, &req); err != nil {
		return err
	}

	installation, err := r.s.repo.GetGitHubRunnerInstallation(ctx, info.Workspace.Id)
	if err == nil && installation != nil && req.Paused != nil {
		installation, err = r.s.repo.PauseGitHubRunnerInstallation(ctx, info.Workspace.Id, *req.Paused)
	}
	if err == nil && installation != nil && req.EnhancedCache != nil {
		installation, err = r.s.repo.SetGitHubRunnerEnhancedCache(ctx, info.Workspace.Id, *req.EnhancedCache)
	}
	if err != nil {
		return err
	}

	if installation == nil {
		return echo.NewHTTPError(http.StatusConflict, "no GitHub account is connected")
	}

	if !installation.EnhancedCache {
		if err := r.s.repo.RetireGitHubRunnerCache(ctx, info.Workspace.Id, 0); err != nil {
			return err
		}
	}

	// The jobs that waited are due, so the loop need not wait for its tick.
	if req.Paused != nil && !installation.Paused {
		select {
		case r.wake <- struct{}{}:
		default:
		}
	}

	return r.respond(c, installation)
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

	full, err := r.full(ctx, info.Workspace.Id)
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

		page.Jobs[i].Waiting = runnerWaiting(page.Jobs[i], paused, full)
		page.Jobs[i].Timeline = runnerTimeline(page.Jobs[i])
	}

	return c.JSON(http.StatusOK, runnerJobPage{Data: page.Jobs, Total: page.Total, Repositories: page.Repositories})
}

// full reports whether a workspace has as many runners as it may.
func (r *githubRunners) full(ctx context.Context, workspaceId uint) (bool, error) {
	if r.limit() == 0 {
		return false, nil
	}

	running, err := r.s.repo.CountGitHubRunners(ctx, workspaceId)
	return running >= r.limit(), err
}

// runnerWaiting says what a queued job that no runner has taken waits for,
// in words that a user is shown as they are. It says nothing of a job that
// no runner is started for any more, as its error says why: its runs-on
// matches no size, or GitHub no longer lets its account's runners register.
// Nor does it of a job that had no runner yet when its account disconnected,
// which gets none.
//
// What holds back every job of the account comes first: that it is paused,
// or that its workspace is full, which a job waits for once it is due for a
// runner. Then comes what the job's row says it waits for.
func runnerWaiting(job types.GitHubRunnerJob, paused, full bool) string {
	_, sized := runnerSizeFor(job.Label)
	due := job.ProvisionAt != nil && !job.ProvisionAt.After(time.Now())
	switch {
	case job.Status != types.GitHubJobQueued || job.RunnerName != "":
		return ""
	case !sized || job.ProvisionAt == nil && (job.Error != "" || job.VM == ""):
		return ""
	case paused:
		return waitingPaused
	case full && due:
		return waitingLimit
	case job.Waiting != "":
		return job.Waiting
	case job.Error != "":
		return waitingRetry
	case job.RunnerReadyAt != nil:
		return waitingAssign
	}

	return waitingStart
}

// runnerTimeline is when each step that got a job its runner was done, as
// the API returns it.
func runnerTimeline(job types.GitHubRunnerJob) types.GitHubRunnerTimeline {
	format := func(at *time.Time) string {
		if at == nil {
			return ""
		}

		return at.UTC().Format(runnerTimeFormat)
	}
	return types.GitHubRunnerTimeline{
		ReceivedAt:    format(job.ReceivedAt),
		RegisteredAt:  format(job.RegisteredAt),
		VMReadyAt:     format(job.VMReadyAt),
		RunnerReadyAt: format(job.RunnerReadyAt),
	}
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
//
// A job that has completed has nothing left to cancel, which is what the
// request was after: a list that was read a moment ago still offers it.
func (r *githubRunners) cancelJob(c echo.Context) error {
	if err := r.ready(); err != nil {
		return err
	}

	job, err := r.job(c)
	if err != nil {
		return err
	}

	if job.Status == types.GitHubJobCompleted {
		return c.NoContent(http.StatusAccepted)
	}

	ctx, _ := requestContext(c)
	err = r.github.cancelRun(ctx, job.InstallationID, job.Repository, job.RunID)
	if githubStatus(err) == http.StatusForbidden {
		return echo.NewHTTPError(http.StatusForbidden, "The GitHub App needs the Actions permission set to read and write; accept the updated permissions on GitHub")
	}

	// GitHub takes no calls for the account for now, which is no fault of
	// the request, and says when it does again.
	var limit *githubLimit
	if errors.As(err, &limit) {
		return echo.NewHTTPError(http.StatusTooManyRequests, limit.Error())
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

		// What the account's jobs left is not kept for whichever account
		// the workspace connects next.
		if err := r.s.repo.RetireGitHubRunnerCache(ctx, installation.WorkspaceID, 0); err != nil {
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
		FullName      string `json:"full_name"`
		DefaultBranch string `json:"default_branch"`
	} `json:"repository"`
	Job struct {
		ID          int64      `json:"id"`
		RunID       int64      `json:"run_id"`
		HeadBranch  string     `json:"head_branch"`
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

	installation, err := r.s.repo.GetGitHubRunnerInstallationById(ctx, event.Installation.ID)
	if err != nil {
		return err
	}

	if err := r.s.repo.DeleteGitHubRunnerInstallation(ctx, event.Installation.ID); err != nil {
		return err
	}

	r.github.forget(event.Installation.ID)
	if installation == nil {
		return nil
	}

	return r.s.repo.RetireGitHubRunnerCache(ctx, installation.WorkspaceID, 0)
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
		// A branch of a fork can have this name too, so this only picks the
		// runners that keep what their jobs add to the cache. Whether it is
		// kept is decided by what GitHub says of the run (trustsRun).
		DefaultBranch: event.Job.HeadBranch != "" && event.Job.HeadBranch == event.Repository.DefaultBranch,
	})
	if err != nil {
		return err
	}

	// The runner that takes a job is seldom the one that was started for it.
	if strings.HasPrefix(event.Job.RunnerName, runnerPrefix) {
		if err := r.s.repo.AdoptGitHubRunner(ctx, installation.WorkspaceID, event.Job.ID, event.Job.RunnerName); err != nil {
			log.Warn().Err(err).Int64("github_job_id", event.Job.ID).Msg("exchange the runners of GitHub jobs")
		}
	}

	// A queued job is due for a runner. A job that started took one, which
	// leaves the jobs that GitHub holds back a spare runner short.
	if status == types.GitHubJobQueued || status == types.GitHubJobInProgress {
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

	// The first sweep of a gateway looks back over the time that none may
	// have been up.
	asked, lookback := time.Time{}, time.Hour
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-r.wake:
		}

		// Recover queued jobs that lost their runner: it took another job,
		// it never connected, or it was one of more than GitHub would give
		// jobs to, and one of the spare ones has now been given one.
		recovery, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := r.s.repo.RearmGitHubRunnerJobs(recovery, runnerReserve, waitingHeld); err != nil {
			log.Warn().Err(err).Msg("recover queued GitHub runner jobs")
		}
		cancel()

		r.provisionDue(ctx)
		r.promoteCaches(ctx)
		if time.Since(asked) < runnerSweep {
			continue
		}

		asked = time.Now()
		swept, err := r.sweep(ctx, asked.Add(-lookback))
		if err != nil {
			log.Warn().Err(err).Msg("redeliver GitHub webhooks")
			continue
		}

		if swept {
			lookback = 5 * time.Minute
		}
	}
}

// sweep has GitHub redeliver the webhooks that failed since a time, unless
// another gateway does: the gateways see the same deliveries, so they take
// the sweep in turns, and one of them sweeps at a time. It reports whether
// this one did.
func (r *githubRunners) sweep(ctx context.Context, since time.Time) (bool, error) {
	redeliver := func(ctx context.Context) error {
		// Retired caches are deleted by one gateway at a time as well.
		r.deleteCaches(ctx)
		return r.redeliver(ctx, since)
	}
	if r.s.stateRepo == nil {
		return true, redeliver(ctx)
	}

	return r.s.stateRepo.SweepGitHubWebhooks(ctx, runnerRecent, redeliver)
}

func (r *githubRunners) provisionDue(ctx context.Context) {
	jobs, err := r.s.repo.ListDueGitHubRunnerJobs(ctx, r.limit())
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

			// Shorter than the minute a claim holds its job for, so that two
			// gateways never start a runner for one job at once.
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
	}

	// GitHub told the account to wait, so the job is due when it may ask
	// again. Asking sooner would spend an attempt on being told to wait.
	if until, limited := r.github.limited(job.InstallationID); limited {
		return r.s.repo.WaitGitHubRunnerJob(ctx, job.ID, waitingGitHub, until)
	}

	// A job that is not claimed is another gateway's, or waits as it is for
	// its account to resume or for its workspace to have a runner less.
	claimed, err := r.s.repo.ClaimGitHubRunnerJob(ctx, &job, runnerPrefix+randomHexID()[:12], r.limit())
	if err != nil || !claimed {
		return err
	}

	err = r.launch(ctx, job, size)
	switch {
	case err == nil:
		return r.s.repo.FinishGitHubRunnerJob(ctx, job.ID, "")
	case runnerRefused(err):
		return errors.Join(err, r.s.repo.FinishGitHubRunnerJob(ctx, job.ID, err.Error()))
	}

	// Whatever else went wrong passes, and GitHub keeps the job queued until
	// it has, so the job is tried again for as long as it takes.
	return errors.Join(err, r.s.repo.FailGitHubRunnerJob(ctx, job.ID, err.Error(), runnerWaits(err)))
}

var errRunnerDisconnected = errors.New("the GitHub account is disconnected")

// runnerRefused reports whether an attempt to start a job's runner failed for
// good: the account disconnected, or GitHub answered that the App is not
// installed on it any more or is suspended, or that it can't reach the
// repository or register runners in it. GitHub gives the same answer until
// the account changes that, so the job ends with it. Once it has, the job
// can be run again.
func runnerRefused(err error) bool {
	switch githubStatus(err) {
	case http.StatusForbidden, http.StatusNotFound:
		return true
	}

	return errors.Is(err, errRunnerDisconnected)
}

// runnerWaits says what a job waits for after an attempt to start its runner
// failed for a reason that passes.
func runnerWaits(err error) string {
	var limit *githubLimit
	if errors.As(err, &limit) {
		return waitingGitHub
	}

	return waitingRetry
}

// launch registers the runner a job has claimed with GitHub, and creates its
// VM, which runs as the Beam token that connected the account.
func (r *githubRunners) launch(ctx context.Context, job types.GitHubRunnerJob, size runnerSize) error {
	installation, err := r.s.repo.GetGitHubRunnerInstallationById(ctx, job.InstallationID)
	if err != nil {
		return err
	}

	if installation == nil {
		return errRunnerDisconnected
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

	// The job's timeline says when, which a runner is not held up for.
	if err := r.s.repo.MarkGitHubRunnerRegistered(ctx, job.ID); err != nil {
		log.Warn().Err(err).Int64("github_job_id", job.ID).Msg("time GitHub runner registration")
	}

	// A runner without the cache is slower, and no worse.
	layer, err := r.cacheLayer(ctx, installation, job)
	if err != nil {
		log.Warn().Err(err).Int64("github_job_id", job.ID).Msg("find the cache of a GitHub runner")
	}

	info := &auth.AuthInfo{Workspace: workspace, Token: token}
	captureExitSnapshot := false
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
			// A runner is deleted with its job and never starts again, so
			// nothing of its disk is worth uploading when it exits.
			CaptureExitSnapshot: &captureExitSnapshot,
			RootLayer:           layer,
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
	if waiting && throttled(v) {
		// Its job no longer waits: GitHub cancelled it, or gave it another
		// runner. This one would start only to be removed, in room that a
		// queued job needs.
		if !runner.Queued {
			r.forget(ctx, v, runner)
			v.DesiredState = types.VMStateDeleted
			return true, s.repo.SaveVM(ctx, v)
		}

		// The workspace's limit has no room for this runner, and would have
		// none for one in its place. It keeps asking: room comes as runners
		// finish.
		v.LaunchAttempts = 0
	}

	exited := state == nil && v.ContainerID != "" || v.Status == types.VMStateError && v.LaunchAttempts >= 5
	if exited && waiting {
		// Some queued job still counts on this runner.
		reason, waits := "the runner stopped before it took a job", waitingRetry
		if s.unplaced(v) {
			reason, waits = "no worker had capacity for the runner", waitingCapacity
		}

		if err := r.s.repo.ReplaceGitHubRunner(ctx, v.WorkspaceID, v.Name, cmp.Or(v.Error, reason), waits); err != nil {
			return true, err
		}

		r.forget(ctx, v, runner)
		v.DesiredState = types.VMStateDeleted
		return true, nil
	}

	r.observe(ctx, v, runner, state)
	done := exited || r.github == nil || !runner.Connected || v.DesiredState != types.VMStateRunning ||
		runner.JobStatus == types.GitHubJobCompleted || time.Since(v.CreatedAt) > runnerLifetime

	if !done && waiting && v.Status == types.VMStateRunning {
		remove, renew, err := r.idle(ctx, v, runner)

		// GitHub takes no calls for the account for now. The runner is no
		// worse for it, and is removed once GitHub does.
		var limit *githubLimit
		if errors.As(err, &limit) {
			return true, nil
		}
		if err != nil {
			return true, err
		}

		if renew {
			return true, s.repo.SaveVM(ctx, v)
		}
		if !remove {
			return true, nil
		}

		done = true
	}

	if !done {
		return true, nil
	}

	v.DesiredState = types.VMStateDeleted
	return true, s.repo.SaveVM(ctx, v)
}

// forget takes a runner that never came online off GitHub's list, if GitHub
// will. GitHub keeps such a runner for a day, and many of them are many
// runners for an account to look through.
func (r *githubRunners) forget(ctx context.Context, v *types.VM, runner *types.GitHubRunner) {
	if r.github == nil || !runner.Connected {
		return
	}

	if _, err := r.github.removeRunner(ctx, runner.InstallationID, runner.Repository, v.Name); err != nil {
		log.Debug().Err(err).Str("vm_id", v.ID).Msg("remove a GitHub runner that never came online")
	}
}

// idle decides what becomes of a runner that is up and that GitHub has given
// no job: whether it is removed, or whether GitHub has work for it after all,
// as when it refuses to remove a runner that it has given a job the gateway
// has yet to hear of. Such a runner is given its time again.
//
// A runner that no queued job is left for goes at once. One that a job is
// queued for is GitHub's to give it, which takes seconds once the runner has
// connected, so GitHub is asked about a runner that is still without one
// after some:
//
//   - It never connected, or no longer is. It takes no job, so after the time
//     a runner is given it is removed, and another is started for the job.
//   - It has been connected and free for runnerHold. GitHub then holds the
//     queued jobs back, as it does those of a workflow that limits how many
//     run at once, and they are told so. A runner started for each of them
//     would only wait, so the jobs share a few spare ones: as many as runners
//     are running the jobs, which is how many GitHub starts at a time, and
//     runnerReserve at least. The first by name stay and the others are
//     removed. When GitHub lets a job go, a spare runner takes it at once,
//     and another is started in its place (RearmGitHubRunnerJobs). One more
//     than that stays, so that a runner is not removed and started again
//     each time a job ends.
func (r *githubRunners) idle(ctx context.Context, v *types.VM, runner *types.GitHubRunner) (remove, renew bool, err error) {
	surplus, err := r.s.repo.IsSurplusGitHubRunner(ctx, v.WorkspaceID, v.Name)
	if err != nil {
		return false, false, err
	}

	if surplus {
		removed, err := r.github.removeRunner(ctx, runner.InstallationID, runner.Repository, v.Name)
		return removed, err == nil && !removed, err
	}

	// Nothing else saves a running runner, so UpdatedAt is when it started
	// or was last found to be busy. Nearly every runner has its job by now,
	// and GitHub is not asked about those.
	waited := time.Since(v.UpdatedAt)
	if waited <= runnerHold/2 {
		return false, false, nil
	}

	listed, err := r.github.runners(ctx, runner.InstallationID, runner.Repository)
	if err != nil {
		return false, false, err
	}

	self, registered := listed[v.Name]
	switch {
	case self.Busy:
		return false, true, nil
	case self.free() && time.Since(self.FreeSince) <= runnerHold:
		return false, false, nil
	case self.free():
	case waited <= runnerIdle:
		return false, false, nil
	case !registered:
		return true, false, nil
	case self.Status != "offline":
		// GitHub has it and does not say that it is gone.
		return false, true, nil
	default:
		removed, err := r.github.deleteRunner(ctx, runner.InstallationID, runner.Repository, self)
		return removed, err == nil && !removed, err
	}

	pool, err := r.s.repo.GetGitHubRunnerPool(ctx, v.WorkspaceID, v.Name)
	if err != nil || pool == nil {
		return false, false, err
	}

	if pool.Unheld > 0 {
		if err := r.s.repo.HoldGitHubRunnerJobs(ctx, v.WorkspaceID, v.Name, waitingHeld, self.FreeSince); err != nil {
			return false, false, err
		}
	}

	spare := 0
	for _, name := range pool.Ahead {
		if other := listed[name]; other.free() && time.Since(other.FreeSince) > runnerHold {
			spare++
		}
	}

	if spare <= max(runnerReserve, pool.Busy) {
		return false, false, nil
	}

	removed, err := r.github.deleteRunner(ctx, runner.InstallationID, runner.Repository, self)
	return removed, err == nil && !removed, err
}

// throttled reports whether a VM's last launch was refused because its
// workspace runs as much as its concurrency limit allows.
func throttled(v *types.VM) bool {
	return v.Status == types.VMStateError && v.ContainerID == "" && strings.Contains(v.Error, concurrencyLimitError)
}

var concurrencyLimitError = (&types.ThrottledByConcurrencyLimitError{}).Error()

// unplaced reports whether the scheduler gave up on finding a VM a worker to
// start on.
func (s *Service) unplaced(v *types.VM) bool {
	status, err := s.containers.GetContainerRequestStatus(v.ContainerID)
	return err == nil && status == types.ContainerRequestStatusFailed
}

// observe tells the job that a runner was started for what its VM is found
// to be doing, when that changes: that the job waits for capacity while the
// VM has no worker to start on, or for room in the workspace's limit, and
// when the VM was ready. The worker says
// when it started, to the second, which is nearer than when a pass finds it
// running, some seconds later. No runner is held up for what its job is told.
func (r *githubRunners) observe(ctx context.Context, v *types.VM, runner *types.GitHubRunner, state *types.ContainerState) {
	waits := ""
	pending := state != nil && state.Status == types.ContainerStatusPending && state.WorkerId == ""
	if runner.JobStatus == "" && pending && time.Since(v.CreatedAt) > runnerPlacement {
		waits = waitingCapacity
	}

	if runner.JobStatus == "" && throttled(v) {
		waits = waitingQuota
	}

	// That GitHub holds the job back is found out of a runner that is up
	// (idle), and stays what the job waits for while nothing else is.
	if waits == "" && runner.Waiting == waitingHeld {
		waits = waitingHeld
	}

	var ready *time.Time
	if state != nil && state.Status == types.ContainerStatusRunning && !runner.Ready {
		at := time.Now()
		if state.StartedAt != 0 {
			at = time.Unix(state.StartedAt, 0)
		}

		ready = &at
	}

	if waits == runner.Waiting && ready == nil {
		return
	}

	if err := r.s.repo.ObserveGitHubRunner(ctx, v.WorkspaceID, v.Name, waits, ready); err != nil {
		log.Warn().Err(err).Str("vm_id", v.ID).Msg("record GitHub runner state")
	}
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
