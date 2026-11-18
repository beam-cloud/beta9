package vm

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"go.yaml.in/yaml/v3"
)

const (
	compactionPrefix         = "ghc-"
	compactionBranch         = "beam/ci-compaction"
	compactionResultRoute    = "/compaction/result"
	compactionLifetime       = 45 * time.Minute
	compactionWeek           = 7 * 24 * time.Hour
	compactionMinimumTime    = 10 * time.Second
	compactionMinimumPercent = 5
	compactionMinimumLines   = 200
	compactionMaxFile        = 512 << 10
	compactionMaxResult      = 4 << 20
	compactionFrameworks     = "go,pytest,jest,vitest"
)

var (
	compactionSHA        = regexp.MustCompile(`^[a-f0-9]{40}$`)
	compactionRepository = regexp.MustCompile(`^[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+$`)
	errCompactionStale   = errors.New("the default branch advanced")
)

type compactionHead struct{ Branch, Commit string }

type compactionClaims struct {
	Workspace string `json:"workspace"`
	Run       string `json:"run"`
	Expires   int64  `json:"exp"`
}

func compactionMAC(secret, payload string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return mac.Sum(nil)
}

func (claims compactionClaims) sign(secret string) string {
	data, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(data)
	return payload + "." + hex.EncodeToString(compactionMAC(secret, payload))
}

func verifyCompactionToken(token, secret string, now time.Time) (claims compactionClaims, ok bool) {
	payload, signature, found := strings.Cut(token, ".")
	if secret == "" || !found || len(token) > 2048 {
		return claims, false
	}
	decoded, err := hex.DecodeString(signature)
	if err != nil || !hmac.Equal(decoded, compactionMAC(secret, payload)) {
		return claims, false
	}
	data, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil || json.Unmarshal(data, &claims) != nil {
		return claims, false
	}
	return claims, claims.Workspace != "" && claims.Run != "" && claims.Expires > now.Unix() &&
		claims.Expires <= now.Add(compactionLifetime).Unix()
}

type compactionCase struct {
	OriginalTest string   `json:"original_test"`
	Name         string   `json:"name"`
	Checks       []string `json:"checks"`
}

type compactionGroup struct {
	File      string           `json:"file"`
	Framework string           `json:"framework,omitempty"`
	NewTest   string           `json:"new_test"`
	Status    string           `json:"status"`
	Cases     []compactionCase `json:"cases"`
}

type compactionProject struct {
	Framework string `json:"framework"`
	Directory string `json:"directory"`
}

type compactionReport struct {
	DetectedProjects []compactionProject `json:"detected_projects,omitempty"`
	Outcome          string              `json:"outcome,omitempty"`
	Reason           string              `json:"reason,omitempty"`
	SkippedPackages  json.RawMessage     `json:"skipped_packages,omitempty"`
	BeforeMS         int64               `json:"median_before_ms"`
	AfterMS          int64               `json:"median_after_ms"`
	SpreadMS         int64               `json:"baseline_spread_ms"`
	LinesBefore      int                 `json:"test_lines_before"`
	LinesAfter       int                 `json:"test_lines_after"`
	TestsBefore      int                 `json:"tests_before"`
	TestsAfter       int                 `json:"tests_after"`
	CoveredBefore    int                 `json:"coverage_lines_before"`
	CoveredAfter     int                 `json:"coverage_lines_after"`
	Coverable        int                 `json:"coverage_lines_total"`
	Caught           int                 `json:"mutations_caught"`
	Preserved        int                 `json:"mutations_preserved"`
	Groups           []compactionGroup   `json:"groups"`
}

type compactionResult struct {
	Commit string            `json:"commit"`
	Status string            `json:"status"`
	Files  map[string]string `json:"files"`
	Report compactionReport  `json:"report"`
}

func (r *githubRunners) compactionEnabled() bool {
	return r.github != nil && r.config.CompactionURL != "" && r.config.CompactionSecret != ""
}

func (r *githubRunners) compactionValues(ctx context.Context, installation *types.GitHubRunnerInstallation, status *runnerStatus) error {
	if !r.compactionEnabled() || !r.github.compactionAllowed(ctx, installation.ID) {
		return nil
	}
	status.CICompaction = &installation.CICompaction
	rows, err := r.s.repo.ListGitHubCompactions(ctx, installation.WorkspaceID)
	for _, row := range rows {
		status.CompactionResults = append(status.CompactionResults, compactionStatus{
			Repository: row.Repository, Status: row.Status, PRURL: row.PRURL, Savings: row.Savings,
		})
	}
	return err
}

type compactionStatus struct {
	Repository string `json:"repository"`
	Status     string `json:"status"`
	PRURL      string `json:"pr_url,omitempty"`
	Savings    string `json:"savings,omitempty"`
}

func (r *githubRunners) markCompaction(ctx context.Context, installation *types.GitHubRunnerInstallation, event runnerEvent, label string) error {
	if !r.compactionEnabled() || !installation.CICompaction || event.Action != types.GitHubJobCompleted ||
		event.Job.Conclusion != "success" || event.Job.HeadBranch == "" || event.Job.HeadBranch != event.Repository.DefaultBranch ||
		!compactionSHA.MatchString(event.Job.HeadSHA) || !compactionRepository.MatchString(event.Repository.FullName) {
		return nil
	}
	if only := r.config.CompactionRepository; only != "" && !strings.EqualFold(only, event.Repository.FullName) {
		return nil
	}
	if _, ok := runnerSizeFor(label); !ok {
		return nil
	}
	return r.s.repo.MarkGitHubCompaction(ctx, types.GitHubCompaction{
		InstallationID: installation.ID, WorkspaceID: installation.WorkspaceID, Repository: strings.ToLower(event.Repository.FullName),
		Branch: event.Repository.DefaultBranch, Head: event.Job.HeadSHA, Label: label, MarkedAt: event.Job.CreatedAt,
	})
}

func (r *githubRunners) compactionPRChanged(ctx context.Context, event runnerEvent) error {
	pr := event.PullRequest
	if pr.Head.Ref != compactionBranch || !strings.EqualFold(pr.Head.Repo.FullName, event.Repository.FullName) ||
		(event.Action != "closed" && event.Action != "reopened") {
		return nil
	}
	return r.s.repo.SetGitHubCompactionPROpen(ctx, event.Installation.ID, strings.ToLower(event.Repository.FullName), pr.Number, event.Action == "reopened")
}

// Claim in Postgres before contacting GitHub or creating a VM. The runners
// loop stays free to provision queued jobs while an analysis is prepared.
func (r *githubRunners) kickCompaction(ctx context.Context) {
	if !r.compactionEnabled() || !r.compactionBusy.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer r.compactionBusy.Store(false)
		if err := r.compactDue(ctx); err != nil {
			log.Warn().Err(err).Msg("schedule GitHub CI compaction")
		}
	}()
}

func (r *githubRunners) compactDue(ctx context.Context) error {
	rows, err := r.s.repo.ListGitHubCompactions(ctx, 0)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.RunID != "" {
			if err := r.finishCompaction(ctx, row, "Analysis expired", false, nil, "", ""); err != nil {
				return err
			}
			continue
		}
		if only := r.config.CompactionRepository; only != "" && !strings.EqualFold(only, row.Repository) {
			continue
		}
		row.RunID = randomHexID()
		row.RunCommit = row.Head
		expires := time.Now().UTC().Add(compactionLifetime).Truncate(time.Second)
		row.RunExpires = &expires
		claimed, err := r.s.repo.ClaimGitHubCompaction(ctx, row)
		if err != nil {
			return err
		}
		if !claimed {
			continue
		}
		if err := r.startCompaction(ctx, row); err != nil {
			log.Warn().Err(err).Str("repository", row.Repository).Msg("start GitHub CI compaction")
			return r.finishCompaction(ctx, row, "Analysis could not start", false, nil, "", "")
		}
	}
	return nil
}

func (r *githubRunners) startCompaction(ctx context.Context, row types.GitHubCompaction) error {
	if !r.github.compactionAllowed(ctx, row.InstallationID) {
		return r.finishCompaction(ctx, row, "GitHub permissions needed", false, nil, "", "")
	}
	token, err := r.github.installationToken(ctx, row.InstallationID)
	if err != nil {
		return err
	}
	site := compactionHead{Branch: row.Branch, Commit: row.RunCommit}
	head, err := r.github.compactionRef(ctx, row, token, site.Branch)
	if err != nil {
		return err
	}
	if head != site.Commit {
		return r.finishCompaction(ctx, row, "Default branch changed", false, nil, "", "")
	}
	files, title, body, saved, err := r.compactPipeline(ctx, row, site, token)
	if err != nil {
		return err
	}
	if len(files) > 0 {
		pr, err := r.github.publishCompaction(ctx, row, token, files, title, body)
		if err != nil {
			if pr != nil {
				row.BranchSHA = pr.SHA
			}
			return r.failCompactionPublication(ctx, row, err)
		}
		return r.finishCompaction(ctx, row, "Pull request open", true, pr, saved, "")
	}
	repoToken, err := r.github.compactionCloneToken(ctx, row)
	if err != nil {
		return err
	}
	size, ok := runnerSizeFor(row.Label)
	if !ok {
		return errors.New("unsupported compaction runner size")
	}
	installation, err := r.s.repo.GetGitHubRunnerInstallationById(ctx, row.InstallationID)
	if err != nil {
		return err
	}
	if installation == nil || !installation.CICompaction || installation.Paused {
		return r.finishCompaction(ctx, row, "CI Compaction paused", false, nil, "", "")
	}
	info, err := r.runnerAuth(ctx, installation)
	if err != nil {
		return err
	}
	claims := compactionClaims{Workspace: info.Workspace.ExternalId, Run: row.RunID, Expires: row.RunExpires.Unix()}
	env := []string{
		"COMPACTION_URL=" + strings.TrimRight(r.config.CompactionURL, "/"),
		"COMPACTION_RUN_TOKEN=" + claims.sign(r.config.CompactionSecret),
		"COMPACTION_RESULT_URL=" + r.s.config.BaseURL + "/api/v1/vm/github-runners" + compactionResultRoute,
		"COMPACTION_REPO=" + row.Repository,
		"COMPACTION_CLONE_TOKEN=" + repoToken,
		"COMPACTION_COMMIT=" + row.RunCommit,
		"COMPACTION_PREVIOUS_COMMIT=" + row.LastCommit,
		"COMPACTION_ADAPTERS=" + compactionFrameworks,
	}
	return r.launchRunnerVM(ctx, info, compactionPrefix+row.RunID, row.Repository, size, env, nil)
}

func (r *githubRunners) finishCompaction(ctx context.Context, row types.GitHubCompaction, status string, processed bool, pr *compactionPR, savings, report string) error {
	row.Status, row.Savings, row.Report = status, savings, report
	if pr != nil {
		row.PRNumber, row.PRURL, row.PROpen, row.BranchSHA = pr.Number, pr.URL, true, pr.SHA
	}
	if processed {
		row.LastCommit = row.RunCommit
	}
	if processed && pr == nil {
		row.WaitWeeks = min(2*max(row.WaitWeeks, 1), 8)
	} else {
		row.WaitWeeks = 1
	}
	return r.s.repo.FinishGitHubCompaction(ctx, row, time.Now().Add(time.Duration(row.WaitWeeks)*compactionWeek))
}

func (r *githubRunners) compactionResult(c echo.Context) error {
	claims, ok := verifyCompactionToken(c.Request().Header.Get("X-Beam-Compaction-Token"), r.config.CompactionSecret, time.Now())
	if !r.compactionEnabled() || !ok {
		return echo.NewHTTPError(http.StatusUnauthorized, "invalid compaction run token")
	}
	ctx := c.Request().Context()
	var result compactionResult
	body, err := io.ReadAll(io.LimitReader(c.Request().Body, compactionMaxResult+1))
	if err != nil || len(body) > compactionMaxResult || json.Unmarshal(body, &result) != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid compaction result")
	}
	row, err := r.s.repo.AcceptGitHubCompactionResult(ctx, claims.Workspace, claims.Run, result.Commit)
	if err != nil {
		return err
	}
	if row == nil {
		return echo.NewHTTPError(http.StatusConflict, "no active compaction for this commit")
	}
	// Once claimed, finish publication even if the reporting VM disconnects.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	defer func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := r.deleteCompactionVM(ctx, *row); err != nil {
			log.Warn().Err(err).Str("repository", row.Repository).Msg("delete CI compaction VM")
		}
	}()
	report, err := json.Marshal(result.Report)
	if err != nil {
		return err
	}
	switch {
	case !validCompactionResult(result):
		if err := r.finishCompaction(ctx, *row, "Result rejected", false, nil, "", ""); err != nil {
			return err
		}
		return echo.NewHTTPError(http.StatusUnprocessableEntity, "compaction result failed verification")
	case result.Status == "failed":
		log.Warn().Str("repository", row.Repository).Str("reason", compactionText(result.Report.Reason, 240)).Msg("CI compaction VM reported a failure")
		err = r.finishCompaction(ctx, *row, "Analysis failed", false, nil, "", string(report))
	case result.Status == "unchanged":
		err = r.finishCompaction(ctx, *row, "No test changes", true, nil, "", string(report))
	case len(result.Files) == 0 || !result.Report.worthwhile():
		status := "No worthwhile change"
		if len(result.Files) == 0 && result.Report.Outcome == "unsupported" {
			status = "Test verification unavailable"
		}
		err = r.finishCompaction(ctx, *row, status, true, nil, "", string(report))
	default:
		err = r.publishTestCompaction(ctx, *row, result, string(report))
	}
	if err != nil {
		return err
	}
	return c.NoContent(http.StatusAccepted)
}

func validCompactionPath(name string, workflow bool) bool {
	if name == "." || name == "" || name != path.Clean(name) || strings.HasPrefix(name, "/") ||
		strings.ContainsAny(name, "\\:") || strings.ContainsFunc(name, unicode.IsControl) {
		return false
	}
	for _, component := range strings.Split(name, "/") {
		if component == ".." || strings.EqualFold(component, ".git") || strings.EqualFold(component, "testdata") ||
			(!workflow && strings.EqualFold(component, ".github")) {
			return false
		}
	}
	return !workflow || path.Dir(name) == ".github/workflows" && slices.Contains([]string{".yml", ".yaml"}, path.Ext(name))
}

// A reported framework selects a gateway rule. It never supplies paths or
// patterns for that rule. Only Go's naming rule excludes tests from builds, so
// the other adapters also need the framework's own discovery and verification.
func validCompactionTestPath(name, framework string) bool {
	if !validCompactionPath(name, false) {
		return false
	}
	base := path.Base(name)
	if framework == "go" {
		return strings.HasSuffix(base, "_test.go")
	}
	var inTests bool
	for _, dir := range strings.Split(path.Dir(name), "/") {
		dir = strings.ToLower(dir)
		if dir != "." && strings.HasPrefix(dir, ".") {
			return false
		}
		switch dir {
		case "__fixtures__", "__mocks__", "__pycache__", "__snapshots__", "build", "config", "coverage", "dist",
			"fixtures", "generated", "helpers", "mocks", "node_modules", "snapshots", "support", "vendor", "venv":
			return false
		case "test", "tests":
			inTests = true
		}
	}

	ext := path.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	switch framework {
	case "pytest":
		if ext != ".py" || !inTests {
			return false
		}
		if name, ok := strings.CutPrefix(stem, "test_"); ok {
			return validCompactionTestStem(strings.TrimSuffix(name, "_test"))
		}
		name, ok := strings.CutSuffix(stem, "_test")
		return ok && validCompactionTestStem(name)
	case "jest", "vitest":
		if !slices.Contains([]string{".js", ".jsx", ".ts", ".tsx", ".cjs", ".mjs", ".cts", ".mts"}, ext) {
			return false
		}
		if name, ok := strings.CutSuffix(stem, ".test"); ok {
			return validCompactionTestStem(name)
		}
		name, ok := strings.CutSuffix(stem, ".spec")
		return ok && validCompactionTestStem(name)
	}
	return false
}

func validCompactionTestStem(stem string) bool {
	stem = strings.ToLower(stem)
	switch stem {
	case "", "config", "configuration", "conftest", "fixture", "fixtures", "helper", "helpers", "mock", "mocks",
		"setup", "setup_tests", "setuptests", "snapshot", "snapshots", "support", "teardown", "test_utils", "testutils":
		return false
	}
	return !strings.HasPrefix(stem, ".") && !strings.HasSuffix(stem, ".config")
}

func validCompactionResult(result compactionResult) bool {
	if !compactionSHA.MatchString(result.Commit) {
		return false
	}
	switch result.Status {
	case "complete":
	case "failed", "unchanged":
		return len(result.Files) == 0
	default:
		return false
	}
	if len(result.Files) == 0 {
		return true
	}
	rep := result.Report
	if len(result.Files) > 8 || len(rep.Groups) > 8 || rep.Caught <= 0 || rep.Preserved != rep.Caught ||
		rep.CoveredBefore <= 0 || rep.CoveredAfter < rep.CoveredBefore || rep.Coverable < rep.CoveredAfter ||
		rep.BeforeMS <= 0 || rep.AfterMS <= 0 || rep.SpreadMS < 0 ||
		rep.TestsBefore <= rep.TestsAfter || rep.TestsAfter < 1 || rep.LinesBefore < 1 || rep.LinesAfter < 1 {
		return false
	}
	verified := make(map[string]string, len(result.Files))
	for _, group := range rep.Groups {
		if group.Status != "merged" {
			continue
		}
		framework := group.Framework
		if framework == "" {
			framework = "go" // Reports from existing analysis VMs only contain Go test edits.
		}
		if group.NewTest == "" || len(group.Cases) < 2 || len(group.Cases) > 6 || !validCompactionTestPath(group.File, framework) {
			return false
		}
		if _, changed := result.Files[group.File]; !changed {
			return false
		}
		if previous := verified[group.File]; previous != "" && previous != framework {
			return false
		}
		verified[group.File] = framework
	}
	for name, data := range result.Files {
		if verified[name] == "" || len(data) == 0 || len(data) > compactionMaxFile {
			return false
		}
	}
	return true
}

func (r compactionReport) savingMS() int64 {
	saving := r.BeforeMS - r.AfterMS
	if saving <= r.SpreadMS {
		return 0
	}
	return saving
}

func (r compactionReport) worthwhile() bool {
	saving := r.savingMS()
	return r.LinesBefore-r.LinesAfter >= compactionMinimumLines ||
		(saving >= compactionMinimumTime.Milliseconds() && float64(saving)*100 >= compactionMinimumPercent*float64(r.BeforeMS))
}

func (r *githubRunners) publishTestCompaction(ctx context.Context, row types.GitHubCompaction, result compactionResult, report string) error {
	token, err := r.github.installationToken(ctx, row.InstallationID)
	if err != nil {
		return r.failCompactionPublication(ctx, row, err)
	}
	runs, err := r.s.repo.CountGitHubCompactionRuns(ctx, row.WorkspaceID, row.Repository, time.Now().Add(-30*24*time.Hour))
	if err != nil {
		return r.failCompactionPublication(ctx, row, err)
	}
	title, body, savings := result.Report.message(row.Label, runs)
	pr, err := r.github.publishCompaction(ctx, row, token, result.Files, title, body)
	if err != nil {
		if pr != nil {
			row.BranchSHA = pr.SHA
		}
		return r.failCompactionPublication(ctx, row, err)
	}
	return r.finishCompaction(ctx, row, "Pull request open", true, pr, savings, report)
}

func (r *githubRunners) failCompactionPublication(ctx context.Context, row types.GitHubCompaction, cause error) error {
	status := "Could not open pull request"
	if errors.Is(cause, errCompactionStale) {
		status = "Default branch changed"
	}
	log.Warn().Err(cause).Str("repository", row.Repository).Msg("publish CI compaction")
	return r.finishCompaction(ctx, row, status, false, nil, "", "")
}

func (r compactionReport) message(label string, runs int) (string, string, string) {
	title := fmt.Sprintf("CI Compaction: %d tests → %d, coverage preserved", r.TestsBefore, r.TestsAfter)
	lines := r.LinesBefore - r.LinesAfter
	saving := r.savingMS()
	savings := fmt.Sprintf("%d test lines removed", lines)
	timeSummary := "no measurable change"
	if saving > 0 {
		timeSummary = fmt.Sprintf("%s saved (%.1f%%)", compactionTime(saving), float64(saving)*100/float64(r.BeforeMS))
		savings = timeSummary
		if lines > 0 {
			savings += fmt.Sprintf(" · %d test lines removed", lines)
		}
	}
	var body strings.Builder
	fmt.Fprintf(&body, "**Test time** %s → %s (%s), %s, median of 3 runs.\n\n", compactionTime(r.BeforeMS), compactionTime(r.AfterMS), timeSummary, label)
	fmt.Fprintf(&body, "**Test code** %d → %d lines (%+d).\n\n", r.LinesBefore, r.LinesAfter, -lines)
	fmt.Fprintf(&body, "**Coverage** %.1f%% → %.1f%% of measured executable lines. Every previously covered line is still covered.\n\n",
		float64(r.CoveredBefore)*100/float64(r.Coverable), float64(r.CoveredAfter)*100/float64(r.Coverable))
	fmt.Fprintf(&body, "**Mutations** %d of %d previously caught case checks are still caught.\n\n", r.Preserved, r.Caught)
	if saving > 0 {
		fmt.Fprintf(&body, "**Projected** %d runs in the last 30 days × %s = %.2f runner-hours per month.\n\n",
			runs, compactionTime(saving), float64(runs)*float64(saving)/float64(time.Hour.Milliseconds()))
	}
	for _, group := range r.Groups {
		if group.Status != "merged" {
			continue
		}
		fmt.Fprintf(&body, "**%s**\n\n", compactionText(group.File, 200))
		for _, c := range group.Cases {
			fmt.Fprintf(&body, "- %s → %s/%s. %s\n", compactionText(c.OriginalTest, 100), compactionText(group.NewTest, 100),
				compactionText(c.Name, 100), compactionText(strings.Join(c.Checks, "; "), 320))
		}
		body.WriteString("\n")
	}
	body.WriteString("To turn this off, open **GitHub Runners** in your Beam workspace and disable **CI Compaction**.\n")
	return title, body.String(), savings
}

func compactionTime(ms int64) time.Duration {
	return time.Duration(min(ms, math.MaxInt64/int64(time.Millisecond))) * time.Millisecond
}

// Repository text appears as plain text, without creating mentions or links.
func compactionText(text string, limit int) string {
	out := []rune(strings.Join(strings.Fields(text), " "))
	return strings.NewReplacer("@", "＠", "<", "‹", ">", "›", "[", "［", "]", "］", "`", "′").Replace(string(out[:min(limit, len(out))]))
}

func (r *githubRunners) deleteCompactionVM(ctx context.Context, row types.GitHubCompaction) error {
	v, err := r.s.repo.GetVM(ctx, row.WorkspaceID, compactionPrefix+row.RunID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	unlock, err := r.s.repo.LockVM(ctx, v.ID)
	if err != nil {
		return err
	}
	defer unlock()
	v, err = r.s.repo.GetVM(ctx, row.WorkspaceID, v.ID)
	if err != nil || v.DesiredState == types.VMStateDeleted {
		return err
	}
	v.DesiredState = types.VMStateDeleted
	return r.s.repo.SaveVM(ctx, v)
}

func (r *githubRunners) reconcileCompaction(ctx context.Context, v *types.VM) (bool, error) {
	state, err := r.s.runtimeState(v)
	if err != nil {
		return true, err
	}
	exited := state == nil && v.ContainerID != "" || v.Status == types.VMStateError && v.LaunchAttempts >= 5
	if !exited && v.DesiredState == types.VMStateRunning && time.Since(v.CreatedAt) < compactionLifetime {
		return true, nil
	}
	v.DesiredState = types.VMStateDeleted
	return true, r.s.repo.SaveVM(ctx, v)
}

func (g *githubApp) compactionCall(ctx context.Context, row types.GitHubCompaction, token, method, suffix string, input, output any) (http.Header, error) {
	return g.ask(ctx, row.InstallationID, method, g.apiURL+"/repos/"+row.Repository+suffix, token, input, output)
}

func (g *githubApp) compactionRef(ctx context.Context, row types.GitHubCompaction, token, branch string) (string, error) {
	var ref struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	_, err := g.compactionCall(ctx, row, token, http.MethodGet, "/git/ref/heads/"+strings.ReplaceAll(url.PathEscape(branch), "%2F", "/"), nil, &ref)
	return ref.Object.SHA, err
}

type compactionPR struct {
	Number int    `json:"number"`
	URL    string `json:"html_url"`
	SHA    string `json:"-"`
	Head   struct {
		SHA string `json:"sha"`
	} `json:"head"`
}

type compactionTreeEntry struct {
	Path    string `json:"path"`
	Mode    string `json:"mode"`
	Type    string `json:"type"`
	Content string `json:"content"`
}

func (g *githubApp) publishCompaction(ctx context.Context, row types.GitHubCompaction, token string, files map[string]string, title, body string) (*compactionPR, error) {
	owner, _, _ := strings.Cut(row.Repository, "/")
	var open []compactionPR
	_, err := g.compactionCall(ctx, row, token, http.MethodGet, "/pulls?state=open&head="+url.QueryEscape(owner+":"+compactionBranch), nil, &open)
	if err != nil {
		return nil, err
	}
	if len(open) > 0 {
		open[0].SHA = open[0].Head.SHA
		return &open[0], nil
	}
	current, err := g.compactionRef(ctx, row, token, row.Branch)
	if err != nil {
		return nil, err
	}
	if current != row.RunCommit {
		return nil, errCompactionStale
	}
	var base struct {
		Tree struct {
			SHA string `json:"sha"`
		} `json:"tree"`
	}
	if _, err := g.compactionCall(ctx, row, token, http.MethodGet, "/git/commits/"+current, nil, &base); err != nil {
		return nil, err
	}
	if err := g.checkCompactionPaths(ctx, row, token, base.Tree.SHA, files); err != nil {
		return nil, err
	}
	return g.commitCompaction(ctx, row, token, base.Tree.SHA, files, title, body)
}

func (g *githubApp) commitCompaction(ctx context.Context, row types.GitHubCompaction, token, baseTree string, files map[string]string, title, body string) (*compactionPR, error) {
	branchSHA, err := g.compactionRef(ctx, row, token, compactionBranch)
	if err != nil && githubStatus(err) != http.StatusNotFound {
		return nil, err
	}
	if branchSHA != "" && branchSHA != row.BranchSHA {
		return nil, errors.New("the CI Compaction branch already exists")
	}
	entries := make([]compactionTreeEntry, 0, len(files))
	for name, content := range files {
		entries = append(entries, compactionTreeEntry{Path: name, Mode: "100644", Type: "blob", Content: content})
	}
	slices.SortFunc(entries, func(a, b compactionTreeEntry) int { return strings.Compare(a.Path, b.Path) })
	var tree, commit struct {
		SHA string `json:"sha"`
	}
	if _, err := g.compactionCall(ctx, row, token, http.MethodPost, "/git/trees",
		map[string]any{"base_tree": baseTree, "tree": entries}, &tree); err != nil {
		return nil, err
	}
	if tree.SHA == baseTree {
		return nil, errors.New("compaction would create an empty diff")
	}
	if _, err := g.compactionCall(ctx, row, token, http.MethodPost, "/git/commits",
		map[string]any{"message": title, "tree": tree.SHA, "parents": []string{row.RunCommit}}, &commit); err != nil {
		return nil, err
	}
	// Avoid applying a result to a branch that advanced during publication.
	current, err := g.compactionRef(ctx, row, token, row.Branch)
	if err != nil {
		return nil, err
	}
	if current != row.RunCommit {
		return nil, errCompactionStale
	}
	ref := "refs/heads/" + compactionBranch
	method, endpoint := http.MethodPost, "/git/refs"
	input := map[string]any{"ref": ref, "sha": commit.SHA}
	if branchSHA != "" {
		method, endpoint = http.MethodPatch, "/git/"+ref
		input = map[string]any{"sha": commit.SHA, "force": true}
	}
	if _, err := g.compactionCall(ctx, row, token, method, endpoint, input, nil); err != nil {
		return nil, err
	}
	pr := &compactionPR{SHA: commit.SHA}
	_, err = g.compactionCall(ctx, row, token, http.MethodPost, "/pulls",
		map[string]string{"title": title, "body": body, "head": compactionBranch, "base": row.Branch}, pr)
	return pr, err
}

// Check each path against GitHub's existing tree. In particular, a parent
// directory or a test file may be a symlink in the analyzed branch.
func (g *githubApp) checkCompactionPaths(ctx context.Context, row types.GitHubCompaction, token, baseTree string, files map[string]string) error {
	var tree struct {
		Entries   []compactionTreeEntry `json:"tree"`
		Truncated bool                  `json:"truncated"`
	}
	if _, err := g.compactionCall(ctx, row, token, http.MethodGet, "/git/trees/"+baseTree+"?recursive=1", nil, &tree); err != nil {
		return err
	}
	if tree.Truncated {
		return errors.New("could not verify the repository tree")
	}
	remaining := len(files)
	for _, entry := range tree.Entries {
		if _, ok := files[entry.Path]; ok && entry.Type == "blob" && entry.Mode == "100644" {
			remaining--
		}
	}
	if remaining != 0 {
		return errors.New("compaction may only edit an existing regular file")
	}
	return nil
}

const pipelineHistoryDays = 30

const pipelineBranchPush = "github.event_name == 'push' && github.ref_type == 'branch' && github.event.repository.default_branch != '' && github.ref_name != github.event.repository.default_branch"

// workflow_ref includes the workflow path and the triggering ref. Give exempt
// runs their own groups because GitHub replaces pending runs even when it
// doesn't cancel the one already in progress.
const pipelineConcurrency = "concurrency:\n" +
	"  group: ${{ " + pipelineBranchPush + " && format('beam-ci:{0}', github.workflow_ref) || format('beam-ci-run:{0}', github.run_id) }}\n" +
	"  cancel-in-progress: ${{ " + pipelineBranchPush + " }}\n"

type pipelineRun struct {
	githubRun
	ID         int64     `json:"id"`
	WorkflowID int64     `json:"workflow_id"`
	Number     int       `json:"run_number"`
	Attempt    int       `json:"run_attempt"`
	Path       string    `json:"path"`
	SHA        string    `json:"head_sha"`
	Status     string    `json:"status"`
	Conclusion string    `json:"conclusion"`
	Started    time.Time `json:"run_started_at"`
	Updated    time.Time `json:"updated_at"`
}

type pipelineRunGroup struct {
	workflow int64
	file     string
	branch   string
}

type pipelineSavings struct {
	runs, superseded int
	runtime, wasted  time.Duration
}

func (s pipelineSavings) worthChanging() bool {
	return s.wasted >= compactionMinimumTime && s.wasted >= s.runtime*compactionMinimumPercent/100
}

func (r *githubRunners) compactPipeline(ctx context.Context, row types.GitHubCompaction, site compactionHead, token string) (map[string]string, string, string, string, error) {
	until := time.Now().UTC().Truncate(time.Second)
	repo := r.github.apiURL + "/repos/" + row.Repository
	runs, err := r.github.pipelineRuns(ctx, row.InstallationID, repo, token, until.AddDate(0, 0, -pipelineHistoryDays), until)
	if err != nil {
		return nil, "", "", "", err
	}

	// REST run history gives a name that can denote either a branch or a tag.
	// Only count savings on refs that GitHub can still identify as branches.
	savings := map[string]pipelineSavings{}
	branches := map[string]bool{}
	for group, stats := range pipelineOverlaps(runs, row.Repository, site.Branch) {
		total := savings[group.file]
		total.runtime += stats.runtime
		total.runs += stats.runs
		if stats.wasted > 0 {
			branch, checked := branches[group.branch]
			if !checked {
				branch, err = r.github.pipelineBranch(ctx, row.InstallationID, repo, token, group.branch)
				if err != nil {
					return nil, "", "", "", err
				}
				branches[group.branch] = branch
			}
			if branch {
				total.superseded += stats.superseded
				total.wasted += stats.wasted
			}
		}
		savings[group.file] = total
	}

	files := map[string]string{}
	var paths []string
	for file, stats := range savings {
		if stats.worthChanging() {
			paths = append(paths, file)
		}
	}
	slices.Sort(paths)

	var body strings.Builder
	var wasted time.Duration
	body.WriteString("CI Compaction found push runs that kept going after a newer push on the same branch started.\n\n")
	body.WriteString("| Workflow | Superseded push runs | Elapsed time after a newer run started |\n")
	body.WriteString("| --- | ---: | ---: |\n")
	for _, file := range paths {
		original, err := r.github.pipelineFile(ctx, row.InstallationID, repo, token, file, site.Commit)
		if err != nil {
			return nil, "", "", "", err
		}
		update, ok := addPipelineConcurrency(original)
		if !ok {
			continue
		}
		files[file] = update
		stats := savings[file]
		wasted += stats.wasted
		fmt.Fprintf(&body, "| `%s` | %d of %d | %s |\n", file, stats.superseded, stats.runs, stats.wasted)
	}
	if len(files) == 0 {
		return nil, "", "", "", nil
	}
	fmt.Fprintf(&body, "\nFigures use GitHub Actions workflow timestamps for the last %d days. Each change adds workflow concurrency for non-default branch pushes.\n", pipelineHistoryDays)
	body.WriteString("\nTurn off CI Compaction in the workspace's GitHub runners settings.\n")
	saved := fmt.Sprintf("%s of superseded workflow time", wasted)
	return files, "CI Compaction: cancel superseded workflow runs", body.String(), saved, nil
}

func (g *githubApp) pipelineRuns(ctx context.Context, installation int64, repo, token string, from, until time.Time) ([]pipelineRun, error) {
	query := url.Values{
		"event":    {"push"},
		"created":  {from.Format(time.RFC3339) + ".." + until.Format(time.RFC3339)},
		"per_page": {"100"},
	}
	var runs []pipelineRun
	for page := repo + "/actions/runs?" + query.Encode(); page != ""; {
		var result struct {
			Total int           `json:"total_count"`
			Runs  []pipelineRun `json:"workflow_runs"`
		}
		header, err := g.ask(ctx, installation, http.MethodGet, page, token, nil, &result)
		if err != nil {
			return nil, err
		}
		// GitHub returns at most 1,000 runs for a filtered query. Split the
		// time range at second precision to keep the full history.
		if result.Total >= 1000 {
			if !from.Before(until) {
				return nil, fmt.Errorf("more workflow runs than GitHub can list at %s", from.Format(time.RFC3339))
			}
			mid := from.Add(until.Sub(from) / 2).Truncate(time.Second)
			early, err := g.pipelineRuns(ctx, installation, repo, token, from, mid)
			if err != nil {
				return nil, err
			}
			late, err := g.pipelineRuns(ctx, installation, repo, token, mid.Add(time.Second), until)
			return append(early, late...), err
		}
		runs = append(runs, result.Runs...)
		page = nextPage(header.Get("Link"))
	}
	return runs, nil
}

func pipelineOverlaps(runs []pipelineRun, repo, defaultBranch string) map[pipelineRunGroup]pipelineSavings {
	groups := map[pipelineRunGroup][]pipelineRun{}
	seen := map[int64]bool{}
	for _, run := range runs {
		branch := run.HeadBranch
		if run.ID == 0 || seen[run.ID] || run.WorkflowID == 0 || run.Number == 0 || run.Attempt != 1 ||
			run.Event != "push" || !strings.EqualFold(run.HeadRepository.FullName, repo) ||
			branch == "" || strings.EqualFold(branch, defaultBranch) || run.SHA == "" || run.Started.IsZero() ||
			(run.Status != "in_progress" && (run.Status != "completed" || run.Conclusion == "skipped" || !run.Updated.After(run.Started))) {
			continue
		}
		file := pipelineWorkflowPath(run.Path, branch)
		if file == "" {
			continue
		}
		seen[run.ID] = true
		group := pipelineRunGroup{workflow: run.WorkflowID, file: file, branch: branch}
		groups[group] = append(groups[group], run)
	}

	result := map[pipelineRunGroup]pipelineSavings{}
	for group, runs := range groups {
		slices.SortFunc(runs, func(a, b pipelineRun) int { return a.Started.Compare(b.Started) })
		var savings pipelineSavings
		for index, old := range runs {
			if old.Status != "completed" {
				continue
			}
			savings.runs++
			savings.runtime += old.Updated.Sub(old.Started)
			for _, next := range runs[index+1:] {
				if !next.Started.Before(old.Updated) {
					break
				}
				if next.Number > old.Number && next.SHA != old.SHA && next.Started.After(old.Started) {
					savings.superseded++
					savings.wasted += old.Updated.Sub(next.Started)
					break
				}
			}
		}
		result[group] = savings
	}
	return result
}

func pipelineWorkflowPath(file, branch string) string {
	if strings.HasSuffix(file, "@refs/tags/"+branch) {
		return ""
	}
	file = strings.TrimSuffix(strings.TrimSuffix(file, "@refs/heads/"+branch), "@"+branch)
	if !validCompactionPath(file, true) || strings.ContainsAny(file, "`|") {
		return ""
	}
	return file
}

func (g *githubApp) pipelineBranch(ctx context.Context, installation int64, repo, token, branch string) (bool, error) {
	for _, kind := range []string{"heads", "tags"} {
		ref := kind + "/" + branch
		_, err := g.ask(ctx, installation, http.MethodGet, repo+"/git/ref/"+kind+"/"+url.PathEscape(branch), token, nil, nil)
		if githubStatus(err) == http.StatusNotFound {
			return kind == "tags", nil
		}
		if err != nil {
			return false, fmt.Errorf("check %s: %w", ref, err)
		}
	}
	return false, nil
}

func (g *githubApp) pipelineFile(ctx context.Context, installation int64, repo, token, file, commit string) (string, error) {
	var result struct {
		Type     string `json:"type"`
		Encoding string `json:"encoding"`
		Content  string `json:"content"`
	}
	endpoint := repo + "/contents/.github/workflows/" + url.PathEscape(path.Base(file)) + "?ref=" + url.QueryEscape(commit)
	_, err := g.ask(ctx, installation, http.MethodGet, endpoint, token, nil, &result)
	if githubStatus(err) == http.StatusNotFound {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if result.Type != "file" || result.Encoding != "base64" {
		return "", nil
	}
	content, err := base64.StdEncoding.DecodeString(result.Content)
	return string(content), err
}

// addPipelineConcurrency preserves the original bytes. Verify the appended
// block still lives at the root of one workflow, with no existing concurrency.
func addPipelineConcurrency(source string) (string, bool) {
	root := pipelineMapping(source)
	if root == nil || pipelineHasConcurrency(root) {
		return "", false
	}
	eol := "\n"
	if strings.Contains(source, "\r\n") && !strings.Contains(strings.ReplaceAll(source, "\r\n", ""), "\n") {
		eol = "\r\n"
	}
	updated := source
	if !strings.HasSuffix(updated, "\n") {
		updated += eol
	}
	updated += strings.ReplaceAll(pipelineConcurrency, "\n", eol)
	appended := pipelineMapping(updated)
	if appended == nil || len(appended.Content) != len(root.Content)+2 || appended.Content[len(root.Content)].Value != "concurrency" {
		return "", false
	}
	return updated, true
}

func pipelineMapping(source string) *yaml.Node {
	decoder := yaml.NewDecoder(strings.NewReader(source))
	var node, extra yaml.Node
	if err := decoder.Decode(&node); err != nil || len(node.Content) != 1 || decoder.Decode(&extra) != io.EOF {
		return nil
	}
	root := node.Content[0]
	if root.Kind != yaml.MappingNode || root.Style&yaml.FlowStyle != 0 || root.Tag != "!!map" || len(root.Content) == 0 {
		return nil
	}
	return root
}

func pipelineHasConcurrency(root *yaml.Node) bool {
	jobs := false
	for i := 0; i < len(root.Content); i += 2 {
		key, value := root.Content[i], root.Content[i+1]
		if key.Kind != yaml.ScalarNode || key.Value == "<<" || key.Value == "concurrency" {
			return true
		}
		if key.Value != "jobs" {
			continue
		}
		if jobs || value.Kind != yaml.MappingNode || len(value.Content) == 0 {
			return true
		}
		jobs = true
		for j := 0; j < len(value.Content); j += 2 {
			job, config := value.Content[j], value.Content[j+1]
			if job.Kind != yaml.ScalarNode || job.Value == "<<" || config.Kind != yaml.MappingNode {
				return true
			}
			for k := 0; k < len(config.Content); k += 2 {
				key := config.Content[k]
				if key.Kind != yaml.ScalarNode || key.Value == "<<" || key.Value == "concurrency" {
					return true
				}
			}
		}
	}
	return !jobs
}
