package vm

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

// githubApp calls GitHub as the App that connects accounts to Beam: as the
// App itself, as one of its installations, or for a user who authorized it.
type githubApp struct {
	id           string
	slug         string
	clientID     string
	clientSecret string
	key          *rsa.PrivateKey
	client       *http.Client
	apiURL       string
	webURL       string

	// What is remembered of each installation: the token that authenticates
	// as it, and when GitHub next takes its calls, if it was told to wait.
	lock      sync.Mutex
	tokens    map[int64]githubToken
	notBefore map[int64]time.Time
	// The runners GitHub last listed of each repository (see runners).
	listed map[string]githubRunnerList
}

type githubToken struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type githubAccount struct {
	Login string `json:"login"`
	Type  string `json:"type"`
}

type githubInstallation struct {
	ID          int64         `json:"id"`
	Account     githubAccount `json:"account"`
	SuspendedAt *time.Time    `json:"suspended_at"`
}

type githubDelivery struct {
	ID          int64     `json:"id"`
	GUID        string    `json:"guid"`
	DeliveredAt time.Time `json:"delivered_at"`
	StatusCode  int       `json:"status_code"`
}

// githubError is a response GitHub refused, with the reason it gave.
type githubError struct {
	Status  int
	Message string
}

func (e *githubError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("GitHub returned HTTP %d", e.Status)
	}

	return fmt.Sprintf("GitHub returned HTTP %d: %s", e.Status, e.Message)
}

// githubStatus is the HTTP status of a response GitHub refused, or zero.
func githubStatus(err error) int {
	var e *githubError
	if errors.As(err, &e) {
		return e.Status
	}

	return 0
}

// githubLimit is a call that GitHub refused, or that was not made, because
// GitHub limits how much an installation asks of it. Until is when it takes
// the installation's calls again.
type githubLimit struct {
	Until time.Time
}

func (e *githubLimit) Error() string {
	return "GitHub is limiting requests until " + e.Until.UTC().Format(time.RFC3339)
}

var errAppKey = errors.New("privateKey must be the App's private key, PEM encoded")

func newGitHubApp(config types.GitHubRunnerConfig) (*githubApp, error) {
	// An environment variable carries the key's line breaks escaped.
	block, _ := pem.Decode([]byte(strings.ReplaceAll(config.PrivateKey, `\n`, "\n")))
	if block == nil {
		return nil, errAppKey
	}

	// GitHub generates the key as PKCS #1. Tools that convert or store keys
	// write PKCS #8.
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		converted, _ := x509.ParsePKCS8PrivateKey(block.Bytes)
		if key, _ = converted.(*rsa.PrivateKey); key == nil {
			return nil, errAppKey
		}
	}

	return &githubApp{
		id:           config.AppID,
		slug:         config.AppSlug,
		clientID:     config.ClientID,
		clientSecret: config.ClientSecret,
		key:          key,
		client:       &http.Client{Timeout: 20 * time.Second},
		apiURL:       "https://api.github.com",
		webURL:       "https://github.com",
		tokens:       map[int64]githubToken{},
		notBefore:    map[int64]time.Time{},
	}, nil
}

// installURL is where a user installs the App on an account, or changes the
// repositories of one that has it.
func (g *githubApp) installURL(state string) string {
	return g.webURL + "/apps/" + url.PathEscape(g.slug) + "/installations/new?state=" + url.QueryEscape(state)
}

// authorizeURL is where a user tells the App who they are.
func (g *githubApp) authorizeURL(state string) string {
	return g.webURL + "/login/oauth/authorize?client_id=" + url.QueryEscape(g.clientID) + "&state=" + url.QueryEscape(state)
}

// authorizedUser returns the login of the user who authorized the App, from
// the code GitHub sent them back with, and a token that acts for them. Both
// are empty for a code GitHub rejects.
func (g *githubApp) authorizedUser(ctx context.Context, code string) (login, token string, err error) {
	var grant struct {
		AccessToken string `json:"access_token"`
	}
	_, err = g.call(ctx, http.MethodPost, g.webURL+"/login/oauth/access_token", "",
		map[string]string{"client_id": g.clientID, "client_secret": g.clientSecret, "code": code}, &grant)
	if err != nil || grant.AccessToken == "" {
		return "", "", err
	}

	var user githubAccount
	if _, err = g.call(ctx, http.MethodGet, g.apiURL+"/user", grant.AccessToken, nil, &user); err != nil {
		return "", "", err
	}

	return user.Login, grant.AccessToken, nil
}

// userInstallations returns the installations of the App that a user can
// reach, which are those on their own account and on organizations they
// belong to. GitHub lists them a page at a time.
func (g *githubApp) userInstallations(ctx context.Context, token string) ([]githubInstallation, error) {
	var installations []githubInstallation
	for page := g.apiURL + "/user/installations?per_page=100"; page != ""; {
		var listed struct {
			Installations []githubInstallation `json:"installations"`
		}
		header, err := g.call(ctx, http.MethodGet, page, token, nil, &listed)
		if err != nil {
			return nil, err
		}

		installations = append(installations, listed.Installations...)
		page = nextPage(header.Get("Link"))
	}

	return installations, nil
}

func (g *githubApp) installation(ctx context.Context, id int64) (*githubInstallation, error) {
	jwt, err := g.jwt()
	if err != nil {
		return nil, err
	}

	var installation githubInstallation
	if _, err := g.call(ctx, http.MethodGet, fmt.Sprintf("%s/app/installations/%d", g.apiURL, id), jwt, nil, &installation); err != nil {
		return nil, err
	}

	return &installation, nil
}

// organizationRole returns a user's role in an organization that installed
// the App, which is empty for a user who is not an active member of it.
func (g *githubApp) organizationRole(ctx context.Context, installation int64, organization, login string) (string, error) {
	token, err := g.installationToken(ctx, installation)
	if err != nil {
		return "", err
	}

	var membership struct {
		State string `json:"state"`
		Role  string `json:"role"`
	}
	endpoint := g.apiURL + "/orgs/" + url.PathEscape(organization) + "/memberships/" + url.PathEscape(login)
	_, err = g.ask(ctx, installation, http.MethodGet, endpoint, token, nil, &membership)
	if githubStatus(err) == http.StatusNotFound || err == nil && membership.State != "active" {
		return "", nil
	}

	return membership.Role, err
}

// runnerWorkFolder is where GitHub's own runners keep a job's files. Paths
// under it end up in workflows, caches and build outputs written for them.
const runnerWorkFolder = "/home/runner/work"

// registerRunner registers a runner that takes one job of a repository, and
// returns the configuration it starts with. It joins the default runner
// group, which every repository of an account can use.
func (g *githubApp) registerRunner(ctx context.Context, installation int64, repository, name, label string) (string, error) {
	token, err := g.installationToken(ctx, installation)
	if err != nil {
		return "", err
	}

	var runner struct {
		Config string `json:"encoded_jit_config"`
	}
	_, err = g.ask(ctx, installation, http.MethodPost, g.apiURL+"/repos/"+repository+"/actions/runners/generate-jitconfig", token,
		map[string]any{"name": name, "runner_group_id": 1, "labels": []string{label}, "work_folder": runnerWorkFolder}, &runner)
	return runner.Config, err
}

// githubRunner is a runner as GitHub lists it: online once it has connected
// to take a job, and busy from when GitHub gives it one.
type githubRunner struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Busy   bool   `json:"busy"`
	// When a runner that is free was first listed so. A runner takes one
	// job and is gone, so it has been free ever since.
	FreeSince time.Time `json:"-"`
}

// free reports whether GitHub could give the runner a job at once.
func (r githubRunner) free() bool {
	return r.Status == "online" && !r.Busy
}

// githubRunnerList is the runners of a repository as GitHub listed them at a
// time, by name.
type githubRunnerList struct {
	at      time.Time
	runners map[string]githubRunner
}

// How long a list of a repository's runners is used for. Every idle runner
// of the repository asks for it on each pass over the VMs, so the list is
// read once for all of them, and a few times a minute at most.
const githubRunnerListAge = 15 * time.Second

// runners lists the runners registered in a repository, by name, as GitHub
// has them now or had them a few seconds ago. What GitHub did since is found
// out when it is asked to remove one: it refuses a runner that got a job.
func (g *githubApp) runners(ctx context.Context, installation int64, repository string) (map[string]githubRunner, error) {
	key := fmt.Sprintf("%d/%s", installation, repository)
	g.lock.Lock()
	cached, ok := g.listed[key]
	g.lock.Unlock()
	if ok && time.Since(cached.at) < githubRunnerListAge {
		return cached.runners, nil
	}

	token, err := g.installationToken(ctx, installation)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	runners := map[string]githubRunner{}
	for page := g.apiURL + "/repos/" + repository + "/actions/runners?per_page=100"; page != ""; {
		var listed struct {
			Runners []githubRunner `json:"runners"`
		}
		header, err := g.ask(ctx, installation, http.MethodGet, page, token, nil, &listed)
		if err != nil {
			return nil, err
		}

		for _, runner := range listed.Runners {
			if before := cached.runners[runner.Name]; runner.free() {
				runner.FreeSince = now
				if before.free() {
					runner.FreeSince = before.FreeSince
				}
			}

			runners[runner.Name] = runner
		}
		page = nextPage(header.Get("Link"))
	}

	g.lock.Lock()
	if g.listed == nil {
		g.listed = map[string]githubRunnerList{}
	}
	g.listed[key] = githubRunnerList{at: now, runners: runners}
	g.lock.Unlock()
	return runners, nil
}

// removeRunner unregisters a runner that has no job. It reports false when
// GitHub refuses, which it does once it has given the runner a job.
func (g *githubApp) removeRunner(ctx context.Context, installation int64, repository, name string) (bool, error) {
	token, err := g.installationToken(ctx, installation)
	if err != nil {
		return false, err
	}

	var registered struct {
		Runners []githubRunner `json:"runners"`
	}
	endpoint := g.apiURL + "/repos/" + repository + "/actions/runners"
	_, err = g.ask(ctx, installation, http.MethodGet, endpoint+"?name="+url.QueryEscape(name), token, nil, &registered)
	if err == nil {
		for _, runner := range registered.Runners {
			if _, err = g.ask(ctx, installation, http.MethodDelete, fmt.Sprintf("%s/%d", endpoint, runner.ID), token, nil, nil); err != nil {
				break
			}
		}
	}

	return removedRunner(err)
}

// deleteRunner unregisters a runner that GitHub listed, as removeRunner does
// one it has yet to find.
func (g *githubApp) deleteRunner(ctx context.Context, installation int64, repository string, runner githubRunner) (bool, error) {
	token, err := g.installationToken(ctx, installation)
	if err != nil {
		return false, err
	}

	endpoint := fmt.Sprintf("%s/repos/%s/actions/runners/%d", g.apiURL, repository, runner.ID)
	_, err = g.ask(ctx, installation, http.MethodDelete, endpoint, token, nil, nil)
	return removedRunner(err)
}

// removedRunner reads GitHub's answer to removing a runner: it refuses one
// that it has given a job, and one that it no longer has is removed.
func removedRunner(err error) (bool, error) {
	switch githubStatus(err) {
	case http.StatusUnprocessableEntity:
		return false, nil
	case http.StatusNotFound:
		return true, nil
	}

	return err == nil, err
}

// cancelRun cancels a workflow run, and with it every job of the run, as
// GitHub cancels no job on its own. A run that has already finished, which
// GitHub refuses to cancel, is as cancelled as it can be.
func (g *githubApp) cancelRun(ctx context.Context, installation int64, repository string, run int64) error {
	token, err := g.installationToken(ctx, installation)
	if err != nil {
		return err
	}

	_, err = g.ask(ctx, installation, http.MethodPost, fmt.Sprintf("%s/repos/%s/actions/runs/%d/cancel", g.apiURL, repository, run), token, nil, nil)
	if githubStatus(err) == http.StatusConflict {
		return nil
	}

	return err
}

// githubRun is a workflow run, as far as the trust in what its jobs leave
// behind depends on it: what started it, which branch of which repository
// its code is from, and that repository's default branch.
type githubRun struct {
	Event          string `json:"event"`
	HeadBranch     string `json:"head_branch"`
	HeadRepository struct {
		FullName string `json:"full_name"`
	} `json:"head_repository"`
	DefaultBranch string `json:"-"`
}

// run returns a workflow run of a repository, as GitHub has it, rather than
// as a job says of itself.
func (g *githubApp) run(ctx context.Context, installation int64, repository string, run int64) (*githubRun, error) {
	token, err := g.installationToken(ctx, installation)
	if err != nil {
		return nil, err
	}

	var found githubRun
	if _, err := g.ask(ctx, installation, http.MethodGet, fmt.Sprintf("%s/repos/%s/actions/runs/%d", g.apiURL, repository, run), token, nil, &found); err != nil {
		return nil, err
	}

	var repo struct {
		DefaultBranch string `json:"default_branch"`
	}
	if _, err := g.ask(ctx, installation, http.MethodGet, g.apiURL+"/repos/"+repository, token, nil, &repo); err != nil {
		return nil, err
	}

	found.DefaultBranch = repo.DefaultBranch
	return &found, nil
}

// deliveries returns a page of the App's webhook deliveries, newest first,
// and the URL of the page after it, if there is one. An empty URL asks for
// the first page.
func (g *githubApp) deliveries(ctx context.Context, page string) ([]githubDelivery, string, error) {
	jwt, err := g.jwt()
	if err != nil {
		return nil, "", err
	}

	if page == "" {
		page = g.apiURL + "/app/hook/deliveries?per_page=100"
	}

	var deliveries []githubDelivery
	header, err := g.call(ctx, http.MethodGet, page, jwt, nil, &deliveries)
	if err != nil {
		return nil, "", err
	}

	return deliveries, nextPage(header.Get("Link")), nil
}

func (g *githubApp) redeliver(ctx context.Context, delivery int64) error {
	jwt, err := g.jwt()
	if err != nil {
		return err
	}

	_, err = g.call(ctx, http.MethodPost, fmt.Sprintf("%s/app/hook/deliveries/%d/attempts", g.apiURL, delivery), jwt, nil, nil)
	return err
}

// nextPage reads the URL of the next page from a Link header.
func nextPage(link string) string {
	for _, part := range strings.Split(link, ",") {
		if target, ok := strings.CutSuffix(strings.TrimSpace(part), `>; rel="next"`); ok {
			return strings.TrimPrefix(target, "<")
		}
	}

	return ""
}

// jwt signs a token that authenticates as the App for the next few minutes.
func (g *githubApp) jwt() (string, error) {
	now := time.Now()
	claims, err := json.Marshal(map[string]any{"iss": g.id, "iat": now.Add(-time.Minute).Unix(), "exp": now.Add(9 * time.Minute).Unix()})
	if err != nil {
		return "", err
	}

	unsigned := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + base64.RawURLEncoding.EncodeToString(claims)
	hash := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, g.key, crypto.SHA256, hash[:])
	if err != nil {
		return "", err
	}

	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// installationToken returns a token that authenticates as an installation,
// which GitHub issues for an hour.
func (g *githubApp) installationToken(ctx context.Context, installation int64) (string, error) {
	g.lock.Lock()
	cached := g.tokens[installation]
	g.lock.Unlock()
	if time.Until(cached.ExpiresAt) > 5*time.Minute {
		return cached.Token, nil
	}

	jwt, err := g.jwt()
	if err != nil {
		return "", err
	}

	var token githubToken
	header, err := g.call(ctx, http.MethodPost, fmt.Sprintf("%s/app/installations/%d/access_tokens", g.apiURL, installation), jwt, nil, &token)
	if err := g.heed(installation, header, err); err != nil {
		return "", err
	}

	g.lock.Lock()
	g.tokens[installation] = token
	g.lock.Unlock()
	return token.Token, nil
}

// forget drops what is remembered of an installation that is gone.
func (g *githubApp) forget(installation int64) {
	g.lock.Lock()
	delete(g.tokens, installation)
	delete(g.notBefore, installation)
	g.lock.Unlock()
}

// ask calls GitHub as an installation, with its token. GitHub limits how much
// an installation asks of it, and says when one that asked too much may ask
// again. Nothing is asked for it until then, as GitHub counts what it is
// asked meanwhile against the installation too.
func (g *githubApp) ask(ctx context.Context, installation int64, method, endpoint, token string, input, output any) (http.Header, error) {
	if until, limited := g.limited(installation); limited {
		return nil, &githubLimit{Until: until}
	}

	header, err := g.call(ctx, method, endpoint, token, input, output)

	// GitHub no longer takes the token, so the next call asks for another.
	if githubStatus(err) == http.StatusUnauthorized {
		g.lock.Lock()
		delete(g.tokens, installation)
		g.lock.Unlock()
	}

	return header, g.heed(installation, header, err)
}

// limited reports whether GitHub has told an installation to wait, and until
// when.
func (g *githubApp) limited(installation int64) (time.Time, bool) {
	g.lock.Lock()
	until := g.notBefore[installation]
	g.lock.Unlock()
	return until, time.Now().Before(until)
}

// heed remembers when GitHub next takes the calls of an installation, if the
// response to one says so. A call that GitHub refused for that reason fails
// as limited, whatever its status, so that it is made again and not taken
// for a refusal that stands.
func (g *githubApp) heed(installation int64, header http.Header, err error) error {
	var refusal *githubError
	errors.As(err, &refusal)
	until := githubWait(header, refusal)
	if until.IsZero() {
		return err
	}

	g.lock.Lock()
	g.notBefore[installation] = until
	g.lock.Unlock()
	if refusal == nil {
		return err
	}

	return &githubLimit{Until: until}
}

// githubWait reads from a response when GitHub next takes a call, and
// returns the zero time for a response that sets no limit. GitHub says how
// long to wait in Retry-After, or that a response is the last before the
// time its limit resets. A refusal that says neither asks for a minute.
func githubWait(header http.Header, refusal *githubError) time.Time {
	if seconds, err := strconv.Atoi(header.Get("Retry-After")); err == nil {
		return time.Now().Add(time.Duration(seconds) * time.Second)
	}

	if reset, err := strconv.ParseInt(header.Get("X-RateLimit-Reset"), 10, 64); err == nil && header.Get("X-RateLimit-Remaining") == "0" {
		return time.Unix(reset, 0)
	}

	if refusal == nil {
		return time.Time{}
	}

	// GitHub refuses with 403 for other reasons too, and then says which.
	limited := refusal.Status == http.StatusForbidden && strings.Contains(strings.ToLower(refusal.Message), "rate limit")
	if limited || refusal.Status == http.StatusTooManyRequests {
		return time.Now().Add(time.Minute)
	}

	return time.Time{}
}

// call makes a request of GitHub and returns the headers of its response,
// also of one that GitHub refused.
func (g *githubApp) call(ctx context.Context, method, endpoint, token string, input, output any) (http.Header, error) {
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

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	response := json.NewDecoder(io.LimitReader(resp.Body, 2<<20))
	if resp.StatusCode >= 300 {
		var refusal struct {
			Message string `json:"message"`
		}
		_ = response.Decode(&refusal)
		return resp.Header, &githubError{Status: resp.StatusCode, Message: refusal.Message}
	}

	if output != nil {
		if err := response.Decode(output); err != nil {
			return resp.Header, err
		}
	}

	return resp.Header, nil
}
