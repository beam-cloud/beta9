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

	tokenLock sync.Mutex
	tokens    map[int64]githubToken
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
// belong to. A user has far fewer than the hundred a page holds.
func (g *githubApp) userInstallations(ctx context.Context, token string) ([]githubInstallation, error) {
	var page struct {
		Installations []githubInstallation `json:"installations"`
	}
	_, err := g.call(ctx, http.MethodGet, g.apiURL+"/user/installations?per_page=100", token, nil, &page)
	return page.Installations, err
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
	_, err = g.call(ctx, http.MethodGet, endpoint, token, nil, &membership)
	if githubStatus(err) == http.StatusNotFound || err == nil && membership.State != "active" {
		return "", nil
	}

	return membership.Role, err
}

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
	_, err = g.call(ctx, http.MethodPost, g.apiURL+"/repos/"+repository+"/actions/runners/generate-jitconfig", token,
		map[string]any{"name": name, "runner_group_id": 1, "labels": []string{label}, "work_folder": "_work"}, &runner)
	return runner.Config, err
}

// removeRunner unregisters a runner that has no job. It reports false when
// GitHub refuses, which it does once it has given the runner a job.
func (g *githubApp) removeRunner(ctx context.Context, installation int64, repository, name string) (bool, error) {
	token, err := g.installationToken(ctx, installation)
	if err != nil {
		return false, err
	}

	var registered struct {
		Runners []struct {
			ID int64 `json:"id"`
		} `json:"runners"`
	}
	endpoint := g.apiURL + "/repos/" + repository + "/actions/runners"
	_, err = g.call(ctx, http.MethodGet, endpoint+"?name="+url.QueryEscape(name), token, nil, &registered)
	for _, runner := range registered.Runners {
		if _, err = g.call(ctx, http.MethodDelete, fmt.Sprintf("%s/%d", endpoint, runner.ID), token, nil, nil); err != nil {
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
	g.tokenLock.Lock()
	cached := g.tokens[installation]
	g.tokenLock.Unlock()
	if time.Until(cached.ExpiresAt) > 5*time.Minute {
		return cached.Token, nil
	}

	jwt, err := g.jwt()
	if err != nil {
		return "", err
	}

	var token githubToken
	if _, err := g.call(ctx, http.MethodPost, fmt.Sprintf("%s/app/installations/%d/access_tokens", g.apiURL, installation), jwt, nil, &token); err != nil {
		return "", err
	}

	g.tokenLock.Lock()
	g.tokens[installation] = token
	g.tokenLock.Unlock()
	return token.Token, nil
}

// forget drops the token of an installation that is gone.
func (g *githubApp) forget(installation int64) {
	g.tokenLock.Lock()
	delete(g.tokens, installation)
	g.tokenLock.Unlock()
}

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
		return nil, &githubError{Status: resp.StatusCode, Message: refusal.Message}
	}

	if output != nil {
		if err := response.Decode(output); err != nil {
			return nil, err
		}
	}

	return resp.Header, nil
}
