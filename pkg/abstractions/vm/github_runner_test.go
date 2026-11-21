package vm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// What GitHub issues an installation's token with once its organization has
// granted the App Self-hosted runners.
var organizationRunners = map[string]string{"organization_self_hosted_runners": "write"}

// organizationGitHub is an App whose cached token for installation 42 has
// that permission.
func organizationGitHub(t *testing.T, handler http.HandlerFunc) *githubApp {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &githubApp{id: "17", client: server.Client(), apiURL: server.URL,
		tokens:    map[int64]githubToken{42: {Token: "installation-token", ExpiresAt: time.Now().Add(time.Hour), Permissions: organizationRunners}},
		notBefore: map[int64]time.Time{}}
}

// grantOrganizationRunners turns organization runners on for a gateway whose
// organization, acme, has granted the App the permission.
func grantOrganizationRunners(r *githubRunners) {
	r.config.OrganizationRunners = true
	r.github.tokens[42] = githubToken{Token: "installation-token", ExpiresAt: time.Now().Add(time.Hour), Permissions: organizationRunners}
}

// registrations are the endpoints that runners registered on, of the calls
// that a fake GitHub served.
func registrations(calls []string) []string {
	var endpoints []string
	for _, call := range calls {
		if endpoint, _, _ := strings.Cut(call, " {"); strings.HasSuffix(endpoint, "/generate-jitconfig") {
			endpoints = append(endpoints, endpoint)
		}
	}
	return endpoints
}

func TestOrganizationRunnerCreatesRepositoryRestrictedGroup(t *testing.T) {
	for _, private := range []bool{true, false} {
		t.Run(fmt.Sprintf("private=%v", private), func(t *testing.T) {
			var calls []string
			app := organizationGitHub(t, func(w http.ResponseWriter, req *http.Request) {
				require.Equal(t, "Bearer installation-token", req.Header.Get("Authorization"))
				calls = append(calls, req.Method+" "+req.URL.Path)
				switch req.Method + " " + req.URL.Path {
				case "GET /repos/acme/repo":
					fmt.Fprintf(w, `{"id":123,"private":%v}`, private)
				case "GET /orgs/acme/actions/runner-groups":
					fmt.Fprint(w, `{"runner_groups":[]}`)
				case "POST /orgs/acme/actions/runner-groups":
					var body map[string]any
					require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
					require.Equal(t, map[string]any{"name": "beam-17-123", "visibility": "selected",
						"selected_repository_ids": []any{float64(123)}, "allows_public_repositories": !private}, body)
					w.WriteHeader(http.StatusCreated)
					fmt.Fprintf(w, `{"id":8,"name":"beam-17-123","visibility":"selected","allows_public_repositories":%v}`, !private)
				case "GET /orgs/acme/actions/runner-groups/8/repositories":
					fmt.Fprint(w, `{"total_count":1,"repositories":[{"id":123}]}`)
				case "POST /orgs/acme/actions/runners/generate-jitconfig":
					var body map[string]any
					require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
					require.Equal(t, map[string]any{"name": "ghr-test", "runner_group_id": float64(8),
						"labels": []any{"beam-4vcpu-ubuntu-2404"}, "work_folder": runnerWorkFolder}, body)
					w.WriteHeader(http.StatusCreated)
					fmt.Fprint(w, `{"encoded_jit_config":"jit-config"}`)
				default:
					t.Errorf("unexpected GitHub request %s %s", req.Method, req.URL)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			config, err := app.registerRunner(context.Background(), 42,
				githubRunnerScope{Repository: "acme/repo", Organization: "acme"}, "ghr-test", "beam-4vcpu-ubuntu-2404")
			require.NoError(t, err)
			require.Equal(t, "jit-config", config)
			require.Len(t, calls, 5)
		})
	}
}

func TestOrganizationRunnerRejectsBroadenedGroups(t *testing.T) {
	for name, group := range map[string]string{
		"all repositories":                `{"id":8,"name":"beam-17-123","visibility":"all"}`,
		"private repositories":            `{"id":8,"name":"beam-17-123","visibility":"private"}`,
		"inherited":                       `{"id":8,"name":"beam-17-123","visibility":"selected","inherited":true}`,
		"public allowed for private repo": `{"id":8,"name":"beam-17-123","visibility":"selected","allows_public_repositories":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			app := organizationGitHub(t, func(w http.ResponseWriter, req *http.Request) {
				require.Equal(t, http.MethodGet, req.Method, "must not repair an existing group or register a runner")
				switch req.URL.Path {
				case "/repos/acme/repo":
					fmt.Fprint(w, `{"id":123,"private":true}`)
				case "/orgs/acme/actions/runner-groups":
					fmt.Fprintf(w, `{"runner_groups":[%s]}`, group)
				default:
					t.Errorf("unexpected GitHub request %s", req.URL)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			_, err := app.registerRunner(context.Background(), 42,
				githubRunnerScope{Repository: "acme/repo", Organization: "acme"}, "ghr-test", "beam-4vcpu-ubuntu-2404")
			require.ErrorContains(t, err, "does not isolate")
			require.ErrorIs(t, err, errRunnerGroupIsolation)
		})
	}
}

// An owner of the organization can change a group after it is created, so
// the repositories it allows are read before every runner joins it.
func TestOrganizationRunnerChecksItsGroupsRepositoriesEveryTime(t *testing.T) {
	for name, changed := range map[string]string{
		"another repository as well":      `{"total_count":2,"repositories":[{"id":123},{"id":456}]}`,
		"another the App is not shown":    `{"total_count":2,"repositories":[{"id":123}]}`,
		"another repository instead":      `{"total_count":1,"repositories":[{"id":456}]}`,
		"no repository":                   `{"total_count":0,"repositories":[]}`,
		"a count without its repository":  `{"total_count":1,"repositories":[]}`,
		"a count GitHub does not include": `{"repositories":[{"id":123}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			var app *githubApp
			registrations, memberships := 0, 0
			app = organizationGitHub(t, func(w http.ResponseWriter, req *http.Request) {
				switch req.Method + " " + req.URL.Path {
				case "GET /repos/acme/repo":
					fmt.Fprint(w, `{"id":123,"private":true}`)
				case "GET /orgs/acme/actions/runner-groups":
					// GitHub lists the groups a page at a time.
					if req.URL.Query().Get("page") == "2" {
						fmt.Fprint(w, `{"runner_groups":[{"id":8,"name":"beam-17-123","visibility":"selected"}]}`)
					} else {
						w.Header().Set("Link", "<"+app.apiURL+req.URL.Path+"?page=2>; rel=\"next\"")
						fmt.Fprint(w, `{"runner_groups":[{"id":2,"name":"unrelated","visibility":"all"}]}`)
					}
				case "GET /orgs/acme/actions/runner-groups/8/repositories":
					if memberships++; memberships > 1 {
						fmt.Fprint(w, changed)
					} else {
						fmt.Fprint(w, `{"total_count":1,"repositories":[{"id":123}]}`)
					}
				case "POST /orgs/acme/actions/runners/generate-jitconfig":
					registrations++
					fmt.Fprint(w, `{"encoded_jit_config":"jit"}`)
				default:
					t.Errorf("unexpected GitHub request %s %s", req.Method, req.URL)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			scope := githubRunnerScope{Repository: "acme/repo", Organization: "acme"}
			_, err := app.registerRunner(context.Background(), 42, scope, "ghr-first", "beam-4vcpu-ubuntu-2404")
			require.NoError(t, err)
			_, err = app.registerRunner(context.Background(), 42, scope, "ghr-second", "beam-4vcpu-ubuntu-2404")
			require.ErrorIs(t, err, errRunnerGroupIsolation)
			require.Equal(t, 1, registrations, "no runner joins a group that another repository can use")
			require.Equal(t, 2, memberships)
		})
	}
}

func TestOrganizationRunnerGroupCreationFailureStopsOnlyPermanentRefusals(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		retryAfter string
		message    string
		terminal   bool
		limited    bool
	}{
		{name: "organization policy", status: 403, message: "Organization runner policy forbids this", terminal: true},
		{name: "unsupported group configuration", status: 422, message: "Validation Failed", terminal: true},
		{name: "secondary rate limit", status: 403, retryAfter: "60", message: "You have exceeded a secondary rate limit", limited: true},
		{name: "too many requests", status: 429, message: "Too many requests", limited: true},
		{name: "server failure", status: 500, message: "Server Error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, store, _, runtime := runnerFixture(t)
			r.config.OrganizationRunners = true
			lists := 0
			r.github = organizationGitHub(t, func(w http.ResponseWriter, req *http.Request) {
				switch req.Method + " " + req.URL.Path {
				case "GET /repos/acme/repo":
					fmt.Fprint(w, `{"id":123,"private":true}`)
				case "GET /orgs/acme/actions/runner-groups":
					lists++
					fmt.Fprint(w, `{"runner_groups":[]}`)
				case "POST /orgs/acme/actions/runner-groups":
					if tc.retryAfter != "" {
						w.Header().Set("Retry-After", tc.retryAfter)
					}
					w.WriteHeader(tc.status)
					_ = json.NewEncoder(w).Encode(map[string]string{"message": tc.message})
				default:
					t.Errorf("unexpected GitHub request %s %s", req.Method, req.URL)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			ctx := context.Background()
			require.NoError(t, store.SaveGitHubRunnerJob(ctx, queuedJob(1)))
			err := r.provision(ctx, *store.jobs[1])
			require.Error(t, err)
			require.Equal(t, tc.terminal, runnerRefused(err))
			var limit *githubLimit
			require.Equal(t, tc.limited, errors.As(err, &limit))
			if tc.terminal {
				require.ErrorIs(t, err, errRunnerGroupUnavailable)
				require.ErrorContains(t, err, "check the organization's runner policies")
				require.Nil(t, store.jobs[1].ProvisionAt, "do not retry a permanent group refusal")
				require.Contains(t, store.jobs[1].Error, tc.message)
			} else {
				require.NotErrorIs(t, err, errRunnerGroupUnavailable)
				require.NotNil(t, store.jobs[1].ProvisionAt, "transient failures must remain retryable")
				if tc.limited {
					require.Equal(t, waitingGitHub, store.jobs[1].Waiting)
				}
			}
			if tc.status == 422 {
				require.Equal(t, 2, lists, "first resolve a possible concurrent group creation")
			} else {
				require.Equal(t, 1, lists)
			}
			require.Empty(t, runtime.requests, "never fall back to an unisolated runner")
		})
	}
}

func TestOrganizationRunnerConcurrentGroupCreation(t *testing.T) {
	for _, raced := range []bool{true, false} {
		t.Run(fmt.Sprintf("another gateway created it=%v", raced), func(t *testing.T) {
			lists, creates := 0, 0
			app := organizationGitHub(t, func(w http.ResponseWriter, req *http.Request) {
				switch req.Method + " " + req.URL.Path {
				case "GET /repos/acme/repo":
					fmt.Fprint(w, `{"id":123,"private":true}`)
				case "GET /orgs/acme/actions/runner-groups":
					lists++
					if lists > 1 && raced {
						fmt.Fprint(w, `{"runner_groups":[{"id":8,"name":"beam-17-123","visibility":"selected"}]}`)
					} else {
						fmt.Fprint(w, `{"runner_groups":[]}`)
					}
				case "POST /orgs/acme/actions/runner-groups":
					creates++
					w.WriteHeader(http.StatusUnprocessableEntity)
					fmt.Fprint(w, `{"message":"group already exists"}`)
				case "GET /orgs/acme/actions/runner-groups/8/repositories":
					fmt.Fprint(w, `{"total_count":1,"repositories":[{"id":123}]}`)
				case "POST /orgs/acme/actions/runners/generate-jitconfig":
					require.True(t, raced)
					fmt.Fprint(w, `{"encoded_jit_config":"jit"}`)
				default:
					t.Errorf("unexpected GitHub request %s %s", req.Method, req.URL)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			config, err := app.registerRunner(context.Background(), 42,
				githubRunnerScope{Repository: "acme/repo", Organization: "acme"}, "ghr-test", "beam-4vcpu-ubuntu-2404")
			if raced {
				require.NoError(t, err)
				require.Equal(t, "jit", config)
			} else {
				require.Equal(t, http.StatusUnprocessableEntity, githubStatus(err))
				require.ErrorIs(t, err, errRunnerGroupUnavailable)
				require.True(t, runnerRefused(err))
				require.Empty(t, config)
			}
			require.Equal(t, 2, lists)
			require.Equal(t, 1, creates)
		})
	}
}

func TestOrganizationRunnerLifecycleUsesOrganizationScope(t *testing.T) {
	var app *githubApp
	lists, deletions := 0, []string{}
	app = organizationGitHub(t, func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/orgs/acme/actions/runners":
			if name := req.URL.Query().Get("name"); name != "" {
				// The organization's own runner is listed too, as it would
				// be if GitHub took the name for less than all of one.
				require.Equal(t, "ghr-remove", name)
				fmt.Fprint(w, `{"runners":[{"id":99,"name":"ghr-remove-not"},{"id":8,"name":"ghr-remove"}]}`)
			} else if req.URL.Query().Get("page") == "2" {
				fmt.Fprint(w, `{"runners":[{"id":9,"name":"ghr-second","status":"online"}]}`)
			} else {
				lists++
				w.Header().Set("Link", "<"+app.apiURL+req.URL.Path+"?page=2>; rel=\"next\"")
				fmt.Fprint(w, `{"runners":[{"id":8,"name":"ghr-first","status":"online"}]}`)
			}
		case req.Method == http.MethodGet && req.URL.Path == "/repos/acme/repo/actions/runners":
			fmt.Fprint(w, `{"runners":[{"id":10,"name":"ghr-repository"}]}`)
		case req.Method == http.MethodDelete && strings.HasPrefix(req.URL.Path, "/orgs/acme/actions/runners/"):
			deletions = append(deletions, req.URL.Path)
			if strings.HasSuffix(req.URL.Path, "/9") {
				w.WriteHeader(http.StatusUnprocessableEntity)
				fmt.Fprint(w, `{"message":"runner is busy"}`)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		default:
			t.Errorf("unexpected GitHub request %s %s", req.Method, req.URL)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	ctx := context.Background()
	scope := githubRunnerScope{Repository: "acme/repo", Organization: "acme"}
	first, err := app.runners(ctx, 42, scope)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.False(t, first["ghr-first"].FreeSince.IsZero())
	otherRepo, err := app.runners(ctx, 42, githubRunnerScope{Repository: "acme/other", Organization: "acme"})
	require.NoError(t, err)
	require.Equal(t, first, otherRepo)
	require.Equal(t, 1, lists, "one organization list is shared across its repository pools")
	legacy, err := app.runners(ctx, 42, githubRunnerScope{Repository: "acme/repo"})
	require.NoError(t, err)
	require.Contains(t, legacy, "ghr-repository", "repo and org caches must remain separate")
	removed, err := app.removeRunner(ctx, 42, scope, "ghr-remove")
	require.NoError(t, err)
	require.True(t, removed, "only the runner with that name is removed")
	removed, err = app.deleteRunner(ctx, 42, scope, first["ghr-second"])
	require.NoError(t, err)
	require.False(t, removed, "a runner that acquired a job is kept")
	require.Equal(t, []string{"/orgs/acme/actions/runners/8", "/orgs/acme/actions/runners/9"}, deletions)
}

// Org runners keep each repository's cache and job pool isolated through a
// selected-repository group, while workflows keep their existing size label.
func TestGitHubRunnerOrganizationLaunchPersistsRegistrationScope(t *testing.T) {
	r, store, _, runtime := runnerFixture(t)
	grantOrganizationRunners(r)
	// The organization is the one the job names, which a rename since the
	// account connected leaves the installation behind on.
	store.installation.Account = "acme-before-rename"
	calls := fakeGitHub(t, r, map[string]string{
		"GET /repos/acme/repo":                                `200 {"id":123,"private":true}`,
		"GET /orgs/acme/actions/runner-groups":                `200 {"runner_groups":[]}`,
		"POST /orgs/acme/actions/runner-groups":               `201 {"id":2,"name":"beam-1-123","visibility":"selected","allows_public_repositories":false}`,
		"GET /orgs/acme/actions/runner-groups/2/repositories": `200 {"total_count":1,"repositories":[{"id":123}]}`,
		"POST /orgs/acme/actions/runners/generate-jitconfig":  `201 {"encoded_jit_config":"org-jit-secret"}`,
	})
	v := startRunner(t, r, store, 1, false)
	require.Equal(t, map[string]string{"github.repository": "acme/repo", "github.organization": "acme"}, v.Metadata)
	require.Contains(t, v.Spec.Env, "BEAM_GITHUB_JIT_CONFIG=org-jit-secret")
	require.EqualValues(t, 4000, v.Spec.CPU)
	require.Len(t, runtime.requests, 1)
	require.NotNil(t, v.Spec.RootLayer, "org registration retains the repository's enhanced cache")
	var jit string
	for _, call := range calls() {
		require.NotContains(t, call, "/repos/acme/repo/actions/runners", "org registration needs no repository administration")
		if strings.HasPrefix(call, "POST /orgs/acme/actions/runners/generate-jitconfig ") {
			jit = strings.TrimPrefix(call, "POST /orgs/acme/actions/runners/generate-jitconfig ")
		}
	}
	require.JSONEq(t, fmt.Sprintf(`{"name":%q,"runner_group_id":2,"labels":["beam-4vcpu-ubuntu-2404"],"work_folder":"/home/runner/work"}`, v.Name), jit)
}

func TestGitHubRunnerOrganizationSeparatesRepositoriesWithSameLabel(t *testing.T) {
	r, store, _, _ := runnerFixture(t)
	grantOrganizationRunners(r)
	calls := fakeGitHub(t, r, map[string]string{
		"GET /repos/acme/repo":  `200 {"id":123,"private":true}`,
		"GET /repos/acme/other": `200 {"id":456,"private":true}`,
		"GET /orgs/acme/actions/runner-groups": `200 {"runner_groups":[
			{"id":2,"name":"beam-1-123","visibility":"selected"},
			{"id":3,"name":"beam-1-456","visibility":"selected"}]}`,
		"GET /orgs/acme/actions/runner-groups/2/repositories": `200 {"total_count":1,"repositories":[{"id":123}]}`,
		"GET /orgs/acme/actions/runner-groups/3/repositories": `200 {"total_count":1,"repositories":[{"id":456}]}`,
		"POST /orgs/acme/actions/runners/generate-jitconfig":  `201 {"encoded_jit_config":"jit"}`,
	})
	first := startRunner(t, r, store, 1, false)
	otherJob := queuedJob(2)
	otherJob.Repository = "acme/other"
	// deliver's webhook fixture always names acme/repo, so queue the
	// second repository directly in the store.
	require.NoError(t, store.SaveGitHubRunnerJob(context.Background(), otherJob))
	require.NoError(t, r.provision(context.Background(), *store.jobs[2]))
	second := store.vm(7, store.jobs[2].VM)
	require.NotNil(t, second)
	require.NotNil(t, first.Spec.RootLayer)
	require.NotNil(t, second.Spec.RootLayer)
	require.NotEqual(t, first.Spec.RootLayer.DiskName, second.Spec.RootLayer.DiskName, "repositories must never start with one another's enhanced cache")
	require.Equal(t, "acme/repo", first.Metadata["github.repository"])
	require.Equal(t, "acme/other", second.Metadata["github.repository"])
	groups := []int64{}
	for _, call := range calls() {
		const prefix = "POST /orgs/acme/actions/runners/generate-jitconfig "
		if !strings.HasPrefix(call, prefix) {
			continue
		}
		var body struct {
			Group  int64    `json:"runner_group_id"`
			Labels []string `json:"labels"`
		}
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(call, prefix)), &body))
		require.Equal(t, beamLabels, body.Labels, "identical workflow labels are isolated by their repository groups")
		groups = append(groups, body.Group)
	}
	require.Equal(t, []int64{2, 3}, groups)
}

const registersOrganizationRunner = "POST /orgs/acme/actions/runners/generate-jitconfig"

// Runners register on an organization only once it has granted the App
// Self-hosted runners. The accounts that installed the App before it asked
// for that, and personal accounts, keep registering on their repositories.
func TestGitHubRunnerRegistersOnTheOrganizationOnlyOnceItGrantsRunners(t *testing.T) {
	for name, tc := range map[string]struct {
		enabled     bool
		accountType string
		permissions map[string]string
		registers   string
	}{
		"organization runners are off":         {false, "Organization", organizationRunners, registersRunner},
		"organization that has yet to accept":  {true, "Organization", map[string]string{"administration": "write"}, registersRunner},
		"organization that grants only a read": {true, "Organization", map[string]string{"organization_self_hosted_runners": "read"}, registersRunner},
		"personal account":                     {true, "User", organizationRunners, registersRunner},
		"organization that has accepted":       {true, "Organization", organizationRunners, registersOrganizationRunner},
	} {
		t.Run(name, func(t *testing.T) {
			r, store, _, _ := runnerFixture(t)
			r.config.OrganizationRunners = tc.enabled
			store.installation.AccountType = tc.accountType
			r.github.tokens[42] = githubToken{Token: "installation-token", ExpiresAt: time.Now().Add(time.Hour), Permissions: tc.permissions}
			routes := map[string]string{tc.registers: `201 {"encoded_jit_config":"jit"}`}
			metadata := map[string]string{"github.repository": "acme/repo"}
			if tc.registers == registersOrganizationRunner {
				routes["GET /repos/acme/repo"] = `200 {"id":123,"private":true}`
				routes["GET /orgs/acme/actions/runner-groups"] = `200 {"runner_groups":[{"id":2,"name":"beam-1-123","visibility":"selected"}]}`
				routes["GET /orgs/acme/actions/runner-groups/2/repositories"] = `200 {"total_count":1,"repositories":[{"id":123}]}`
				metadata["github.organization"] = "acme"
			}
			calls := fakeGitHub(t, r, routes)
			v := startRunner(t, r, store, 1, false)
			require.Equal(t, []string{tc.registers}, registrations(calls()))
			require.Equal(t, metadata, v.Metadata, "the VM says where its runner is removed from")
		})
	}
}

// The flag, and what an organization grants, say where new runners register.
// A VM keeps the scope its own runner registered on across a rollout or a
// rollback, so cleanup reaches the runner GitHub knows.
func TestGitHubRunnerCleanupUsesPersistedScopeAcrossRollout(t *testing.T) {
	for _, tc := range []struct {
		name, organization, endpoint string
		organizationRunners          bool
	}{
		{"old repository runner after enabling org mode", "", "/repos/acme/repo/actions/runners", true},
		{"organization runner after rollback", "acme", "/orgs/acme/actions/runners", false},
	} {
		for _, operation := range []string{"offline", "never connected", "busy"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				r, store, _, _ := runnerFixture(t)
				r.config.OrganizationRunners = tc.organizationRunners
				store.jobs[1] = queuedJob(1)
				store.jobs[1].VM = "ghr-scope"
				v := &types.VM{ID: "vm-scope", Name: "ghr-scope", WorkspaceID: 7,
					DesiredState: types.VMStateRunning, Status: types.VMStateRunning,
					UpdatedAt: time.Now().Add(-runnerIdle - time.Minute),
					Metadata:  map[string]string{"github.repository": "acme/repo"}}
				if tc.organization != "" {
					v.Metadata["github.organization"] = tc.organization
				}
				store.rows[v.ID] = v
				busy := operation == "busy"
				routes := map[string]string{
					"GET " + tc.endpoint: fmt.Sprintf(`200 {"runners":[{"id":9,"name":"ghr-scope","status":"offline","busy":%t}]}`, busy),
				}
				if !busy {
					routes["DELETE "+tc.endpoint+"/9"] = "204 {}"
				}
				calls := fakeGitHub(t, r, routes)
				runner, err := store.GetGitHubRunner(context.Background(), 7, v.Name)
				require.NoError(t, err)
				require.NotNil(t, runner)
				if operation == "never connected" {
					r.forget(context.Background(), v, runner)
				} else {
					removed, renew, err := r.idle(context.Background(), v, runner)
					require.NoError(t, err)
					require.Equal(t, !busy, removed)
					require.Equal(t, busy, renew)
				}
				expected := []string{"GET " + tc.endpoint}
				if !busy {
					expected = append(expected, "DELETE "+tc.endpoint+"/9")
				}
				require.Equal(t, expected, calls())
			})
		}
	}
}

func TestGitHubRunnerOrganizationStopUsesPersistedScope(t *testing.T) {
	for _, deregister := range []string{"204", "422", "500"} {
		t.Run(deregister, func(t *testing.T) {
			r, store, info, runtime := runnerFixture(t)
			// A rollback changes where new runners register, not where this
			// already registered runner is stopped.
			r.config.OrganizationRunners = false
			calls := fakeGitHub(t, r, map[string]string{
				"GET /orgs/acme/actions/runners":      `200 {"runners":[{"id":9,"name":"ghr-stop"}]}`,
				"DELETE /orgs/acme/actions/runners/9": deregister + " {}",
			})
			store.jobs[1] = queuedJob(1)
			store.jobs[1].VM = "ghr-stop"
			job := queuedJob(2)
			job.Status, job.RunnerName = types.GitHubJobInProgress, "ghr-stop"
			store.jobs[2] = job
			v := &types.VM{ID: "vm-stop", Name: "ghr-stop", WorkspaceID: 7, TokenID: "token",
				DesiredState: types.VMStateRunning, Status: types.VMStateRunning, EverRunning: true,
				ContainerID: "runner-container", StubID: "stub", CreatedAt: time.Now(),
				Metadata: map[string]string{"github.repository": "acme/repo", "github.organization": "acme"}}
			runtime.containers.states[v.ContainerID] = &types.ContainerState{ContainerId: v.ContainerID, Status: types.ContainerStatusRunning}
			store.rows[v.ID] = v
			code, body := runnerRequest(t, info, r.stopJob, "2", "")
			require.Equal(t, 202, code, body)
			require.Empty(t, body)
			require.Equal(t, types.VMStateDeleted, store.rows[v.ID].Status)
			require.Equal(t, 1, r.s.gateway.(*vmGateway).stops, "explicit stop removes the VM even if GitHub refuses or is unreachable")
			require.Equal(t, []string{"GET /orgs/acme/actions/runners", "DELETE /orgs/acme/actions/runners/9"}, calls())
		})
	}
}

func TestGitHubRunnerOrganizationIsolationRefusalEndsProvisioning(t *testing.T) {
	r, store, _, runtime := runnerFixture(t)
	grantOrganizationRunners(r)
	calls := fakeGitHub(t, r, map[string]string{
		"GET /repos/acme/repo":                 `200 {"id":123,"private":true}`,
		"GET /orgs/acme/actions/runner-groups": `200 {"runner_groups":[{"id":2,"name":"beam-1-123","visibility":"all"}]}`,
	})
	require.NoError(t, store.SaveGitHubRunnerJob(context.Background(), queuedJob(1)))
	err := r.provision(context.Background(), *store.jobs[1])
	require.ErrorIs(t, err, errRunnerGroupIsolation)
	require.Nil(t, store.jobs[1].ProvisionAt, "an invalid group requires an owner to repair it before a workflow is rerun")
	require.Contains(t, store.jobs[1].Error, "does not isolate")
	require.Empty(t, runtime.requests)
	require.Equal(t, []string{"GET /repos/acme/repo", "GET /orgs/acme/actions/runner-groups"}, calls())
}

// An organization that installed the App before the App asked for Self-hosted
// runners is not interrupted: its runners keep registering on its
// repositories, and the first one after an owner accepts the permission
// registers on the organization.
func TestGitHubRunnerMovesToTheOrganizationWhenItAcceptsRunners(t *testing.T) {
	r, store, _, _ := runnerFixture(t)
	r.config.OrganizationRunners = true
	cache := &types.GitHubRunnerCache{WorkspaceID: 7, Repository: "acme/repo", DiskName: types.RootLayerDiskPrefix + "existing",
		SnapshotID: "existing-snapshot", CreatedAt: time.Now()}
	store.caches = append(store.caches, cache)
	calls := fakeGitHub(t, r, map[string]string{
		registersRunner: `201 {"encoded_jit_config":"repository-jit"}`,
		// The token GitHub issues once the organization has accepted.
		"POST /app/installations/42/access_tokens":            `201 {"token":"accepted-token","expires_at":"2099-01-01T00:00:00Z","permissions":{"organization_self_hosted_runners":"write"}}`,
		"GET /repos/acme/repo":                                `200 {"id":123,"private":true}`,
		"GET /orgs/acme/actions/runner-groups":                `200 {"runner_groups":[]}`,
		"POST /orgs/acme/actions/runner-groups":               `201 {"id":2,"name":"beam-1-123","visibility":"selected"}`,
		"GET /orgs/acme/actions/runner-groups/2/repositories": `200 {"total_count":1,"repositories":[{"id":123}]}`,
		registersOrganizationRunner:                           `201 {"encoded_jit_config":"organization-jit"}`,
	})

	before := startRunner(t, r, store, 1, false)
	require.Contains(t, before.Spec.Env, "BEAM_GITHUB_JIT_CONFIG=repository-jit")
	require.NotContains(t, before.Metadata, "github.organization")
	require.Len(t, calls(), 1, "the token that the gateway holds says what the organization grants")

	require.Equal(t, 202, deliver(t, r, "installation", "new_permissions_accepted", nil, queuedJob(0), true))
	require.NotNil(t, store.installation, "accepting a permission keeps the account connected")
	require.Equal(t, cache, store.current(7, "acme/repo"), "and its cache")

	after := startRunner(t, r, store, 2, false)
	require.Contains(t, after.Spec.Env, "BEAM_GITHUB_JIT_CONFIG=organization-jit")
	require.Equal(t, "acme", after.Metadata["github.organization"])
	require.Equal(t, cache.DiskName, after.Spec.RootLayer.DiskName)
	require.Equal(t, []string{registersRunner, registersOrganizationRunner}, registrations(calls()))
	require.Equal(t, "accepted-token", r.github.tokens[42].Token, "the gateway asks for the token again when the permissions change")
}

func TestGitHubRunnerCustomLabelPrefixIsolatesStagingFromProduction(t *testing.T) {
	r, store, info, runtime := runnerFixture(t)
	grantOrganizationRunners(r)
	r.config.LabelPrefix = " Stage-Org-Permissions- "
	stageLabel := "stage-org-permissions-2vcpu-ubuntu-2404"
	stageLabels := []string{strings.ToUpper(stageLabel)}
	calls := fakeGitHub(t, r, map[string]string{
		"GET /repos/acme/repo":                                `200 {"id":123,"private":true}`,
		"GET /orgs/acme/actions/runner-groups":                `200 {"runner_groups":[]}`,
		"POST /orgs/acme/actions/runner-groups":               `201 {"id":2,"name":"beam-1-123","visibility":"selected"}`,
		"GET /orgs/acme/actions/runner-groups/2/repositories": `200 {"total_count":1,"repositories":[{"id":123}]}`,
		"POST /orgs/acme/actions/runners/generate-jitconfig":  `201 {"encoded_jit_config":"stage-jit"}`,
	})
	production, productionStore, _, _ := runnerFixture(t)
	productionCalls := fakeGitHub(t, production, nil)
	require.Equal(t, 202, deliver(t, production, "workflow_job", "queued", stageLabels, queuedJob(1), true))
	require.Empty(t, productionStore.jobs, "production ignores staging labels even when its App receives the same repo webhook")
	require.Empty(t, productionCalls())
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", beamLabels, queuedJob(1), true))
	require.Empty(t, store.jobs, "staging ignores production labels")
	require.Equal(t, 202, deliver(t, r, "workflow_job", "queued", stageLabels, queuedJob(2), true))
	require.Equal(t, stageLabel, store.jobs[2].Label, "GitHub labels are case insensitive")
	require.Equal(t, waitingStart, r.waiting(*store.jobs[2], false, false))
	page := listedJobs(t, r, info)
	var waiting string
	require.NoError(t, json.Unmarshal(page[2]["waiting"], &waiting))
	require.Equal(t, waitingStart, waiting, "the jobs API recognizes configured size labels")
	c, rec := actionContext(info, "", "", "")
	require.NoError(t, r.status(c))
	var status runnerStatus
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &status))
	require.Len(t, status.Sizes, len(runnerSizes))
	for i, size := range status.Sizes {
		require.Equal(t, "stage-org-permissions-"+strings.TrimPrefix(runnerSizes[i].Label, runnerLabelPrefix), size.Label)
		require.Equal(t, runnerSizes[i].CPU, size.CPU)
	}
	require.NoError(t, r.provision(context.Background(), *store.jobs[2]))
	v := store.vm(7, store.jobs[2].VM)
	require.NotNil(t, v)
	require.EqualValues(t, 2000, v.Spec.CPU)
	require.Len(t, runtime.requests, 1)
	var jit string
	for _, call := range calls() {
		const prefix = "POST /orgs/acme/actions/runners/generate-jitconfig "
		if strings.HasPrefix(call, prefix) {
			jit = strings.TrimPrefix(call, prefix)
		}
	}
	require.JSONEq(t, fmt.Sprintf(`{"name":%q,"runner_group_id":2,"labels":[%q],"work_folder":"/home/runner/work"}`, v.Name, stageLabel), jit)
}

func TestGitHubRunnerCustomLabelPrefixReportsConfiguredSizes(t *testing.T) {
	r, store, _, runtime := runnerFixture(t)
	r.config.LabelPrefix = "stage-org-permissions-"
	calls := fakeGitHub(t, r, nil)
	job := queuedJob(1)
	job.Label = "stage-org-permissions-3vcpu-ubuntu-2404"
	require.NoError(t, store.SaveGitHubRunnerJob(context.Background(), job))
	require.NoError(t, r.provision(context.Background(), *store.jobs[1]))
	require.Contains(t, store.jobs[1].Error, "runs-on must be exactly one of stage-org-permissions-2vcpu-ubuntu-2404")
	require.NotContains(t, store.jobs[1].Error, "beam-")
	require.Empty(t, runtime.requests)
	require.Empty(t, calls())
	require.Nil(t, store.jobs[1].ProvisionAt)
}

func TestGitHubRunnerInitRejectsLabelPrefixesRecognizedByProduction(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, normalized string
		reject                   bool
	}{
		{name: "default", normalized: "beam-"},
		{name: "whitespace default", prefix: " \t ", normalized: "beam-"},
		{name: "explicit default", prefix: "beam-", normalized: "beam-"},
		{name: "normalized default", prefix: " BEAM- ", normalized: "beam-"},
		{name: "staging", prefix: "stage-org-permissions-", normalized: "stage-org-permissions-"},
		{name: "normalized staging", prefix: " STAGE-ORG-PERMISSIONS- ", normalized: "stage-org-permissions-"},
		{name: "similar safe prefix", prefix: "beamer-", normalized: "beamer-"},
		{name: "overlapping staging", prefix: "beam-stage-", reject: true},
		{name: "uppercase overlapping staging", prefix: "BEAM-STAGE-", reject: true},
		{name: "trimmed overlapping staging", prefix: " Beam-Stage- ", reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, info, _, _ := fixture()
			// Validate even before an App is enabled, so a dormant setting
			// cannot later send an intended staging job to production.
			s.config.GitHubRunners = types.GitHubRunnerConfig{LabelPrefix: tc.prefix}
			c, _ := actionContext(info, "", "", "")
			err := s.initGitHubRunners(c.Echo().Group("/api/v1/vm"))
			if tc.reject {
				require.ErrorContains(t, err, "abstractions.vm.githubRunners.labelPrefix")
				require.ErrorContains(t, err, "production would start runners for its jobs as well")
				require.Nil(t, s.runners.github)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.normalized, s.runners.labelPrefix())
		})
	}
}
