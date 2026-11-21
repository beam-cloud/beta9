package vm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func organizationGitHub(t *testing.T, handler http.HandlerFunc) *githubApp {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &githubApp{id: "17", client: server.Client(), apiURL: server.URL,
		tokens:    map[int64]githubToken{42: {Token: "installation-token", ExpiresAt: time.Now().Add(time.Hour)}},
		notBefore: map[int64]time.Time{}}
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
					fmt.Fprint(w, `{"repositories":[{"id":123}]}`)
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
			config, err := app.registerScopedRunner(context.Background(), 42,
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
		"missing ID":                      `{"name":"beam-17-123","visibility":"selected"}`,
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
			_, err := app.registerScopedRunner(context.Background(), 42,
				githubRunnerScope{Repository: "acme/repo", Organization: "acme"}, "ghr-test", "beam-4vcpu-ubuntu-2404")
			require.ErrorContains(t, err, "does not isolate")
			require.ErrorIs(t, err, errRunnerGroupIsolation)
		})
	}
}

func TestOrganizationRunnerValidatesAllGroupRepositoryPagesEveryTime(t *testing.T) {
	var app *githubApp
	registrations, metadata, groups, memberships := 0, 0, 0, 0
	app = organizationGitHub(t, func(w http.ResponseWriter, req *http.Request) {
		switch req.Method + " " + req.URL.Path {
		case "GET /repos/acme/repo":
			metadata++
			fmt.Fprint(w, `{"id":123,"private":true}`)
		case "GET /orgs/acme/actions/runner-groups":
			if req.URL.Query().Get("page") == "2" {
				groups++
				fmt.Fprint(w, `{"runner_groups":[{"id":8,"name":"beam-17-123","visibility":"selected"}]}`)
			} else {
				w.Header().Set("Link", "<"+app.apiURL+req.URL.Path+"?page=2>; rel=\"next\"")
				fmt.Fprint(w, `{"runner_groups":[{"id":2,"name":"unrelated","visibility":"all"}]}`)
			}
		case "GET /orgs/acme/actions/runner-groups/8/repositories":
			if req.URL.Query().Get("page") == "2" {
				fmt.Fprint(w, `{"repositories":[{"id":456}]}`)
			} else {
				memberships++
				if memberships > 1 {
					w.Header().Set("Link", "<"+app.apiURL+req.URL.Path+"?page=2>; rel=\"next\"")
				}
				fmt.Fprint(w, `{"repositories":[{"id":123}]}`)
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
	_, err := app.registerScopedRunner(context.Background(), 42, scope, "ghr-first", "beam-4vcpu-ubuntu-2404")
	require.NoError(t, err)
	_, err = app.registerScopedRunner(context.Background(), 42, scope, "ghr-second", "beam-4vcpu-ubuntu-2404")
	require.ErrorContains(t, err, "does not isolate")
	require.Equal(t, 1, registrations)
	require.Equal(t, 2, metadata)
	require.Equal(t, 2, groups)
	require.Equal(t, 2, memberships)
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
					fmt.Fprint(w, `{"repositories":[{"id":123}]}`)
				case "POST /orgs/acme/actions/runners/generate-jitconfig":
					require.True(t, raced)
					fmt.Fprint(w, `{"encoded_jit_config":"jit"}`)
				default:
					t.Errorf("unexpected GitHub request %s %s", req.Method, req.URL)
					w.WriteHeader(http.StatusNotFound)
				}
			})
			config, err := app.registerScopedRunner(context.Background(), 42,
				githubRunnerScope{Repository: "acme/repo", Organization: "acme"}, "ghr-test", "beam-4vcpu-ubuntu-2404")
			if raced {
				require.NoError(t, err)
				require.Equal(t, "jit", config)
			} else {
				require.Equal(t, http.StatusUnprocessableEntity, githubStatus(err))
				require.Empty(t, config)
			}
			require.Equal(t, 2, lists)
			require.Equal(t, 1, creates)
		})
	}
}

func TestOrganizationRunnerRequiresRepositoryFromItsOrganization(t *testing.T) {
	app := organizationGitHub(t, func(w http.ResponseWriter, req *http.Request) {
		t.Errorf("unexpected GitHub request %s", req.URL)
		w.WriteHeader(http.StatusNotFound)
	})
	for _, repository := range []string{"elsewhere/repo", "acme", "acme/", "acme/repo/extra"} {
		_, err := app.registerScopedRunner(context.Background(), 42,
			githubRunnerScope{Repository: repository, Organization: "acme"}, "ghr-test", "beam-4vcpu-ubuntu-2404")
		require.ErrorContains(t, err, "must belong")
		require.ErrorIs(t, err, errRunnerGroupIsolation)
	}
}

func TestOrganizationRunnerLifecycleUsesOrganizationScope(t *testing.T) {
	var app *githubApp
	lists, deletions := 0, []string{}
	app = organizationGitHub(t, func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/orgs/acme/actions/runners":
			if name := req.URL.Query().Get("name"); name != "" {
				require.Equal(t, "ghr-remove", name)
				if req.URL.Query().Get("page") == "2" {
					fmt.Fprint(w, `{"runners":[{"id":8,"name":"ghr-remove"}]}`)
				} else {
					w.Header().Set("Link", "<"+app.apiURL+req.URL.Path+"?name=ghr-remove&page=2>; rel=\"next\"")
					fmt.Fprint(w, `{"runners":[{"id":99,"name":"unrelated"}]}`)
				}
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
	first, err := app.scopedRunners(ctx, 42, scope)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.False(t, first["ghr-first"].FreeSince.IsZero())
	otherRepo, err := app.scopedRunners(ctx, 42, githubRunnerScope{Repository: "acme/other", Organization: "ACME"})
	require.NoError(t, err)
	require.Equal(t, first, otherRepo)
	require.Equal(t, 1, lists, "one organization list is shared across its repository pools")
	legacy, err := app.runners(ctx, 42, "acme/repo")
	require.NoError(t, err)
	require.Contains(t, legacy, "ghr-repository", "repo and org caches must remain separate")
	removed, err := app.removeScopedRunner(ctx, 42, scope, "ghr-remove")
	require.NoError(t, err)
	require.True(t, removed)
	removed, err = app.deleteScopedRunner(ctx, 42, scope, first["ghr-second"])
	require.NoError(t, err)
	require.False(t, removed, "a runner that acquired a job is kept")
	require.Equal(t, []string{"/orgs/acme/actions/runners/8", "/orgs/acme/actions/runners/9"}, deletions)
}
