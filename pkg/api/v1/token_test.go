package apiv1

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type tokenTypeBackendRepo struct {
	repository.BackendRepository
	created bool
}

func (r *tokenTypeBackendRepo) GetWorkspaceByExternalId(context.Context, string) (types.Workspace, error) {
	return types.Workspace{Id: 1, ExternalId: "workspace"}, nil
}

func (r *tokenTypeBackendRepo) CreateToken(context.Context, uint, string, bool) (types.Token, error) {
	r.created = true
	return types.Token{}, nil
}

func TestWorkspaceTokenEndpointCannotMintClusterAdminToken(t *testing.T) {
	repo := &tokenTypeBackendRepo{}
	group := &TokenGroup{backendRepo: repo}
	e := echo.New()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/token/workspace?token_type=admin", nil)
	ctx := e.NewContext(request, httptest.NewRecorder())
	ctx.SetParamNames("workspaceId")
	ctx.SetParamValues("workspace")
	authCtx := &auth.HttpAuthContext{
		Context: ctx,
		AuthInfo: &auth.AuthInfo{
			Workspace: &types.Workspace{ExternalId: "workspace"},
			Token:     &types.Token{TokenType: types.TokenTypeWorkspacePrimary},
		},
	}

	err := group.CreateWorkspaceToken(authCtx)
	if err == nil || repo.created {
		t.Fatalf("admin token request reached repository: err=%v created=%v", err, repo.created)
	}
}

type clusterAdminTokenBackendRepo struct {
	repository.BackendRepository
	updated map[string]bool
}

func (r *clusterAdminTokenBackendRepo) GetWorkspaceByExternalId(context.Context, string) (types.Workspace, error) {
	return types.Workspace{Id: 1, ExternalId: "workspace"}, nil
}

func (r *clusterAdminTokenBackendRepo) ListTokens(context.Context, uint) ([]types.Token, error) {
	return []types.Token{
		{ExternalId: "token-1", Key: "key-1"},
		{ExternalId: "token-2", Key: "key-2"},
	}, nil
}

func (r *clusterAdminTokenBackendRepo) UpdateTokenAsClusterAdmin(_ context.Context, tokenId string, disabled bool) error {
	r.updated[tokenId] = disabled
	return nil
}

type revokingWorkspaceRepo struct {
	repository.WorkspaceRepository
	revoked []string
}

func (r *revokingWorkspaceRepo) RevokeToken(tokenKey string) error {
	r.revoked = append(r.revoked, tokenKey)
	return nil
}

func TestClusterAdminUpdateAllWorkspaceTokensRevokesAuthorizationCache(t *testing.T) {
	for _, disabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("disabled=%t", disabled), func(t *testing.T) {
			backendRepo := &clusterAdminTokenBackendRepo{updated: map[string]bool{}}
			workspaceRepo := &revokingWorkspaceRepo{}
			group := &TokenGroup{backendRepo: backendRepo, workspaceRepo: workspaceRepo}
			e := echo.New()
			request := httptest.NewRequest(
				http.MethodPatch,
				"/api/v1/token/admin/workspace",
				strings.NewReader(fmt.Sprintf(`{"disabled":%t}`, disabled)),
			)
			request.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			ctx := e.NewContext(request, httptest.NewRecorder())
			ctx.SetParamNames("workspaceId")
			ctx.SetParamValues("workspace")

			if err := group.ClusterAdminUpdateAllWorkspaceTokens(ctx); err != nil {
				t.Fatalf("update tokens failed: %v", err)
			}

			if got, want := len(backendRepo.updated), 2; got != want {
				t.Fatalf("updated token count = %d, want %d", got, want)
			}
			for _, tokenId := range []string{"token-1", "token-2"} {
				if got := backendRepo.updated[tokenId]; got != disabled {
					t.Fatalf("token %s disabled=%t, want %t", tokenId, got, disabled)
				}
			}
			if got, want := strings.Join(workspaceRepo.revoked, ","), "key-1,key-2"; got != want {
				t.Fatalf("revoked token keys = %q, want %q", got, want)
			}
		})
	}
}
