package apiv1

import (
	"context"
	"database/sql"
	"errors"
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

// oneWorkspaceBackendRepo is a backend with a single workspace.
type oneWorkspaceBackendRepo struct{ repository.BackendRepository }

func (oneWorkspaceBackendRepo) GetWorkspaceByExternalId(context.Context, string) (types.Workspace, error) {
	return types.Workspace{Id: 1, ExternalId: "workspace"}, nil
}

type tokenTypeBackendRepo struct {
	oneWorkspaceBackendRepo
	created bool
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
	oneWorkspaceBackendRepo
	disabled *bool
}

func (r *clusterAdminTokenBackendRepo) SetWorkspaceTokensDisabled(_ context.Context, _ uint, disabled bool) ([]string, error) {
	r.disabled = &disabled
	return []string{"key-1", "key-2"}, nil
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
			backendRepo := &clusterAdminTokenBackendRepo{}
			workspaceRepo := &revokingWorkspaceRepo{}
			group := &TokenGroup{backendRepo: backendRepo, tokens: auth.NewTokens(backendRepo, workspaceRepo)}
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

			if backendRepo.disabled == nil || *backendRepo.disabled != disabled {
				t.Fatalf("tokens disabled=%v, want %t", backendRepo.disabled, disabled)
			}
			if got, want := strings.Join(workspaceRepo.revoked, ","), "key-1,key-2"; got != want {
				t.Fatalf("revoked token keys = %q, want %q", got, want)
			}
		})
	}
}

type changeTokenBackendRepo struct {
	oneWorkspaceBackendRepo
	token   *types.Token
	err     error
	deleted bool
}

func (r *changeTokenBackendRepo) GetTokenByExternalId(context.Context, uint, string) (*types.Token, error) {
	return r.token, r.err
}

func (r *changeTokenBackendRepo) DeleteToken(context.Context, uint, string) error {
	r.deleted = true
	return nil
}

func TestDeleteWorkspaceTokenReportsWhyItCannot(t *testing.T) {
	for _, tc := range []struct {
		name    string
		token   *types.Token
		err     error
		status  int
		message string
		deleted bool
	}{
		{"workspace token", &types.Token{Key: "key", TokenType: types.TokenTypeWorkspace}, nil, http.StatusOK, "", true},
		{"primary token", &types.Token{Key: "key", TokenType: types.TokenTypeWorkspacePrimary}, nil, http.StatusBadRequest, auth.ErrPrimaryToken.Error(), false},
		{"worker token", &types.Token{Key: "key", TokenType: types.TokenTypeWorkerPrivate}, nil, http.StatusBadRequest, auth.ErrProtectedToken.Error(), false},
		{"cluster admin token", &types.Token{Key: "key", TokenType: types.TokenTypeClusterAdmin}, nil, http.StatusBadRequest, auth.ErrProtectedToken.Error(), false},
		{"missing token", nil, fmt.Errorf("token not found: %w", sql.ErrNoRows), http.StatusBadRequest, auth.ErrTokenNotFound.Error(), false},
		{"database outage", nil, errors.New("connection refused"), http.StatusInternalServerError, "Failed to delete token", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backendRepo := &changeTokenBackendRepo{token: tc.token, err: tc.err}
			workspaceRepo := &revokingWorkspaceRepo{}
			group := &TokenGroup{backendRepo: backendRepo, tokens: auth.NewTokens(backendRepo, workspaceRepo)}
			e := echo.New()
			recorder := httptest.NewRecorder()
			ctx := e.NewContext(httptest.NewRequest(http.MethodDelete, "/api/v1/token/workspace/token", nil), recorder)
			ctx.SetParamNames("workspaceId", "tokenId")
			ctx.SetParamValues("workspace", "token")
			authCtx := &auth.HttpAuthContext{Context: ctx, AuthInfo: &auth.AuthInfo{Token: &types.Token{TokenType: types.TokenTypeWorkspace}}}

			err := group.DeleteWorkspaceToken(authCtx)
			status, message := recorder.Code, ""
			var httpErr *echo.HTTPError
			if errors.As(err, &httpErr) {
				status = httpErr.Code
				body, _ := httpErr.Message.(map[string]interface{})
				message, _ = body["message"].(string)
			}
			if status != tc.status || message != tc.message || backendRepo.deleted != tc.deleted {
				t.Fatalf("status=%d message=%q deleted=%t, want %d %q %t (err=%v)", status, message, backendRepo.deleted, tc.status, tc.message, tc.deleted, err)
			}
			if tc.deleted && strings.Join(workspaceRepo.revoked, ",") != "key,key" {
				t.Fatalf("evicted %v, want the key before and after the delete", workspaceRepo.revoked)
			}
		})
	}
}
