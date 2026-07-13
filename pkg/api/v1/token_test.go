package apiv1

import (
	"context"
	"net/http"
	"net/http/httptest"
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
