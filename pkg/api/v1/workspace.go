package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type WorkspaceGroup struct {
	routerGroup *echo.Group
	config      types.AppConfig
	backendRepo repository.BackendRepository
}

func NewWorkspaceGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig) *WorkspaceGroup {
	group := &WorkspaceGroup{routerGroup: g,
		backendRepo: backendRepo,
		config:      config,
	}

	g.POST("", group.CreateWorkspace)
	g.GET("/current", auth.WithAuth(group.CurrentWorkspace))

	return group
}

type CreateWorkspaceRequest struct {
}

func (g *WorkspaceGroup) CreateWorkspace(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return HTTPUnauthorized("Invalid token")
	}

	var request CreateWorkspaceRequest
	if err := ctx.Bind(&request); err != nil {
		return HTTPBadRequest("Invalid payload")
	}

	workspace, err := g.backendRepo.CreateWorkspace(ctx.Request().Context())
	if err != nil {
		return HTTPInternalServerError("Unable to create workspace")
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"workspace_id": workspace.ExternalId,
	})
}

func (g *WorkspaceGroup) CurrentWorkspace(ctx echo.Context) error {
	authContext, _ := ctx.(*auth.HttpAuthContext)

	return ctx.JSON(http.StatusOK, authContext.AuthInfo.Workspace)
}
