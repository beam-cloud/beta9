package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type StubGroup struct {
	routerGroup *echo.Group
	config      types.AppConfig
	backendRepo repository.BackendRepository
}

func NewStubGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig) *StubGroup {
	group := &StubGroup{routerGroup: g,
		backendRepo: backendRepo,
		config:      config,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListStubsByWorkspaceId)) // Allows workspace admins to list stubs specific to their workspace
	g.GET("", auth.WithClusterAdminAuth(group.ListStubs))                        // Allows cluster admins to list all stubs

	return group
}

func (g *StubGroup) ListStubsByWorkspaceId(ctx echo.Context) error {
	workspaceID := ctx.Param("workspaceId")

	var filters types.StubFilter
	if err := ctx.Bind(&filters); err != nil {
		return HTTPBadRequest("Failed to decode query parameters")
	}

	filters.WorkspaceID = workspaceID

	if stubs, err := g.backendRepo.ListStubs(ctx.Request().Context(), filters); err != nil {
		return HTTPInternalServerError("Failed to list stubs")
	} else {
		return ctx.JSON(http.StatusOK, stubs)
	}
}

func (g *StubGroup) ListStubs(ctx echo.Context) error {
	var filters types.StubFilter
	if err := ctx.Bind(&filters); err != nil {
		return HTTPBadRequest("Failed to decode query parameters")
	}

	if stubs, err := g.backendRepo.ListStubs(ctx.Request().Context(), filters); err != nil {
		return HTTPInternalServerError("Failed to list stubs")
	} else {
		return ctx.JSON(http.StatusOK, stubs)
	}
}
