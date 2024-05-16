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

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListStubs))

	return group
}

func (g *StubGroup) ListStubs(ctx echo.Context) error {
	var filters types.StubFilter
	if err := ctx.Bind(&filters); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Failed to decode query parameters")
	}

	if stubs, err := g.backendRepo.ListStubs(ctx.Request().Context(), filters); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list stubs")
	} else {
		return ctx.JSON(http.StatusOK, stubs)
	}
}
