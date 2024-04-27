package apiv1

import (
	"log"
	"net/http"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
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

	g.GET("/", group.ListStubs)

	return group
}

func (g *StubGroup) ListStubs(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	var filters types.StubFilter
	if err := ctx.Bind(&filters); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Failed to decode query parameters")
	}

	if stubs, err := g.backendRepo.ListStubs(ctx.Request().Context(), filters); err != nil {
		log.Println(err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list stubs")
	} else {
		return ctx.JSON(http.StatusOK, stubs)
	}
}
