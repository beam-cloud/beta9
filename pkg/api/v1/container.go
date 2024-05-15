package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type ContainerGroup struct {
	routerGroup   *echo.Group
	config        types.AppConfig
	backendRepo   repository.BackendRepository
	containerRepo repository.ContainerRepository
}

func NewContainerGroup(
	g *echo.Group,
	backendRepo repository.BackendRepository,
	containerRepo repository.ContainerRepository,
	config types.AppConfig,
) *ContainerGroup {
	group := &ContainerGroup{routerGroup: g,
		backendRepo:   backendRepo,
		containerRepo: containerRepo,
		config:        config,
	}

	g.GET("/:workspaceId", group.ListContainersByWorkspaceId)

	return group
}

func (c *ContainerGroup) ListContainersByWorkspaceId(ctx echo.Context) error {
	_, err := c.authorize(ctx)
	if err != nil {
		return err
	}

	workspaceId := ctx.Param("workspaceId")
	_, err = c.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	containerStates, err := c.containerRepo.GetActiveContainersByWorkspaceId(workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get containers")
	}

	return ctx.JSON(http.StatusOK, containerStates)
}

func (g *ContainerGroup) authorize(ctx echo.Context) (*auth.HttpAuthContext, error) {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return nil, echo.NewHTTPError(http.StatusUnauthorized)
	}
	return cc, nil
}
