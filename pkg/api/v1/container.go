package apiv1

import (
	"net/http"

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

	g.GET("/:workspaceId", WithWorkspaceAuth(group.ListContainersByWorkspaceId))

	return group
}

func (c *ContainerGroup) ListContainersByWorkspaceId(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	containerStates, err := c.containerRepo.GetActiveContainersByWorkspaceId(workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get containers")
	}

	return ctx.JSON(http.StatusOK, containerStates)
}
