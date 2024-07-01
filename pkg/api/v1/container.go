package apiv1

import (
	"log"
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type ContainerGroup struct {
	routerGroup   *echo.Group
	config        types.AppConfig
	backendRepo   repository.BackendRepository
	scheduler     scheduler.Scheduler
	containerRepo repository.ContainerRepository
}

func NewContainerGroup(
	g *echo.Group,
	backendRepo repository.BackendRepository,
	containerRepo repository.ContainerRepository,
	scheduler scheduler.Scheduler,
	config types.AppConfig,
) *ContainerGroup {
	group := &ContainerGroup{routerGroup: g,
		backendRepo:   backendRepo,
		containerRepo: containerRepo,
		scheduler:     scheduler,
		config:        config,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListContainersByWorkspaceId))
	g.GET("/:workspaceId/:containerId", auth.WithWorkspaceAuth(group.GetContainer))
	g.POST("/:workspaceId/stop-all", auth.WithWorkspaceAuth(group.StopAllWorkspaceContainers))

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

func (c *ContainerGroup) GetContainer(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	containerId := ctx.Param("containerId")

	containerState, err := c.containerRepo.GetContainerState(containerId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid container id")
	}

	if containerState.WorkspaceId != workspaceId {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid workspace id")
	}

	return ctx.JSON(http.StatusOK, containerState)
}

func (c *ContainerGroup) StopAllWorkspaceContainers(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	containerStates, err := c.containerRepo.GetActiveContainersByWorkspaceId(workspaceId)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"message": "failed to stop containers",
		})
	}

	for _, state := range containerStates {
		err := c.scheduler.Stop(state.ContainerId)
		if err != nil {
			log.Println("failed to stop container", state.ContainerId, err)
		}
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"message": "all containers stopped",
	})
}
