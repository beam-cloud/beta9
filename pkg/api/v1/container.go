package apiv1

import (
	"net/http"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
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
	g.POST("/:workspaceId/:containerId/stop", auth.WithWorkspaceAuth(group.StopContainer))
	g.POST("/:workspaceId/stop-all", auth.WithWorkspaceAuth(group.StopAllWorkspaceContainers))

	return group
}

func (c *ContainerGroup) ListContainersByWorkspaceId(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	containerStates, err := c.containerRepo.GetActiveContainersByWorkspaceId(workspaceId)
	if err != nil {
		return HTTPInternalServerError("Failed to get containers")
	}

	return ctx.JSON(http.StatusOK, containerStates)
}

func (c *ContainerGroup) GetContainer(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	containerId := ctx.Param("containerId")

	containerState, err := c.containerRepo.GetContainerState(containerId)
	if err != nil {
		return HTTPBadRequest("Invalid container id")
	}

	if containerState.WorkspaceId != workspaceId {
		return HTTPBadRequest("Invalid workspace id")
	}

	return ctx.JSON(http.StatusOK, containerState)
}

func (c *ContainerGroup) StopAllWorkspaceContainers(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	containerStates, err := c.containerRepo.GetActiveContainersByWorkspaceId(workspaceId)
	if err != nil {
		return HTTPInternalServerError("Failed to stop containers")
	}

	for _, state := range containerStates {
		err := c.scheduler.Stop(&types.StopContainerArgs{ContainerId: state.ContainerId})
		if err != nil {
			log.Error().Str("container_id", state.ContainerId).Err(err).Msg("failed to stop container")
		}
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"message": "all containers stopped",
	})
}

func (c *ContainerGroup) StopContainer(ctx echo.Context) error {
	workspaceId := ctx.Param("workspaceId")
	containerId := ctx.Param("containerId")
	force := ctx.QueryParam("force") == "true"

	state, err := c.containerRepo.GetContainerState(containerId)
	if err != nil {
		return HTTPBadRequest("Invalid container id")
	}

	if state.WorkspaceId != workspaceId {
		return HTTPBadRequest("Invalid workspace id")
	}

	err = c.scheduler.Stop(&types.StopContainerArgs{ContainerId: containerId, Force: force})
	if err != nil {
		if strings.Contains(err.Error(), "event already exists") {
			return HTTPConflict("Container is already stopping")
		}
		return HTTPInternalServerError("Failed to stop container")
	}

	return ctx.NoContent(http.StatusOK)
}
