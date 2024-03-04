package apiv1

import (
	"net/http"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type TaskGroup struct {
	routerGroup *echo.Group
	config      types.AppConfig
	backendRepo repository.BackendRepository
}

func NewTaskGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig) *TaskGroup {
	group := &TaskGroup{routerGroup: g,
		backendRepo: backendRepo,
		config:      config,
	}

	g.GET("/:workspaceId", group.ListTasks)
	g.GET("/:workspaceId/", group.ListTasks)

	g.GET("/:workspaceId/:taskId", group.RetrieveTask)
	g.GET("/:workspaceId/:taskId/", group.RetrieveTask)

	return group
}

func (g *TaskGroup) ListTasks(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	var filters types.TaskFilter
	if err := ctx.Bind(&filters); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Failed to decode query parameters")
	}

	filters.WorkspaceID = workspace.Id

	if tasks, err := g.backendRepo.ListTasksWithRelated(ctx.Request().Context(), filters); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list tasks")
	} else {
		return ctx.JSON(http.StatusOK, tasks)
	}
}

func (g *TaskGroup) RetrieveTask(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	taskId := ctx.Param("taskId")
	if task, err := g.backendRepo.GetTask(ctx.Request().Context(), taskId); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve task")
	} else {
		return ctx.JSON(http.StatusOK, task)
	}
}
