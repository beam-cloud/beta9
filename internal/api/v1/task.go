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

	g.GET("/:workspaceId", group.ListTasksPaginated)
	g.GET("/:workspaceId/", group.ListTasksPaginated)

	g.GET("/:workspaceId/list", group.ListTasksForMetrics)
	g.GET("/:workspaceId/list/", group.ListTasksForMetrics)

	g.GET("/:workspaceId/task-count-by-deployment", group.GetTaskCountByDeployment)
	g.GET("/:workspaceId/task-count-by-deployment/", group.GetTaskCountByDeployment)

	g.GET("/:workspaceId/aggregate-by-time-window", group.AggregateTasksByTimeWindow)
	g.GET("/:workspaceId/aggregate-by-time-window/", group.AggregateTasksByTimeWindow)

	g.GET("/:workspaceId/:taskId", group.RetrieveTask)
	g.GET("/:workspaceId/:taskId/", group.RetrieveTask)

	return group
}

func (g *TaskGroup) ListTasksForMetrics(ctx echo.Context) error {
	_, err := g.authenticate(ctx)
	if err != nil {
		return err
	}

	filters, err := g.preprocessFilters(ctx)
	if err != nil {
		return err
	}

	if tasks, err := g.backendRepo.ListTasksForMetrics(ctx.Request().Context(), *filters); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list tasks")
	} else {
		return ctx.JSON(http.StatusOK, tasks)
	}
}

func (g *TaskGroup) GetTaskCountByDeployment(ctx echo.Context) error {
	_, err := g.authenticate(ctx)
	if err != nil {
		return err
	}

	filters, err := g.preprocessFilters(ctx)
	if err != nil {
		return err
	}

	if tasks, err := g.backendRepo.GetTaskCountPerDeployment(ctx.Request().Context(), *filters); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list tasks")
	} else {
		return ctx.JSON(http.StatusOK, tasks)
	}
}

func (g *TaskGroup) AggregateTasksByTimeWindow(ctx echo.Context) error {
	_, err := g.authenticate(ctx)
	if err != nil {
		return err
	}

	filters, err := g.preprocessFilters(ctx)
	if err != nil {
		return err
	}

	if tasks, err := g.backendRepo.AggregateTasksByTimeWindow(ctx.Request().Context(), *filters); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list tasks")
	} else {
		return ctx.JSON(http.StatusOK, tasks)
	}
}

func (g *TaskGroup) ListTasksPaginated(ctx echo.Context) error {
	_, err := g.authenticate(ctx)
	if err != nil {
		return err
	}

	filters, err := g.preprocessFilters(ctx)
	if err != nil {
		return err
	}

	if tasks, err := g.backendRepo.ListTasksWithRelatedPaginated(ctx.Request().Context(), *filters); err != nil {
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
	if task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), taskId); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve task")
	} else {
		return ctx.JSON(http.StatusOK, task)
	}
}

func (g *TaskGroup) authenticate(ctx echo.Context) (*auth.HttpAuthContext, error) {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return nil, echo.NewHTTPError(http.StatusUnauthorized)
	}
	return cc, nil
}

func (g *TaskGroup) preprocessFilters(ctx echo.Context) (*types.TaskFilter, error) {
	var filters types.TaskFilter
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	if err := ctx.Bind(&filters); err != nil {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "Failed to decode query parameters")
	}

	filters.WorkspaceID = workspace.Id

	return &filters, nil
}
