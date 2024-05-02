package apiv1

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/task"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type TaskGroup struct {
	routerGroup    *echo.Group
	config         types.AppConfig
	backendRepo    repository.BackendRepository
	redisClient    *common.RedisClient
	taskDispatcher *task.Dispatcher
}

func NewTaskGroup(g *echo.Group, redisClient *common.RedisClient, backendRepo repository.BackendRepository, taskDispatcher *task.Dispatcher, config types.AppConfig) *TaskGroup {
	group := &TaskGroup{routerGroup: g,
		backendRepo:    backendRepo,
		config:         config,
		redisClient:    redisClient,
		taskDispatcher: taskDispatcher,
	}

	g.GET("/:workspaceId", group.ListTasksPaginated)
	g.GET("/:workspaceId/task-count-by-deployment", group.GetTaskCountByDeployment)
	g.GET("/:workspaceId/aggregate-by-time-window", group.AggregateTasksByTimeWindow)
	g.DELETE("/:workspaceId", group.StopTasks)
	g.GET("/:workspaceId/:taskId", group.RetrieveTask)

	return group
}

func (g *TaskGroup) GetTaskCountByDeployment(ctx echo.Context) error {
	_, err := g.authorize(ctx)
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
	_, err := g.authorize(ctx)
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
	_, err := g.authorize(ctx)
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

type StopTasksRequest struct {
	TaskIds types.StringSlice `query:"task_ids"`
}

func (g *TaskGroup) StopTasks(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	var req StopTasksRequest
	if err := ctx.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Failed to decode task ids")
	}

	for _, taskId := range req.TaskIds {
		task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), taskId)
		if err != nil {
			continue
		}

		err = g.stopTask(ctx.Request().Context(), task)
		if err != nil {
			continue
		}
	}

	return nil
}

func (g *TaskGroup) stopTask(ctx context.Context, task *types.TaskWithRelated) error {
	if task.Status.IsCompleted() {
		return nil
	}

	err := g.taskDispatcher.Complete(ctx, task.Workspace.Name, task.Stub.ExternalId, task.ExternalId)
	if err != nil {
		return errors.New("failed to complete task")
	}

	err = g.redisClient.Publish(ctx, common.RedisKeys.TaskCancel(task.Workspace.Name, task.Stub.ExternalId, task.ExternalId), task.ExternalId).Err()
	if err != nil {
		return errors.New("failed to cancel task")
	}

	task.Status = types.TaskStatusCancelled
	task.EndedAt = sql.NullTime{Time: time.Now(), Valid: true}
	if _, err := g.backendRepo.UpdateTask(ctx, task.ExternalId, task.Task); err != nil {
		return errors.New("failed to update task")
	}

	return nil
}

func (g *TaskGroup) authorize(ctx echo.Context) (*auth.HttpAuthContext, error) {
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
