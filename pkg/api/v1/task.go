package apiv1

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/abstractions/output"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

var DefaultTaskOutputExpirationS uint32 = 3600

type TaskGroup struct {
	routerGroup    *echo.Group
	config         types.AppConfig
	backendRepo    repository.BackendRepository
	taskRepo       repository.TaskRepository
	containerRepo  repository.ContainerRepository
	redisClient    *common.RedisClient
	taskDispatcher *task.Dispatcher
}

func NewTaskGroup(g *echo.Group, redisClient *common.RedisClient, taskRepo repository.TaskRepository, containerRepo repository.ContainerRepository, backendRepo repository.BackendRepository, taskDispatcher *task.Dispatcher, config types.AppConfig) *TaskGroup {
	group := &TaskGroup{routerGroup: g,
		backendRepo:    backendRepo,
		taskRepo:       taskRepo,
		containerRepo:  containerRepo,
		config:         config,
		redisClient:    redisClient,
		taskDispatcher: taskDispatcher,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListTasksPaginated))
	g.GET("/:workspaceId/task-count-by-deployment", auth.WithWorkspaceAuth(group.GetTaskCountByDeployment))
	g.GET("/:workspaceId/aggregate-by-time-window", auth.WithWorkspaceAuth(group.AggregateTasksByTimeWindow))
	g.DELETE("/:workspaceId", auth.WithWorkspaceAuth(group.StopTasks))
	g.GET("/:workspaceId/:taskId", auth.WithWorkspaceAuth(group.RetrieveTask))

	return group
}

func (g *TaskGroup) GetTaskCountByDeployment(ctx echo.Context) error {
	filters, err := g.preprocessFilters(ctx)
	if err != nil {
		return err
	}

	if tasks, err := g.backendRepo.GetTaskCountPerDeployment(ctx.Request().Context(), *filters); err != nil {
		return HTTPInternalServerError("Failed to list tasks")
	} else {
		return ctx.JSON(http.StatusOK, tasks)
	}
}

func (g *TaskGroup) AggregateTasksByTimeWindow(ctx echo.Context) error {
	filters, err := g.preprocessFilters(ctx)
	if err != nil {
		return err
	}

	if tasks, err := g.backendRepo.AggregateTasksByTimeWindow(ctx.Request().Context(), *filters); err != nil {
		return HTTPInternalServerError("Failed to list tasks")
	} else {
		return ctx.JSON(http.StatusOK, tasks)
	}
}

func (g *TaskGroup) ListTasksPaginated(ctx echo.Context) error {
	filters, err := g.preprocessFilters(ctx)
	if err != nil {
		return err
	}
	skipDetails, _ := strconv.ParseBool(ctx.QueryParam("skip_details"))

	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), ctx.Param("workspaceId"))
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	if tasks, err := g.backendRepo.ListTasksWithRelatedPaginated(ctx.Request().Context(), *filters); err != nil {
		return HTTPInternalServerError("Failed to list tasks")
	} else {

		for i := range tasks.Data {
			tasks.Data[i].Stub.SanitizeConfig()
			if !skipDetails {
				g.addOutputsToTask(ctx.Request().Context(), workspace.Name, &tasks.Data[i])
				g.addStatsToTask(ctx.Request().Context(), workspace.Name, &tasks.Data[i])
			}
		}
		return ctx.JSON(http.StatusOK, tasks)
	}
}

func (g *TaskGroup) RetrieveTask(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	taskId := ctx.Param("taskId")
	if task, err := g.backendRepo.GetTaskByWorkspace(ctx.Request().Context(), taskId, &workspace); err != nil {
		return HTTPInternalServerError("Failed to retrieve task")
	} else {
		if task == nil {
			return HTTPNotFound()
		}
		task.Stub.SanitizeConfig()
		g.addOutputsToTask(ctx.Request().Context(), cc.AuthInfo.Workspace.Name, task)
		g.addStatsToTask(ctx.Request().Context(), cc.AuthInfo.Workspace.Name, task)

		return ctx.JSON(http.StatusOK, task)
	}
}

func (g *TaskGroup) addOutputsToTask(ctx context.Context, workspaceName string, task *types.TaskWithRelated) error {
	task.Outputs = []types.TaskOutput{}
	outputFiles := output.GetTaskOutputFiles(workspaceName, task)

	for outputId, fileName := range outputFiles {
		url, err := output.SetPublicURL(ctx, g.config, g.backendRepo, g.redisClient, workspaceName, task.ExternalId, outputId, fileName, DefaultTaskOutputExpirationS)
		if err != nil {
			return err
		}
		task.Outputs = append(task.Outputs, types.TaskOutput{Name: fileName, URL: url, ExpiresIn: DefaultTaskOutputExpirationS})
	}

	return nil
}

func (g *TaskGroup) addStatsToTask(ctx context.Context, workspaceName string, task *types.TaskWithRelated) error {
	tasksInFlight, err := g.taskRepo.TasksInFlight(ctx, workspaceName, task.Stub.ExternalId)
	if err != nil {
		return err
	}
	task.Stats.QueueDepth = uint32(tasksInFlight)

	activeContainers, err := g.containerRepo.GetActiveContainersByStubId(task.Stub.ExternalId)
	if err != nil {
		return err
	}
	task.Stats.ActiveContainers = uint32(len(activeContainers))

	return nil
}

type StopTasksRequest struct {
	TaskIds types.StringSlice `query:"task_ids"`
}

func (g *TaskGroup) StopTasks(ctx echo.Context) error {
	var req StopTasksRequest
	if err := ctx.Bind(&req); err != nil {
		return HTTPBadRequest("Failed to decode task ids")
	}

	for _, taskId := range req.TaskIds {
		task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), taskId)
		if err != nil {
			continue
		}

		if task == nil {
			continue
		}

		err = g.stopTask(ctx.Request().Context(), task)
		if err != nil {
			continue
		}
	}

	return ctx.JSON(http.StatusOK, map[string]interface{}{})
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

func (g *TaskGroup) preprocessFilters(ctx echo.Context) (*types.TaskFilter, error) {
	var filters types.TaskFilter
	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return nil, HTTPBadRequest("Invalid workspace ID")
	}

	if err := ctx.Bind(&filters); err != nil {
		return nil, HTTPBadRequest("Failed to decode query parameters")
	}

	filters.WorkspaceID = workspace.Id

	return &filters, nil
}
