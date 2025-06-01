package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/abstractions/output"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/types/serializer"
	"github.com/labstack/echo/v4"
)

var (
	DefaultTaskOutputExpirationS  uint32 = 3600
	DefaultTaskSubscribeIntervalS uint32 = 1
)

type TaskGroup struct {
	routerGroup    *echo.Group
	config         types.AppConfig
	backendRepo    repository.BackendRepository
	taskRepo       repository.TaskRepository
	containerRepo  repository.ContainerRepository
	redisClient    *common.RedisClient
	taskDispatcher *task.Dispatcher
	scheduler      *scheduler.Scheduler
}

func NewTaskGroup(g *echo.Group, redisClient *common.RedisClient, taskRepo repository.TaskRepository, containerRepo repository.ContainerRepository, backendRepo repository.BackendRepository, taskDispatcher *task.Dispatcher, scheduler *scheduler.Scheduler, config types.AppConfig) *TaskGroup {
	group := &TaskGroup{routerGroup: g,
		backendRepo:    backendRepo,
		taskRepo:       taskRepo,
		containerRepo:  containerRepo,
		config:         config,
		redisClient:    redisClient,
		taskDispatcher: taskDispatcher,
		scheduler:      scheduler,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListTasksPaginated))
	g.GET("/:workspaceId/task-count-by-deployment", auth.WithWorkspaceAuth(group.GetTaskCountByDeployment))
	g.GET("/:workspaceId/aggregate-by-time-window", auth.WithWorkspaceAuth(group.AggregateTasksByTimeWindow))
	g.DELETE("/:workspaceId", auth.WithWorkspaceAuth(group.StopTasks))
	g.GET("/:workspaceId/:taskId", auth.WithWorkspaceAuth(group.RetrieveTask))
	g.GET("/metrics", auth.WithClusterAdminAuth(group.GetClusterTaskMetrics))
	g.GET("/subscribe/:taskId", auth.WithWorkspaceAuth(group.Subscribe))
	g.GET("/result/:taskId", auth.WithWorkspaceAuth(group.GetTaskResult))

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

func (g *TaskGroup) GetTaskResult(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	taskId := ctx.Param("taskId")
	if taskId == "" {
		return HTTPBadRequest("Missing task ID")
	}

	task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), taskId)
	if err != nil {
		return HTTPInternalServerError("Failed to retrieve task")
	}

	if task == nil {
		return HTTPNotFound()
	}

	// on task completion, store result in bucket automatically
	// using task result url, we can look it up, download it, and return it as a response

	if task.Workspace.Id != cc.AuthInfo.Workspace.Id && *task.ExternalWorkspaceId != cc.AuthInfo.Workspace.Id {
		return HTTPNotFound()
	}

	if task.Status.IsCompleted() {
		return ctx.JSON(http.StatusOK, task)
	}

	return ctx.JSON(http.StatusOK, task)
}

func (g *TaskGroup) ListTasksPaginated(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
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
				g.addOutputsToTask(ctx.Request().Context(), cc.AuthInfo, &tasks.Data[i])
				g.addStatsToTask(ctx.Request().Context(), workspace.Name, &tasks.Data[i])
			}
		}

		serializedTasks, err := serializer.Serialize(tasks)
		if err != nil {
			return HTTPInternalServerError("Failed to serialize response")
		}

		return ctx.JSON(http.StatusOK, serializedTasks)
	}
}

func (g *TaskGroup) Subscribe(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	taskId := ctx.Param("taskId")
	if taskId == "" {
		return HTTPBadRequest("Missing task ID")
	}

	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), cc.AuthInfo.Workspace.ExternalId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	ctx.Response().Header().Set(echo.HeaderContentType, "text/event-stream")
	ctx.Response().Header().Set(echo.HeaderCacheControl, "no-cache")
	ctx.Response().Header().Set(echo.HeaderConnection, "keep-alive")
	ctx.Response().WriteHeader(http.StatusOK)
	flusher, ok := ctx.Response().Writer.(http.Flusher)
	if !ok {
		return HTTPInternalServerError("Streaming unsupported")
	}

	var lastStatus types.TaskStatus
	for {
		select {
		case <-ctx.Request().Context().Done():
			return nil
		default:
		}

		var retrieveTaskFunc func(ctx context.Context, taskId string, workspace *types.Workspace) (*types.TaskWithRelated, error)
		public, _ := strconv.ParseBool(ctx.QueryParam("public"))
		if public {
			retrieveTaskFunc = g.backendRepo.GetPublicTaskByWorkspace
		} else {
			retrieveTaskFunc = g.backendRepo.GetTaskByWorkspace
		}

		task, err := retrieveTaskFunc(ctx.Request().Context(), taskId, &workspace)
		if err != nil {
			return HTTPInternalServerError("Failed to retrieve task")
		}
		if task == nil {
			return HTTPNotFound()
		}
		task.Stub.SanitizeConfig()

		g.addOutputsToTask(ctx.Request().Context(), cc.AuthInfo, task)
		g.addStatsToTask(ctx.Request().Context(), cc.AuthInfo.Workspace.Name, task)

		status := task.Status
		if status != lastStatus {
			serializedTask, err := serializer.Serialize(task)
			if err != nil {
				return HTTPInternalServerError("Failed to serialize task")
			}
			jsonBytes, err := json.Marshal(serializedTask)
			if err != nil {
				return HTTPInternalServerError("Failed to marshal task to JSON")
			}

			// Write SSE event
			ctx.Response().Write([]byte("event: status\n"))
			ctx.Response().Write([]byte("data: "))
			ctx.Response().Write(jsonBytes)
			ctx.Response().Write([]byte("\n\n"))
			flusher.Flush()
			lastStatus = status
		}

		// If task is completed, break
		if task.Status.IsCompleted() {
			break
		}

		time.Sleep(time.Duration(DefaultTaskSubscribeIntervalS) * time.Second)
	}

	return nil
}

func (g *TaskGroup) RetrieveTask(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return HTTPBadRequest("Invalid workspace ID")
	}

	var retrieveTaskFunc func(ctx context.Context, taskId string, workspace *types.Workspace) (*types.TaskWithRelated, error)
	public, _ := strconv.ParseBool(ctx.QueryParam("public"))
	if public {
		retrieveTaskFunc = g.backendRepo.GetPublicTaskByWorkspace
	} else {
		retrieveTaskFunc = g.backendRepo.GetTaskByWorkspace
	}

	taskId := ctx.Param("taskId")
	if task, err := retrieveTaskFunc(ctx.Request().Context(), taskId, &workspace); err != nil {
		return HTTPInternalServerError("Failed to retrieve task")
	} else {
		if task == nil {
			return HTTPNotFound()
		}
		task.Stub.SanitizeConfig()
		g.addOutputsToTask(ctx.Request().Context(), cc.AuthInfo, task)
		g.addStatsToTask(ctx.Request().Context(), cc.AuthInfo.Workspace.Name, task)

		serializedTask, err := serializer.Serialize(task)
		if err != nil {
			return err
		}

		return ctx.JSON(http.StatusOK, serializedTask)
	}
}

func (g *TaskGroup) addOutputsToTask(ctx context.Context, authInfo *auth.AuthInfo, task *types.TaskWithRelated) error {
	task.Outputs = []types.TaskOutput{}
	outputFiles := output.GetTaskOutputFiles(authInfo.Workspace.Name, task)

	for outputId, fileName := range outputFiles {
		url, err := output.SetPublicURL(ctx, g.config, g.backendRepo, g.redisClient, authInfo, task.ExternalId, outputId, fileName, DefaultTaskOutputExpirationS)
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

	// If the stub type is function and we have the container id, force stop the container immediately
	if task.Stub.Type.Kind() == types.StubTypeFunction && task.ContainerId != "" {
		err = g.scheduler.Stop(&types.StopContainerArgs{
			ContainerId: task.ContainerId,
			Reason:      types.StopContainerReasonUser,
			Force:       true,
		})
		if err != nil {
			log.Error().Str("container_id", task.ContainerId).Msgf("failed to stop container: %v", err)
		}
	}

	task.Status = types.TaskStatusCancelled
	task.EndedAt = types.NullTime{}.Now()
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

	public, _ := strconv.ParseBool(ctx.QueryParam("public"))
	if public {
		filters.ExternalWorkspaceID = workspace.Id
		filters.WorkspaceID = 0
	} else {
		filters.WorkspaceID = workspace.Id
	}

	return &filters, nil
}

func (g *TaskGroup) GetClusterTaskMetrics(ctx echo.Context) error {
	query := ctx.QueryParams()

	startedAtTimestamp, err := strconv.ParseInt(query.Get("started_at"), 10, 64)
	if err != nil {
		return HTTPBadRequest("Invalid started_at")
	}

	startedAt := time.Unix(startedAtTimestamp, 0)
	if err != nil {
		return HTTPBadRequest("Invalid started_at")
	}

	endedAtTimestamp, err := strconv.ParseInt(query.Get("ended_at"), 10, 64)
	if err != nil {
		return HTTPBadRequest("Invalid ended_at")
	}

	endedAt := time.Unix(endedAtTimestamp, 0)
	if err != nil {
		return HTTPBadRequest("Invalid ended_at")
	}

	if metrics, err := g.backendRepo.GetTaskMetrics(ctx.Request().Context(), startedAt, endedAt); err != nil {
		return HTTPInternalServerError("Failed to retrieve cluster task metrics")
	} else {
		return ctx.JSON(http.StatusOK, metrics)
	}
}
