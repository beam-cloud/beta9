package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/abstractions/output"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
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
	routerGroup        *echo.Group
	config             types.AppConfig
	backendRepo        repository.BackendRepository
	taskRepo           repository.TaskRepository
	containerRepo      repository.ContainerRepository
	redisClient        *common.RedisClient
	taskDispatcher     *task.Dispatcher
	scheduler          *scheduler.Scheduler
	storageClientCache sync.Map
}

func NewTaskGroup(g *echo.Group, redisClient *common.RedisClient, taskRepo repository.TaskRepository, containerRepo repository.ContainerRepository, backendRepo repository.BackendRepository, taskDispatcher *task.Dispatcher, scheduler *scheduler.Scheduler, config types.AppConfig) *TaskGroup {
	group := &TaskGroup{routerGroup: g,
		backendRepo:        backendRepo,
		taskRepo:           taskRepo,
		containerRepo:      containerRepo,
		config:             config,
		redisClient:        redisClient,
		taskDispatcher:     taskDispatcher,
		scheduler:          scheduler,
		storageClientCache: sync.Map{},
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListTasksPaginated))
	g.GET("/:workspaceId/task-count-by-deployment", auth.WithWorkspaceAuth(group.GetTaskCountByDeployment))
	g.GET("/:workspaceId/aggregate-by-time-window", auth.WithWorkspaceAuth(group.AggregateTasksByTimeWindow))
	g.DELETE("/:workspaceId", auth.WithWorkspaceAuth(group.StopTasks))
	g.GET("/:workspaceId/:taskId", auth.WithWorkspaceAuth(group.RetrieveTask))
	g.GET("/:workspaceId/:taskId/subscribe", auth.WithWorkspaceAuth(group.SubscribeTask))
	g.GET("/metrics", auth.WithClusterAdminAuth(group.GetClusterTaskMetrics))

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

func (g *TaskGroup) SubscribeTask(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	taskId := ctx.Param("taskId")
	if taskId == "" {
		return HTTPBadRequest("Missing task ID")
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

		task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), taskId)
		if err != nil {
			return HTTPInternalServerError("Failed to retrieve task")
		}

		if task == nil {
			return HTTPNotFound()
		}

		if task.WorkspaceId != cc.AuthInfo.Workspace.Id && *task.ExternalWorkspaceId != cc.AuthInfo.Workspace.Id {
			return HTTPNotFound()
		}

		task.Workspace = *cc.AuthInfo.Workspace
		task.Stub.SanitizeConfig()

		g.addOutputsToTask(ctx.Request().Context(), cc.AuthInfo, task)
		g.addStatsToTask(ctx.Request().Context(), cc.AuthInfo.Workspace.Name, task)
		g.addResultToTask(ctx.Request().Context(), task, cc.AuthInfo)

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

	taskId := ctx.Param("taskId")
	if task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), taskId); err != nil {
		return HTTPInternalServerError("Failed to retrieve task")
	} else {
		if task == nil {
			return HTTPNotFound()
		}

		if task.WorkspaceId != cc.AuthInfo.Workspace.Id && *task.ExternalWorkspaceId != cc.AuthInfo.Workspace.Id {
			return HTTPNotFound()
		}

		task.Workspace = *cc.AuthInfo.Workspace
		task.Stub.SanitizeConfig()

		g.addOutputsToTask(ctx.Request().Context(), cc.AuthInfo, task)
		g.addStatsToTask(ctx.Request().Context(), cc.AuthInfo.Workspace.Name, task)
		g.addResultToTask(ctx.Request().Context(), task, cc.AuthInfo)

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

func (g *TaskGroup) addResultToTask(ctx context.Context, t *types.TaskWithRelated, authInfo *auth.AuthInfo) error {
	var storageClient *clients.WorkspaceStorageClient
	var err error

	if authInfo.Workspace.StorageAvailable() {
		if cachedStorageClient, ok := g.storageClientCache.Load(authInfo.Workspace.Name); ok {
			storageClient = cachedStorageClient.(*clients.WorkspaceStorageClient)
		} else {
			storageClient, err = clients.NewWorkspaceStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
			if err != nil {
				return err
			}

			g.storageClientCache.Store(authInfo.Workspace.Name, storageClient)
		}

		fullPath := task.GetTaskResultPath(t.ExternalId)
		result, err := storageClient.Download(ctx, fullPath)
		if err != nil {
			return err
		}

		if len(result) > 0 {
			err = json.Unmarshal(result, &t.Result)
			if err != nil {
				return err
			}
		}
	}

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
