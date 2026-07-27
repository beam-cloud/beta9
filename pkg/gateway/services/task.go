package gatewayservices

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	taskmetrics "github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (gws *GatewayService) StartTask(ctx context.Context, in *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	if in == nil {
		return &pb.StartTaskResponse{Ok: false}, nil
	}

	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !canUseTaskLifecycle(authInfo) {
		log.Warn().Str("task_id", in.TaskId).Str("container_id", in.ContainerId).Msg("start task rejected by auth")
		return &pb.StartTaskResponse{
			Ok: false,
		}, nil
	}

	task, err := gws.backendRepo.GetTaskWithRelated(ctx, in.TaskId)
	if err != nil {
		log.Warn().Err(err).Str("task_id", in.TaskId).Str("container_id", in.ContainerId).Msg("start task failed to load task")
		return &pb.StartTaskResponse{
			Ok: false,
		}, nil
	}

	if task == nil {
		log.Warn().Str("task_id", in.TaskId).Str("container_id", in.ContainerId).Msg("start task missing task")
		return &pb.StartTaskResponse{Ok: false}, nil
	}

	if task.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		log.Warn().
			Str("task_id", in.TaskId).
			Str("container_id", in.ContainerId).
			Str("task_workspace_id", task.Workspace.ExternalId).
			Str("auth_workspace_id", authInfo.Workspace.ExternalId).
			Msg("start task rejected by workspace mismatch")
		return &pb.StartTaskResponse{Ok: false}, nil
	}

	if task.Status.IsCompleted() {
		log.Warn().Str("task_id", in.TaskId).Str("container_id", in.ContainerId).Str("status", string(task.Status)).Msg("start task rejected because task is completed")
		return &pb.StartTaskResponse{Ok: false}, nil
	}

	startedAt := time.Now()
	task.StartedAt = types.NullTime{Time: startedAt, Valid: true}
	task.Status = types.TaskStatusRunning

	if in.ContainerId != "" {
		task.ContainerId = in.ContainerId
	}

	err = gws.taskDispatcher.Claim(ctx, task.Workspace.Name, task.Stub.ExternalId, task.ExternalId, task.ContainerId)
	if err != nil {
		log.Warn().
			Err(err).
			Str("task_id", task.ExternalId).
			Str("container_id", task.ContainerId).
			Str("workspace_id", task.Workspace.ExternalId).
			Str("workspace_name", task.Workspace.Name).
			Str("stub_id", task.Stub.ExternalId).
			Msg("start task failed to claim task")
		return &pb.StartTaskResponse{Ok: false}, nil
	}

	if _, err = gws.backendRepo.UpdateTask(ctx, task.ExternalId, task.Task); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		releaseErr := gws.taskDispatcher.Release(cleanupCtx, task.Workspace.Name, task.Stub.ExternalId, task.ExternalId)
		cancel()
		log.Warn().
			Err(errors.Join(err, releaseErr)).
			Str("task_id", task.ExternalId).
			Str("container_id", task.ContainerId).
			Str("workspace_id", task.Workspace.ExternalId).
			Str("stub_id", task.Stub.ExternalId).
			Msg("start task failed to update task")
		return &pb.StartTaskResponse{Ok: false}, nil
	}

	phaseMetrics := taskmetrics.NewPhaseMetrics(gws.redisClient)
	phaseLabels := taskmetrics.FunctionPhaseLabelsFromTask(task)
	if err := phaseMetrics.StoreLabels(ctx, task.Workspace.Name, task.ExternalId, phaseLabels); err != nil {
		log.Debug().Err(err).Str("task_id", task.ExternalId).Msg("failed to store function phase metric labels")
	}
	if err := phaseMetrics.Mark(ctx, task.Workspace.Name, task.ExternalId, taskmetrics.FunctionPhaseStartTask, startedAt); err != nil {
		log.Debug().Err(err).Str("task_id", task.ExternalId).Msg("failed to mark function start_task phase")
	}
	if !task.CreatedAt.IsZero() {
		metrics.RecordFunctionTaskPhase("task_created_to_start_task", startedAt.Sub(task.CreatedAt.Time), phaseLabels)
	}
	phaseMetrics.RecordSince(ctx, task.Workspace.Name, task.ExternalId, "container_request_ready_to_start_task", taskmetrics.FunctionPhaseContainerRequestReady, startedAt, phaseLabels)
	if gws.eventRepo != nil {
		gws.eventRepo.PushTaskStartEvents(ctx, gws.redisClient, task, in.ContainerId, startedAt)
	}
	gws.recordContainerToStartTaskPhases(ctx, task, startedAt, phaseLabels)

	return &pb.StartTaskResponse{Ok: true}, nil
}

func (gws *GatewayService) EndTask(ctx context.Context, in *pb.EndTaskRequest) (*pb.EndTaskResponse, error) {
	if in == nil {
		return &pb.EndTaskResponse{Ok: false}, nil
	}

	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !canUseTaskLifecycle(authInfo) {
		return &pb.EndTaskResponse{
			Ok: false,
		}, nil
	}

	task, err := gws.backendRepo.GetTaskWithRelated(ctx, in.TaskId)
	if err != nil {
		return &pb.EndTaskResponse{
			Ok: false,
		}, nil
	}

	if task == nil {
		return &pb.EndTaskResponse{Ok: false}, nil
	}

	if task.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		return &pb.EndTaskResponse{Ok: false}, nil
	}

	requestedStatus := types.TaskStatus(in.TaskStatus)
	if task.Status.IsCompleted() && !(requestedStatus == types.TaskStatusComplete && task.Status != types.TaskStatusComplete) {
		return &pb.EndTaskResponse{
			Ok: true,
		}, nil
	}

	endedAt := time.Now()
	task.EndedAt = types.NullTime{Time: endedAt, Valid: true}
	task.Status = requestedStatus

	if in.ContainerId != "" {
		task.ContainerId = in.ContainerId
	}

	phaseMetrics := taskmetrics.NewPhaseMetrics(gws.redisClient)
	phaseLabels := taskmetrics.FunctionPhaseLabelsFromTask(task)
	if task.Status != types.TaskStatusComplete {
		phaseLabels["success"] = "false"
	}
	phaseMetrics.RecordSince(ctx, task.Workspace.Name, task.ExternalId, "set_result_to_end_task", taskmetrics.FunctionPhaseSetResult, endedAt, phaseLabels)
	phaseMetrics.RecordSince(ctx, task.Workspace.Name, task.ExternalId, "start_task_to_end_task", taskmetrics.FunctionPhaseStartTask, endedAt, phaseLabels)
	if gws.eventRepo != nil {
		gws.eventRepo.PushTaskEndEvents(ctx, gws.redisClient, task, endedAt)
	}

	var workspace *types.Workspace = authInfo.Workspace

	// Track cost for external/public tasks
	if task.ExternalWorkspaceId != nil {
		workspace, err = gws.backendRepo.GetWorkspace(context.Background(), *task.ExternalWorkspaceId)
		if err != nil {
			return &pb.EndTaskResponse{
				Ok: false,
			}, nil

		}

		duration := time.Duration(float64(in.TaskDuration) * float64(time.Millisecond))
		err = gws.trackExternalTaskCost(task, workspace, duration)
		if err != nil {
			return &pb.EndTaskResponse{
				Ok: false,
			}, nil
		}
	}

	// Store task result in persistent storage
	if in.Result != nil && workspace.StorageAvailable() {
		err = gws.taskDispatcher.StoreTaskResult(workspace, task.ExternalId, in.Result)
		if err != nil {
			log.Error().Err(err).Msgf("error storing task result for task <%s>", task.ExternalId)
		}
	}

	err = gws.taskDispatcher.Complete(ctx, task.Workspace.Name, task.Stub.ExternalId, in.TaskId)
	if err != nil {
		return &pb.EndTaskResponse{
			Ok: false,
		}, nil
	}

	_, err = gws.backendRepo.UpdateTask(ctx, task.ExternalId, task.Task)
	if err == nil && gws.eventRepo != nil {
		gws.eventRepo.PushTaskEndPersisted(task)
	}
	return &pb.EndTaskResponse{
		Ok: err == nil,
	}, nil
}

func canUseTaskLifecycle(authInfo *auth.AuthInfo) bool {
	if authInfo == nil || authInfo.Token == nil || authInfo.Workspace == nil {
		return false
	}
	if authInfo.Token.TokenType == types.TokenTypeWorkspaceRestricted {
		return true
	}
	return auth.HasPermission(authInfo)
}

func (gws *GatewayService) recordContainerToStartTaskPhases(ctx context.Context, task *types.TaskWithRelated, startedAt time.Time, labels map[string]string) {
	if task == nil || task.ContainerId == "" || gws.containerRepo == nil {
		return
	}

	containerState, err := gws.containerRepo.GetContainerState(task.ContainerId)
	if err != nil {
		log.Debug().Err(err).Str("container_id", task.ContainerId).Msg("failed to load container state for function phase metrics")
		return
	}

	containerLabels := copyPhaseLabels(labels)
	containerLabels["container_status"] = string(containerState.Status)

	if containerState.ScheduledAt > 0 {
		scheduledAt := time.Unix(containerState.ScheduledAt, 0)
		if !startedAt.Before(scheduledAt) {
			metrics.RecordFunctionTaskPhase("container_scheduled_to_start_task", startedAt.Sub(scheduledAt), containerLabels)
		}
	}

	if containerState.StartedAt > 0 {
		runningAt := time.Unix(containerState.StartedAt, 0)
		if !startedAt.Before(runningAt) {
			metrics.RecordFunctionTaskPhase("container_running_to_start_task", startedAt.Sub(runningAt), containerLabels)
			if gws.eventRepo != nil {
				gws.eventRepo.PushContainerRunningToStartTask(task, runningAt, startedAt, containerState.Status)
			}
		}
	}
}

func copyPhaseLabels(labels map[string]string) map[string]string {
	copied := make(map[string]string, len(labels))
	for key, value := range labels {
		copied[key] = value
	}
	return copied
}

func (gws *GatewayService) trackExternalTaskCost(task *types.TaskWithRelated, externalWorkspace *types.Workspace, duration time.Duration) error {
	stubWithRelated, err := gws.backendRepo.GetStubByExternalId(context.Background(), task.Stub.ExternalId)
	if err != nil {
		return err
	}

	stubConfig := types.StubConfigV1{}
	err = json.Unmarshal([]byte(task.Stub.Config), &stubConfig)
	if err != nil {
		return err
	}

	abstractions.TrackTaskCost(duration, stubWithRelated, stubConfig.Pricing, gws.usageMetricsRepo, task.ExternalId, externalWorkspace.ExternalId)
	return nil
}

func (gws *GatewayService) ListTasks(ctx context.Context, in *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	var taskFilter types.TaskFilter = types.TaskFilter{
		WorkspaceID: authInfo.Workspace.Id,
	}

	limit := uint32(1000)
	if in.Limit > 0 && in.Limit < limit {
		limit = in.Limit
	}
	taskFilter.Limit = limit

	// Maps filter key to db field
	for clientField, value := range in.Filters {
		switch clientField {
		case "status":
			taskFilter.Status = strings.Join(value.Values, ",")
		case "stub-id", "stub-ids", "stub_id", "stub_ids":
			taskFilter.StubIds = value.Values
		case "stub-name", "stub-names", "stub_name", "stub_names":
			taskFilter.StubNames = value.Values
		case "id", "ids", "task-id", "task-ids", "task_id", "task_ids":
			taskFilter.TaskIds = value.Values
		case "container-id", "container-ids", "container_id", "container_ids":
			taskFilter.ContainerIds = value.Values
		}
	}

	tasks, err := gws.backendRepo.ListTasksWithRelated(ctx, taskFilter)
	if err != nil {
		return &pb.ListTasksResponse{
			Ok:     false,
			ErrMsg: "Failed to retrieve tasks.",
		}, nil
	}

	response := &pb.ListTasksResponse{
		Ok:    true,
		Total: int32(len(tasks)),
		Tasks: make([]*pb.Task, len(tasks)),
	}
	for i, task := range tasks {
		response.Tasks[i] = &pb.Task{
			Id:            task.ExternalId,
			Status:        string(task.Status),
			ContainerId:   task.ContainerId,
			StartedAt:     timestamppb.New(task.StartedAt.Time),
			EndedAt:       timestamppb.New(task.EndedAt.Time),
			WorkspaceId:   task.Workspace.ExternalId,
			WorkspaceName: task.Workspace.Name,
			StubId:        task.Stub.ExternalId,
			StubName:      task.Stub.Name,
			CreatedAt:     timestamppb.New(task.CreatedAt.Time),
			UpdatedAt:     timestamppb.New(task.UpdatedAt.Time),
		}
	}

	return response, nil
}

func (gws *GatewayService) StopTasks(ctx context.Context, in *pb.StopTasksRequest) (*pb.StopTasksResponse, error) {
	authInfo, authenticated := auth.AuthInfoFromContext(ctx)
	if !authenticated {
		return &pb.StopTasksResponse{Ok: false, ErrMsg: "Invalid token"}, nil
	}

	for _, taskId := range in.TaskIds {
		task, err := gws.backendRepo.GetTaskWithRelated(ctx, taskId)
		if err != nil {
			return &pb.StopTasksResponse{Ok: false, ErrMsg: err.Error()}, nil
		}

		if task == nil {
			return &pb.StopTasksResponse{Ok: false, ErrMsg: "Task not found"}, nil
		}

		if task.Workspace.ExternalId != authInfo.Workspace.ExternalId {
			return &pb.StopTasksResponse{Ok: false, ErrMsg: "Invalid workspace ID"}, nil
		}

		err = gws.stopTask(ctx, authInfo, task)
		if err != nil {
			return &pb.StopTasksResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
	}

	return &pb.StopTasksResponse{Ok: true}, nil
}

// TODO: consolidate this logic with the stopTask function in /api/v1/task.go
func (gws *GatewayService) stopTask(ctx context.Context, authInfo *auth.AuthInfo, task *types.TaskWithRelated) error {
	if task.Status.IsCompleted() {
		return nil
	}

	if gws.eventRepo != nil {
		gws.eventRepo.PushTaskCancelRequested(task, types.EventSourceGatewayStopTask, types.EventMessageGatewayTaskStopRequested)
	}
	err := gws.taskDispatcher.Complete(ctx, task.Workspace.Name, task.Stub.ExternalId, task.ExternalId)
	if err != nil {
		return errors.New("failed to complete task")
	}

	err = gws.redisClient.Publish(ctx, common.RedisKeys.TaskCancel(authInfo.Workspace.Name, task.Stub.ExternalId, task.ExternalId), task.ExternalId).Err()
	if err != nil {
		return errors.New("failed to cancel task")
	}

	// Pod run containers have no runner inside listening for task cancellation,
	// so force stop the container directly
	if task.Stub.Type.Kind() == types.StubTypePod && task.ContainerId != "" && gws.scheduler != nil {
		err = gws.scheduler.Stop(&types.StopContainerArgs{
			ContainerId: task.ContainerId,
			Reason:      types.StopContainerReasonUser,
			Force:       true,
		})
		if err != nil {
			log.Error().Str("container_id", task.ContainerId).Err(err).Msg("failed to stop pod run container")
		}
	}

	task.Status = types.TaskStatusCancelled
	task.EndedAt = types.NullTime{}.Now()
	if _, err := gws.backendRepo.UpdateTask(ctx, task.ExternalId, task.Task); err != nil {
		return errors.New("failed to update task")
	}
	if gws.eventRepo != nil {
		gws.eventRepo.PushTaskCancelApplied(task, types.EventSourceGatewayStopTask, types.EventMessageGatewayTaskCancellationApplied)
	}

	return nil
}
