package gatewayservices

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (gws *GatewayService) StartTask(ctx context.Context, in *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	task, err := gws.backendRepo.GetTaskWithRelated(ctx, in.TaskId)
	if err != nil {
		return &pb.StartTaskResponse{
			Ok: false,
		}, nil
	}

	if task == nil {
		return &pb.StartTaskResponse{Ok: false}, nil
	}

	if task.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		return &pb.StartTaskResponse{Ok: false}, nil
	}

	task.StartedAt = types.NullTime{
		NullTime: sql.NullTime{
			Time:  time.Now(),
			Valid: true,
		},
	}
	task.Status = types.TaskStatusRunning

	if in.ContainerId != "" {
		task.ContainerId = in.ContainerId
	}

	err = gws.taskDispatcher.Claim(ctx, task.Workspace.Name, task.Stub.ExternalId, task.ExternalId, task.ContainerId)
	if err != nil {
		return &pb.StartTaskResponse{
			Ok: false,
		}, nil
	}

	_, err = gws.backendRepo.UpdateTask(ctx, task.ExternalId, task.Task)
	return &pb.StartTaskResponse{
		Ok: err == nil,
	}, nil
}

func (gws *GatewayService) EndTask(ctx context.Context, in *pb.EndTaskRequest) (*pb.EndTaskResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

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

	if task.Status.IsCompleted() {
		return &pb.EndTaskResponse{
			Ok: true,
		}, nil
	}

	task.EndedAt = types.NullTime{
		NullTime: sql.NullTime{
			Time:  time.Now(),
			Valid: true,
		},
	}
	task.Status = types.TaskStatus(in.TaskStatus)

	if in.ContainerId != "" {
		task.ContainerId = in.ContainerId
	}

	err = gws.taskDispatcher.Complete(ctx, task.Workspace.Name, task.Stub.ExternalId, in.TaskId)
	if err != nil {
		return &pb.EndTaskResponse{
			Ok: false,
		}, nil
	}

	_, err = gws.backendRepo.UpdateTask(ctx, task.ExternalId, task.Task)
	return &pb.EndTaskResponse{
		Ok: err == nil,
	}, nil
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

	err := gws.taskDispatcher.Complete(ctx, task.Workspace.Name, task.Stub.ExternalId, task.ExternalId)
	if err != nil {
		return errors.New("failed to complete task")
	}

	err = gws.redisClient.Publish(ctx, common.RedisKeys.TaskCancel(authInfo.Workspace.Name, task.Stub.ExternalId, task.ExternalId), task.ExternalId).Err()
	if err != nil {
		return errors.New("failed to cancel task")
	}

	task.Status = types.TaskStatusCancelled
	task.EndedAt = types.NullTime{
		NullTime: sql.NullTime{
			Time:  time.Now(),
			Valid: true,
		},
	}
	if _, err := gws.backendRepo.UpdateTask(ctx, task.ExternalId, task.Task); err != nil {
		return errors.New("failed to update task")
	}

	return nil
}
