package gatewayservices

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (gws *GatewayService) StartTask(ctx context.Context, in *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	task, err := gws.backendRepo.GetTaskWithRelated(ctx, in.TaskId)
	if err != nil {
		return &pb.StartTaskResponse{
			Ok: false,
		}, nil
	}

	task.StartedAt = sql.NullTime{Time: time.Now(), Valid: true}
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
	task, err := gws.backendRepo.GetTaskWithRelated(ctx, in.TaskId)
	if err != nil {
		return &pb.EndTaskResponse{
			Ok: false,
		}, nil
	}

	task.EndedAt = sql.NullTime{Time: time.Now(), Valid: true}
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
			CreatedAt:     timestamppb.New(task.CreatedAt),
			UpdatedAt:     timestamppb.New(task.UpdatedAt),
		}
	}

	return response, nil
}

func (gws *GatewayService) StopTask(ctx context.Context, in *pb.StopTaskRequest) (*pb.StopTaskResponse, error) {
	cc, _ := auth.
	task, err := gws.backendRepo.GetTaskWithRelated(ctx, in.TaskId)
	if err != nil {
		return &pb.StopTaskResponse{
			Ok:     false,
			ErrMsg: "Failed to retrieve task",
		}, nil
	}

	if task.Status.IsCompleted() {
		return &pb.StopTaskResponse{Ok: true}, nil
	}

	err = gws.taskDispatcher.Complete(ctx, task.Workspace.Name, task.Stub.ExternalId, in.TaskId)
	if err != nil {
		return &pb.StopTaskResponse{
			Ok:     false,
			ErrMsg: "Failed to complete task",
		}, nil
	}

	// gws.redisClient.Publish(ctx, common.RedisKeys.TaskCancel())

	if err := gws.scheduler.Stop(task.ContainerId); err != nil {
		return &pb.StopTaskResponse{
			Ok:     false,
			ErrMsg: "Failed to stop container",
		}, nil
	}

	task.Status = types.TaskStatusCancelled
	task.EndedAt = sql.NullTime{Time: time.Now(), Valid: true}
	if _, err := gws.backendRepo.UpdateTask(ctx, in.TaskId, task.Task); err != nil {
		return &pb.StopTaskResponse{
			Ok:     false,
			ErrMsg: "Failed to update task",
		}, nil
	}

	return &pb.StopTaskResponse{Ok: true}, nil
}
