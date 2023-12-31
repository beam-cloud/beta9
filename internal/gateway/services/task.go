package gatewayservices

import (
	"context"
	"database/sql"
	"time"

	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
)

func (gws *GatewayService) StartTask(ctx context.Context, in *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	task, err := gws.backendRepo.GetTask(ctx, in.TaskId)
	if err != nil {
		return &pb.StartTaskResponse{
			Ok: false,
		}, nil
	}

	task.StartedAt = sql.NullTime{Time: time.Now(), Valid: true}
	task.Status = types.TaskStatusRunning

	_, err = gws.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	return &pb.StartTaskResponse{
		Ok: err == nil,
	}, nil
}

func (gws *GatewayService) EndTask(ctx context.Context, in *pb.EndTaskRequest) (*pb.EndTaskResponse, error) {
	task, err := gws.backendRepo.GetTask(ctx, in.TaskId)
	if err != nil {
		return &pb.EndTaskResponse{
			Ok: false,
		}, nil
	}

	task.EndedAt = sql.NullTime{Time: time.Now(), Valid: true}
	task.Status = in.TaskStatus

	_, err = gws.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	return &pb.EndTaskResponse{
		Ok: err == nil,
	}, nil
}
