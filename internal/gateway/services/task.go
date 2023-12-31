package gatewayservices

import (
	"context"
	"database/sql"
	"time"

	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
)

// func (gws *GatewayService) GetNextTask(ctx context.Context, in *pb.GetNextTaskRequest) (*pb.GetNextTaskResponse, error) {
// 	taskAvailable := false

// 	task, err := wbs.Scheduler.taskRepo.GetNextTask(in.QueueName, in.ContainerId, identity.ExternalId)
// 	if task != nil && err == nil {
// 		taskAvailable = true
// 	}

// 	return &pb.GetNextTaskResponse{
// 		Task:          task,
// 		TaskAvailable: taskAvailable,
// 	}, nil
// }

// func (gws *GatewayService) GetTaskStream(req *pb.GetTaskStreamRequest, stream pb.Scheduler_GetTaskStreamServer) error {
// 	identity, authorized, err := wbs.Scheduler.beamRepo.AuthorizeServiceToServiceToken(req.S2SToken)
// 	if err != nil || !authorized {
// 		return err
// 	}

// 	return wbs.Scheduler.taskRepo.GetTaskStream(req.QueueName, req.ContainerId, identity.ExternalId, stream)
// }

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
	task.Status = types.TaskStatusComplete

	_, err = gws.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	return &pb.EndTaskResponse{
		Ok: err == nil,
	}, nil
}

// func (gws *GatewayService) MonitorTask(req *pb.MonitorTaskRequest, stream pb.Scheduler_MonitorTaskServer) error {
// 	identity, authorized, err := wbs.Scheduler.beamRepo.AuthorizeServiceToServiceToken(req.S2SToken)
// 	if err != nil || !authorized {
// 		return errors.New("invalid s2s token")
// 	}

// 	task, err := wbs.Scheduler.beamRepo.GetAppTask(req.TaskId)
// 	if err != nil {
// 		return err
// 	}

// 	taskPolicy := types.TaskPolicy{}
// 	err = json.Unmarshal([]byte(task.TaskPolicy), &taskPolicy)
// 	if err != nil {
// 		taskPolicy = common.DefaultTaskPolicy
// 	}

// 	timeoutCallback := func() error {
// 		_, err = wbs.Scheduler.beamRepo.UpdateActiveTask(
// 			task.TaskId,
// 			types.BeamAppTaskStatusTimeout,
// 			identity.ExternalId,
// 		)
// 		if err != nil {
// 			return err
// 		}

// 		return nil
// 	}

// 	return wbs.Scheduler.taskRepo.MonitorTask(
// 		task,
// 		req.QueueName,
// 		req.ContainerId,
// 		identity.ExternalId,
// 		int64(taskPolicy.Timeout),
// 		stream,
// 		timeoutCallback,
// 	)
// }
