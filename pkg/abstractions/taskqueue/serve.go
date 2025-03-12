package taskqueue

import (
	"context"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	serveKeepAliveInterval = 5 * time.Second
)

func (tq *RedisTaskQueue) StartTaskQueueServe(in *pb.StartTaskQueueServeRequest, stream pb.TaskQueueService_StartTaskQueueServeServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	instance, err := tq.getOrCreateQueueInstance(in.StubId,
		withEntryPoint(func(instance *taskQueueInstance) []string {
			return []string{instance.StubConfig.PythonVersion, "-m", "beta9.runner.serve"}
		}),
		withAutoscaler(func(instance *taskQueueInstance) *abstractions.Autoscaler[*taskQueueInstance, *taskQueueAutoscalerSample] {
			return abstractions.NewAutoscaler(instance, taskQueueAutoscalerSampleFunc, taskQueueServeScaleFunc)
		}),
	)
	if err != nil {
		return stream.Send(&pb.StartTaskQueueServeResponse{Ok: false, ErrorMsg: err.Error()})
	}

	if authInfo.Workspace.ExternalId != instance.Workspace.ExternalId {
		return stream.Send(&pb.StartTaskQueueServeResponse{Ok: false})
	}

	go tq.eventRepo.PushServeStubEvent(instance.Workspace.ExternalId, &instance.Stub.Stub)

	var timeoutDuration time.Duration = taskQueueServeContainerTimeout
	if in.Timeout > 0 {
		timeoutDuration = time.Duration(in.Timeout) * time.Second
	}

	// Set lock (used by autoscaler to scale up the single serve container)
	instance.Rdb.SetEx(
		context.Background(),
		Keys.taskQueueServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
		1,
		timeoutDuration,
	)
	defer instance.Rdb.Del(
		context.Background(),
		Keys.taskQueueServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
	)

	container, err := instance.WaitForContainer(ctx, taskQueueServeContainerTimeout)
	if err != nil {
		return stream.Send(&pb.StartTaskQueueServeResponse{Ok: false, ErrorMsg: err.Error()})
	}

	// Remove the container lock and rely on the serve lock to keep the container alive
	instance.Rdb.Del(
		context.Background(),
		Keys.taskQueueKeepWarmLock(instance.Workspace.Name, instance.Stub.ExternalId, container.ContainerId),
	)

	response := &pb.StartTaskQueueServeResponse{
		Ok:          true,
		ContainerId: container.ContainerId,
	}

	if err := stream.Send(response); err != nil {
		return err
	}

	// Keep the container alive
	ticker := time.NewTicker(serveKeepAliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			instance.Rdb.SetEx(
				context.Background(),
				Keys.taskQueueServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
				1,
				timeoutDuration,
			)

			if err := stream.Send(response); err != nil {
				return err
			}
		}
	}
}
