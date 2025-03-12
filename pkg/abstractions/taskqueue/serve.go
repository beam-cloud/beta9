package taskqueue

import (
	"context"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (tq *RedisTaskQueue) StartTaskQueueServe(ctx context.Context, in *pb.StartTaskQueueServeRequest) (*pb.StartTaskQueueServeResponse, error) {
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
		return &pb.StartTaskQueueServeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	if authInfo.Workspace.ExternalId != instance.Workspace.ExternalId {
		return &pb.StartTaskQueueServeResponse{Ok: false}, nil
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

	container, err := instance.WaitForContainer(ctx, taskQueueServeContainerTimeout)
	if err != nil {
		return &pb.StartTaskQueueServeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	return &pb.StartTaskQueueServeResponse{Ok: true, ContainerId: container.ContainerId}, nil
}

func (tq *RedisTaskQueue) StopTaskQueueServe(ctx context.Context, in *pb.StopTaskQueueServeRequest) (*pb.StopTaskQueueServeResponse, error) {
	instance, err := tq.getOrCreateQueueInstance(in.StubId,
		withEntryPoint(func(instance *taskQueueInstance) []string {
			return []string{instance.StubConfig.PythonVersion, "-m", "beta9.runner.serve"}
		}),
		withAutoscaler(func(instance *taskQueueInstance) *abstractions.Autoscaler[*taskQueueInstance, *taskQueueAutoscalerSample] {
			return abstractions.NewAutoscaler(instance, taskQueueAutoscalerSampleFunc, taskQueueServeScaleFunc)
		}),
	)
	if err != nil {
		return &pb.StopTaskQueueServeResponse{Ok: false}, nil
	}

	// Delete serve timeout lock
	instance.Rdb.Del(
		context.Background(),
		Keys.taskQueueServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
	)

	// Delete all keep warms
	// With serves, there should only ever be one container running, but this is the easiest way to find that container
	containers, err := instance.ContainerRepo.GetActiveContainersByStubId(instance.Stub.ExternalId)
	if err != nil {
		return nil, err
	}

	for _, container := range containers {
		if container.Status == types.ContainerStatusStopping || container.Status == types.ContainerStatusPending {
			continue
		}

		instance.Rdb.Del(
			context.Background(),
			Keys.taskQueueKeepWarmLock(instance.Workspace.Name, instance.Stub.ExternalId, container.ContainerId),
		)

	}

	return &pb.StopTaskQueueServeResponse{Ok: true}, nil
}

func (tq *RedisTaskQueue) TaskQueueServeKeepAlive(ctx context.Context, in *pb.TaskQueueServeKeepAliveRequest) (*pb.TaskQueueServeKeepAliveResponse, error) {
	instance, exists := tq.queueInstances.Get(in.StubId)
	if !exists {
		return &pb.TaskQueueServeKeepAliveResponse{Ok: false}, nil
	}

	var timeoutDuration time.Duration = taskQueueServeContainerTimeout
	if in.Timeout != 0 {
		timeoutDuration = time.Duration(in.Timeout) * time.Second
	}

	if in.Timeout < 0 {
		// There is no timeout, so we can just return
		return &pb.TaskQueueServeKeepAliveResponse{Ok: true}, nil
	}

	// Update lock expiration
	instance.Rdb.SetEx(
		context.Background(),
		Keys.taskQueueServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
		1,
		timeoutDuration,
	)

	return &pb.TaskQueueServeKeepAliveResponse{Ok: true}, nil
}
