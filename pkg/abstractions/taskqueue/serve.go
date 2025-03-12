package taskqueue

import (
	"context"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
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
		common.RedisKeys.SchedulerServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
		1,
		timeoutDuration,
	)

	container, err := instance.WaitForContainer(ctx, taskQueueServeContainerTimeout)
	if err != nil {
		return &pb.StartTaskQueueServeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	// Remove the container lock and rely on the serve lock to keep the container alive
	instance.Rdb.Del(
		context.Background(),
		Keys.taskQueueKeepWarmLock(instance.Workspace.Name, instance.Stub.ExternalId, container.ContainerId),
	)

	return &pb.StartTaskQueueServeResponse{
		Ok:          true,
		ContainerId: container.ContainerId,
	}, nil
}
