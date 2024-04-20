package taskqueue

import (
	"context"
	"time"

	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/auth"
	common "github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (tq *RedisTaskQueue) StartTaskQueueServe(in *pb.StartTaskQueueServeRequest, stream pb.TaskQueueService_StartTaskQueueServeServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	err := tq.createQueueInstance(in.StubId,
		withEntryPoint(func(instance *taskQueueInstance) []string {
			return []string{instance.StubConfig.PythonVersion, "-m", "beta9.runner.serve"}
		}),
		withAutoscaler(func(instance *taskQueueInstance) *abstractions.Autoscaler[*taskQueueInstance, *taskQueueAutoscalerSample] {
			return abstractions.NewAutoscaler(instance, taskQueueAutoscalerSampleFunc, taskQueueServeScaleFunc)
		}),
	)
	if err != nil {
		return err
	}

	// Set lock (used by autoscaler to scale up the single serve container)
	instance, _ := tq.queueInstances.Get(in.StubId)
	instance.Rdb.SetEx(
		context.Background(),
		Keys.taskQueueServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
		1,
		taskQueueServeContainerTimeout,
	)

	container, err := instance.WaitForContainer(ctx, taskQueueServeContainerTimeout)
	if err != nil {
		return err
	}

	sendCallback := func(o common.OutputMsg) error {
		if err := stream.Send(&pb.StartTaskQueueServeResponse{Output: o.Msg, Done: o.Done}); err != nil {
			return err
		}

		return nil
	}

	exitCallback := func(exitCode int32) error {
		if err := stream.Send(&pb.StartTaskQueueServeResponse{Done: true, ExitCode: int32(exitCode)}); err != nil {
			return err
		}
		return nil
	}

	// Keep serve container active for as long as user has their terminal open
	// We can handle timeouts on the client side
	go func() {
		ticker := time.NewTicker(taskQueueServeContainerKeepaliveInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				instance.Rdb.SetEx(
					context.Background(),
					Keys.taskQueueServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
					1,
					taskQueueServeContainerTimeout,
				)
			}
		}
	}()

	logStream, err := abstractions.NewLogStream(abstractions.LogStreamOpts{
		SendCallback:    sendCallback,
		ExitCallback:    exitCallback,
		ContainerRepo:   tq.containerRepo,
		Config:          tq.config,
		Tailscale:       tq.tailscale,
		KeyEventManager: tq.keyEventManager,
	})
	if err != nil {
		return err
	}

	return logStream.Stream(ctx, authInfo, container.ContainerId)
}

func (tq *RedisTaskQueue) StopTaskQueueServe(ctx context.Context, in *pb.StopTaskQueueServeRequest) (*pb.StopTaskQueueServeResponse, error) {
	_, exists := tq.queueInstances.Get(in.StubId)
	if !exists {
		err := tq.createQueueInstance(in.StubId,
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
	}

	instance, _ := tq.queueInstances.Get(in.StubId)

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
