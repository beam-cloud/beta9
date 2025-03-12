package taskqueue

import (
	"context"
	"fmt"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
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
		return err
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
	defer instance.Rdb.Del(context.Background(), Keys.taskQueueServeLock(instance.Workspace.Name, instance.Stub.ExternalId))

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
		output := "\nContainer was stopped."
		if exitCode != 0 {
			output = fmt.Sprintf("Container failed with exit code %d", exitCode)
			if types.ContainerExitCode(exitCode) == types.ContainerExitCodeOomKill {
				output = "Container was killed due to an out-of-memory error"
			}
		}

		return stream.Send(&pb.StartTaskQueueServeResponse{
			Done:     true,
			ExitCode: int32(exitCode),
			Output:   output,
		})
	}

	ctx, cancel := common.MergeContexts(tq.ctx, ctx)
	defer cancel()

	// Keep serve container active for as long as user has their terminal open
	// We can handle timeouts on the client side
	// If timeout is set to negative, we want to keep the container alive indefinitely while the user is connected
	if in.Timeout < 0 {
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
						timeoutDuration,
					)
				}
			}
		}()
	}

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
