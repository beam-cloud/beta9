package taskqueue

import (
	"context"
	"errors"
	"time"

	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
	"github.com/beam-cloud/beta9/internal/auth"
	common "github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (tq *RedisTaskQueue) waitForContainer(ctx context.Context, stubId string) (*types.ContainerState, error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeout := time.After(taskQueueServeContainerTimeout)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, errors.New("timed out waiting for a container")
		case <-ticker.C:
			containers, err := tq.containerRepo.GetActiveContainersByStubId(stubId)
			if err != nil {
				return nil, err
			}

			if len(containers) > 0 {
				return &containers[0], nil
			}
		}
	}
}

func (tq *RedisTaskQueue) StartTaskQueueServe(in *pb.StartTaskQueueServeRequest, stream pb.TaskQueueService_StartTaskQueueServeServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	err := tq.createQueueInstance(in.StubId,
		withEntryPoint(func(instance *taskQueueInstance) []string {
			return []string{instance.stubConfig.PythonVersion, "-m", "beta9.runner.serve"}
		}),
		withAutoscaler(func(instance *taskQueueInstance) *abstractions.AutoScaler[*taskQueueInstance, *taskQueueAutoscalerSample] {
			return abstractions.NewAutoscaler(instance, taskQueueAutoscalerSampleFunc, taskQueueServeScaleFunc)
		}),
	)
	if err != nil {
		return err
	}

	// Set lock (used by autoscaler to scale up the single serve container)
	instance, _ := tq.queueInstances.Get(in.StubId)
	instance.rdb.SetEx(
		context.Background(),
		Keys.taskQueueServeLock(instance.workspace.Name, instance.stub.ExternalId),
		1,
		taskQueueServeContainerTimeout,
	)

	container, err := tq.waitForContainer(ctx, in.StubId)
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
				instance.rdb.SetEx(
					context.Background(),
					Keys.taskQueueServeLock(instance.workspace.Name, instance.stub.ExternalId),
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
