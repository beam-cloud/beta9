package endpoint

import (
	"context"
	"fmt"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"

	pb "github.com/beam-cloud/beta9/proto"
)

func (es *HttpEndpointService) StartEndpointServe(in *pb.StartEndpointServeRequest, stream pb.EndpointService_StartEndpointServeServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	instance, err := es.getOrCreateEndpointInstance(ctx, in.StubId,
		withEntryPoint(func(instance *endpointInstance) []string {
			return []string{instance.StubConfig.PythonVersion, "-m", "beta9.runner.serve"}
		}),
		withAutoscaler(func(instance *endpointInstance) *abstractions.Autoscaler[*endpointInstance, *endpointAutoscalerSample] {
			return abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointServeScaleFunc)
		}),
	)
	if err != nil {
		return err
	}

	go es.eventRepo.PushServeStubEvent(instance.Workspace.ExternalId, &instance.Stub.Stub)

	var timeoutDuration time.Duration = endpointServeContainerTimeout
	if in.Timeout > 0 {
		timeoutDuration = time.Duration(in.Timeout) * time.Second
	}

	// Set lock (used by autoscaler to scale up the single serve container)
	instance.Rdb.SetEx(
		context.Background(),
		Keys.endpointServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
		1,
		timeoutDuration,
	)
	defer instance.Rdb.Del(context.Background(), Keys.endpointServeLock(instance.Workspace.Name, instance.Stub.ExternalId))

	container, err := instance.WaitForContainer(ctx, endpointServeContainerTimeout)
	if err != nil {
		return err
	}

	// Remove the container lock and rely on the serve lock to keep container alive
	instance.Rdb.Del(
		context.Background(),
		Keys.endpointKeepWarmLock(instance.Workspace.Name, instance.Stub.ExternalId, container.ContainerId),
	)

	sendCallback := func(o common.OutputMsg) error {
		if err := stream.Send(&pb.StartEndpointServeResponse{Output: o.Msg, Done: o.Done}); err != nil {
			return err
		}

		return nil
	}

	exitCallback := func(exitCode int32) error {
		output := "Container was stopped."
		if exitCode != 0 {
			output = fmt.Sprintf("Container failed with exit code %d", exitCode)
			if exitCode == 137 {
				output = "Container was killed due to an out-of-memory error"
			}
		}

		return stream.Send(&pb.StartEndpointServeResponse{
			Done:     true,
			ExitCode: int32(exitCode),
			Output:   output,
		})
	}

	ctx, cancel := common.MergeContexts(es.ctx, ctx)
	defer cancel()

	// Keep serve container active for as long as user has their terminal open
	// We can handle timeouts on the client side
	// If timeout is set to negative, we want to keep the container alive indefinitely while the user is connected
	if in.Timeout < 0 {
		go func() {
			ticker := time.NewTicker(endpointServeContainerKeepaliveInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					instance.Rdb.SetEx(
						context.Background(),
						Keys.endpointServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
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
		ContainerRepo:   es.containerRepo,
		Config:          es.config,
		Tailscale:       es.tailscale,
		KeyEventManager: es.keyEventManager,
	})
	if err != nil {
		return err
	}

	return logStream.Stream(ctx, authInfo, container.ContainerId)
}

func (es *HttpEndpointService) StopEndpointServe(ctx context.Context, in *pb.StopEndpointServeRequest) (*pb.StopEndpointServeResponse, error) {
	instance, err := es.getOrCreateEndpointInstance(ctx, in.StubId,
		withEntryPoint(func(instance *endpointInstance) []string {
			return []string{instance.StubConfig.PythonVersion, "-m", "beta9.runner.serve"}
		}),
		withAutoscaler(func(instance *endpointInstance) *abstractions.Autoscaler[*endpointInstance, *endpointAutoscalerSample] {
			return abstractions.NewAutoscaler(instance, endpointSampleFunc, endpointServeScaleFunc)
		}),
	)
	if err != nil {
		return &pb.StopEndpointServeResponse{Ok: false}, nil
	}

	// Delete serve timeout lock
	instance.Rdb.Del(
		context.Background(),
		Keys.endpointServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
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
			Keys.endpointKeepWarmLock(instance.Workspace.Name, instance.Stub.ExternalId, container.ContainerId),
		)

	}

	return &pb.StopEndpointServeResponse{Ok: true}, nil
}

func (es *HttpEndpointService) EndpointServeKeepAlive(ctx context.Context, in *pb.EndpointServeKeepAliveRequest) (*pb.EndpointServeKeepAliveResponse, error) {
	instance, exists := es.endpointInstances.Get(in.StubId)
	if !exists {
		return &pb.EndpointServeKeepAliveResponse{Ok: false}, nil
	}

	var timeoutDuration time.Duration = endpointServeContainerTimeout
	if in.Timeout != 0 {
		timeoutDuration = time.Duration(in.Timeout) * time.Second
	}

	if in.Timeout < 0 {
		// There is no timeout, so we can just return
		return &pb.EndpointServeKeepAliveResponse{Ok: true}, nil
	}

	// Set lock (used by autoscaler to scale up the single serve container)
	instance.Rdb.SetEx(
		context.Background(),
		Keys.endpointServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
		1,
		timeoutDuration,
	)

	return &pb.EndpointServeKeepAliveResponse{Ok: true}, nil
}
