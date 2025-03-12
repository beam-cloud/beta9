package endpoint

import (
	"context"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"

	"github.com/beam-cloud/beta9/pkg/auth"

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
		return stream.Send(&pb.StartEndpointServeResponse{Ok: false, ErrorMsg: err.Error()})
	}

	if authInfo.Workspace.ExternalId != instance.Workspace.ExternalId {
		return stream.Send(&pb.StartEndpointServeResponse{Ok: false})
	}

	go es.eventRepo.PushServeStubEvent(instance.Workspace.ExternalId, &instance.Stub.Stub)

	var timeoutDuration time.Duration = endpointServeContainerTimeout
	if in.Timeout > 0 {
		timeoutDuration = time.Duration(in.Timeout) * time.Second
	}

	// If timeout is non-negative, set the initial keepalive lock
	if in.Timeout >= 0 {
		instance.Rdb.SetEx(
			context.Background(),
			Keys.endpointServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
			1,
			timeoutDuration,
		)
	}
	defer instance.Rdb.Del(
		context.Background(),
		Keys.endpointServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
	)

	container, err := instance.WaitForContainer(ctx, endpointServeContainerTimeout)
	if err != nil {
		return stream.Send(&pb.StartEndpointServeResponse{Ok: false, ErrorMsg: err.Error()})
	}

	// Remove the container lock and rely on the serve lock to keep the container alive
	instance.Rdb.Del(
		context.Background(),
		Keys.endpointKeepWarmLock(instance.Workspace.Name, instance.Stub.ExternalId, container.ContainerId),
	)

	response := &pb.StartEndpointServeResponse{
		Ok:          true,
		ContainerId: container.ContainerId,
	}

	if err := stream.Send(response); err != nil {
		return err
	}

	// Keep the container alive
	ticker := time.NewTicker(timeoutDuration / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			instance.Rdb.SetEx(
				context.Background(),
				Keys.endpointServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
				1,
				timeoutDuration,
			)

			if err := stream.Send(response); err != nil {
				return err
			}
		}
	}
}
