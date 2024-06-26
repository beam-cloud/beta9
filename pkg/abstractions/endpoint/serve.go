package endpoint

import (
	"context"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"

	pb "github.com/beam-cloud/beta9/proto"
)

func (es *HttpEndpointService) StartEndpointServe(in *pb.StartEndpointServeRequest, stream pb.EndpointService_StartEndpointServeServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	instance, err := es.getOrCreateEndpointInstance(in.StubId,
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

	// Set lock (used by autoscaler to scale up the single serve container)
	instance.Rdb.SetEx(
		context.Background(),
		Keys.endpointServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
		1,
		endpointServeContainerTimeout,
	)

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
		if err := stream.Send(&pb.StartEndpointServeResponse{Done: true, ExitCode: int32(exitCode)}); err != nil {
			return err
		}
		return nil
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
	instance, err := es.getOrCreateEndpointInstance(in.StubId,
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

	// Set lock (used by autoscaler to scale up the single serve container)
	instance.Rdb.SetEx(
		context.Background(),
		Keys.endpointServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
		1,
		endpointServeContainerTimeout,
	)

	return &pb.EndpointServeKeepAliveResponse{Ok: true}, nil
}
