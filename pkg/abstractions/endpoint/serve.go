package endpoint

import (
	"context"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/types"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"

	pb "github.com/beam-cloud/beta9/proto"
)

func (es *HttpEndpointService) StartEndpointServe(ctx context.Context, in *pb.StartEndpointServeRequest) (*pb.StartEndpointServeResponse, error) {
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
		return &pb.StartEndpointServeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	if authInfo.Workspace.ExternalId != instance.Workspace.ExternalId {
		return &pb.StartEndpointServeResponse{Ok: false}, nil
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

	container, err := instance.WaitForContainer(ctx, endpointServeContainerTimeout)
	if err != nil {
		return &pb.StartEndpointServeResponse{Ok: false, ErrorMsg: err.Error()}, nil
	}

	// Remove the container lock and rely on the serve lock to keep container alive
	instance.Rdb.Del(
		context.Background(),
		Keys.endpointKeepWarmLock(instance.Workspace.Name, instance.Stub.ExternalId, container.ContainerId),
	)

	ctx, cancel := common.MergeContexts(es.ctx, ctx)
	defer cancel()

	return &pb.StartEndpointServeResponse{Ok: true, ContainerId: container.ContainerId}, nil
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
