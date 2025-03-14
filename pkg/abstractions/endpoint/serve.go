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

func (es *HttpEndpointService) StartEndpointServe(ctx context.Context, req *pb.StartEndpointServeRequest) (*pb.StartEndpointServeResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	instance, err := es.getOrCreateEndpointInstance(ctx, req.StubId,
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

	timeout := types.DefaultServeContainerTimeout
	if req.Timeout > 0 {
		timeout = time.Duration(req.Timeout) * time.Second
	}

	instance.Rdb.SetEx(
		context.Background(),
		common.RedisKeys.SchedulerServeLock(instance.Workspace.Name, instance.Stub.ExternalId),
		timeout.String(),
		timeout,
	)

	container, err := instance.WaitForContainer(ctx, timeout)
	if err != nil {
		return &pb.StartEndpointServeResponse{Ok: false, ErrorMsg: err.Error()}, nil
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

	return response, nil
}
