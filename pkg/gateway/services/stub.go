package gatewayservices

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/beam-cloud/beta9/pkg/abstractions/endpoint"
	"github.com/beam-cloud/beta9/pkg/abstractions/function"
	"github.com/beam-cloud/beta9/pkg/abstractions/taskqueue"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func (gws *GatewayService) GetOrCreateStub(ctx context.Context, in *pb.GetOrCreateStubRequest) (*pb.GetOrCreateStubResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	var warning string

	if in.Memory > int64(gws.appConfig.GatewayService.StubLimits.Memory) {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("Memory must be %dGiB or less.", gws.appConfig.GatewayService.StubLimits.Memory/1024),
		}, nil
	}

	if in.Gpu != "" {
		concurrencyLimit, err := gws.backendRepo.GetConcurrencyLimitByWorkspaceId(ctx, authInfo.Workspace.ExternalId)
		if err != nil && concurrencyLimit != nil && concurrencyLimit.GPULimit <= 0 {
			return &pb.GetOrCreateStubResponse{
				Ok:     false,
				ErrMsg: "GPU concurrency limit is 0.",
			}, nil
		}

		gpuCounts, err := gws.providerRepo.GetGPUCounts(gws.appConfig.Worker.Pools)
		if err != nil {
			return &pb.GetOrCreateStubResponse{
				Ok:     false,
				ErrMsg: "Failed to get GPU counts.",
			}, nil
		}

		if gpuCounts[in.Gpu] <= 1 && in.Gpu != types.GPU_T4.String() {
			warning = fmt.Sprintf("GPU capacity for %s is currently low.", in.Gpu)
		}
	}

	autoscaler := &types.Autoscaler{}
	if in.Autoscaler.Type == "" {
		autoscaler.Type = types.QueueDepthAutoscaler
		autoscaler.MaxContainers = 1
		autoscaler.TasksPerContainer = 1
	} else {
		autoscaler.Type = types.AutoscalerType(in.Autoscaler.Type)
		autoscaler.MaxContainers = uint(in.Autoscaler.MaxContainers)
		autoscaler.TasksPerContainer = uint(in.Autoscaler.TasksPerContainer)
	}

	if in.TaskPolicy == nil {
		in.TaskPolicy = &pb.TaskPolicy{
			MaxRetries: in.Retries,
			Timeout:    in.Timeout,
		}
	}

	stubConfig := types.StubConfigV1{
		Runtime: types.Runtime{
			Cpu:     in.Cpu,
			Gpu:     types.GpuType(in.Gpu),
			Memory:  in.Memory,
			ImageId: in.ImageId,
		},
		Handler:         in.Handler,
		OnStart:         in.OnStart,
		CallbackUrl:     in.CallbackUrl,
		PythonVersion:   in.PythonVersion,
		TaskPolicy:      gws.configureTaskPolicy(in.TaskPolicy, types.StubType(in.StubType)),
		KeepWarmSeconds: uint(in.KeepWarmSeconds),
		Workers:         uint(in.Workers),
		MaxPendingTasks: uint(in.MaxPendingTasks),
		Volumes:         in.Volumes,
		Secrets:         []types.Secret{},
		Authorized:      in.Authorized,
		Autoscaler:      autoscaler,
	}

	// Get secrets
	for _, secret := range in.Secrets {
		secret, err := gws.backendRepo.GetSecretByName(ctx, authInfo.Workspace, secret.Name)
		if err != nil {
			if err == sql.ErrNoRows {
				continue // Skip secret if not found
			}

			return &pb.GetOrCreateStubResponse{
				Ok: false,
			}, nil
		}

		stubConfig.Secrets = append(stubConfig.Secrets, types.Secret{
			Name:  secret.Name,
			Value: secret.Value,
		})
	}

	err := gws.configureVolumes(ctx, in.Volumes, authInfo.Workspace)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	object, err := gws.backendRepo.GetObjectByExternalId(ctx, in.ObjectId, authInfo.Workspace.Id)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok: false,
		}, nil
	}

	err = common.ExtractObjectFile(ctx, object.ExternalId, authInfo.Workspace.Name)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok: false,
		}, nil
	}

	stub, err := gws.backendRepo.GetOrCreateStub(ctx, in.Name, in.StubType, stubConfig, object.Id, authInfo.Workspace.Id, in.ForceCreate)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok: false,
		}, nil
	}

	return &pb.GetOrCreateStubResponse{
		Ok:      err == nil,
		StubId:  stub.ExternalId,
		WarnMsg: warning,
	}, nil
}

func (gws *GatewayService) DeployStub(ctx context.Context, in *pb.DeployStubRequest) (*pb.DeployStubResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	stub, err := gws.backendRepo.GetStubByExternalId(ctx, in.StubId)
	if err != nil || stub.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		return &pb.DeployStubResponse{
			Ok: false,
		}, nil
	}

	lastestDeployment, err := gws.backendRepo.GetLatestDeploymentByName(ctx, authInfo.Workspace.Id, in.Name, string(stub.Type), false)
	if err != nil {
		return &pb.DeployStubResponse{
			Ok: false,
		}, nil
	}

	version := uint(1)
	if lastestDeployment != nil {
		if stub.Type == types.StubType(types.StubTypeScheduledJobDeployment) {
			if err := gws.stopDeployments([]types.DeploymentWithRelated{*lastestDeployment}, ctx); err != nil {
				return &pb.DeployStubResponse{
					Ok: false,
				}, nil
			}
		}

		version = lastestDeployment.Version + 1
	}

	deployment, err := gws.backendRepo.CreateDeployment(ctx, authInfo.Workspace.Id, in.Name, version, stub.Id, string(stub.Type))
	if err != nil {
		return &pb.DeployStubResponse{
			Ok: false,
		}, nil
	}

	invokeURL := fmt.Sprintf("%s/%s/%s/v%d", gws.appConfig.GatewayService.ExternalURL, stub.Type.Kind(), in.Name, deployment.Version)
	if stubConfig, err := stub.UnmarshalConfig(); err == nil && !stubConfig.Authorized {
		invokeURL = fmt.Sprintf("%s/%s/%s/%s", gws.appConfig.GatewayService.ExternalURL, stub.Type.Kind(), "public", stub.ExternalId)
	}

	go gws.eventRepo.PushDeployStubEvent(authInfo.Workspace.ExternalId, &stub.Stub)

	return &pb.DeployStubResponse{
		Ok:           true,
		DeploymentId: deployment.ExternalId,
		Version:      uint32(deployment.Version),
		InvokeUrl:    invokeURL,
	}, nil
}

func (gws *GatewayService) configureVolumes(ctx context.Context, volumes []*pb.Volume, workspace *types.Workspace) error {
	for i, volume := range volumes {
		if volume.Config != nil {
			// De-reference secrets
			accessKey, err := gws.backendRepo.GetSecretByName(ctx, workspace, volume.Config.AccessKey)
			if err != nil {
				return fmt.Errorf("Failed to get secret: %s", volume.Config.AccessKey)
			}
			volumes[i].Config.AccessKey = accessKey.Value

			secretKey, err := gws.backendRepo.GetSecretByName(ctx, workspace, volume.Config.SecretKey)
			if err != nil {
				return fmt.Errorf("Failed to get secret: %s", volume.Config.SecretKey)
			}
			volumes[i].Config.SecretKey = secretKey.Value
		}
	}

	return nil
}

func (gws *GatewayService) configureTaskPolicy(policy *pb.TaskPolicy, stubType types.StubType) types.TaskPolicy {
	p := types.TaskPolicy{
		MaxRetries: uint(math.Min(float64(policy.MaxRetries), float64(types.MaxTaskRetries))),
		Timeout:    int(policy.Timeout),
		TTL:        uint32(math.Min(float64(policy.Ttl), float64(types.MaxTaskTTL))),
	}

	switch stubType.Kind() {
	case types.StubTypeASGI:
		fallthrough
	case types.StubTypeEndpoint:
		p.Timeout = int(math.Min(float64(policy.Timeout), float64(endpoint.DefaultEndpointRequestTimeoutS)))
		if p.Timeout <= 0 {
			p.Timeout = endpoint.DefaultEndpointRequestTimeoutS
		}
		p.MaxRetries = 0
		p.TTL = endpoint.DefaultEndpointRequestTTL // Endpoints should still transition to expire from pending if it never runs

	case types.StubTypeScheduledJob:
		fallthrough
	case types.StubTypeFunction:
		if p.TTL == 0 {
			p.TTL = function.DefaultFunctionTaskTTL
		}
	case types.StubTypeTaskQueue:
		if p.TTL == 0 {
			p.TTL = taskqueue.DefaultTaskQueueTaskTTL
		}
	}

	return p
}
