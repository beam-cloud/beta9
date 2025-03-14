package gatewayservices

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

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

	gpus := types.GPUTypesFromString(in.Gpu)

	autoscaler := &types.Autoscaler{}
	if in.Autoscaler.Type == "" {
		autoscaler.Type = types.QueueDepthAutoscaler
		autoscaler.MaxContainers = 1
		autoscaler.TasksPerContainer = 1
	} else {
		autoscaler.Type = types.AutoscalerType(in.Autoscaler.Type)
		autoscaler.MaxContainers = uint(in.Autoscaler.MaxContainers)
		autoscaler.TasksPerContainer = uint(in.Autoscaler.TasksPerContainer)
		autoscaler.MinContainers = uint(in.Autoscaler.MinContainers)
	}

	// By default, pod deployments are scaled to 1 container
	if in.StubType == types.StubTypePodDeployment {
		autoscaler.MaxContainers = 1

		// If we keep warm is set, allow scaling down to 0 containers
		if in.KeepWarmSeconds > 0 {
			autoscaler.MinContainers = 0
		} else {
			in.KeepWarmSeconds = 0
			autoscaler.MinContainers = 1
		}
	}

	if in.TaskPolicy == nil {
		in.TaskPolicy = &pb.TaskPolicy{
			MaxRetries: in.Retries,
			Timeout:    in.Timeout,
		}
	}

	if in.Extra == "" {
		in.Extra = "{}"
	}

	if in.GpuCount > gws.appConfig.GatewayService.StubLimits.MaxGpuCount {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("GPU count must be %d or less.", gws.appConfig.GatewayService.StubLimits.MaxGpuCount),
		}, nil
	}

	if in.GpuCount > 1 && !authInfo.Workspace.MultiGpuEnabled {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: "Multi-GPU containers are not enabled for this workspace.",
		}, nil
	}

	stubConfig := types.StubConfigV1{
		Runtime: types.Runtime{
			Cpu:      in.Cpu,
			Gpus:     gpus,
			GpuCount: in.GpuCount,
			Memory:   in.Memory,
			ImageId:  in.ImageId,
		},
		Handler:            in.Handler,
		OnStart:            in.OnStart,
		OnDeploy:           in.OnDeploy,
		OnDeployStubId:     in.OnDeployStubId,
		CallbackUrl:        in.CallbackUrl,
		PythonVersion:      in.PythonVersion,
		TaskPolicy:         gws.configureTaskPolicy(in.TaskPolicy, types.StubType(in.StubType)),
		KeepWarmSeconds:    uint(in.KeepWarmSeconds),
		Workers:            uint(in.Workers),
		ConcurrentRequests: uint(in.ConcurrentRequests),
		MaxPendingTasks:    uint(in.MaxPendingTasks),
		Volumes:            in.Volumes,
		Secrets:            []types.Secret{},
		Authorized:         in.Authorized,
		Autoscaler:         autoscaler,
		Extra:              json.RawMessage(in.Extra),
		CheckpointEnabled:  in.CheckpointEnabled,
		EntryPoint:         in.Entrypoint,
		Ports:              in.Ports,
		Env:                in.Env,
	}

	// Ensure GPU count is at least 1 if a GPU is required
	if stubConfig.RequiresGPU() && in.GpuCount == 0 {
		stubConfig.Runtime.GpuCount = 1
	}

	if stubConfig.RequiresGPU() {
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

		if types.StubType(in.GetStubType()).IsServe() {
			gpuNames := []string{}
			hasCapacity := false

			for _, gpu := range gpus {
				if gpuCounts[gpu.String()] > 0 {
					hasCapacity = true
					break
				}
				gpuNames = append(gpuNames, gpu.String())
			}

			if !hasCapacity {
				return &pb.GetOrCreateStubResponse{
					Ok:     false,
					ErrMsg: fmt.Sprintf("There is currently no GPU capacity for %s.", strings.Join(gpuNames, ", ")),
				}, nil
			}
		}

		// T4s are currently in a different pool than other GPUs and won't show up in gpu counts
		lowGpus := []string{}

		for _, gpu := range gpus {
			if gpuCounts[gpu.String()] <= 1 && gpu.String() != types.GPU_T4.String() {
				lowGpus = append(lowGpus, gpu.String())
			}
		}

		if len(lowGpus) > 0 {
			warning = fmt.Sprintf("GPU capacity for %s is currently low.", strings.Join(lowGpus, ", "))
		}
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
			Name:      secret.Name,
			Value:     secret.Value,
			CreatedAt: secret.CreatedAt,
			UpdatedAt: secret.UpdatedAt,
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

	// TODO: Remove this field once `pkg/api/v1/stub.go:GetURL()` is used by frontend and SDK version can be force upgraded
	invokeUrl := common.BuildDeploymentURL(gws.appConfig.GatewayService.HTTP.GetExternalURL(), common.InvokeUrlTypePath, stub, deployment)

	go gws.eventRepo.PushDeployStubEvent(authInfo.Workspace.ExternalId, &stub.Stub)

	var config types.StubConfigV1
	if err := json.Unmarshal([]byte(stub.Config), &config); err != nil {
		return &pb.DeployStubResponse{
			Ok: false,
		}, nil
	}

	if config.Autoscaler.MinContainers > 0 {
		// Publish reload instance event
		eventBus := common.NewEventBus(gws.redisClient)
		eventBus.Send(&common.Event{Type: common.EventTypeReloadInstance, Retries: 3, LockAndDelete: false, Args: map[string]any{
			"stub_id":   stub.ExternalId,
			"stub_type": stub.Type,
			"timestamp": time.Now().Unix(),
		}})
	}

	return &pb.DeployStubResponse{
		Ok:           true,
		DeploymentId: deployment.ExternalId,
		Version:      uint32(deployment.Version),
		InvokeUrl:    invokeUrl,
	}, nil
}

func (gws *GatewayService) GetURL(ctx context.Context, in *pb.GetURLRequest) (*pb.GetURLResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	stub, err := gws.backendRepo.GetStubByExternalId(ctx, in.StubId)
	if err != nil {
		return &pb.GetURLResponse{
			Ok:     false,
			ErrMsg: "Unable to get stub",
		}, nil
	}
	if stub == nil || stub.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		return &pb.GetURLResponse{
			Ok:     false,
			ErrMsg: "Invalid stub ID",
		}, nil
	}

	if in.UrlType == "" {
		in.UrlType = gws.appConfig.GatewayService.InvokeURLType
	}

	// Get URL for Serves, Shells, or Pods
	if stub.Type.IsServe() || stub.Type.Kind() == types.StubTypeShell {
		invokeUrl := common.BuildStubURL(gws.appConfig.GatewayService.HTTP.GetExternalURL(), in.UrlType, stub)
		return &pb.GetURLResponse{
			Ok:  true,
			Url: invokeUrl,
		}, nil
	} else if stub.Type.Kind() == types.StubTypePod {
		stubConfig := &types.StubConfigV1{}
		if err := json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
			return &pb.GetURLResponse{
				Ok:     false,
				ErrMsg: "Unable to get config",
			}, nil
		}

		return &pb.GetURLResponse{
			Ok:  true,
			Url: common.BuildPodURL(gws.appConfig.GatewayService.HTTP.GetExternalURL(), in.UrlType, stub, stubConfig),
		}, nil
	}

	// Get URL for Deployments
	if in.DeploymentId == "" {
		return &pb.GetURLResponse{
			Ok:     false,
			ErrMsg: "Deployment ID is required",
		}, nil
	}

	deployment, err := gws.backendRepo.GetDeploymentByExternalId(ctx, authInfo.Workspace.Id, in.DeploymentId)
	if err != nil {
		return &pb.GetURLResponse{
			Ok:     false,
			ErrMsg: "Unable to get deployment",
		}, nil
	}

	invokeUrl := common.BuildDeploymentURL(gws.appConfig.GatewayService.HTTP.GetExternalURL(), in.UrlType, stub, &deployment.Deployment)
	return &pb.GetURLResponse{
		Ok:  true,
		Url: invokeUrl,
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
