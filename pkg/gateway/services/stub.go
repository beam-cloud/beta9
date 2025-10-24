package gatewayservices

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path"
	"slices"
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

	valid, errorMsg := types.ValidateCpuAndMemory(in.Cpu, in.Memory, gws.appConfig.GatewayService.StubLimits)
	if !valid {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: errorMsg,
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

	if len(gws.appConfig.GatewayService.StubLimits.GPUBlackList.GPUTypes) > 0 {
		if slices.Contains(gws.appConfig.GatewayService.StubLimits.GPUBlackList.GPUTypes, in.Gpu) {
			return &pb.GetOrCreateStubResponse{
				Ok:     false,
				ErrMsg: gws.appConfig.GatewayService.StubLimits.GPUBlackList.Message,
			}, nil
		}
	}

	var pricing *types.PricingPolicy = nil
	if in.Pricing != nil {
		pricing = &types.PricingPolicy{
			CostModel:             string(in.Pricing.CostModel),
			MaxInFlight:           int(in.Pricing.MaxInFlight),
			CostPerTask:           float64(in.Pricing.CostPerTask),
			CostPerTaskDurationMs: float64(in.Pricing.CostPerTaskDurationMs),
		}
	}

	// If checkpoint/restore is enabled, we need to handle a few additional things to ensure dump/restore will work properly
	if in.CheckpointEnabled {
		err := gws.handleCheckpointEnabled(ctx, authInfo, in, gpus)
		if err != nil {
			return &pb.GetOrCreateStubResponse{
				Ok:     false,
				ErrMsg: err.Error(),
			}, nil
		}
	}

	var inputs *types.Schema = nil
	if in.Inputs != nil {
		inputs = types.NewSchemaFromProto(in.Inputs)
	}

	var outputs *types.Schema = nil
	if in.Outputs != nil {
		outputs = types.NewSchemaFromProto(in.Outputs)
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
		Pricing:            pricing,
		Inputs:             inputs,
		Outputs:            outputs,
		TCP:                in.Tcp,
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

		if types.StubType(in.GetStubType()).IsServe() {
			hasCapacity, err := gws.anyGpuAvailable(gpus)
			if err != nil {
				return &pb.GetOrCreateStubResponse{
					Ok:     false,
					ErrMsg: "Failed to check GPU availability.",
				}, nil
			}

			if !hasCapacity {
				return &pb.GetOrCreateStubResponse{
					Ok:     false,
					ErrMsg: fmt.Sprintf("There is currently no GPU capacity for %s.", in.Gpu),
				}, nil
			}
		}

		lowCapacityGpus, err := gws.getLowCapacityGpus(gpus)
		if err != nil {
			return &pb.GetOrCreateStubResponse{
				Ok:     false,
				ErrMsg: "Failed to check GPU availability.",
			}, nil
		}

		if len(lowCapacityGpus) > 0 {
			warning = fmt.Sprintf("GPU capacity for %s is currently low.", strings.Join(lowCapacityGpus, ", "))
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

	appName := in.AppName
	if appName == "" {
		appName = in.Name
	}

	app, err := gws.backendRepo.GetOrCreateApp(ctx, authInfo.Workspace.Id, appName)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: "Failed to get or create app",
		}, nil
	}

	object, err := gws.backendRepo.GetObjectByExternalId(ctx, in.ObjectId, authInfo.Workspace.Id)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok: false,
		}, nil
	}

	stub, err := gws.backendRepo.GetOrCreateStub(ctx, in.Name, in.StubType, stubConfig, object.Id, authInfo.Workspace.Id, in.ForceCreate, app.Id)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: "Failed to get or create stub",
		}, nil
	}

	return &pb.GetOrCreateStubResponse{
		Ok:      err == nil,
		StubId:  stub.ExternalId,
		WarnMsg: warning,
	}, nil
}

func (gws *GatewayService) handleCheckpointEnabled(ctx context.Context, authInfo *auth.AuthInfo, in *pb.GetOrCreateStubRequest, gpus []types.GpuType) error {
	workspace := authInfo.Workspace

	if in.GpuCount > 1 {
		return fmt.Errorf("Checkpoints are yet not supported for multi-GPU")
	}

	if len(gpus) > 1 {
		return fmt.Errorf("Checkpoints are yet not supported between multiple GPUs")
	}

	// Disable checkpoint for serves
	if types.StubType(in.StubType).IsServe() {
		in.CheckpointEnabled = false
		return nil
	}

	volumeName := "checkpoint-model-cache"
	modelCachePath := fmt.Sprintf("/%s", volumeName)
	volume, err := gws.backendRepo.GetOrCreateVolume(ctx, authInfo.Workspace.Id, volumeName)
	if err != nil {
		return err
	}

	if !workspace.StorageAvailable() {
		volumePath := path.Join(append([]string{types.DefaultVolumesPath, workspace.Name, volume.ExternalId})...)
		if _, err := os.Stat(volumePath); os.IsNotExist(err) {
			os.MkdirAll(volumePath, os.FileMode(0755))
		}
	}

	// Force set cache vars
	in.Env = append(in.Env, fmt.Sprintf("TRITON_CACHE_DIR=%s", modelCachePath))
	in.Env = append(in.Env, fmt.Sprintf("TRANSFORMERS_CACHE=%s", modelCachePath))
	in.Env = append(in.Env, fmt.Sprintf("HF_HOME=%s", modelCachePath))
	in.Env = append(in.Env, "HF_HUB_DISABLE_XET=1")
	in.Env = append(in.Env, "HF_HUB_ENABLE_HF_TRANSFER=0")

	in.Volumes = append(in.Volumes, &pb.Volume{
		Id:        volume.ExternalId,
		MountPath: modelCachePath,
	})

	return nil
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

	deployment, err := gws.backendRepo.CreateDeployment(ctx, authInfo.Workspace.Id, in.Name, version, stub.Id, string(stub.Type), stub.AppId)
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

func (gws *GatewayService) hasStubAccess(ctx context.Context, authInfo *auth.AuthInfo, stub *types.StubWithRelated, config *types.StubConfigV1) (bool, error) {
	if !config.Authorized {
		return true, nil
	}

	if config.Pricing != nil {
		return true, nil
	}

	if stub.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		return false, nil
	}

	return true, nil
}

func (gws *GatewayService) GetURL(ctx context.Context, in *pb.GetURLRequest) (*pb.GetURLResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	stub, err := gws.backendRepo.GetStubByExternalId(ctx, in.StubId)
	if err != nil || stub == nil {
		return &pb.GetURLResponse{
			Ok:     false,
			ErrMsg: "Invalid stub ID",
		}, nil
	}

	stubConfig := &types.StubConfigV1{}
	if err := json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
		return &pb.GetURLResponse{
			Ok:     false,
			ErrMsg: "Unable to get stub config",
		}, nil
	}

	hasAccess, err := gws.hasStubAccess(ctx, authInfo, stub, stubConfig)
	if err != nil || !hasAccess {
		return &pb.GetURLResponse{
			Ok:     false,
			ErrMsg: "Invalid stub ID",
		}, nil
	}

	if in.UrlType == "" {
		in.UrlType = gws.appConfig.GatewayService.InvokeURLType
	}

	// Get URL for Serves, Shells, or Pods
	if stub.Type.IsServe() || stub.Type.Kind() == types.StubTypeShell || in.IsShell {
		if in.IsShell {
			stub.Type = types.StubType(types.StubTypeShell)
		}

		invokeUrl := common.BuildStubURL(gws.appConfig.GatewayService.HTTP.GetExternalURL(), in.UrlType, stub)
		return &pb.GetURLResponse{
			Ok:  true,
			Url: invokeUrl,
		}, nil
	} else if stub.Type.Kind() == types.StubTypePod || stub.Type.Kind() == types.StubTypeSandbox {
		stubConfig := &types.StubConfigV1{}
		if err := json.Unmarshal([]byte(stub.Config), &stubConfig); err != nil {
			return &pb.GetURLResponse{
				Ok:     false,
				ErrMsg: "Unable to get config",
			}, nil
		}

		externalUrl := gws.appConfig.GatewayService.HTTP.GetExternalURL()
		if stubConfig.TCP {
			externalUrl = gws.appConfig.Abstractions.Pod.TCP.GetExternalURL()
			in.UrlType = common.InvokeUrlTypeHost
		}

		return &pb.GetURLResponse{
			Ok:  true,
			Url: common.BuildPodURL(externalUrl, in.UrlType, stub, stubConfig),
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
		if p.TTL == 0 {
			p.TTL = endpoint.DefaultEndpointRequestTTL // Endpoints should still transition to expire from pending if it never runs
		}
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

func (gws *GatewayService) anyGpuAvailable(gpus []types.GpuType) (bool, error) {
	gpuAvailability, err := gws.workerRepo.GetGpuAvailability()
	if err != nil {
		return false, err
	}
	hasCapacity := false

	if slices.Contains(gpus, types.GPU_ANY) {
		hasCapacity = true
	} else {
		for _, gpu := range gpus {
			if gpuAvailability[gpu.String()] {
				hasCapacity = true
				break
			}
		}
	}
	return hasCapacity, nil
}

func (gws *GatewayService) getLowCapacityGpus(gpus []types.GpuType) ([]string, error) {
	preemptibleGpus := gws.workerRepo.GetPreemptibleGpus()
	gpuCounts, err := gws.workerRepo.GetFreeGpuCounts()
	if err != nil {
		return nil, err
	}

	lowGpus := []string{}
	for _, gpu := range gpus {
		if gpuCounts[gpu.String()] <= 1 && !slices.Contains(preemptibleGpus, gpu.String()) {
			lowGpus = append(lowGpus, gpu.String())
		}
	}
	return lowGpus, nil
}
