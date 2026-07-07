package gatewayservices

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
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
	"github.com/rs/zerolog/log"
)

func (gws *GatewayService) GetOrCreateStub(ctx context.Context, in *pb.GetOrCreateStubRequest) (*pb.GetOrCreateStubResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	var warning string

	resourcePolicy, err := gws.stubResourcePolicy(ctx, authInfo.Workspace.ExternalId, in.Pool, in.Name)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	gpus := gpuTypesForStubRequest(in)

	autoscaler := autoscalerFromProto(in.Autoscaler)

	keepWarmSeconds := normalizeKeepWarmSeconds(in.KeepWarmSeconds, types.StubType(in.StubType))

	// Pod keep-warm semantics:
	//   -1: keep one container running until the deployment is stopped
	//    0: scale to zero as soon as there are no active connections
	//   >0: keep idle containers warm for that many seconds, then scale to zero
	if in.StubType == types.StubTypePodDeployment {
		if err := configurePodDeploymentAutoscaler(
			autoscaler,
			keepWarmSeconds,
			resourcePolicy.maxReplicasLimit(gws.appConfig.GatewayService.StubLimits.MaxReplicas),
		); err != nil {
			return &pb.GetOrCreateStubResponse{
				Ok:     false,
				ErrMsg: err.Error(),
			}, nil
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

	if errMsg := resourcePolicy.validateManagedLimits(gws, in, authInfo.Workspace); errMsg != "" {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: errMsg,
		}, nil
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
		checkpointWarning, err := gws.handleCheckpointEnabled(ctx, authInfo, in, gpus)
		if err != nil {
			return &pb.GetOrCreateStubResponse{
				Ok:     false,
				ErrMsg: err.Error(),
			}, nil
		}
		warning = appendWarning(warning, checkpointWarning)
	}

	var inputs *types.Schema = nil
	if in.Inputs != nil {
		inputs = types.NewSchemaFromProto(in.Inputs)
	}

	var outputs *types.Schema = nil
	if in.Outputs != nil {
		outputs = types.NewSchemaFromProto(in.Outputs)
	}

	servingConfig := servingConfigFromProto(in.Serving)

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
		KeepWarmSeconds:    keepWarmSeconds,
		Workers:            uint(in.Workers),
		ConcurrentRequests: uint(in.ConcurrentRequests),
		MaxPendingTasks:    uint(in.MaxPendingTasks),
		Volumes:            in.Volumes,
		Secrets:            []types.Secret{},
		Authorized:         in.Authorized,
		Autoscaler:         autoscaler,
		Extra:              json.RawMessage(in.Extra),
		CheckpointEnabled:  in.CheckpointEnabled,
		CheckpointTrigger:  types.NewCheckpointTriggerFromProto(in.CheckpointTrigger),
		EntryPoint:         in.Entrypoint,
		Ports:              in.Ports,
		Env:                in.Env,
		Pricing:            pricing,
		Inputs:             inputs,
		Outputs:            outputs,
		TCP:                in.Tcp,
		BlockNetwork:       in.BlockNetwork,
		AllowList:          in.AllowList,
		DockerEnabled:      in.DockerEnabled,
		AllowMarketplace:   in.AllowMarketplace,
		IsService:          in.IsService,
		Serving:            servingConfig,
		Pool:               resourcePolicy.pool,
		Disks:              in.Disks,
	}

	// Ensure GPU count is at least 1 if a GPU is required
	if stubConfig.RequiresGPU() && in.GpuCount == 0 {
		stubConfig.Runtime.GpuCount = 1
	}

	if stubConfig.RequiresGPU() {
		if gws.computeService != nil {
			if err := gws.computeService.ValidatePrivatePoolGPURequest(ctx, authInfo.Workspace.ExternalId, poolConfigName(resourcePolicy.pool), gpus); err != nil {
				return &pb.GetOrCreateStubResponse{
					Ok:     false,
					ErrMsg: err.Error(),
				}, nil
			}
		}

		if resourcePolicy.checkManagedGPUCapacity() {
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
					errMsg := fmt.Sprintf("There is currently no GPU capacity for %s.", gpuList(gpus))
					if gws.computeService != nil {
						msg, err := gws.computeService.MissingPrivatePoolGPUWarning(ctx, authInfo.Workspace.ExternalId, gpus)
						if err != nil {
							return &pb.GetOrCreateStubResponse{
								Ok:     false,
								ErrMsg: "Failed to check private pool GPU availability.",
							}, nil
						}
						if msg != "" {
							errMsg = msg
						}
					}
					return &pb.GetOrCreateStubResponse{
						Ok:     false,
						ErrMsg: errMsg,
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
				warning = appendWarning(warning, fmt.Sprintf("GPU capacity for %s is currently low.", strings.Join(lowCapacityGpus, ", ")))
			}
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

	err = gws.configureVolumes(ctx, in.Volumes, authInfo.Workspace)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	if err := gws.configureDurableDiskPlacement(ctx, authInfo.Workspace, &stubConfig); err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	// Register a first-class disk record for each durable disk declared on the
	// stub so disks are listable as standalone resources (dashboard + CLI).
	for _, durableDisk := range stubConfig.Disks {
		if durableDisk == nil || durableDisk.Name == "" {
			continue
		}
		if _, err := gws.backendRepo.GetOrCreateDisk(ctx, authInfo.Workspace.Id, &types.Disk{
			Name:       types.SafeDurableDiskName(durableDisk.Name),
			Size:       durableDisk.Size,
			Filesystem: durableDisk.Filesystem,
			Driver:     durableDisk.Driver,
			MountPath:  durableDisk.MountPath,
		}); err != nil {
			log.Error().Err(err).Str("disk_name", durableDisk.Name).Msg("failed to register durable disk record")
		}
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

	var object types.Object
	if strings.TrimSpace(in.ObjectId) == "" {
		object, err = gws.ensureEmptyStubObject(ctx, authInfo.Workspace)
	} else {
		object, err = gws.backendRepo.GetObjectByExternalId(ctx, in.ObjectId, authInfo.Workspace.Id)
	}
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: "Failed to prepare stub object",
		}, nil
	}

	stub, err := gws.backendRepo.GetOrCreateStub(ctx, in.Name, in.StubType, stubConfig, object.Id, authInfo.Workspace.Id, in.ForceCreate, app.Id)
	if err != nil {
		return &pb.GetOrCreateStubResponse{
			Ok:     false,
			ErrMsg: "Failed to get or create stub",
		}, nil
	}

	if resourcePolicy.pool != nil && resourcePolicy.pool.RequiresReservation() {
		res, err := gws.LaunchPoolCapacity(ctx, &pb.LaunchPoolCapacityRequest{
			Pool: poolConfigToProto(resourcePolicy.pool),
		})
		if err != nil {
			return nil, err
		}
		if !res.Ok {
			return &pb.GetOrCreateStubResponse{
				Ok:     false,
				ErrMsg: res.ErrMsg,
			}, nil
		}
	}

	return &pb.GetOrCreateStubResponse{
		Ok:      err == nil,
		StubId:  stub.ExternalId,
		WarnMsg: warning,
	}, nil
}

func gpuTypesForStubRequest(in *pb.GetOrCreateStubRequest) []types.GpuType {
	gpus := types.GPUTypesFromString(in.Gpu)
	if in.GpuCount > 0 && len(gpus) == 0 {
		return []types.GpuType{types.GPU_ANY}
	}
	return gpus
}

func normalizeKeepWarmSeconds(raw float32, stubType types.StubType) int {
	seconds := int(raw)
	if seconds > 0 && seconds < 10 {
		return 10
	}

	if seconds < 0 && !stubTypeSupportsInfiniteKeepWarm(stubType) {
		return 0
	}

	return seconds
}

func stubTypeSupportsInfiniteKeepWarm(stubType types.StubType) bool {
	switch string(stubType) {
	case types.StubTypePodDeployment, types.StubTypePodRun, types.StubTypeSandbox:
		return true
	default:
		return false
	}
}

func autoscalerFromProto(in *pb.Autoscaler) *types.Autoscaler {
	if in == nil || in.Type == "" {
		return &types.Autoscaler{
			Type:              types.QueueDepthAutoscaler,
			MaxContainers:     1,
			TasksPerContainer: 1,
		}
	}

	return &types.Autoscaler{
		Type:              types.AutoscalerType(in.Type),
		MaxContainers:     uint(in.MaxContainers),
		TasksPerContainer: uint(in.TasksPerContainer),
		MinContainers:     uint(in.MinContainers),
	}
}

func configurePodDeploymentAutoscaler(autoscaler *types.Autoscaler, keepWarmSeconds int, maxReplicas uint64) error {
	if autoscaler.MaxContainers == 0 {
		autoscaler.MaxContainers = 1
	}

	if keepWarmSeconds < 0 && autoscaler.MinContainers == 0 {
		autoscaler.MinContainers = 1
	}

	if autoscaler.MinContainers > autoscaler.MaxContainers {
		return fmt.Errorf("min replicas must be less than or equal to max replicas")
	}

	if maxReplicas > 0 && uint64(autoscaler.MaxContainers) > maxReplicas {
		return fmt.Errorf("max replicas must be %d or less", maxReplicas)
	}

	return nil
}

func poolConfigFromProto(in *pb.PoolConfig) *types.PoolConfig {
	if in == nil {
		return nil
	}
	return &types.PoolConfig{
		Name:           in.Name,
		GPUs:           types.NormalizeGPUStrings(in.Gpu),
		Nodes:          in.Nodes,
		OfferID:        in.OfferId,
		TTL:            in.Ttl,
		MaxSpend:       in.MaxSpend,
		Providers:      in.Providers,
		Regions:        in.Regions,
		MinReliability: in.MinReliability,
		Selector:       in.Selector,
		Fallback:       in.Fallback,
	}
}

func llmConfigFromProto(in *pb.LLMConfig) *types.LLMConfig {
	if in == nil {
		return nil
	}

	return &types.LLMConfig{
		ModelID:         in.ModelId,
		Engine:          in.Engine,
		ServedModelName: in.ServedModelName,
		ContextLength:   int(in.ContextLength),
		Tokenizer:       in.Tokenizer,
		MetricsPath:     in.MetricsPath,
		SLOTier:         in.SloTier,
	}
}

func databaseConfigFromProto(in *pb.DatabaseServingConfig) *types.DatabaseServingConfig {
	if in == nil {
		return nil
	}

	return &types.DatabaseServingConfig{
		Kind:                    in.Kind,
		Port:                    in.Port,
		ReadinessProbe:          in.ReadinessProbe,
		ConnectionEnvName:       in.ConnectionEnvName,
		CredentialSecretNames:   append([]string(nil), in.CredentialSecretNames...),
		DurabilityMode:          in.DurabilityMode,
		UsernameSecretName:      in.UsernameSecretName,
		PasswordSecretName:      in.PasswordSecretName,
		DatabaseSecretName:      in.DatabaseSecretName,
		ConnectionURLSecretName: in.ConnectionUrlSecretName,
	}
}

func servingConfigFromProto(in *pb.ServingConfig) *types.ServingConfig {
	if in == nil {
		return nil
	}

	return compactServingConfig(&types.ServingConfig{
		AppKind:         in.AppKind,
		ServingProtocol: in.ServingProtocol,
		LLM:             llmConfigFromProto(in.Llm),
		Database:        databaseConfigFromProto(in.Database),
	})
}

func compactServingConfig(in *types.ServingConfig) *types.ServingConfig {
	if in == nil {
		return nil
	}
	if strings.TrimSpace(in.AppKind) == "" && strings.TrimSpace(in.ServingProtocol) == "" && in.LLM == nil && in.Database == nil {
		return nil
	}
	return in
}

func poolConfigToProto(in *types.PoolConfig) *pb.PoolConfig {
	if in == nil {
		return nil
	}
	return &pb.PoolConfig{
		Name:           in.Name,
		Gpu:            in.GPUs,
		Nodes:          in.Nodes,
		OfferId:        in.OfferID,
		Ttl:            in.TTL,
		MaxSpend:       in.MaxSpend,
		Providers:      in.Providers,
		Regions:        in.Regions,
		MinReliability: in.MinReliability,
		Selector:       in.Selector,
		Fallback:       in.Fallback,
	}
}

func poolConfigName(pool *types.PoolConfig) string {
	if pool == nil {
		return ""
	}
	return pool.Name
}

type stubResourcePolicy struct {
	pool                *types.PoolConfig
	privatePoolTargeted bool
	fallback            string
}

func (gws *GatewayService) stubResourcePolicy(ctx context.Context, workspaceID string, protoPool *pb.PoolConfig, stubName string) (stubResourcePolicy, error) {
	policy := stubResourcePolicy{pool: poolConfigFromProto(protoPool)}
	if policy.pool == nil {
		return policy, nil
	}
	configurePoolSelector(policy.pool, workspaceID, stubName)

	policy.privatePoolTargeted = policy.pool.RequiresReservation()
	policy.fallback = privatePoolFallback(policy.pool)

	if !policy.privatePoolTargeted {
		targeted, fallback, err := gws.lookupPrivatePoolPolicy(ctx, workspaceID, policy.pool)
		if err != nil {
			return policy, err
		}
		policy.privatePoolTargeted = targeted
		if policy.pool.Fallback == "" && fallback != "" {
			policy.fallback = fallback
		}
	}

	policy.pool.Fallback = policy.fallback
	return policy, nil
}

func (p stubResourcePolicy) privatePoolOnly() bool {
	return p.privatePoolTargeted && (p.fallback == types.PrivatePoolFallbackFail || p.fallback == types.PrivatePoolFallbackWait)
}

func (p stubResourcePolicy) checkManagedGPUCapacity() bool {
	return !p.privatePoolOnly()
}

func (p stubResourcePolicy) maxReplicasLimit(defaultLimit uint64) uint64 {
	if p.privatePoolOnly() {
		return 0
	}
	return defaultLimit
}

func (p stubResourcePolicy) validateManagedLimits(gws *GatewayService, in *pb.GetOrCreateStubRequest, workspace *types.Workspace) string {
	if p.privatePoolOnly() {
		return ""
	}
	errMsg := gws.managedStubLimitError(in, workspace)
	if errMsg == "" {
		return ""
	}
	if p.privatePoolTargeted {
		return "private pool workloads that exceed managed stub limits must set pool fallback to fail or wait"
	}
	return errMsg
}

func privatePoolFallback(pool *types.PoolConfig) string {
	if pool == nil || pool.Fallback == "" {
		return types.PrivatePoolFallbackInternal
	}
	return pool.Fallback
}

func (gws *GatewayService) lookupPrivatePoolPolicy(ctx context.Context, workspaceID string, pool *types.PoolConfig) (bool, string, error) {
	if pool == nil || gws == nil || gws.computeRepo == nil || workspaceID == "" {
		return false, "", nil
	}
	for _, name := range []string{pool.Name, pool.Selector} {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		state, err := gws.computeRepo.GetPoolState(ctx, workspaceID, name)
		if err != nil {
			return false, "", err
		}
		if state == nil || (state.Mode != "" && state.Mode != string(types.PoolModePrivate)) {
			continue
		}
		if state.Config != nil && state.Config.Fallback != "" {
			return true, state.Config.Fallback, nil
		}
		return true, state.Fallback, nil
	}
	return false, "", nil
}

func (gws *GatewayService) managedStubLimitError(in *pb.GetOrCreateStubRequest, workspace *types.Workspace) string {
	valid, errorMsg := types.ValidateCpuAndMemory(in.Cpu, in.Memory, gws.appConfig.GatewayService.StubLimits)
	if !valid {
		return errorMsg
	}
	if in.GpuCount > gws.appConfig.GatewayService.StubLimits.MaxGpuCount {
		return fmt.Sprintf("GPU count must be %d or less.", gws.appConfig.GatewayService.StubLimits.MaxGpuCount)
	}
	if in.GpuCount > 1 && (workspace == nil || !workspace.MultiGpuEnabled) {
		return "Multi-GPU containers are not enabled for this workspace."
	}
	if len(gws.appConfig.GatewayService.StubLimits.GPUBlackList.GPUTypes) > 0 {
		if slices.Contains(gws.appConfig.GatewayService.StubLimits.GPUBlackList.GPUTypes, in.Gpu) {
			return gws.appConfig.GatewayService.StubLimits.GPUBlackList.Message
		}
	}
	return ""
}

func gpuList(gpus []types.GpuType) string {
	values := types.GpuTypesToStrings(gpus)
	if len(values) == 0 {
		return "GPU"
	}
	return strings.Join(values, ", ")
}

func configurePoolSelector(pool *types.PoolConfig, workspaceID, stubName string) {
	requiresReservation := pool.RequiresReservation()
	if pool.Selector != "" {
		if pool.Name == "" && requiresReservation {
			pool.Name = pool.Selector
		}
		return
	}
	if pool.Name != "" {
		pool.Selector = pool.Name
		return
	}
	pool.Selector = sanitizePoolSelector(fmt.Sprintf("private-%s-%s", workspaceID, stubName))
	if pool.Name == "" && requiresReservation {
		pool.Name = pool.Selector
	}
}

func sanitizePoolSelector(value string) string {
	value = strings.ToLower(value)
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-'
		if ok {
			b.WriteRune(r)
			lastDash = r == '-'
			continue
		}
		if !lastDash {
			b.WriteRune('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-._")
	if out == "" {
		return "private-pool"
	}
	if len(out) > 128 {
		return out[:128]
	}
	return out
}

// checkpointTriggerTypeHTTP mirrors the trigger type the worker uses to decide between
// HTTP readiness polling and the runner signal file (see pkg/worker/criu.go)
const checkpointTriggerTypeHTTP = "http"

const checkpointCompileCacheRoot = "/tmp/beam-checkpoint-compile-cache"

func appendWarning(existing, warning string) string {
	if existing == "" {
		return warning
	}
	if warning == "" {
		return existing
	}
	return existing + " " + warning
}

func checkpointModelCacheVolumeName(in *pb.GetOrCreateStubRequest) string {
	name := in.Name
	if in.AppName != "" {
		name = in.AppName
	}
	return types.CheckpointModelCacheVolumeName(name)
}

func checkpointCacheEnv(modelCachePath string) []string {
	// Persist model files across checkpoint boots, but keep compiler/JIT artifacts on
	// local disk so Triton/TorchInductor can compile before the checkpoint is created.
	return []string{
		fmt.Sprintf("HF_HOME=%s", modelCachePath),
		fmt.Sprintf("HF_HUB_CACHE=%s/hub", modelCachePath),
		fmt.Sprintf("TRANSFORMERS_CACHE=%s", modelCachePath),
		fmt.Sprintf("TRITON_CACHE_DIR=%s/triton", checkpointCompileCacheRoot),
		fmt.Sprintf("TORCHINDUCTOR_CACHE_DIR=%s/torchinductor", checkpointCompileCacheRoot),
		fmt.Sprintf("VLLM_CACHE_ROOT=%s/vllm", checkpointCompileCacheRoot),
		fmt.Sprintf("CUDA_CACHE_PATH=%s/cuda", checkpointCompileCacheRoot),
		"HF_HUB_DISABLE_XET=1",
		"HF_HUB_ENABLE_HF_TRANSFER=0",
	}
}

func upsertCheckpointEnv(env []string, updates []string) []string {
	for _, update := range updates {
		name, _, ok := strings.Cut(update, "=")
		if !ok {
			continue
		}
		replaced := false
		for i, existing := range env {
			existingName, _, ok := strings.Cut(existing, "=")
			if ok && existingName == name {
				env[i] = update
				replaced = true
			}
		}
		if !replaced {
			env = append(env, update)
		}
	}
	return env
}

// handleCheckpointEnabled validates and configures a stub request that has checkpointing
// enabled. It returns a user-facing warning message (empty if none) and an error.
func (gws *GatewayService) handleCheckpointEnabled(ctx context.Context, authInfo *auth.AuthInfo, in *pb.GetOrCreateStubRequest, gpus []types.GpuType) (string, error) {
	workspace := authInfo.Workspace

	if in.GpuCount > 1 {
		return "", fmt.Errorf("Checkpoints are yet not supported for multi-GPU")
	}

	if len(gpus) > 1 {
		return "", fmt.Errorf("Checkpoints are yet not supported between multiple GPUs")
	}

	// Disable checkpoint for serves, but let the user know instead of failing silently
	if types.StubType(in.StubType).IsServe() {
		in.CheckpointEnabled = false
		return "checkpointing is not supported for serve sessions; it has been disabled", nil
	}

	// Pods have no beta9 runner process to write the checkpoint readiness signal file,
	// so automatic checkpoints require an HTTP readiness path. Sandboxes are exempt
	// because they checkpoint via the manual snapshot APIs instead.
	if types.StubType(in.StubType).Kind() == types.StubTypePod {
		trigger := in.CheckpointTrigger
		if trigger == nil || !strings.EqualFold(trigger.Type, checkpointTriggerTypeHTTP) || trigger.HttpPath == "" {
			return "", fmt.Errorf("pods with checkpoint_enabled require checkpoint_readiness_path to be set (there is no runner to signal readiness)")
		}
	}

	if !workspace.StorageAvailable() {
		return "", fmt.Errorf("workspace storage is required for checkpoints")
	}

	volumeName := checkpointModelCacheVolumeName(in)
	modelCachePath := fmt.Sprintf("/%s", volumeName)
	volume, err := gws.backendRepo.GetOrCreateVolume(ctx, authInfo.Workspace.Id, volumeName)
	if err != nil {
		return "", err
	}

	in.Env = upsertCheckpointEnv(in.Env, checkpointCacheEnv(modelCachePath))

	in.Volumes = append(in.Volumes, &pb.Volume{
		Id:        volume.ExternalId,
		MountPath: modelCachePath,
	})

	return "", nil
}

func (gws *GatewayService) DeployStub(ctx context.Context, in *pb.DeployStubRequest) (*pb.DeployStubResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	stub, err := gws.backendRepo.GetStubByExternalId(ctx, in.StubId)
	if err != nil || stub.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		return &pb.DeployStubResponse{
			Ok: false,
		}, nil
	}

	latestDeployment, err := gws.backendRepo.GetLatestDeploymentByName(ctx, authInfo.Workspace.Id, in.Name, string(stub.Type), false)
	if err != nil {
		return &pb.DeployStubResponse{
			Ok: false,
		}, nil
	}

	version := uint(1)
	if latestDeployment != nil {
		version = latestDeployment.Version + 1
	}

	var config types.StubConfigV1
	if err := json.Unmarshal([]byte(stub.Config), &config); err != nil {
		return &pb.DeployStubResponse{
			Ok:     false,
			ErrMsg: "Unable to parse stub configuration",
		}, nil
	}

	rolloutPlan, err := gws.planDeploymentRollout(deploymentRolloutInput{
		workspace: authInfo.Workspace,
		stub:      stub,
		config:    &config,
		latest:    latestDeployment,
		mode:      normalizeRolloutMode(in.Rollout),
	})
	if err != nil {
		return &pb.DeployStubResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}
	if rolloutPlan.stopLatest {
		if err := gws.stopDeployments([]types.DeploymentWithRelated{*latestDeployment}, ctx); err != nil {
			return &pb.DeployStubResponse{
				Ok:     false,
				ErrMsg: "Unable to stop previous deployment before rollout",
			}, nil
		}
	}

	deployment, err := gws.backendRepo.CreateDeployment(ctx, authInfo.Workspace.Id, in.Name, version, stub.Id, string(stub.Type), stub.AppId)
	if err != nil {
		return &pb.DeployStubResponse{
			Ok: false,
		}, nil
	}

	// TODO: Remove this field once `pkg/api/v1/stub.go:GetURL()` is used by frontend and SDK version can be force upgraded
	invokeUrl := common.BuildDeploymentURL(gws.appConfig.GatewayService.HTTP.GetExternalURL(), common.InvokeUrlTypePath, stub, deployment)

	if gws.eventRepo != nil {
		go gws.eventRepo.PushDeployStubEvent(authInfo.Workspace.ExternalId, &stub.Stub)
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
		Ok:            true,
		DeploymentId:  deployment.ExternalId,
		Version:       uint32(deployment.Version),
		InvokeUrl:     invokeUrl,
		WarnMsg:       rolloutPlan.warning,
		RolloutAction: rolloutPlan.action,
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
			if in.DeploymentId != "" {
				deployment, err := gws.backendRepo.GetDeploymentByExternalId(ctx, authInfo.Workspace.Id, in.DeploymentId)
				if err != nil {
					return &pb.GetURLResponse{
						Ok:     false,
						ErrMsg: "Unable to get deployment",
					}, nil
				}
				return &pb.GetURLResponse{
					Ok:  true,
					Url: common.BuildPodDeploymentURL(externalUrl, in.UrlType, &deployment.Deployment, stubConfig),
				}, nil
			}
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
