package gatewayservices

import (
	"context"
	"database/sql"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

type checkpointVolumeBackendRepo struct {
	repository.BackendRepository
}

type missingSecretBackendRepo struct {
	repository.BackendRepository
}

func (r *missingSecretBackendRepo) GetSecretByName(context.Context, *types.Workspace, string) (*types.Secret, error) {
	return nil, sql.ErrNoRows
}

type fakeGpuPoolChecker struct {
	supported map[string]bool
}

func (f fakeGpuPoolChecker) HasManagedPoolForGPU(gpuType string, allowMarketplace bool) bool {
	return f.supported[gpuType]
}

type fakePrivatePoolFinder struct {
	pool string
	err  error
}

func (f fakePrivatePoolFinder) FindReadyPrivatePoolForGPU(ctx context.Context, workspaceID string, gpus []types.GpuType) (string, error) {
	return f.pool, f.err
}

func TestCachePreparedStub(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(common.PreparedStubCacheMetadata, "cache-key"),
	)
	(&GatewayService{redisClient: rdb}).cachePreparedStub(ctx, "workspace-1", "stub-1")

	stubID, err := rdb.Get(
		context.Background(),
		common.RedisKeys.GatewayPreparedStub("workspace-1", "cache-key"),
	).Result()
	require.NoError(t, err)
	require.Equal(t, "stub-1", stubID)
}

func TestGetOrCreateStubReportsMissingSecret(t *testing.T) {
	service := &GatewayService{
		appConfig:   types.AppConfig{GatewayService: types.GatewayServiceConfig{StubLimits: types.StubLimits{Cpu: 2, Memory: 2}}},
		backendRepo: &missingSecretBackendRepo{},
	}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "workspace-1"}})

	resp, err := service.GetOrCreateStub(ctx, &pb.GetOrCreateStubRequest{
		Cpu: 1, Memory: 1, Secrets: []*pb.SecretVar{{Name: "HF_TOKEN"}},
	})
	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Equal(t, `Secret "HF_TOKEN" does not exist in this workspace.`, resp.ErrMsg)
}

func TestComputeCapacityVerdictAvailableWhenAnyGPUSupported(t *testing.T) {
	checker := fakeGpuPoolChecker{supported: map[string]bool{"T4": true}}

	verdict, err := computeCapacityVerdict(context.Background(), checker, nil, "ws-1", []types.GpuType{types.GpuType("A6000"), types.GpuType("T4")}, false, false)
	require.NoError(t, err)
	require.Equal(t, StubCapacityStatusAvailable, verdict.status)
	require.Equal(t, []string{"A6000"}, verdict.unsupportedGpus)
	require.Empty(t, verdict.matchedPrivatePool)
}

func TestComputeCapacityVerdictLowCapacity(t *testing.T) {
	checker := fakeGpuPoolChecker{supported: map[string]bool{"T4": true}}

	verdict, err := computeCapacityVerdict(context.Background(), checker, nil, "ws-1", []types.GpuType{types.GpuType("T4")}, false, true)
	require.NoError(t, err)
	require.Equal(t, StubCapacityStatusLow, verdict.status)
}

func TestComputeCapacityVerdictNoneWhenNoPoolSupportsGPU(t *testing.T) {
	checker := fakeGpuPoolChecker{supported: map[string]bool{}}

	verdict, err := computeCapacityVerdict(context.Background(), checker, nil, "ws-1", []types.GpuType{types.GpuType("A6000")}, false, false)
	require.NoError(t, err)
	require.Equal(t, StubCapacityStatusNone, verdict.status)
	require.Equal(t, []string{"A6000"}, verdict.unsupportedGpus)
}

func TestComputeCapacityVerdictMatchesReadyPrivatePool(t *testing.T) {
	checker := fakeGpuPoolChecker{supported: map[string]bool{}}
	finder := fakePrivatePoolFinder{pool: "ondemand-a6000"}

	verdict, err := computeCapacityVerdict(context.Background(), checker, finder, "ws-1", []types.GpuType{types.GpuType("A6000")}, false, false)
	require.NoError(t, err)
	require.Equal(t, StubCapacityStatusAvailable, verdict.status)
	require.Equal(t, "ondemand-a6000", verdict.matchedPrivatePool)
}

func TestComputeCapacityVerdictSkipsNoGPU(t *testing.T) {
	checker := fakeGpuPoolChecker{supported: map[string]bool{}}

	verdict, err := computeCapacityVerdict(context.Background(), checker, nil, "ws-1", []types.GpuType{types.NO_GPU, types.GpuType("A6000")}, false, false)
	require.NoError(t, err)
	require.Equal(t, StubCapacityStatusNone, verdict.status)
	require.Equal(t, []string{"A6000"}, verdict.unsupportedGpus)
}

func (r *checkpointVolumeBackendRepo) GetOrCreateVolume(ctx context.Context, workspaceId uint, name string) (*types.Volume, error) {
	return &types.Volume{ExternalId: "volume-123", WorkspaceId: workspaceId, Name: name}, nil
}

type getOrCreateStubBackendRepo struct {
	repository.BackendRepository
	stubConfig types.StubConfigV1
}

func (r *getOrCreateStubBackendRepo) GetOrCreateApp(ctx context.Context, workspaceId uint, appName string) (*types.App, error) {
	return &types.App{Id: 3, WorkspaceId: workspaceId, Name: appName}, nil
}

func (r *getOrCreateStubBackendRepo) GetObjectByExternalId(ctx context.Context, externalId string, workspaceId uint) (types.Object, error) {
	return types.Object{Id: 2, ExternalId: externalId, WorkspaceId: workspaceId}, nil
}

func (r *getOrCreateStubBackendRepo) GetOrCreateStub(ctx context.Context, name, stubType string, config types.StubConfigV1, objectId, workspaceId uint, forceCreate bool, appId uint) (types.Stub, error) {
	r.stubConfig = config
	return types.Stub{Id: 4, ExternalId: "stub-123", Name: name, Type: types.StubType(stubType), ObjectId: objectId, WorkspaceId: workspaceId, AppId: appId}, nil
}

func TestConfigureDurableDiskPlacementDefaultsSnapshotDriver(t *testing.T) {
	config := &types.StubConfigV1{
		Disks: []*pb.DurableDisk{{Name: "pg-data"}},
	}

	require.NoError(t, (&GatewayService{}).configureDurableDiskPlacement(context.Background(), nil, config))
	require.Equal(t, types.DurableDiskDriverSnapshot, config.Disks[0].Driver)
}

func TestConfigureDurableDiskPlacementRejectsUnsupportedDriver(t *testing.T) {
	config := &types.StubConfigV1{
		Disks: []*pb.DurableDisk{{
			Name:   "pg-data",
			Driver: "unsupported",
		}},
	}

	err := (&GatewayService{}).configureDurableDiskPlacement(context.Background(), nil, config)
	require.ErrorContains(t, err, `unsupported driver "unsupported"`)
}

func TestConfigureDurableDiskPlacementRejectsWritableDiskWithMultipleContainers(t *testing.T) {
	config := &types.StubConfigV1{
		Autoscaler: &types.Autoscaler{MaxContainers: 2},
		Disks:      []*pb.DurableDisk{{Name: "data"}},
	}

	err := (&GatewayService{}).configureDurableDiskPlacement(context.Background(), nil, config)
	require.ErrorContains(t, err, "writable durable disks support one container")
}

func TestConfigureDurableDiskPlacementRejectsWritableDiskWithMultipleMinContainers(t *testing.T) {
	config := &types.StubConfigV1{
		Autoscaler: &types.Autoscaler{MinContainers: 2},
		Disks:      []*pb.DurableDisk{{Name: "data"}},
	}

	err := (&GatewayService{}).configureDurableDiskPlacement(context.Background(), nil, config)
	require.ErrorContains(t, err, "writable durable disks support one container")
}

func TestConfigureDurableDiskPlacementAllowsReadOnlyDiskWithMultipleContainers(t *testing.T) {
	config := &types.StubConfigV1{
		Autoscaler: &types.Autoscaler{MaxContainers: 4},
		Disks:      []*pb.DurableDisk{{Name: "data", ReadOnly: true}},
	}

	require.NoError(t, (&GatewayService{}).configureDurableDiskPlacement(context.Background(), nil, config))
}

func TestHandleCheckpointEnabledDisablesCheckpointForServesWithWarning(t *testing.T) {
	authInfo := &auth.AuthInfo{Workspace: &types.Workspace{}}
	in := &pb.GetOrCreateStubRequest{
		StubType:          types.StubTypeEndpointServe,
		CheckpointEnabled: true,
	}

	warning, err := (&GatewayService{}).handleCheckpointEnabled(context.Background(), authInfo, in, nil)
	require.NoError(t, err)
	require.Contains(t, warning, "checkpointing is not supported for serve sessions")
	require.False(t, in.CheckpointEnabled)
}

func TestHandleCheckpointEnabledRequiresReadinessPathForPods(t *testing.T) {
	authInfo := &auth.AuthInfo{Workspace: &types.Workspace{}}

	for _, stubType := range []string{types.StubTypePod, types.StubTypePodDeployment, types.StubTypePodRun} {
		t.Run(stubType, func(t *testing.T) {
			// No trigger at all
			in := &pb.GetOrCreateStubRequest{StubType: stubType, CheckpointEnabled: true}
			_, err := (&GatewayService{}).handleCheckpointEnabled(context.Background(), authInfo, in, nil)
			require.ErrorContains(t, err, "checkpoint_readiness_path")

			// Trigger without an HTTP path
			in = &pb.GetOrCreateStubRequest{
				StubType:          stubType,
				CheckpointEnabled: true,
				CheckpointTrigger: &pb.CheckpointTrigger{Type: "http"},
			}
			_, err = (&GatewayService{}).handleCheckpointEnabled(context.Background(), authInfo, in, nil)
			require.ErrorContains(t, err, "checkpoint_readiness_path")

			// Valid HTTP trigger passes pod validation (fails later on workspace storage instead)
			in = &pb.GetOrCreateStubRequest{
				StubType:          stubType,
				CheckpointEnabled: true,
				CheckpointTrigger: &pb.CheckpointTrigger{Type: "http", HttpPath: "/ready"},
			}
			_, err = (&GatewayService{}).handleCheckpointEnabled(context.Background(), authInfo, in, nil)
			require.ErrorContains(t, err, "workspace storage is required")
		})
	}
}

func TestHandleCheckpointEnabledExemptsSandboxesFromReadinessPath(t *testing.T) {
	authInfo := &auth.AuthInfo{Workspace: &types.Workspace{}}
	in := &pb.GetOrCreateStubRequest{
		StubType:          types.StubTypeSandbox,
		CheckpointEnabled: true,
	}

	// Sandboxes checkpoint via manual snapshot APIs, so no readiness path is required;
	// validation proceeds to the workspace storage check instead.
	_, err := (&GatewayService{}).handleCheckpointEnabled(context.Background(), authInfo, in, nil)
	require.ErrorContains(t, err, "workspace storage is required")
}

func TestCheckpointModelCacheVolumeNamePrefersAppName(t *testing.T) {
	in := &pb.GetOrCreateStubRequest{
		Name:    types.StubTypePodDeployment,
		AppName: "qwen",
	}
	require.Equal(t, "checkpoint-model-cache-qwen", checkpointModelCacheVolumeName(in))
}

func TestCheckpointModelCacheVolumeNameFallsBackToStubName(t *testing.T) {
	in := &pb.GetOrCreateStubRequest{Name: "qwen/prod"}
	require.Equal(t, "checkpoint-model-cache-qwen-prod", checkpointModelCacheVolumeName(in))
}

func TestHandleCheckpointEnabledSplitsModelAndCompilerCaches(t *testing.T) {
	storageId := uint(1)
	authInfo := &auth.AuthInfo{Workspace: &types.Workspace{
		Id:        7,
		StorageId: &storageId,
		Storage:   &types.WorkspaceStorage{Id: &storageId},
	}}
	in := &pb.GetOrCreateStubRequest{
		Name:              "qwen",
		StubType:          types.StubTypePodDeployment,
		CheckpointEnabled: true,
		CheckpointTrigger: &pb.CheckpointTrigger{Type: "http", HttpPath: "/v1/models"},
		Env:               []string{"TRITON_CACHE_DIR=/bad", "HF_HOME=/bad", "USER_ENV=1"},
	}

	warning, err := (&GatewayService{backendRepo: &checkpointVolumeBackendRepo{}}).handleCheckpointEnabled(context.Background(), authInfo, in, nil)

	require.NoError(t, err)
	require.Empty(t, warning)
	require.ElementsMatch(t, []string{
		"HF_HOME=/checkpoint-model-cache-qwen",
		"HF_HUB_CACHE=/checkpoint-model-cache-qwen/hub",
		"HF_XET_CACHE=/tmp/beam-checkpoint-compile-cache/hf-xet",
		"TRANSFORMERS_CACHE=/checkpoint-model-cache-qwen",
		"TRITON_CACHE_DIR=/tmp/beam-checkpoint-compile-cache/triton",
		"TORCHINDUCTOR_CACHE_DIR=/tmp/beam-checkpoint-compile-cache/torchinductor",
		"VLLM_CACHE_ROOT=/tmp/beam-checkpoint-compile-cache/vllm",
		"CUDA_CACHE_PATH=/tmp/beam-checkpoint-compile-cache/cuda",
		"USER_ENV=1",
	}, in.Env)
	require.Len(t, in.Volumes, 1)
	require.Equal(t, "volume-123", in.Volumes[0].Id)
	require.Equal(t, "/checkpoint-model-cache-qwen", in.Volumes[0].MountPath)
}

func TestCheckpointCacheEnvLeavesHubTransportSelectionToRuntime(t *testing.T) {
	env := checkpointCacheEnv("/model-cache")

	require.NotContains(t, env, "HF_HUB_DISABLE_XET=1")
	require.NotContains(t, env, "HF_HUB_ENABLE_HF_TRANSFER=0")
	require.Contains(t, env, "HF_XET_CACHE=/tmp/beam-checkpoint-compile-cache/hf-xet")
}
