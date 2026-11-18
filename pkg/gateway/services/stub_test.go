package gatewayservices

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

type parallelStubIdentityRepo struct {
	repository.BackendRepository
	started    chan string
	release    chan struct{}
	failObject bool
}

func (r *parallelStubIdentityRepo) wait(ctx context.Context, record string) error {
	r.started <- record
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *parallelStubIdentityRepo) GetOrCreateApp(ctx context.Context, ws uint, name string) (*types.App, error) {
	if err := r.wait(ctx, "app"); err != nil {
		return nil, err
	}
	return &types.App{WorkspaceId: ws, Name: name}, nil
}

func (r *parallelStubIdentityRepo) GetOrCreateDisk(ctx context.Context, _ uint, disk *types.Disk) (*types.Disk, error) {
	return disk, r.wait(ctx, "disk")
}

func (r *parallelStubIdentityRepo) GetObjectByExternalId(ctx context.Context, id string, ws uint) (types.Object, error) {
	if r.failObject {
		r.started <- "object"
		return types.Object{}, errors.New("object unavailable")
	}
	return types.Object{ExternalId: id, WorkspaceId: ws}, r.wait(ctx, "object")
}

func TestStubIdentityPreparesIndependentRecordsAndCancelsFailures(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "overlap", true: "cancel"}[fail], func(t *testing.T) {
			repo := &parallelStubIdentityRepo{started: make(chan string, 3), release: make(chan struct{}), failObject: fail}
			service := &GatewayService{backendRepo: repo}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				app, object, err := service.prepareStubIdentity(ctx, &types.Workspace{Id: 7}, &pb.GetOrCreateStubRequest{Name: "dev", ObjectId: "object"}, []*pb.DurableDisk{{Name: "root"}})
				if err == nil && (app.Name != "dev" || app.WorkspaceId != 7 || object.WorkspaceId != 7 || object.ExternalId != "object") {
					err = errors.New("wrong stub identity or ownership")
				}
				result <- err
			}()
			started := map[string]bool{}
			for len(started) < 3 {
				select {
				case record := <-repo.started:
					started[record] = true
				case <-ctx.Done():
					t.Fatal("stub identity did not prepare all independent records")
				}
			}
			if !fail {
				select {
				case <-result:
					t.Fatal("stub identity returned before prerequisites completed")
				default:
				}
				close(repo.release)
			}
			select {
			case err := <-result:
				if fail {
					require.ErrorContains(t, err, "prepare stub object")
				} else {
					require.NoError(t, err)
				}
			case <-ctx.Done():
				t.Fatal("stub identity did not finish or cancel its prerequisites")
			}
		})
	}
}

func TestVMStubRequiresMicroVMAndAllowsOneGPU(t *testing.T) {
	in := &pb.GetOrCreateStubRequest{StubType: types.StubTypeVM}
	require.Equal(t, "VMs require use_vm", validateUseVM(in, nil))
	in.UseVm = true
	require.Empty(t, validateUseVM(in, nil))
	require.Empty(t, validateUseVM(in, []types.GpuType{types.GPU_RTX4090}))
	in.GpuCount = 1
	require.Empty(t, validateUseVM(in, []types.GpuType{types.GPU_RTX4090}))
	in.GpuCount = 2
	require.NotEmpty(t, validateUseVM(in, []types.GpuType{types.GPU_RTX4090}))
	in.GpuCount = 1
	require.NotEmpty(t, validateUseVM(in, []types.GpuType{types.GPU_RTX4090, types.GPU_H100}))
	require.Equal(t, -1, normalizeKeepWarmSeconds(-1, types.StubType(types.StubTypeVM)))
}

type checkpointVolumeBackendRepo struct {
	repository.BackendRepository
}

type volumeLookupBackendRepo struct {
	repository.BackendRepository
	volumes map[string]*types.Volume // owned by workspace 42
}

func (r *volumeLookupBackendRepo) GetVolumeByExternalId(_ context.Context, workspaceID uint, volumeID string) (*types.Volume, error) {
	if workspaceID != 42 {
		return nil, nil
	}
	return r.volumes[volumeID], nil
}

func TestConfigureVolumesRequiresWorkspaceOwnership(t *testing.T) {
	repo := &volumeLookupBackendRepo{volumes: map[string]*types.Volume{"volume-1": {ExternalId: "volume-1"}}}
	service := &GatewayService{backendRepo: repo}
	configure := func(workspaceID uint, volumeID string) error {
		return service.configureVolumes(context.Background(), []*pb.Volume{{Id: volumeID, MountPath: "/data"}}, &types.Workspace{Id: workspaceID})
	}
	require.NoError(t, configure(42, "volume-1"))
	require.Error(t, configure(42, "volume-2"), "unknown volume")
	require.Error(t, configure(7, "volume-1"), "another workspace's volume")
	require.Error(t, configure(42, "../../.."), "traversal")
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

func (f fakeGpuPoolChecker) HasManagedPoolForGPU(gpuType string) bool {
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

func TestPreparedStubCacheKey(t *testing.T) {
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(common.PreparedStubCacheMetadata, "cache-key"),
	)
	require.Equal(t, "cache-key", preparedStubCacheKey(ctx))
	require.Empty(t, preparedStubCacheKey(context.Background()))
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

	verdict, err := computeCapacityVerdict(context.Background(), checker, nil, "ws-1", []types.GpuType{types.GpuType("A6000"), types.GpuType("T4")}, false)
	require.NoError(t, err)
	require.Equal(t, StubCapacityStatusAvailable, verdict.status)
	require.Equal(t, []string{"A6000"}, verdict.unsupportedGpus)
	require.Empty(t, verdict.matchedPrivatePool)
}

func TestComputeCapacityVerdictLowCapacity(t *testing.T) {
	checker := fakeGpuPoolChecker{supported: map[string]bool{"T4": true}}

	verdict, err := computeCapacityVerdict(context.Background(), checker, nil, "ws-1", []types.GpuType{types.GpuType("T4")}, true)
	require.NoError(t, err)
	require.Equal(t, StubCapacityStatusLow, verdict.status)
}

func TestComputeCapacityVerdictNoneWhenNoPoolSupportsGPU(t *testing.T) {
	checker := fakeGpuPoolChecker{supported: map[string]bool{}}

	verdict, err := computeCapacityVerdict(context.Background(), checker, nil, "ws-1", []types.GpuType{types.GpuType("A6000")}, false)
	require.NoError(t, err)
	require.Equal(t, StubCapacityStatusNone, verdict.status)
	require.Equal(t, []string{"A6000"}, verdict.unsupportedGpus)
}

func TestComputeCapacityVerdictMatchesReadyPrivatePool(t *testing.T) {
	checker := fakeGpuPoolChecker{supported: map[string]bool{}}
	finder := fakePrivatePoolFinder{pool: "ondemand-a6000"}

	verdict, err := computeCapacityVerdict(context.Background(), checker, finder, "ws-1", []types.GpuType{types.GpuType("A6000")}, false)
	require.NoError(t, err)
	require.Equal(t, StubCapacityStatusAvailable, verdict.status)
	require.Equal(t, "ondemand-a6000", verdict.matchedPrivatePool)
}

func TestComputeCapacityVerdictSkipsNoGPU(t *testing.T) {
	checker := fakeGpuPoolChecker{supported: map[string]bool{}}

	verdict, err := computeCapacityVerdict(context.Background(), checker, nil, "ws-1", []types.GpuType{types.NO_GPU, types.GpuType("A6000")}, false)
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

type deployStubCustomDomainRepo struct {
	repository.BackendRepository
	stub              *types.StubWithRelated
	existingDomains   map[string]*types.CustomDomain
	latestDeployment  *types.DeploymentWithRelated
	activeDeployments []types.DeploymentWithRelated
	upsertErr         error
	upserted          []*types.CustomDomain
	created           []types.Deployment
	deleted           []types.Deployment
	updated           []types.Deployment
	listedDomains     []types.CustomDomain
}

func (r *deployStubCustomDomainRepo) GetStubByExternalId(_ context.Context, externalId string, _ ...types.QueryFilter) (*types.StubWithRelated, error) {
	if r.stub == nil || r.stub.ExternalId != externalId {
		return nil, errors.New("not found")
	}
	return r.stub, nil
}

func (r *deployStubCustomDomainRepo) GetLatestDeploymentByName(_ context.Context, _ uint, _ string, _ string, _ bool) (*types.DeploymentWithRelated, error) {
	return r.latestDeployment, nil
}

func (r *deployStubCustomDomainRepo) ListDeploymentsWithRelated(_ context.Context, _ types.DeploymentFilter) ([]types.DeploymentWithRelated, error) {
	return r.activeDeployments, nil
}

func (r *deployStubCustomDomainRepo) GetCustomDomainByIdentifier(_ context.Context, workspaceId uint, idOrDomain string) (*types.CustomDomain, error) {
	if r.existingDomains == nil {
		return nil, nil
	}
	if cd, ok := r.existingDomains[idOrDomain]; ok && cd != nil && (cd.WorkspaceId == 0 || cd.WorkspaceId == workspaceId) {
		return cd, nil
	}
	return nil, nil
}

func (r *deployStubCustomDomainRepo) PruneDeployCustomDomains(_ context.Context, workspaceId uint, deploymentName, stubType string, keep []string) ([]types.CustomDomain, error) {
	keepSet := make(map[string]struct{}, len(keep))
	for _, k := range keep {
		keepSet[types.NormalizeDomain(k)] = struct{}{}
	}
	var pruned []types.CustomDomain
	remaining := make([]types.CustomDomain, 0, len(r.listedDomains))
	for _, cd := range r.listedDomains {
		if cd.WorkspaceId == workspaceId && cd.DeploymentName == deploymentName && (cd.StubType == "" || stubType == "" || cd.StubType == stubType) && cd.Source == types.CustomDomainSourceDeploy {
			if _, ok := keepSet[cd.Domain]; !ok {
				pruned = append(pruned, cd)
				continue
			}
		}
		remaining = append(remaining, cd)
	}
	r.listedDomains = remaining
	return pruned, nil
}

func (r *deployStubCustomDomainRepo) CreateDeployment(_ context.Context, workspaceId uint, name string, version uint, stubId uint, stubType string, appId uint) (*types.Deployment, error) {
	dep := types.Deployment{
		Id:          99,
		ExternalId:  "dep-new",
		Name:        name,
		Active:      true,
		Subdomain:   name + "-abc1234",
		WorkspaceId: workspaceId,
		StubId:      stubId,
		StubType:    stubType,
		Version:     version,
		AppId:       appId,
	}
	r.created = append(r.created, dep)
	return &dep, nil
}

func (r *deployStubCustomDomainRepo) UpdateDeployment(_ context.Context, dep types.Deployment) (*types.Deployment, error) {
	r.updated = append(r.updated, dep)
	return &dep, nil
}

func (r *deployStubCustomDomainRepo) DeleteDeployment(_ context.Context, dep types.Deployment) error {
	r.deleted = append(r.deleted, dep)
	return nil
}

func (r *deployStubCustomDomainRepo) UpsertCustomDomains(_ context.Context, domains []*types.CustomDomain) ([]types.CustomDomain, error) {
	if r.upsertErr != nil {
		return nil, r.upsertErr
	}
	r.upserted = append(r.upserted, domains...)
	out := make([]types.CustomDomain, len(domains))
	for i, d := range domains {
		out[i] = *d
		r.listedDomains = append(r.listedDomains, *d)
	}
	return out, nil
}

func (r *deployStubCustomDomainRepo) ListCustomDomains(_ context.Context, _ uint, _ string) ([]types.CustomDomain, error) {
	return r.listedDomains, nil
}

func TestDeployStubCustomDomainValidationAndRollback(t *testing.T) {
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Id: 1, ExternalId: "ws-1", Name: "default"},
	})
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	// 1. Rejects unsupported stub types and raw TCP / portless pods without creating a deployment.
	for _, tc := range []struct {
		name     string
		stubType string
		cfg      string
		errPart  string
	}{
		{"function", types.StubTypeFunctionDeployment, `{"extra":{"custom_domain":["a.customer.io"]}}`, "not supported"},
		{"taskqueue", types.StubTypeTaskQueueDeployment, `{"extra":{"custom_domain":["a.customer.io"]}}`, "not supported"},
		{"tcp_pod", types.StubTypePodDeployment, `{"ports":[5432],"tcp":true,"extra":{"custom_domain":["db.customer.io"]}}`, "raw TCP"},
		{"portless_pod", types.StubTypePodDeployment, `{"extra":{"custom_domain":["pod.customer.io"]}}`, "exposed port"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &deployStubCustomDomainRepo{
				stub: &types.StubWithRelated{
					Stub:      types.Stub{Id: 1, ExternalId: "stub-1", Type: types.StubType(tc.stubType), Config: tc.cfg},
					Workspace: types.Workspace{Id: 1, ExternalId: "ws-1"},
				},
			}
			svc := &GatewayService{backendRepo: repo, redisClient: rdb}
			resp, callErr := svc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-1", Name: "my-app"})
			require.NoError(t, callErr)
			require.False(t, resp.Ok)
			require.Contains(t, resp.ErrMsg, tc.errPart)
			require.Empty(t, repo.created)
		})
	}

	// 2. Rolls back newly created deployment and restores stopped previous deployment if UpsertCustomDomains fails.
	prevDep := types.DeploymentWithRelated{
		Deployment: types.Deployment{
			Id: 10, ExternalId: "dep-v1", Name: "my-api", Active: true,
			WorkspaceId: 1, StubType: types.StubTypeEndpointDeployment, Version: 1,
		},
		Stub: types.Stub{ExternalId: "stub-v1", Type: types.StubType(types.StubTypeEndpointDeployment), Config: `{}`},
	}
	durableDiskCfg, err := json.Marshal(types.StubConfigV1{
		Disks: []*pb.DurableDisk{{Name: "data"}},
		Extra: json.RawMessage(`{"custom_domain":["a.customer.io","b.customer.io"]}`),
	})
	require.NoError(t, err)
	rollbackRepo := &deployStubCustomDomainRepo{
		stub: &types.StubWithRelated{
			Stub:      types.Stub{Id: 2, ExternalId: "stub-v2", Type: types.StubType(types.StubTypeEndpointDeployment), Config: string(durableDiskCfg)},
			Workspace: types.Workspace{Id: 1, ExternalId: "ws-1"},
		},
		latestDeployment:  &prevDep,
		activeDeployments: []types.DeploymentWithRelated{prevDep},
		upsertErr:         types.ErrCustomDomainAlreadyExists,
	}
	svc := &GatewayService{backendRepo: rollbackRepo, containerRepo: &deploymentLifecycleContainerRepo{}, redisClient: rdb}
	resp, err := svc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-v2", Name: "my-api"})
	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrMsg, "Failed to bind custom domains")
	require.Len(t, rollbackRepo.deleted, 1)
	require.Equal(t, "dep-new", rollbackRepo.deleted[0].ExternalId)
	// First UpdateDeployment deactivated v1 in stopDeployments; second restored Active=true on rollback.
	require.Len(t, rollbackRepo.updated, 2)
	require.False(t, rollbackRepo.updated[0].Active)
	require.True(t, rollbackRepo.updated[1].Active)

	// 3. Happy path binds all custom domains atomically with source='deploy' and invalidates Redis cache.
	require.NoError(t, rdb.Set(ctx, common.CustomDomainHandlerCacheKey("a.customer.io"), "-", time.Minute).Err())
	happyRepo := &deployStubCustomDomainRepo{
		stub: &types.StubWithRelated{
			Stub:      types.Stub{Id: 3, ExternalId: "stub-ep", Type: types.StubType(types.StubTypeEndpointDeployment), Config: `{"extra":{"custom_domain":["a.customer.io","b.customer.io"]}}`},
			Workspace: types.Workspace{Id: 1, ExternalId: "ws-1"},
		},
	}
	happySvc := &GatewayService{backendRepo: happyRepo, redisClient: rdb}
	resp, err = happySvc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-ep", Name: "my-api"})
	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.Len(t, happyRepo.upserted, 2)
	require.Equal(t, types.CustomDomainSourceDeploy, happyRepo.upserted[0].Source)
	require.Equal(t, types.CustomDomainSourceDeploy, happyRepo.upserted[1].Source)
	exists, err := rdb.Exists(ctx, common.CustomDomainHandlerCacheKey("a.customer.io")).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), exists)

	// 4. Rejects binding a domain already owned by another deployment in the same workspace.
	conflictRepo := &deployStubCustomDomainRepo{
		stub: &types.StubWithRelated{
			Stub:      types.Stub{Id: 4, ExternalId: "stub-conflict", Type: types.StubType(types.StubTypeEndpointDeployment), Config: `{"extra":{"custom_domain":["taken.customer.io"]}}`},
			Workspace: types.Workspace{Id: 1, ExternalId: "ws-1"},
		},
		existingDomains: map[string]*types.CustomDomain{
			"taken.customer.io": {
				WorkspaceId:    1,
				Domain:         "taken.customer.io",
				DeploymentName: "other-api",
				StubType:       types.StubTypeEndpointDeployment,
			},
		},
	}
	conflictSvc := &GatewayService{backendRepo: conflictRepo, redisClient: rdb}
	resp, err = conflictSvc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-conflict", Name: "my-api"})
	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrMsg, "other-api")
	require.Empty(t, conflictRepo.created)

	// 5. Re-deploying with fewer domains prunes removed deploy-sourced domains but keeps API-sourced domains, and invalidates pruned cache.
	require.NoError(t, rdb.Set(ctx, common.CustomDomainHandlerCacheKey("removed.customer.io"), "/endpoint/my-api/latest", time.Minute).Err())
	pruneRepo := &deployStubCustomDomainRepo{
		stub: &types.StubWithRelated{
			Stub:      types.Stub{Id: 5, ExternalId: "stub-prune", Type: types.StubType(types.StubTypeEndpointDeployment), Config: `{"extra":{"custom_domain":["kept.customer.io"]}}`},
			Workspace: types.Workspace{Id: 1, ExternalId: "ws-1"},
		},
		listedDomains: []types.CustomDomain{
			{WorkspaceId: 1, Domain: "kept.customer.io", DeploymentName: "my-api", StubType: types.StubTypeEndpointDeployment, Source: types.CustomDomainSourceDeploy},
			{WorkspaceId: 1, Domain: "removed.customer.io", DeploymentName: "my-api", StubType: types.StubTypeEndpointDeployment, Source: types.CustomDomainSourceDeploy},
			{WorkspaceId: 1, Domain: "manual.customer.io", DeploymentName: "my-api", StubType: types.StubTypeEndpointDeployment, Source: types.CustomDomainSourceAPI},
		},
	}
	pruneSvc := &GatewayService{backendRepo: pruneRepo, redisClient: rdb}
	resp, err = pruneSvc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-prune", Name: "my-api"})
	require.NoError(t, err)
	require.True(t, resp.Ok)
	remainingNames := common.CustomDomainNames(pruneRepo.listedDomains)
	require.Contains(t, remainingNames, "kept.customer.io")
	require.Contains(t, remainingNames, "manual.customer.io")
	require.NotContains(t, remainingNames, "removed.customer.io")
	prunedExists, err := rdb.Exists(ctx, common.CustomDomainHandlerCacheKey("removed.customer.io")).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), prunedExists)
}
