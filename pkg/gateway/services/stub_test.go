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

func TestConfigureDurableDiskPlacementNormalizesBlockVolume(t *testing.T) {
	config := &types.StubConfigV1{
		Disks: []*pb.DurableDisk{{Name: "pg/data", Size: " 50Gi ", MountPath: "/data/", SourceGenerationId: " 7aee3365-2963-4a6d-b9fb-2c934924880d "}},
	}

	require.NoError(t, (&GatewayService{}).configureDurableDiskPlacement(context.Background(), nil, config))
	require.Equal(t, "pg-data", config.Disks[0].Name)
	require.Equal(t, "50Gi", config.Disks[0].Size)
	require.Equal(t, "/data", config.Disks[0].MountPath)
	require.Equal(t, "7aee3365-2963-4a6d-b9fb-2c934924880d", config.Disks[0].SourceGenerationId)
}

func TestPersistentRootFromProtoRequiresPositiveSize(t *testing.T) {
	root, err := persistentRootFromProto(&pb.PersistentRoot{Size: " 50Gi "})
	require.NoError(t, err)
	require.Equal(t, &types.PersistentRoot{Size: "50Gi"}, root)

	for _, size := range []string{"", "nope", "0", "-1Gi"} {
		_, err := persistentRootFromProto(&pb.PersistentRoot{Size: size})
		require.Error(t, err)
	}
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
		Disks:      []*pb.DurableDisk{{Name: "data", Size: "50Gi", MountPath: "/data", ReadOnly: true}},
	}

	require.NoError(t, (&GatewayService{}).configureDurableDiskPlacement(context.Background(), nil, config))
}
