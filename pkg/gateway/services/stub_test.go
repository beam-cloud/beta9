package gatewayservices

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type checkpointVolumeBackendRepo struct {
	repository.BackendRepository
}

func (r *checkpointVolumeBackendRepo) GetOrCreateVolume(ctx context.Context, workspaceId uint, name string) (*types.Volume, error) {
	return &types.Volume{ExternalId: "volume-123", WorkspaceId: workspaceId, Name: name}, nil
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
		"TRANSFORMERS_CACHE=/checkpoint-model-cache-qwen",
		"TRITON_CACHE_DIR=/tmp/beam-checkpoint-compile-cache/triton",
		"TORCHINDUCTOR_CACHE_DIR=/tmp/beam-checkpoint-compile-cache/torchinductor",
		"VLLM_CACHE_ROOT=/tmp/beam-checkpoint-compile-cache/vllm",
		"CUDA_CACHE_PATH=/tmp/beam-checkpoint-compile-cache/cuda",
		"HF_HUB_DISABLE_XET=1",
		"HF_HUB_ENABLE_HF_TRANSFER=0",
		"USER_ENV=1",
	}, in.Env)
	require.Len(t, in.Volumes, 1)
	require.Equal(t, "volume-123", in.Volumes[0].Id)
	require.Equal(t, "/checkpoint-model-cache-qwen", in.Volumes[0].MountPath)
}
