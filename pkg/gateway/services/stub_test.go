package gatewayservices

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

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
