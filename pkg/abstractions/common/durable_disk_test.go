package abstractions

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestConfigureDurableDiskPlacementNormalizesBlockVolumeInputs(t *testing.T) {
	config := &types.StubConfigV1{Disks: []*pb.DurableDisk{{
		Name:               "  model.cache  ",
		Size:               " 40Gi ",
		MountPath:          " /workspace/cache/../models ",
		SourceGenerationId: " 7aee3365-2963-4a6d-b9fb-2c934924880d ",
	}}}

	err := ConfigureDurableDiskPlacement(context.Background(), DurableDiskPlacementRepos{}, &types.Workspace{}, config)

	require.NoError(t, err)
	require.Equal(t, "model.cache", config.Disks[0].Name)
	require.Equal(t, "40Gi", config.Disks[0].Size)
	require.Equal(t, "/workspace/models", config.Disks[0].MountPath)
	require.Equal(t, "7aee3365-2963-4a6d-b9fb-2c934924880d", config.Disks[0].SourceGenerationId)
}

func TestConfigureDurableDiskPlacementRejectsInvalidOrDuplicateMounts(t *testing.T) {
	tests := map[string][]*pb.DurableDisk{
		"missing size":              {{Name: "models", MountPath: "/models"}},
		"root mount":                {{Name: "models", Size: "4Gi", MountPath: "/"}},
		"relative":                  {{Name: "models", Size: "4Gi", MountPath: "models"}},
		"invalid source generation": {{Name: "models", Size: "4Gi", MountPath: "/models", SourceGenerationId: "generation-17"}},
		"duplicate name": {
			{Name: "models", Size: "4Gi", MountPath: "/models-a"},
			{Name: "models", Size: "4Gi", MountPath: "/models-b"},
		},
		"duplicate mount": {
			{Name: "models-a", Size: "4Gi", MountPath: "/models"},
			{Name: "models-b", Size: "4Gi", MountPath: "/models"},
		},
	}
	for name, disks := range tests {
		t.Run(name, func(t *testing.T) {
			err := ConfigureDurableDiskPlacement(context.Background(), DurableDiskPlacementRepos{}, &types.Workspace{}, &types.StubConfigV1{Disks: disks})
			require.Error(t, err)
		})
	}
}

func TestConfigureDurableDiskPlacementEnforcesOneWriter(t *testing.T) {
	config := &types.StubConfigV1{
		Autoscaler: &types.Autoscaler{MaxContainers: 2},
		Disks:      []*pb.DurableDisk{{Name: "models", Size: "4Gi", MountPath: "/models"}},
	}
	require.ErrorContains(t,
		ConfigureDurableDiskPlacement(context.Background(), DurableDiskPlacementRepos{}, &types.Workspace{}, config),
		"one container")

	config.Disks[0].ReadOnly = true
	require.NoError(t, ConfigureDurableDiskPlacement(context.Background(), DurableDiskPlacementRepos{}, &types.Workspace{}, config))
}

func TestUnavailablePrivatePoolIsNeverSilentlyRewritten(t *testing.T) {
	config := &types.StubConfigV1{Pool: &types.PoolConfig{Selector: "private-pool"}}
	require.NoError(t, ConfigureUnavailablePrivatePoolFallback(context.Background(), DurableDiskPlacementRepos{}, &types.Workspace{}, config))
	require.Equal(t, "private-pool", config.PoolSelector())
}
