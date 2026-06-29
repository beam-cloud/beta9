package gatewayservices

import (
	"context"
	"testing"

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
