package abstractions

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestConfigureContainerRequestDurableDiskPlacementDoesNotTargetDevPrimary(t *testing.T) {
	request := &types.ContainerRequest{}
	ConfigureContainerRequestDurableDiskPlacement(request, types.StubConfigV1{
		Disks: []*pb.DurableDisk{{
			Driver: types.DurableDiskDriverDev,
			Replication: &pb.DiskReplication{
				PrimaryWorkerId: "node-a",
			},
		}},
	})

	require.Empty(t, request.TargetWorkerId)
}

func TestConfigureContainerRequestDurableDiskPlacementTargetsDRBDPrimary(t *testing.T) {
	request := &types.ContainerRequest{}
	ConfigureContainerRequestDurableDiskPlacement(request, types.StubConfigV1{
		Disks: []*pb.DurableDisk{{
			Driver: types.DurableDiskDriverDRBD,
			Replication: &pb.DiskReplication{
				PrimaryWorkerId: "node-a",
			},
		}},
	})

	require.Equal(t, "node-a", request.TargetWorkerId)
}
