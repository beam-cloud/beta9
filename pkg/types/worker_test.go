package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMountProtoRoundTripPreservesDurableDisk(t *testing.T) {
	mount := &Mount{
		LocalPath: "/data/durable-disks/workspace/pg-data",
		MountPath: "/var/lib/postgresql/data",
		MountType: "durable_disk",
		DurableDisk: &DurableDiskMountConfig{
			Name:             "pg-data",
			Size:             "10Gi",
			Filesystem:       "ext4",
			Driver:           "dev",
			Replicas:         3,
			Mode:             "sync",
			Quorum:           "majority",
			PrimaryWorkerID:  "worker-a",
			ReplicaWorkerIDs: []string{"worker-a", "worker-b", "worker-c"},
		},
	}

	roundTrip := NewMountFromProto(mount.ToProto())

	require.Equal(t, mount.LocalPath, roundTrip.LocalPath)
	require.Equal(t, mount.MountPath, roundTrip.MountPath)
	require.Equal(t, mount.MountType, roundTrip.MountType)
	require.NotNil(t, roundTrip.DurableDisk)
	require.Equal(t, mount.DurableDisk, roundTrip.DurableDisk)
}
