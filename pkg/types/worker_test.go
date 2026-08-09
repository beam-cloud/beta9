package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMountProtoRoundTripKeepsTheDurableDiskSeed(t *testing.T) {
	mount := &Mount{
		LocalPath: "/var/lib/beta9/durable-disks/workspace/the-fork-disk",
		MountPath: "/home/mote",
		MountType: StorageModeDurableDisk,
		DurableDisk: &DurableDiskMountConfig{
			Name:             "the-fork-disk",
			Size:             "50Gi",
			Filesystem:       "ext4",
			Driver:           DurableDiskDriverSnapshot,
			SourceSnapshotId: "snapshot-of-the-source-disk",
		},
	}

	wire := mount.ToProto()
	require.Equal(t, "snapshot-of-the-source-disk", wire.DurableDisk.SourceSnapshotId)

	back := NewMountFromProto(wire)
	require.Equal(t, mount.DurableDisk, back.DurableDisk)
}

func TestMountProtoRoundTripLeavesAnUnseededDiskAlone(t *testing.T) {
	mount := &Mount{
		MountType:   StorageModeDurableDisk,
		DurableDisk: &DurableDiskMountConfig{Name: "pg-data", Driver: DurableDiskDriverSnapshot},
	}

	back := NewMountFromProto(mount.ToProto())

	require.Empty(t, back.DurableDisk.SourceSnapshotId)
}
