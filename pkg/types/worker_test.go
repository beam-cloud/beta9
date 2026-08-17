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
			Name:               "the-fork-disk",
			Size:               "50Gi",
			SourceGenerationId: "7aee3365-2963-4a6d-b9fb-2c934924880d",
			VolumeId:           "08d476ee-9830-49c9-bf74-4d148e535c2f",
			Initialize:         false,
			AttachmentToken:    "94b7c473-9436-4275-b7da-a99ad2fe0da3",
			FencingToken:       19,
			LeaseExpiresAtUnix: 1923840000,
		},
	}

	wire := mount.ToProto()
	require.Equal(t, "7aee3365-2963-4a6d-b9fb-2c934924880d", wire.DurableDisk.SourceGenerationId)
	require.Equal(t, "08d476ee-9830-49c9-bf74-4d148e535c2f", wire.DurableDisk.VolumeId)
	require.EqualValues(t, 19, wire.DurableDisk.FencingToken)
	require.EqualValues(t, 1923840000, wire.DurableDisk.LeaseExpiresAtUnix)

	back := NewMountFromProto(wire)
	require.Equal(t, mount.DurableDisk, back.DurableDisk)
}

func TestMountProtoRoundTripLeavesAnUnseededDiskAlone(t *testing.T) {
	mount := &Mount{
		MountType:   StorageModeDurableDisk,
		DurableDisk: &DurableDiskMountConfig{Name: "pg-data"},
	}

	back := NewMountFromProto(mount.ToProto())

	require.Empty(t, back.DurableDisk.SourceGenerationId)
}
