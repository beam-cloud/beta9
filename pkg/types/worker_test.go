package types

import (
	"encoding/json"
	"strings"
	"testing"

	pb "github.com/beam-cloud/beta9/proto"
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

func TestUnsetCaptureExitSnapshotMeansCaptured(t *testing.T) {
	captured, discarded := true, false
	require.True(t, CapturesExitSnapshot(nil))
	require.True(t, CapturesExitSnapshot(&captured))
	require.False(t, CapturesExitSnapshot(&discarded))
}

// Every VM and stub stored so far lacks capture_exit_snapshot. Reading and
// writing them again must neither add it nor stop capturing their disks.
func TestStoredSpecsAndStubsStayWithoutCaptureExitSnapshot(t *testing.T) {
	stored, err := json.Marshal(VMSpec{ImageID: "base", SourceSnapshotID: "template-root"})
	require.NoError(t, err)
	require.NotContains(t, string(stored), "capture_exit_snapshot")
	var spec VMSpec
	require.NoError(t, json.Unmarshal(stored, &spec))
	require.Nil(t, spec.CaptureExitSnapshot)
	require.True(t, CapturesExitSnapshot(spec.CaptureExitSnapshot))

	stored, err = json.Marshal(StubConfigV1{Disks: []*pb.DurableDisk{{Name: "vm-root", MountPath: "/", Driver: DurableDiskDriverQcow}}})
	require.NoError(t, err)
	require.NotContains(t, string(stored), "capture_exit_snapshot")
	var stub StubConfigV1
	require.NoError(t, json.Unmarshal(stored, &stub))
	require.Nil(t, stub.Disks[0].CaptureExitSnapshot)
	require.False(t, NewDurableDiskMountConfigFromProto(stub.Disks[0]).SkipExitSnapshot)

	discarded := false
	stored, err = json.Marshal(VMSpec{ImageID: "base", CaptureExitSnapshot: &discarded})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(stored, &spec))
	require.False(t, CapturesExitSnapshot(spec.CaptureExitSnapshot))
}

func TestMountProtoRoundTripSkipsOnlyADiskThatSaysSo(t *testing.T) {
	captured, discarded := true, false
	for name, test := range map[string]struct {
		setting *bool
		skip    bool
	}{
		"unset":        {nil, false},
		"captured":     {&captured, false},
		"not captured": {&discarded, true},
	} {
		t.Run(name, func(t *testing.T) {
			disk := &pb.DurableDisk{Name: "vm-root", Size: "50Gi", MountPath: "/", Driver: DurableDiskDriverQcow, CaptureExitSnapshot: test.setting}
			stored, err := json.Marshal(disk)
			require.NoError(t, err)
			var read pb.DurableDisk
			require.NoError(t, json.Unmarshal(stored, &read))
			require.Equal(t, test.setting, read.CaptureExitSnapshot, "a stub keeps the setting as it was given")

			mount := &Mount{MountType: StorageModeDurableDisk, DurableDisk: NewDurableDiskMountConfigFromProto(&read)}
			require.Equal(t, test.skip, mount.DurableDisk.SkipExitSnapshot)
			queued, err := json.Marshal(mount)
			require.NoError(t, err)
			require.Equal(t, test.skip, strings.Contains(string(queued), "skip_exit_snapshot"))

			wire := mount.ToProto()
			require.Equal(t, test.skip, wire.DurableDisk.SkipExitSnapshot)
			back := NewMountFromProto(wire)
			require.Equal(t, mount.DurableDisk, back.DurableDisk)
		})
	}

	// A mount that does not say has its disk captured.
	old := NewMountFromProto(&pb.Mount{DurableDisk: &pb.DurableDiskMountConfig{Name: "vm-root", Driver: DurableDiskDriverQcow}})
	require.False(t, old.DurableDisk.SkipExitSnapshot)
}
