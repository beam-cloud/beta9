package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDurableDiskCommandArgsRoundTrip(t *testing.T) {
	args := DurableDiskCommandArgs{
		StorageNodeID: "node-a",
		Action:        DurableDiskCommandActionPrepare,
		Mount: Mount{
			LocalPath: "/data/durable-disks/workspace/pg-data",
			MountPath: "/data",
			DurableDisk: &DurableDiskMountConfig{
				Name:             "pg-data",
				PrimaryWorkerID:  "worker-a",
				ReplicaWorkerIDs: []string{"worker-a", "worker-b"},
			},
		},
		Nonce: "nonce",
	}

	raw, err := args.ToMap()
	require.NoError(t, err)
	got, err := ToDurableDiskCommandArgs(raw)
	require.NoError(t, err)

	require.Equal(t, args.StorageNodeID, got.StorageNodeID)
	require.Equal(t, args.Action, got.Action)
	require.Equal(t, args.Mount.DurableDisk, got.Mount.DurableDisk)
	require.Equal(t, args.Nonce, got.Nonce)
}

func TestSafeDurableDiskName(t *testing.T) {
	require.Equal(t, "pg-data", SafeDurableDiskName("pg-data"))
	require.Equal(t, "pg-data", SafeDurableDiskName("pg/data"))
	require.Equal(t, "disk", SafeDurableDiskName(".."))
}
