package repository_services

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDiskSnapshotPublicProtoRoundTrip(t *testing.T) {
	snapshot := diskSnapshotFromProto(diskSnapshotToProto(&types.DiskSnapshot{Public: true}))
	require.True(t, snapshot.Public)
}

func TestDiskSnapshotManifestChunkKeys(t *testing.T) {
	data, err := json.Marshal(types.DiskSnapshotManifest{Files: []types.DiskSnapshotFile{{
		Chunks: []types.DiskSnapshotChunk{{ObjectKey: "durable-disks/source/chunks/allowed"}},
	}}})
	require.NoError(t, err)
	digest := sha256.Sum256(data)

	keys, err := diskSnapshotManifestChunkKeys(data, "sha256:"+hex.EncodeToString(digest[:]))
	require.NoError(t, err)
	snapshot := &types.DiskSnapshot{ManifestKey: "durable-disks/source/snapshots/1/manifest.json"}
	require.True(t, diskSnapshotObjectKeyAllowed(snapshot, snapshot.ManifestKey, keys))
	require.True(t, diskSnapshotObjectKeyAllowed(snapshot, "durable-disks/source/chunks/allowed", keys))
	require.False(t, diskSnapshotObjectKeyAllowed(snapshot, "durable-disks/source/chunks/private", keys))

	_, err = diskSnapshotManifestChunkKeys(data, "sha256:wrong")
	require.ErrorContains(t, err, "digest mismatch")
}
