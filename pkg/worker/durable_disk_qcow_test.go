package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConcurrentQcowRestoresGoStraightToOriginOnLocalCacheMiss(t *testing.T) {
	content := []byte("durable qcow layer")
	store := &fakeDurableDiskSnapshotStore{objects: map[string][]byte{"layer": content}}
	// No network cache configuration: entering distributed cache discovery on
	// a local miss would fail instead of restoring from the durable origin.
	source := &qcowChunkSource{cacheReader: localSnapshotCacheReader{&cache.Client{}}, stores: []durableDiskSnapshotStore{store}}
	for range 100 {
		t.Run("restore", func(t *testing.T) {
			t.Parallel()
			dest := make([]byte, len(content))
			err := source.ReadChunk(context.Background(), types.DiskSnapshotChunk{ObjectKey: "layer", Digest: "sha256:missing"}, dest)
			require.NoError(t, err)
			require.Equal(t, content, dest)
		})
	}
}

type backloggedQcowVolume struct {
	pending []disk.SealedLayer
	depth   int
	writes  int
	events  []string
}

func (v *backloggedQcowVolume) Unpublished() []disk.SealedLayer { return v.pending }
func (v *backloggedQcowVolume) Depth() int                      { return v.depth }
func (v *backloggedQcowVolume) Compact(context.Context) error {
	v.events = append(v.events, "compact")
	if len(v.pending) == 0 {
		v.depth = 2
	}
	return nil
}
func (v *backloggedQcowVolume) Seal(_ context.Context, force bool) ([]disk.SealedLayer, bool, error) {
	v.events = append(v.events, "seal")
	if v.depth >= disk.DefaultMaxChainDepth {
		return nil, false, disk.ErrMaxChainDepth
	}
	if !force && v.writes == 0 {
		return nil, true, nil
	}
	v.pending = append(v.pending, disk.SealedLayer{Path: fmt.Sprintf("head-%d", v.writes)})
	v.depth++
	v.writes = 0
	return v.pending, false, nil
}
func (v *backloggedQcowVolume) publish(layers []disk.SealedLayer) error {
	for _, layer := range layers {
		v.events = append(v.events, "publish "+layer.Path)
	}
	v.pending = nil
	return nil
}

func TestQcowSnapshotDrainsFullBacklogBeforeCapturingCurrentWrites(t *testing.T) {
	for _, mode := range []durableDiskSyncMode{durableDiskSyncExplicit, durableDiskSyncFinal, durableDiskSyncFailed} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			volume := &backloggedQcowVolume{pending: []disk.SealedLayer{{Path: "older"}, {Path: "newer"}}, depth: disk.DefaultMaxChainDepth, writes: 1}
			publish := func(layers []disk.SealedLayer) error {
				// Writes continue while the old sealed layers upload.
				if layers[0].Path == "older" {
					volume.writes++
				}
				return volume.publish(layers)
			}
			require.NoError(t, snapshotQcowVolume(context.Background(), volume, mode, false, publish))
			require.Equal(t, []string{"publish older", "publish newer", "compact", "seal", "publish head-2"}, volume.events)
			require.Empty(t, volume.pending)
		})
	}
}

func TestQcowSnapshotRetriesPublicationWithoutAnotherPivot(t *testing.T) {
	volume := &backloggedQcowVolume{pending: []disk.SealedLayer{{Path: "sealed"}}, depth: disk.DefaultMaxChainDepth, writes: 1}
	unavailable := errors.New("object storage unavailable")
	require.ErrorIs(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, false, func([]disk.SealedLayer) error { return unavailable }), unavailable)
	require.Empty(t, volume.events, "failed publication must leave both the backlog and current head intact")
	require.Len(t, volume.pending, 1)
	require.Equal(t, 1, volume.writes)
	require.NoError(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, false, volume.publish))
	require.Equal(t, []string{"publish sealed", "compact", "seal", "publish head-1"}, volume.events)
}

func TestQcowJournalCheckpointPublishesBacklogWithoutAnotherSeal(t *testing.T) {
	volume := &backloggedQcowVolume{pending: []disk.SealedLayer{{Path: "sealed"}}, depth: disk.DefaultMaxChainDepth, writes: 1}
	require.NoError(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncCheckpoint, false, volume.publish))
	require.Equal(t, []string{"publish sealed", "compact"}, volume.events)
	require.Equal(t, 1, volume.writes, "the journal retains writes beyond its published checkpoint")
}

func TestQcowSnapshotWithoutBacklogKeepsNormalSealBehavior(t *testing.T) {
	for _, tc := range []struct {
		writes int
		force  bool
		events []string
	}{
		{writes: 1, events: []string{"seal", "publish head-1"}},
		{events: []string{"seal"}},
		{force: true, events: []string{"seal", "publish head-0"}},
	} {
		volume := &backloggedQcowVolume{depth: 2, writes: tc.writes}
		require.NoError(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, tc.force, volume.publish))
		require.Equal(t, tc.events, volume.events)
	}
}

func TestQcowSnapshotCompactsPublishedChainBeforeRetryingSeal(t *testing.T) {
	volume := &backloggedQcowVolume{depth: disk.DefaultMaxChainDepth, writes: 1}
	require.NoError(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, false, volume.publish))
	require.Equal(t, []string{"seal", "compact", "seal", "publish head-1"}, volume.events)
}

func TestQcowSnapshotDoesNotForceAnEmptyHeadAfterFirstPublication(t *testing.T) {
	volume := &backloggedQcowVolume{pending: []disk.SealedLayer{{Path: "first"}}, depth: 2}
	require.NoError(t, snapshotQcowVolume(context.Background(), volume, durableDiskSyncExplicit, true, volume.publish))
	require.Equal(t, []string{"publish first", "seal"}, volume.events)
}
