package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// A qcow snapshot publishes only the layer it sealed. Once the chain is deeper
// than qcowMergeDepth, a background merge commits the newest layers into a
// layer below them, publishes that layer as one generation, and republishes
// whatever was published on top of the old chain meanwhile.
const qcowMergeDepth = disk.DefaultFlattenDepth

var errQcowMergeStopped = errors.New("qcow disk is detaching")

type qcowMerge struct{ stop context.CancelCauseFunc }

// scheduleQcowMerge merges a volume's layers in the background unless a merge
// is already running for it or its volume is detaching.
func (s *Worker) scheduleQcowMerge(request *types.ContainerRequest, mount *types.Mount, key string) {
	ctx, stop := context.WithCancelCause(s.durableDiskContext(nil))
	merge := &qcowMerge{stop: stop}
	if _, running := s.qcowMerges.LoadOrStore(key, merge); running {
		stop(nil)
		return
	}
	go func() {
		defer s.qcowMerges.CompareAndDelete(key, merge)
		defer stop(nil)
		ctx, stopWatchdog := withDurableDiskInactivityWatchdog(ctx, durableDiskSnapshotInactivityTimeout)
		defer stopWatchdog()
		if err := s.mergeQcowLayers(ctx, request, mount, key); err != nil && !errors.Is(context.Cause(ctx), errQcowMergeStopped) {
			log.Warn().Err(err).Str("container_id", request.ContainerId).Str("disk", mount.DurableDisk.Name).
				Msg("failed to merge qcow disk layers")
		}
	}()
}

// stopQcowMerge cancels a volume's merge, which would hold the disk lock its
// final snapshot needs, and keeps another from starting until resume runs.
func (s *Worker) stopQcowMerge(key string) (resume func()) {
	detaching := &qcowMerge{stop: func(error) {}}
	if merge, ok := s.qcowMerges.Swap(key, detaching); ok {
		merge.(*qcowMerge).stop(errQcowMergeStopped)
	}
	return func() { s.qcowMerges.CompareAndDelete(key, detaching) }
}

// mergeQcowLayers merges the newest layers, unless an earlier merge is still
// unpublished, and publishes the result. The disk lock is held to commit and
// to publish but not for the upload in between.
func (s *Worker) mergeQcowLayers(ctx context.Context, request *types.ContainerRequest, mount *types.Mount, key string) error {
	var merged disk.PublishedLayer
	err := withDurableDiskLock(ctx, mount, func() (err error) {
		volume, ok := s.diskManager.VolumeOwned(key, request.ContainerId)
		if !ok {
			return nil
		}
		layers, pending := volume.Published()
		chain := s.qcowChain(key)
		i, ok := qcowMergedLayer(layers, chain)
		if pending || !ok {
			return nil
		}
		if i >= 0 {
			merged = layers[i]
		} else if base, ok := qcowMergeBase(chain); ok {
			merged, err = volume.Merge(ctx, base)
		}
		return err
	})
	if err != nil || merged.Path == "" {
		return err
	}

	store, err := newDurableDiskSnapshotWriteStore(ctx, request)
	if err != nil {
		return err
	}
	layer, err := s.uploadQcowLayer(ctx, mount, store, key, merged.Path)
	if err != nil {
		return err
	}
	return withDurableDiskLock(ctx, mount, func() error {
		volume, ok := s.diskManager.VolumeOwned(key, request.ContainerId)
		if !ok {
			return nil
		}
		return s.publishQcowMerge(ctx, request, mount, volume, store, key, merged, layer)
	})
}

type qcowMergeVolume interface {
	Published() ([]disk.PublishedLayer, bool)
	Republished(map[string]string) error
	Check() error
}

// publishQcowMerge publishes the merged layer as one generation, then a copy
// of each generation above it, re-parented onto the copy below. The copies
// keep their generations except the last, which outranks the disk's head, so
// readers move to the new chain at once. The old rows stay valid for memory
// checkpoints paired with them.
func (s *Worker) publishQcowMerge(ctx context.Context, request *types.ContainerRequest, mount *types.Mount, volume qcowMergeVolume, store *durableDiskSnapshotBucketStore, key string, merged disk.PublishedLayer, layer *types.DiskSnapshotFile) (err error) {
	layers, _ := volume.Published()
	chain := s.qcowChain(key)
	first, ok := qcowMergedLayer(layers, chain)
	if !ok || first < 0 || layers[first] != merged {
		// A compaction or a flattened publish replaced what was uploaded.
		return nil
	}
	latest, err := s.latestQcowSnapshotRow(ctx, request, mount)
	if err != nil {
		return err
	}
	if latest == nil || latest.ExternalId != chain[len(chain)-1].row.ExternalId {
		return fmt.Errorf("disk %q has a generation this worker did not publish", mount.DurableDisk.Name)
	}

	parentID := ""
	if first > 0 {
		parentID = chain[first-1].row.ExternalId
	}
	republished := slices.Clone(chain[:first])
	defer func() {
		// A retry creates its own rows, and the last may already outrank the
		// disk's head.
		if err != nil {
			for _, entry := range slices.Backward(republished[first:]) {
				s.withdrawQcowSnapshot(ctx, request, entry.row, err)
			}
		}
	}()
	renamed := make(map[string]string)
	replaced := chain[first+len(chain)-len(layers):]
	for i, entry := range replaced {
		file, generation := layer, entry.row.Generation
		if i > 0 {
			if file, err = qcowManifestLayer(entry.manifest); err != nil {
				return err
			}
		}
		if i == len(replaced)-1 {
			if generation, err = nextDurableDiskSnapshotGeneration(time.Now().UnixNano(), latest); err != nil {
				return err
			}
		}
		row, manifest, err := s.createQcowGeneration(ctx, request, mount, store, file, parentID, generation, entry.manifest.LogicalSizeBytes, volume.Check)
		if err != nil {
			return err
		}
		republished = append(republished, qcowChainEntry{row: row, manifest: manifest})
		renamed[entry.row.ExternalId] = row.ExternalId
		parentID = row.ExternalId
	}
	if err := volume.Republished(renamed); err != nil {
		return err
	}
	s.qcowChains.Store(key, republished)
	s.reportQcowChainContent(request, republished)
	log.Info().Str("container_id", request.ContainerId).Str("disk", mount.DurableDisk.Name).Int("depth", len(republished)).
		Int64("bytes", disk.StoredBytes(layer)).Msg("published merged qcow layers")
	return nil
}

// qcowMergedLayer returns the local layer a merge left unpublished, or -1, and
// whether the local layers match the published chain at all. A merged layer
// carries the ID of the newest generation it holds, so it is the first whose
// ID is not its own generation's; each layer above it holds one generation.
func qcowMergedLayer(layers []disk.PublishedLayer, chain []qcowChainEntry) (int, bool) {
	extra := len(chain) - len(layers)
	if len(layers) == 0 || extra < 0 {
		return -1, false
	}
	merged := -1
	for i, layer := range layers {
		if merged < 0 && layer.SnapshotID != chain[i].row.ExternalId {
			merged = i
		}
		if merged >= 0 && layer.SnapshotID != chain[i+extra].row.ExternalId {
			return -1, false
		}
	}
	return merged, (merged >= 0) == (extra > 0)
}

// qcowMergeBase picks the layer to merge the newest ones into: the nearest
// that is at least as large as each of them. Data then only moves into larger
// layers, so the large layers a copy shares with its template stay as they are.
func qcowMergeBase(chain []qcowChainEntry) (int, bool) {
	n := len(chain)
	if n <= qcowMergeDepth {
		return 0, false
	}
	largest := chain[n-1].row.StoredSizeBytes
	for base := n - 2; base >= 0; base-- {
		size := chain[base].row.StoredSizeBytes
		if size >= largest {
			return base, true
		}
		largest = max(largest, size)
	}
	return 0, false
}
