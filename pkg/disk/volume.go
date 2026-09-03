package disk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Volume is one attached qcow2-backed disk: a backing chain of immutable
// layers, a writable head, one qemu-storage-daemon, and one NBD device
// mounted as ext4 at Mountpoint.
type Volume struct {
	manager *Manager
	mu      sync.Mutex

	dir     string
	state   *volumeState
	qsd     *qsdProcess
	nbd     *nbdDevice
	fmtNode string

	// freshHead is true when the current head file was created within this
	// daemon session, which is what makes a zero write-offset on its file
	// node a safe "nothing changed" signal. Reused heads and adopted volumes
	// may hold data written before the daemon's statistics started counting,
	// so their first seal is never skipped.
	freshHead bool
}

// AttachSpec describes a volume attachment.
type AttachSpec struct {
	// Key is a stable, path-safe identifier. Attachments with the same key
	// reuse locally cached layers across container restarts.
	Key              string
	VirtualSizeBytes int64
	ReadOnly         bool
	// Mountpoint is the host directory where the ext4 filesystem is mounted.
	Mountpoint string
	// Chain is the published backing chain to materialize, base first. Empty
	// means a fresh formatted disk (or reuse of whatever exists locally).
	Chain []ChainLayer
}

// ChainLayer is one published generation of a volume.
type ChainLayer struct {
	SnapshotID string
	Layer      *types.DiskSnapshotFile
}

// SealedLayer is an immutable local layer that has not been published yet.
type SealedLayer struct {
	Path string
	// ParentSnapshotID is the published generation this layer builds on. It is
	// empty when the parent is itself unpublished (the adapter must publish
	// sealed layers in order, so this only ever refers to the previous seal).
	ParentSnapshotID string
}

func (v *Volume) Mountpoint() string { return v.state.Mountpoint }
func (v *Volume) Depth() int         { return v.state.depth() }
func (v *Volume) ReadOnly() bool     { return v.state.ReadOnly }

// attach materializes the chain and brings the volume online. Called with the
// manager registration already reserved for this key.
func (m *Manager) attach(ctx context.Context, spec AttachSpec, source ChunkSource) (*Volume, error) {
	dir := m.volumeDir(spec.Key)
	layersDir := filepath.Join(dir, layersSubdir)
	if err := os.MkdirAll(layersDir, 0o700); err != nil {
		return nil, err
	}

	state, err := loadVolumeState(dir)
	if err != nil {
		return nil, err
	}
	if state != nil && state.Attached {
		// Recovery marks crashed volumes detached at startup, so a state that
		// still claims attachment belongs to a live volume.
		return nil, fmt.Errorf("volume %s is already attached", spec.Key)
	}

	freshHead := false
	if !reusableState(state, spec) {
		if state != nil {
			log.Info().Str("volume", spec.Key).Msg("discarding stale local volume state")
		}
		if err := os.RemoveAll(layersDir); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(layersDir, 0o700); err != nil {
			return nil, err
		}
		state, err = m.materializeChain(ctx, spec, layersDir, source)
		if err != nil {
			return nil, err
		}
		freshHead = true
	} else {
		log.Info().Str("volume", spec.Key).Int("layers", state.depth()).Msg("reusing local volume state")
	}
	state.Mountpoint = spec.Mountpoint
	state.ReadOnly = spec.ReadOnly
	state.VirtualSizeBytes = spec.VirtualSizeBytes

	volume := &Volume{manager: m, dir: dir, state: state, freshHead: freshHead}
	if err := volume.start(ctx); err != nil {
		return nil, err
	}
	return volume, nil
}

// reusableState reports whether the local layer stack can serve the requested
// chain without refetching. Local state is reusable when it already contains
// the newest requested generation: it is then equal to or ahead of the remote
// chain (pending seals and head writes that were never published).
func reusableState(state *volumeState, spec AttachSpec) bool {
	if state == nil || spec.ReadOnly {
		return false
	}
	if len(spec.Chain) == 0 {
		// No remote generations exist. Local state is only reusable if it never
		// published anything either; otherwise the remote rows were deleted and
		// the local stack is stale.
		return len(state.Chain) == 0 && state.HeadPath != "" && fileExists(state.HeadPath) && layersExist(state)
	}
	latest := spec.Chain[len(spec.Chain)-1].SnapshotID
	if latest == "" || !state.publishedIDs()[latest] {
		return false
	}
	return state.HeadPath != "" && fileExists(state.HeadPath) && layersExist(state)
}

func layersExist(state *volumeState) bool {
	for _, layer := range append(append([]stateLayer{}, state.Chain...), state.Pending...) {
		if !fileExists(layer.Path) {
			return false
		}
	}
	return true
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// materializeChain downloads every published layer and links them into a local
// backing chain, then creates the writable head (or a fresh formatted base).
func (m *Manager) materializeChain(ctx context.Context, spec AttachSpec, layersDir string, source ChunkSource) (*volumeState, error) {
	state := &volumeState{Key: spec.Key, VirtualSizeBytes: spec.VirtualSizeBytes, ReadOnly: spec.ReadOnly}

	// Layers are independent objects; fetch them in parallel and link the
	// chain afterwards. Chunk parallelism is a single budget shared by every
	// layer, so restore speed does not depend on how the data is split.
	localPaths := make([]string, len(spec.Chain))
	gate := newChunkGate(chunkFetchConcurrency)
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(layerFetchConcurrency)
	for i, chainLayer := range spec.Chain {
		if chainLayer.Layer == nil || chainLayer.SnapshotID == "" {
			return nil, fmt.Errorf("chain layer %d for volume %s is incomplete", i, spec.Key)
		}
		localPaths[i] = filepath.Join(layersDir, fmt.Sprintf("%03d-%s.qcow2", i, chainLayer.SnapshotID))
		group.Go(func() error {
			if err := fetchLayer(groupCtx, source, chainLayer.Layer, localPaths[i], gate); err != nil {
				return fmt.Errorf("fetch layer %s: %w", chainLayer.SnapshotID, err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	previousPath := ""
	for i, chainLayer := range spec.Chain {
		if i > 0 {
			// Downloaded layers carry the origin host's backing path; repoint
			// them at the local copy of their parent.
			if err := m.rebaseQcow(ctx, localPaths[i], previousPath); err != nil {
				return nil, err
			}
		}
		state.Chain = append(state.Chain, stateLayer{SnapshotID: chainLayer.SnapshotID, Path: localPaths[i]})
		previousPath = localPaths[i]
	}

	if spec.ReadOnly {
		if previousPath == "" {
			return nil, fmt.Errorf("read-only volume %s requires at least one published layer", spec.Key)
		}
		return state, nil
	}

	headPath := headLayerPath(layersDir, state.PivotCount)
	if previousPath == "" {
		if err := m.createQcowBase(ctx, headPath, spec.VirtualSizeBytes); err != nil {
			return nil, err
		}
	} else if err := m.createQcowOverlay(ctx, headPath, previousPath, spec.VirtualSizeBytes); err != nil {
		return nil, err
	}
	state.HeadPath = headPath
	return state, nil
}

// start launches the daemon, connects the NBD device, formats fresh disks,
// and mounts the filesystem.
func (v *Volume) start(ctx context.Context) error {
	m := v.manager
	state := v.state
	freshDisk := !state.ReadOnly && len(state.Chain) == 0 && !state.Formatted

	openPath := state.HeadPath
	if state.ReadOnly {
		openPath = state.lastLayerPath()
	}
	v.fmtNode = fmtNodeName(state.PivotCount)

	// Step timings surface in the attach log so a slow attach can be blamed
	// on the daemon, the kernel, mkfs, or the mount without a profiler.
	stepStart := time.Now()
	step := func(name string, event *zerolog.Event) {
		event.Dur(name, time.Since(stepStart))
		stepStart = time.Now()
	}
	attachLog := log.Info().Str("volume", state.Key).Bool("fresh", freshDisk).Int("layers", len(state.Chain))

	qsd, err := m.startQSD(ctx, m.runtimeDir(state.Key), openPath, v.fmtNode, state.ReadOnly)
	if err != nil {
		return err
	}
	step("qsd", attachLog)
	cleanupOnError := func() {
		if v.nbd != nil {
			_ = m.disconnectNBDDevice(context.Background(), v.nbd)
			v.nbd = nil
		}
		_ = m.stopQSD(context.Background(), qsd)
	}

	nbd, err := m.acquireNBDDevice(ctx, qsd.nbdSocket, state.VirtualSizeBytes)
	if err != nil {
		cleanupOnError()
		return err
	}
	v.nbd = nbd
	step("nbd", attachLog)

	if freshDisk {
		if err := m.formatExt4(ctx, nbd.Path); err != nil {
			cleanupOnError()
			return err
		}
		state.Formatted = true
		step("mkfs", attachLog)
	}
	if err := m.mountExt4(ctx, nbd.Path, state.Mountpoint, state.ReadOnly); err != nil {
		cleanupOnError()
		return err
	}
	step("mount", attachLog)
	attachLog.Msg("qcow volume attached")

	v.qsd = qsd
	state.Attached = true
	state.QSDPid = qsd.pid
	state.QMPSocket = qsd.qmpSocket
	state.NBDSocket = qsd.nbdSocket
	state.NBDDevice = nbd.Path
	if err := saveVolumeState(v.dir, state); err != nil {
		_ = m.unmount(context.Background(), state.Mountpoint)
		cleanupOnError()
		return err
	}
	return nil
}

// Seal pivots the writable head onto a new empty overlay and returns every
// sealed-but-unpublished layer, oldest first. The caller must publish them in
// order and confirm each with MarkPublished. When force is false and nothing
// was written since the last pivot (and nothing is pending), Seal skips the
// pivot and returns skipped=true.
func (v *Volume) Seal(ctx context.Context, force bool) ([]SealedLayer, bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	state := v.state
	if !state.Attached || state.ReadOnly {
		return nil, false, fmt.Errorf("volume %s is not attached writable", state.Key)
	}

	client, err := dialQMP(ctx, state.QMPSocket)
	if err != nil {
		return nil, false, err
	}
	defer client.Close()

	if !force && len(state.Pending) == 0 && v.freshHead {
		written, err := client.writtenBytes(ctx, qsdFileNodePrefix+v.fmtNode)
		if err == nil && written == 0 {
			return nil, true, nil
		}
	}

	if state.depth() >= v.manager.maxChainDepth {
		return nil, false, fmt.Errorf("volume %s reached the maximum chain depth of %d; compaction is failing or falling behind", state.Key, v.manager.maxChainDepth)
	}

	// Pre-create the empty overlay, then record the intent before asking the
	// daemon to pivot: after a crash the state file must never point at a
	// layer that other layers build on.
	sealedPath := state.HeadPath
	newPivot := state.PivotCount + 1
	newHeadPath := headLayerPath(filepath.Join(v.dir, layersSubdir), newPivot)
	newNode := fmtNodeName(newPivot)
	if err := v.manager.createQcowOverlay(ctx, newHeadPath, sealedPath, state.VirtualSizeBytes); err != nil {
		return nil, false, err
	}

	previousState := *state
	state.Pending = append(state.Pending, stateLayer{Path: sealedPath})
	state.HeadPath = newHeadPath
	state.PivotCount = newPivot
	if err := saveVolumeState(v.dir, state); err != nil {
		*state = previousState
		os.Remove(newHeadPath)
		return nil, false, err
	}

	if err := client.addOverlay(ctx, newNode, qsdFileNodePrefix+newNode, newHeadPath); err != nil {
		v.rollbackSeal(previousState, newHeadPath)
		return nil, false, fmt.Errorf("add overlay for volume %s: %w", state.Key, err)
	}
	thaw, err := v.manager.freezeFS(ctx, state.Mountpoint)
	if err != nil {
		_ = client.removeNode(ctx, newNode)
		v.rollbackSeal(previousState, newHeadPath)
		return nil, false, err
	}
	pivotErr := client.pivot(ctx, v.fmtNode, newNode)
	thaw()

	if pivotErr != nil {
		// A lost reply is indeterminate: ask the daemon whether the overlay
		// got wired into the chain before deciding to roll back.
		if committed := v.pivotCommitted(ctx, newNode); !committed {
			_ = client.removeNode(ctx, newNode)
			v.rollbackSeal(previousState, newHeadPath)
			return nil, false, fmt.Errorf("pivot volume %s: %w", state.Key, pivotErr)
		}
	}
	v.fmtNode = newNode
	v.freshHead = true

	sealed := make([]SealedLayer, 0, len(state.Pending))
	parentID := ""
	if n := len(state.Chain); n > 0 {
		parentID = state.Chain[n-1].SnapshotID
	}
	for i, layer := range state.Pending {
		if i > 0 {
			parentID = ""
		}
		sealed = append(sealed, SealedLayer{Path: layer.Path, ParentSnapshotID: parentID})
	}
	return sealed, false, nil
}

func (v *Volume) rollbackSeal(previous volumeState, newHeadPath string) {
	*v.state = previous
	if err := saveVolumeState(v.dir, v.state); err != nil {
		log.Error().Str("volume", v.state.Key).Err(err).Msg("failed to roll back seal state")
		return
	}
	os.Remove(newHeadPath)
}

// pivotCommitted reports whether the overlay actually became the active
// head. Merely existing is not enough: addOverlay creates it with no backing,
// and only a committed pivot wires the old head underneath it.
func (v *Volume) pivotCommitted(ctx context.Context, newNode string) bool {
	client, err := dialQMP(ctx, v.state.QMPSocket)
	if err != nil {
		return false
	}
	defer client.Close()
	nodes, err := client.namedBlockNodes(ctx)
	if err != nil {
		return false
	}
	node, ok := nodes[newNode]
	return ok && node.BackingFileDepth > 0
}

// reconcileHeadNode aligns adopted state with the daemon's actual graph. A
// crash between recording a seal intent and committing the pivot leaves the
// state one pivot ahead of the daemon; the intent is rolled back so writes
// continue landing in the layer the daemon is actually using. The overlay
// node may already exist without being wired in (crash between blockdev-add
// and the pivot transaction), which counts as uncommitted.
func (v *Volume) reconcileHeadNode(ctx context.Context) error {
	client, err := dialQMP(ctx, v.state.QMPSocket)
	if err != nil {
		return err
	}
	defer client.Close()
	nodes, err := client.namedBlockNodes(ctx)
	if err != nil {
		return err
	}
	if node, ok := nodes[v.fmtNode]; ok {
		// A pivot intent (Pending non-empty) is only committed once the
		// overlay has a backing chain; a base head never has one.
		if len(v.state.Pending) == 0 || node.BackingFileDepth > 0 {
			return nil
		}
		_ = client.removeNode(ctx, v.fmtNode)
	}

	previousNode := fmtNodeName(v.state.PivotCount - 1)
	if _, ok := nodes[previousNode]; !ok || len(v.state.Pending) == 0 {
		return fmt.Errorf("daemon graph has neither %s nor %s", v.fmtNode, previousNode)
	}
	orphanHead := v.state.HeadPath
	last := len(v.state.Pending) - 1
	v.state.HeadPath = v.state.Pending[last].Path
	v.state.Pending = v.state.Pending[:last]
	v.state.PivotCount--
	v.fmtNode = previousNode
	if err := saveVolumeState(v.dir, v.state); err != nil {
		return err
	}
	os.Remove(orphanHead)
	log.Warn().Str("volume", v.state.Key).Msg("rolled back uncommitted pivot intent during adoption")
	return nil
}

// MarkPublished records that a sealed layer was durably published as the
// given snapshot, moving it from the pending list into the chain.
func (v *Volume) MarkPublished(sealedPath, snapshotID string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.state.Pending) == 0 || v.state.Pending[0].Path != sealedPath {
		return fmt.Errorf("sealed layer %s is not the oldest pending layer of volume %s", sealedPath, v.state.Key)
	}
	v.state.Chain = append(v.state.Chain, stateLayer{SnapshotID: snapshotID, Path: sealedPath})
	v.state.Pending = v.state.Pending[1:]
	return saveVolumeState(v.dir, v.state)
}

// Flatten collapses a sealed layer and its whole backing chain into one
// parentless image at destPath, used to bound published chain depth.
func (v *Volume) Flatten(ctx context.Context, sealedPath, destPath string) error {
	return v.manager.flattenQcow(ctx, sealedPath, destPath)
}

// Compact folds every published layer into the base image with a live
// intermediate block-commit; the daemon keeps serving I/O. Pending layers are
// deltas the publisher still needs byte-for-byte, so compaction waits until
// the whole chain is published.
func (v *Volume) Compact(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	state := v.state
	if !state.Attached || state.ReadOnly || len(state.Pending) > 0 || len(state.Chain) < 2 {
		return nil
	}
	base, top := state.Chain[0], state.Chain[len(state.Chain)-1]

	client, err := dialQMP(ctx, state.QMPSocket)
	if err != nil {
		return err
	}
	defer client.Close()

	// The live graph decides what needs merging: a crash after a completed
	// commit but before the state save leaves the files already merged.
	live, err := client.backingFilenames(ctx, v.fmtNode)
	if err != nil {
		return err
	}
	if live[top.Path] {
		if err := client.commitChain(ctx, v.fmtNode, top.Path, base.Path); err != nil {
			return fmt.Errorf("compact volume %s: %w", state.Key, err)
		}
	} else if !live[base.Path] {
		return fmt.Errorf("compact volume %s: neither %s nor %s is in the live chain", state.Key, top.Path, base.Path)
	}

	// The base file now holds the newest published generation in full. A
	// partially committed base stays invisible behind the intact overlays, so
	// merged files are deleted only after the collapsed state is durable.
	merged := state.Chain[1:]
	state.Chain = []stateLayer{{SnapshotID: top.SnapshotID, Path: base.Path}}
	if err := saveVolumeState(v.dir, state); err != nil {
		return err
	}
	for _, layer := range merged {
		os.Remove(layer.Path)
	}
	log.Info().Str("volume", state.Key).Int("layers", len(merged)+1).Msg("compacted qcow backing chain")
	return nil
}

// detach unmounts, disconnects, and stops the daemon. Layer files and state
// stay behind for reuse by the next attachment.
func (v *Volume) detach(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.state.Attached {
		return nil
	}

	if err := v.manager.unmount(ctx, v.state.Mountpoint); err != nil {
		return err
	}
	var disconnectErr error
	if v.nbd != nil {
		if err := v.manager.disconnectNBDDevice(ctx, v.nbd); err != nil {
			disconnectErr = err
		} else {
			v.nbd = nil
		}
	}
	stopErr := v.manager.stopQSD(ctx, v.qsd)
	if stopErr == nil {
		v.qsd = nil
	}
	// A dead daemon closes its NBD socket. If that cleared a failed
	// disconnect, allow the state to be finalized instead of leaking it.
	if v.nbd != nil && !v.manager.nbdDeviceBusy(v.nbd.name) {
		v.nbd = nil
		disconnectErr = nil
	}
	if err := errors.Join(disconnectErr, stopErr); err != nil {
		return err
	}

	v.state.Attached = false
	v.state.QSDPid = 0
	v.state.QMPSocket = ""
	v.state.NBDSocket = ""
	v.state.NBDDevice = ""
	if err := saveVolumeState(v.dir, v.state); err != nil {
		return err
	}
	return os.RemoveAll(v.manager.runtimeDir(v.state.Key))
}
