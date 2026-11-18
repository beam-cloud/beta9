package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/beam-cloud/beta9/pkg/types"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

// A root layer is a generation of a workspace's durable disk under a VM's
// image (types.RootLayer). This worker attaches a generation once, mounts
// its filesystem read-only on the host, and gives the directory to every VM
// that asks for the disk, as a lower directory of the overlay their roots
// are made of.
//
// A VM never waits for a layer. A generation this worker does not have is
// attached in the background, and until it is there a VM gets the newest one
// that is, or starts without. What a layer holds is addressed by its content,
// so an older generation is a smaller cache and never a wrong one. A worker
// only moves to a newer generation: VMs that were told of an older one, and
// start late, get the one it has.
//
// The disk is written here too, and only here: when a VM that may publish
// exits, what its workload added to the layer's directories goes into a new
// generation (publishRootLayer). The host mounts these filesystems, so no
// guest ever has the disk as a block device.

const (
	// rootLayerIdle is how long a generation no VM uses stays attached.
	rootLayerIdle = 30 * time.Minute
	// rootLayerAttachTimeout bounds fetching and mounting a generation.
	rootLayerAttachTimeout = 15 * time.Minute
	// rootLayerPublishTimeout bounds adding a VM's export to the disk.
	rootLayerPublishTimeout = 20 * time.Minute
	// rootLayerMinFreeBytes is the room a layer's filesystem keeps. A
	// publish that would leave less is dropped: the gateway replaces a disk
	// on a schedule, which is what makes room again.
	rootLayerMinFreeBytes = 1 << 30

	// rootLayerMaxExportBytes is the size of the filesystem a guest exports
	// into, and the largest export a publish reads.
	rootLayerMaxExportBytes = 16 << 30

	rootLayersDir       = ".root-layers"
	rootLayerExportsDir = ".root-layer-exports"
)

type rootLayers struct {
	worker *Worker

	mu sync.Mutex
	// disks are the layer disks this worker has a generation of, by
	// workspace and disk name.
	disks map[string]*rootLayerDisk
	// attached reports whether the disk manager still has a volume.
	attached func(key string) bool
}

type rootLayerDisk struct {
	// ready is the newest attached generation, attaching the one on its way.
	ready     *rootLayerMount
	attaching string
	// retired generations still have a VM on them.
	retired []*rootLayerMount
	// older are generations found to be older than the one that is ready.
	older map[string]struct{}
	// publish holds one publish to the disk at a time on this worker, and
	// publishers is how many hold or wait for it: until there are none, the
	// entry stays, and with it the lock.
	publish    sync.Mutex
	publishers int
}

type rootLayerMount struct {
	snapshotID string
	generation int64
	key        string
	dir        string
	refs       int
	usedAt     time.Time
}

func newRootLayers(worker *Worker) *rootLayers {
	return &rootLayers{worker: worker, disks: map[string]*rootLayerDisk{}, attached: func(key string) bool {
		_, attached := worker.diskManager.Volume(key)
		return attached
	}}
}

// requestRootLayer is the layer a request asks for, if its runtime can have
// one: a guest whose root is an overlay over the host's.
func (s *Worker) requestRootLayer(request *types.ContainerRequest) *types.RootLayer {
	if s == nil || s.rootLayers == nil || s.diskManager == nil || request == nil || !s.runtimeOwnsBlockRoot() {
		return nil
	}
	config := requestStubConfig(request)
	if config == nil || config.RootLayer == nil || !strings.HasPrefix(config.RootLayer.DiskName, types.RootLayerDiskPrefix) {
		return nil
	}
	// A root disk that is restored would come back over whichever generation
	// its next worker has, or none, and lose the files it had from this one.
	if root := qcowRootDiskMount(request); root == nil || !root.DurableDisk.SkipExitSnapshot {
		return nil
	}
	return config.RootLayer
}

func rootLayerDiskKey(request *types.ContainerRequest, layer *types.RootLayer) string {
	return cacheRequestWorkspaceID(request) + "/" + layer.DiskName
}

// rootLayerMountFor describes the layer disk to the durable disk code, which
// works on a request's mounts.
func rootLayerMountFor(layer *types.RootLayer, dir string, readOnly bool) *types.Mount {
	return &types.Mount{
		LocalPath: dir,
		ReadOnly:  readOnly,
		MountType: types.StorageModeDurableDisk,
		DurableDisk: &types.DurableDiskMountConfig{
			Name:       layer.DiskName,
			Size:       layer.Size,
			Filesystem: types.DiskFilesystemExt4,
			Driver:     types.DurableDiskDriverQcow,
		},
	}
}

// acquire returns the directory of the newest generation of the request's
// layer that this worker has attached, and what releases it. The directory is
// empty when there is none yet. It never waits: a generation that is not
// here is attached in the background for the VMs after this one.
func (l *rootLayers) acquire(request *types.ContainerRequest) (string, func()) {
	none := func() {}
	if l == nil {
		return "", none
	}
	layer := l.worker.requestRootLayer(request)
	if layer == nil || layer.SnapshotID == "" {
		return "", none
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	key := rootLayerDiskKey(request, layer)
	entry := l.disks[key]
	if entry == nil {
		entry = &rootLayerDisk{}
		l.disks[key] = entry
	}
	// An idle worker detaches its volumes, and a restarted one starts
	// without them. A generation that went that way is not there to use.
	if entry.ready != nil && !l.attached(entry.ready.key) {
		entry.ready = nil
	}
	if entry.ready == nil {
		// Older than a generation that is gone: any generation is better
		// than none.
		entry.older = nil
	}
	_, older := entry.older[layer.SnapshotID]
	if (entry.ready == nil || entry.ready.snapshotID != layer.SnapshotID) && entry.attaching == "" && !older {
		entry.attaching = layer.SnapshotID
		// The request is copied while its container is alive: the copy
		// carries the workspace's storage credentials, which a worker gets
		// for a container it runs.
		copied := *request
		copied.Mounts = nil
		go l.attach(key, entry, &copied, *layer)
	}
	mount := entry.ready
	if mount == nil {
		return "", none
	}
	mount.refs++
	mount.usedAt = time.Now()
	var once sync.Once
	return mount.dir, func() { once.Do(func() { l.release(entry, mount) }) }
}

func (l *rootLayers) release(entry *rootLayerDisk, mount *rootLayerMount) {
	l.mu.Lock()
	mount.refs--
	mount.usedAt = time.Now()
	retired := mount.refs == 0 && entry.ready != mount
	if retired {
		for i, old := range entry.retired {
			if old == mount {
				entry.retired = append(entry.retired[:i], entry.retired[i+1:]...)
				break
			}
		}
	}
	l.mu.Unlock()
	if retired {
		l.detach(mount)
	}
}

func (l *rootLayers) detach(mount *rootLayerMount) {
	ctx, cancel := context.WithTimeout(l.worker.durableDiskCleanupContext(), durableDiskCleanupGrace)
	defer cancel()
	if err := l.worker.diskManager.Detach(ctx, mount.key); err != nil {
		log.Warn().Err(err).Str("snapshot_id", mount.snapshotID).Msg("failed to detach a root layer")
		return
	}
	// A read-only volume's layers are a copy nothing else reads.
	if err := l.worker.diskManager.Remove(mount.key); err != nil {
		log.Warn().Err(err).Str("snapshot_id", mount.snapshotID).Msg("failed to remove a root layer's local copy")
	}
	_ = os.Remove(mount.dir)
}

// attach fetches one generation and mounts it, then makes it the one the
// disk's next VMs get.
func (l *rootLayers) attach(key string, entry *rootLayerDisk, request *types.ContainerRequest, layer types.RootLayer) {
	s := l.worker
	ctx, cancel := context.WithTimeout(s.durableDiskContext(s.ctx), rootLayerAttachTimeout)
	defer cancel()

	start := time.Now()
	mount, err := l.attachGeneration(ctx, entry, request, &layer)

	l.mu.Lock()
	entry.attaching = ""
	var superseded *rootLayerMount
	if errors.Is(err, errRootLayerOlder) {
		// Few generations are handed out at a time, so few are remembered.
		if entry.older == nil || len(entry.older) >= 16 {
			entry.older = map[string]struct{}{}
		}
		entry.older[layer.SnapshotID] = struct{}{}
		l.mu.Unlock()
		return
	}
	if err == nil {
		if old := entry.ready; old != nil && old != mount {
			if old.refs > 0 {
				entry.retired = append(entry.retired, old)
			} else {
				superseded = old
			}
		}
		entry.ready = mount
	}
	l.mu.Unlock()

	if err != nil {
		log.Warn().Err(err).Str("disk", layer.DiskName).Str("snapshot_id", layer.SnapshotID).
			Dur("took", time.Since(start)).Msg("failed to attach a root layer; VMs start without it")
		return
	}
	log.Info().Str("disk", layer.DiskName).Str("snapshot_id", layer.SnapshotID).Str("dir", mount.dir).
		Dur("took", time.Since(start)).Msg("root layer attached")
	if superseded != nil {
		l.detach(superseded)
	}
}

// errRootLayerOlder is a generation that is older than the one a worker has.
var errRootLayerOlder = errors.New("the worker has a newer generation of the root layer")

func (l *rootLayers) attachGeneration(ctx context.Context, entry *rootLayerDisk, request *types.ContainerRequest, layer *types.RootLayer) (*rootLayerMount, error) {
	s := l.worker
	head, err := s.diskSnapshotByID(ctx, request, layer.SnapshotID)
	if err != nil {
		return nil, fmt.Errorf("get root layer generation %s: %w", layer.SnapshotID, err)
	}
	if head == nil {
		return nil, fmt.Errorf("root layer generation %s is not known", layer.SnapshotID)
	}
	// Each generation is made from the one before it, so the numbers of the
	// ones VMs are given only go up.
	l.mu.Lock()
	ready := entry.ready
	l.mu.Unlock()
	if ready != nil && head.Generation <= ready.generation {
		return nil, errRootLayerOlder
	}

	volumeKey := fmt.Sprintf("%s-layer-%s", cacheRequestWorkspaceID(request), layer.SnapshotID)
	dir := filepath.Join(s.diskManager.Root(), rootLayersDir, volumeKey)
	mount := &rootLayerMount{snapshotID: layer.SnapshotID, generation: head.Generation, key: volumeKey, dir: dir, usedAt: time.Now()}

	// A worker that restarted has the volumes that were still mounted.
	if volume, attached := s.diskManager.Volume(volumeKey); attached {
		if volume.ReadOnly() && volume.Mountpoint() != "" {
			mount.dir = volume.Mountpoint()
			return mount, nil
		}
		return nil, fmt.Errorf("volume %s is attached as something else", volumeKey)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if _, err := s.attachRootLayerVolume(ctx, request, rootLayerMountFor(layer, dir, true), volumeKey, layer.SnapshotID, true); err != nil {
		return nil, err
	}
	return mount, nil
}

// attachRootLayerVolume attaches the layer disk at one generation, mounted on
// the host, and returns the chain it attached. An empty snapshotID is a disk
// with nothing published: a writable attach formats it.
//
// The durable disk attach of a container's mounts is not used here: under a
// VM runtime it hands the disk to the guest, and it attaches a disk's latest
// generation, which for a layer may be one the gateway never chose.
func (s *Worker) attachRootLayerVolume(ctx context.Context, request *types.ContainerRequest, mount *types.Mount, volumeKey, snapshotID string, readOnly bool) ([]qcowChainEntry, error) {
	sizeBytes, err := durableDiskSizeBytes(mount.DurableDisk.Size)
	if err != nil {
		return nil, fmt.Errorf("root layer %q requires a valid size: %w", mount.DurableDisk.Name, err)
	}
	if err := s.ensureDurableDiskSnapshotStorage(ctx, request); err != nil {
		return nil, err
	}

	var entries []qcowChainEntry
	var stores []durableDiskSnapshotStore
	if snapshotID != "" {
		head, err := s.diskSnapshotByID(ctx, request, snapshotID)
		if err != nil {
			return nil, fmt.Errorf("get root layer generation %s: %w", snapshotID, err)
		}
		if head == nil || head.DiskName != mount.DurableDisk.Name || head.ManifestKey == "" || head.Status != types.DiskSnapshotStatusAvailable {
			return nil, fmt.Errorf("root layer generation %s is not an available generation of %s", snapshotID, mount.DurableDisk.Name)
		}
		rows, err := s.resolveQcowSnapshotChain(ctx, request, mount, head)
		if err != nil {
			return nil, err
		}
		if stores, err = s.qcowChainStores(ctx, request, rows); err != nil {
			return nil, err
		}
		if entries, err = loadQcowChainManifests(ctx, s.durableDiskSnapshotCacheReader(), rows, stores); err != nil {
			return nil, err
		}
	}
	chain := make([]disk.ChainLayer, 0, len(entries))
	for _, entry := range entries {
		file, err := qcowManifestLayer(entry.manifest)
		if err != nil {
			return nil, fmt.Errorf("root layer generation %s: %w", entry.row.ExternalId, err)
		}
		chain = append(chain, disk.ChainLayer{SnapshotID: entry.row.ExternalId, Layer: file})
	}
	_, err = s.diskManager.Attach(ctx, disk.AttachSpec{
		Key:              volumeKey,
		VirtualSizeBytes: sizeBytes,
		ReadOnly:         readOnly,
		Mountpoint:       mount.LocalPath,
		Chain:            chain,
		Owner:            rootLayerOwner,
		Export:           disk.ExportNBD,
	}, &qcowChunkSource{cacheReader: s.durableDiskSnapshotCacheReader(), stores: stores})
	if err != nil {
		return nil, fmt.Errorf("attach root layer %q: %w", mount.DurableDisk.Name, err)
	}
	if readOnly {
		// Keeps the chain's chunks on the locality's cache hosts, where the
		// next worker to attach it reads them.
		s.reportQcowChainContent(request, entries)
	}
	return entries, nil
}

// rootLayerOwner owns the layer volumes in the disk manager, which no
// container's cleanup may detach.
const rootLayerOwner = "root-layer"

// prune detaches the generations no VM has used for rootLayerIdle.
func (l *rootLayers) prune() {
	if l == nil {
		return
	}
	var idle []*rootLayerMount
	l.mu.Lock()
	for key, entry := range l.disks {
		if mount := entry.ready; mount != nil && mount.refs == 0 && time.Since(mount.usedAt) > rootLayerIdle {
			idle = append(idle, mount)
			entry.ready = nil
		}
		if entry.ready == nil && entry.attaching == "" && len(entry.retired) == 0 && entry.publishers == 0 {
			delete(l.disks, key)
		}
	}
	l.mu.Unlock()
	for _, mount := range idle {
		l.detach(mount)
	}
}

func (l *rootLayers) run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.prune()
		}
	}
}

// publisher is the entry of a disk for a publish to it, which keeps the
// entry until done is called.
func (l *rootLayers) publisher(key string) (entry *rootLayerDisk, done func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry = l.disks[key]
	if entry == nil {
		entry = &rootLayerDisk{}
		l.disks[key] = entry
	}
	entry.publishers++
	return entry, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		entry.publishers--
	}
}

// rootLayerExportDir is the host directory a VM's guest writes its cache
// export into. It is beside the disks: an export can be as large as a layer.
func (s *Worker) rootLayerExportDir(containerID string) string {
	return filepath.Join(s.diskManager.Root(), rootLayerExportsDir, containerID)
}

// makeRootLayerExport mounts the filesystem a guest exports into at dir:
// ext4 in a sparse file of size bytes beside it. The guest can write there
// what it likes, as root, and takes no more of the worker's disk than that.
// It writes one file, so the filesystem has few inodes and no journal.
func makeRootLayerExport(dir string, size int64) (err error) {
	removeRootLayerExport(dir)
	defer func() {
		if err != nil {
			removeRootLayerExport(dir)
		}
	}()
	image := dir + ".img"
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(image, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	err = file.Truncate(size)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	run := func(command ...string) (string, error) {
		out, err := exec.Command(command[0], command[1:]...).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("%s: %w: %s", command[0], err, strings.TrimSpace(string(out)))
		}
		return strings.TrimSpace(string(out)), nil
	}
	if _, err := run("mkfs.ext4", "-F", "-q", "-m", "0", "-N", "64", "-O", "^has_journal", "-E", "nodiscard", image); err != nil {
		return err
	}
	// The image is written past the page cache. What a guest writes is cached
	// once, in the export's filesystem, as in a directory: cached again in
	// the image, it would count twice against the VM's memory.
	attach := []string{"losetup", "--find", "--show", "--direct-io=on", image}
	device, err := run(attach...)
	if err != nil {
		// The kernel adds a loop device when all are in use, and a
		// container's /dev has no file for one added since it started.
		addLoopDevices()
		device, err = run(attach...)
	}
	if err != nil {
		return err
	}
	// A kernel that cannot write this image directly attaches it all the
	// same, and caches it. No export is better than one that can have its
	// VM killed.
	direct, _ := os.ReadFile(filepath.Join("/sys/block", filepath.Base(device), "loop", "dio"))
	if strings.TrimSpace(string(direct)) != "1" {
		err = fmt.Errorf("%s is not written past the page cache", device)
	}
	if err == nil {
		_, err = run("mount", "-t", "ext4", "-o", "noatime,nosuid,nodev,noexec", device, dir)
	}
	// A device that is mounted is detached once it is unmounted.
	_, _ = run("losetup", "-d", device)
	if err != nil {
		return err
	}
	return os.Chmod(dir, 0o700)
}

// addLoopDevices makes the device files of the loop devices that the kernel
// has and /dev lacks.
func addLoopDevices() {
	blocks, _ := filepath.Glob("/sys/block/loop*")
	for _, block := range blocks {
		device := filepath.Join("/dev", filepath.Base(block))
		if _, err := os.Lstat(device); err == nil {
			continue
		}
		number, err := os.ReadFile(filepath.Join(block, "dev"))
		if err != nil {
			continue
		}
		var major, minor uint32
		if _, err := fmt.Sscanf(strings.TrimSpace(string(number)), "%d:%d", &major, &minor); err != nil {
			continue
		}
		if err := unix.Mknod(device, unix.S_IFBLK|0o660, int(unix.Mkdev(major, minor))); err != nil {
			log.Warn().Err(err).Str("device", device).Msg("unable to add a loop device")
		}
	}
}

// removeRootLayerExport unmounts and removes an export. One that is still in
// use goes once it is not.
func removeRootLayerExport(dir string) {
	// Nothing is mounted where there is no directory.
	if _, err := os.Lstat(dir); err == nil {
		if exec.Command("umount", dir).Run() != nil {
			_ = exec.Command("umount", "-l", dir).Run()
		}
		_ = os.RemoveAll(dir)
	}
	_ = os.Remove(dir + ".img")
}

// discardRootLayerExport removes the export of a VM that never ran: one
// whose start failed after its spec was made, which no exit collects.
func (s *Worker) discardRootLayerExport(containerID string, request *types.ContainerRequest) {
	if layer := s.requestRootLayer(request); layer != nil && layer.Publish && containerID != "" {
		removeRootLayerExport(s.rootLayerExportDir(containerID))
	}
}

// removeRootLayerExports removes the exports that the VMs of an earlier
// worker process left. It runs before this one starts any VM.
func (s *Worker) removeRootLayerExports() {
	root := filepath.Join(s.diskManager.Root(), rootLayerExportsDir)
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, entry := range entries {
		removeRootLayerExport(filepath.Join(root, strings.TrimSuffix(entry.Name(), ".img")))
	}
	if len(entries) > 0 {
		log.Info().Int("entries", len(entries)).Msg("removed root layer exports left from before")
	}
}

// annotateRootLayer tells the guest of a VM with a root layer how to keep
// its root, and where to leave what it adds to the layer's directories.
func (s *Worker) annotateRootLayer(request *types.ContainerRequest, spec *specs.Spec) error {
	layer := s.requestRootLayer(request)
	if layer == nil {
		return nil
	}
	// A job touches every cache entry it uses. Copying each one to the root
	// disk whole would cost the VM the size of the layer in writes.
	spec.Annotations[runtime.MicroVMRootMetacopyAnnotation] = "true"

	var paths []string
	for _, path := range layer.Paths {
		if path = filepath.Clean(path); filepath.IsAbs(path) && path != "/" && !strings.Contains(path, ":") {
			paths = append(paths, path)
		}
	}
	if !layer.Publish || len(paths) == 0 {
		return nil
	}
	dir := s.rootLayerExportDir(request.ContainerId)
	if err := makeRootLayerExport(dir, rootLayerMaxExportBytes); err != nil {
		// The VM runs as it would have, and leaves nothing to the layer.
		log.Warn().Err(err).Str("container_id", request.ContainerId).Msg("no root layer export for a VM")
		return nil
	}
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Type:        "none",
		Source:      dir,
		Destination: microvm.CacheExportDir,
		Options:     []string{"rbind", "rw", "nosuid", "nodev", "noexec"},
	})
	spec.Annotations[runtime.MicroVMCacheExportAnnotation] = strings.Join(paths, ":")
	return nil
}

// collectRootLayerExport takes what an exited VM's guest exported and adds it
// to the layer disk in the background. The container's exit does not wait.
func (s *Worker) collectRootLayerExport(containerID string, request *types.ContainerRequest) {
	layer := s.requestRootLayer(request)
	if layer == nil || !layer.Publish || containerID == "" {
		return
	}
	dir := s.rootLayerExportDir(containerID)
	export := filepath.Join(dir, microvm.CacheExportFile)
	info, err := os.Lstat(export)
	if err != nil || !info.Mode().IsRegular() || info.Size() > rootLayerMaxExportBytes {
		// The guest wrote nothing, was gone before it finished, or wrote
		// more than a layer takes.
		if err == nil {
			log.Warn().Str("container_id", containerID).Int64("bytes", info.Size()).Msg("ignored a root layer export")
		}
		removeRootLayerExport(dir)
		return
	}
	// The workspace's storage credentials are given for a container that
	// exists, which this one is until its exit is recorded.
	ctx, cancel := context.WithTimeout(s.durableDiskCleanupContext(), durableDiskCleanupGrace)
	err = s.ensureDurableDiskSnapshotStorage(ctx, request)
	cancel()
	if err != nil {
		log.Warn().Err(err).Str("container_id", containerID).Msg("no storage credentials to publish a root layer export")
		removeRootLayerExport(dir)
		return
	}
	copied := *request
	copied.Mounts = nil
	parent := *layer
	go func() {
		defer removeRootLayerExport(dir)
		ctx, cancel := context.WithTimeout(s.durableDiskCleanupContext(), rootLayerPublishTimeout)
		defer cancel()
		start := time.Now()
		snapshot, stats, err := s.publishRootLayer(ctx, &copied, parent, export)
		event, message := log.Info(), "published a root layer generation"
		switch {
		case errors.Is(err, errRootLayerUnchanged):
			message = "root layer export added nothing"
		case err != nil:
			event, message = log.Warn().Err(err), "failed to publish a root layer export"
		}
		if snapshot != nil {
			event = event.Str("snapshot_id", snapshot.ExternalId)
		}
		event.Str("container_id", containerID).Str("disk", parent.DiskName).Str("parent", parent.SnapshotID).
			Int("files", stats.files).Int64("bytes", stats.bytes).Int("used", stats.touched).Int("refused", stats.refused).
			Bool("full", stats.full).Dur("took", time.Since(start)).Msg(message)
	}()
}

// errRootLayerUnchanged is a publish that had nothing to add.
var errRootLayerUnchanged = errors.New("the export adds nothing to the layer")

// publishRootLayer adds a guest's export to the layer disk and publishes the
// result as a generation whose parent is the one the VM started on. The
// gateway decides whether any VM ever gets it.
//
// The volume is attached from that parent every time and removed afterwards:
// a local copy could hold a generation the gateway turned down.
func (s *Worker) publishRootLayer(ctx context.Context, request *types.ContainerRequest, layer types.RootLayer, export string) (*types.DiskSnapshot, cacheMergeStats, error) {
	var stats cacheMergeStats
	entry, done := s.rootLayers.publisher(rootLayerDiskKey(request, &layer))
	defer done()
	entry.publish.Lock()
	defer entry.publish.Unlock()

	volumeKey := fmt.Sprintf("%s-layer-%s-publish", cacheRequestWorkspaceID(request), types.SafeDurableDiskName(layer.DiskName))
	dir := filepath.Join(s.diskManager.Root(), rootLayersDir, volumeKey)
	mount := rootLayerMountFor(&layer, dir, false)
	// Before and after: a worker that restarted during a publish has the
	// volume of that one.
	remove := func() {
		detachCtx, cancel := context.WithTimeout(s.durableDiskCleanupContext(), durableDiskCleanupGrace)
		defer cancel()
		s.qcowChains.Delete(volumeKey)
		if err := s.diskManager.Detach(detachCtx, volumeKey); err != nil {
			log.Warn().Err(err).Str("disk", layer.DiskName).Msg("failed to detach a root layer's publish volume")
			return
		}
		if err := s.diskManager.Remove(volumeKey); err != nil {
			log.Warn().Err(err).Str("disk", layer.DiskName).Msg("failed to remove a root layer's publish volume")
		}
		_ = os.Remove(dir)
	}
	remove()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, stats, err
	}
	entries, err := s.attachRootLayerVolume(ctx, request, mount, volumeKey, layer.SnapshotID, false)
	if err != nil {
		return nil, stats, err
	}
	defer remove()
	// The chunks the parent chain already has in the bucket are not uploaded
	// again.
	s.qcowChains.Store(volumeKey, entries)

	stats, err = mergeCacheExport(export, dir, layer.Paths, rootLayerMinFreeBytes)
	if err != nil {
		return nil, stats, fmt.Errorf("add export to root layer: %w", err)
	}
	// A generation that differs only in when its files were used is not
	// worth every worker fetching it.
	if stats.files == 0 {
		return nil, stats, errRootLayerUnchanged
	}

	volume, attached := s.diskManager.Volume(volumeKey)
	if !attached {
		return nil, stats, fmt.Errorf("root layer %q is no longer attached", layer.DiskName)
	}
	store, err := newDurableDiskSnapshotWriteStore(ctx, request)
	if err != nil {
		return nil, stats, err
	}
	var parent *types.DiskSnapshot
	if len(entries) > 0 {
		parent = entries[len(entries)-1].row
	}
	var published *types.DiskSnapshot
	latest := parent
	publish := func(sealed []disk.SealedLayer) error {
		parentID := sealed[0].ParentSnapshotID
		for _, sealedLayer := range sealed {
			row, manifest, err := s.publishRootLayerGeneration(ctx, request, mount, volume, store, volumeKey, sealedLayer.Path, parentID, latest)
			if err != nil {
				return err
			}
			if err := volume.MarkPublished(sealedLayer.Path, row.ExternalId); err != nil {
				s.withdrawQcowSnapshot(ctx, request, row, err)
				return err
			}
			s.appendQcowChain(volumeKey, qcowChainEntry{row: row, manifest: manifest})
			published, latest, parentID = row, row, row.ExternalId
		}
		return nil
	}
	if err := snapshotQcowVolume(ctx, volume, durableDiskSyncFinal, true, publish); err != nil {
		return published, stats, err
	}
	return published, stats, nil
}

// publishRootLayerGeneration is publishQcowLayer for a volume whose key is
// not the one a request's mount has.
func (s *Worker) publishRootLayerGeneration(ctx context.Context, request *types.ContainerRequest, mount *types.Mount, volume *disk.Volume, store *durableDiskSnapshotBucketStore, volumeKey, sealedPath, parentID string, latest *types.DiskSnapshot) (*types.DiskSnapshot, *types.DiskSnapshotManifest, error) {
	var layer *types.DiskSnapshotFile
	if parentID != "" && flattenQcowChainForced(len(s.qcowChain(volumeKey))) {
		// A parentless generation of the whole disk keeps the chain a worker
		// has to fetch short.
		flatPath := sealedPath + ".flat"
		defer os.Remove(flatPath)
		flat, err := s.uploadFlattenedQcowLayer(ctx, mount, volume, store, volumeKey, sealedPath, flatPath)
		if err != nil {
			return nil, nil, err
		}
		layer, parentID = flat, ""
	}
	if layer == nil {
		var err error
		if layer, err = s.uploadQcowLayer(ctx, mount, store, volumeKey, sealedPath); err != nil {
			return nil, nil, err
		}
	}
	generation, err := nextDurableDiskSnapshotGeneration(time.Now().UnixNano(), latest)
	if err != nil {
		return nil, nil, err
	}
	sizeBytes, _ := durableDiskSizeBytes(mount.DurableDisk.Size)
	return s.createQcowGeneration(ctx, request, mount, store, layer, parentID, generation, sizeBytes, volume.Check)
}
