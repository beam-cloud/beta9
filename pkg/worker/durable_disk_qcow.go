package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/disk"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

// The qcow driver keeps the durable-disk control plane (DiskSnapshot rows,
// object layout, required-content sync) identical to the snapshot driver but
// replaces the worker mechanism with qcow2 block volumes from pkg/disk.
// Snapshots are near-instant pivots; a mount path of "/" turns the disk into
// the machine root: the container's overlay upper layer lives on the volume,
// so the entire root filesystem is preserved across snapshot and fork.

func isQcowDurableDiskMount(mount *types.Mount) bool {
	return mount != nil && mount.DurableDisk != nil &&
		durableDiskDriver(mount.DurableDisk.Driver) == types.DurableDiskDriverQcow
}

func isQcowRootDiskMount(mount *types.Mount) bool {
	return isQcowDurableDiskMount(mount) && mount.MountPath == types.DurableDiskRootMountPath
}

// qcowRootDiskMount returns the machine-root disk mount of a request, if any.
func qcowRootDiskMount(request *types.ContainerRequest) *types.Mount {
	if request == nil {
		return nil
	}
	for i := range request.Mounts {
		if isQcowRootDiskMount(&request.Mounts[i]) {
			return &request.Mounts[i]
		}
	}
	return nil
}

// qcowVolumeKey is stable across container restarts for writable volumes so
// locally cached layers are reused. Read-only attachments get per-container
// volumes since they may coexist on one host.
func (s *Worker) qcowVolumeKey(request *types.ContainerRequest, mount *types.Mount) string {
	key := fmt.Sprintf("%s-%s", cacheRequestWorkspaceID(request), types.SafeDurableDiskName(mount.DurableDisk.Name))
	if mount.ReadOnly {
		key = fmt.Sprintf("%s-ro-%s", key, request.ContainerId)
	}
	return key
}

func (s *Worker) prepareQcowDurableDiskMount(request *types.ContainerRequest, mount *types.Mount) error {
	if s.diskManager == nil {
		return fmt.Errorf("qcow durable disks are not enabled on this worker")
	}
	sizeBytes, err := durableDiskSizeBytes(mount.DurableDisk.Size)
	if err != nil {
		return fmt.Errorf("qcow durable disk %q requires a valid size: %w", mount.DurableDisk.Name, err)
	}
	ctx := s.durableDiskContext(nil)

	rows, err := s.resolveQcowSnapshotChain(ctx, request, mount)
	if err != nil {
		return err
	}

	chain := make([]disk.ChainLayer, 0, len(rows))
	manifests := make([]*types.DiskSnapshotManifest, 0, len(rows))
	source := &qcowChunkSource{cacheReader: s.durableDiskSnapshotCacheReader()}
	for _, row := range rows {
		if !row.Public {
			if err := s.ensureDurableDiskSnapshotStorage(ctx, request); err != nil {
				return err
			}
		}
		store, err := newDurableDiskSnapshotReadStore(ctx, request, row, s.backendRepoClient)
		if err != nil {
			return err
		}
		manifest, err := loadDurableDiskSnapshotManifest(ctx, store, s.durableDiskSnapshotCacheReader(), row)
		if err != nil {
			return fmt.Errorf("load qcow snapshot manifest %s: %w", row.ExternalId, err)
		}
		layer, err := qcowManifestLayer(manifest)
		if err != nil {
			return fmt.Errorf("qcow snapshot %s: %w", row.ExternalId, err)
		}
		chain = append(chain, disk.ChainLayer{SnapshotID: row.ExternalId, Layer: layer})
		manifests = append(manifests, manifest)
		source.stores = append(source.stores, store)
	}

	sizeBytes = max(sizeBytes, qcowChainVirtualSize(manifests))
	_, err = s.diskManager.Attach(ctx, disk.AttachSpec{
		Key:              s.qcowVolumeKey(request, mount),
		VirtualSizeBytes: sizeBytes,
		ReadOnly:         mount.ReadOnly,
		Mountpoint:       mount.LocalPath,
		Chain:            chain,
	}, source)
	if err != nil {
		return fmt.Errorf("attach qcow durable disk %q: %w", mount.DurableDisk.Name, err)
	}

	// The chain is rooted at its last parentless row, so its length is the
	// generation count since the last flatten, which drives the periodic
	// flattened publish that bounds restore chains.
	entries := make([]qcowChainEntry, len(rows))
	for i := range rows {
		entries[i] = qcowChainEntry{row: rows[i], manifest: manifests[i]}
	}
	s.qcowChains.Store(s.qcowVolumeKey(request, mount), entries)
	s.reportQcowChainContent(request, entries)
	return nil
}

// qcowChainEntry is one published generation of a volume's live chain.
type qcowChainEntry struct {
	row      *types.DiskSnapshot
	manifest *types.DiskSnapshotManifest
}

func (s *Worker) qcowChain(key string) []qcowChainEntry {
	if value, ok := s.qcowChains.Load(key); ok {
		chain, _ := value.([]qcowChainEntry)
		return chain
	}
	return nil
}

// appendQcowChain records a newly published generation. A parentless row
// (first generation or a flattened publish) is self-contained and supersedes
// the whole previous chain.
func (s *Worker) appendQcowChain(key string, entry qcowChainEntry) []qcowChainEntry {
	chain := s.qcowChain(key)
	if entry.row.ParentSnapshotId == "" {
		chain = []qcowChainEntry{entry}
	} else {
		chain = append(chain, entry)
	}
	s.qcowChains.Store(key, chain)
	return chain
}

// reportQcowChainContent reports every layer of the live chain tagged with
// the head generation. The recency index keeps only a disk's newest
// generation, and qcow layers are deltas: tagging parents with the head
// generation keeps the whole restore chain protected and locality-replicated
// until a flatten or newer chain supersedes it, at which point the old chain
// ages out through the normal recency prune.
func (s *Worker) reportQcowChainContent(request *types.ContainerRequest, chain []qcowChainEntry) {
	if len(chain) == 0 {
		return
	}
	head := chain[len(chain)-1].row.Generation
	for _, entry := range chain {
		s.reportDurableDiskSnapshotContent(request, entry.row, entry.manifest, head)
	}
}

// qcowUploadChunks returns a copy of layer without the chunks the live
// chain's manifests already reference: chunk objects are content-addressed
// and never deleted, so those are known to exist in the bucket. Flattened
// publishes overlap heavily with the previous flatten, so this usually
// reduces the periodic full-image publish to just the clusters that changed.
func (s *Worker) qcowUploadChunks(key string, layer *types.DiskSnapshotFile) *types.DiskSnapshotFile {
	existing := make(map[string]bool)
	for _, entry := range s.qcowChain(key) {
		if entry.manifest == nil {
			continue
		}
		for _, file := range entry.manifest.Files {
			for _, chunk := range file.Chunks {
				existing[chunk.ObjectKey] = true
			}
		}
	}
	if len(existing) == 0 {
		return layer
	}
	upload := *layer
	upload.Chunks = nil
	for _, chunk := range layer.Chunks {
		if !existing[chunk.ObjectKey] {
			upload.Chunks = append(upload.Chunks, chunk)
		}
	}
	return &upload
}

// resolveQcowSnapshotChain walks ParentSnapshotId links from the newest
// generation (or a seed snapshot for forks) back to a parentless root and
// returns the rows base first.
func (s *Worker) resolveQcowSnapshotChain(ctx context.Context, request *types.ContainerRequest, mount *types.Mount) ([]*types.DiskSnapshot, error) {
	newest, err := s.latestQcowSnapshotRow(ctx, request, mount)
	if err != nil {
		return nil, err
	}
	if (newest == nil || newest.ManifestKey == "") && mount.DurableDisk.SourceSnapshotId != "" {
		// Fork: seed a brand new disk from another disk's snapshot chain.
		seed, err := s.seedDurableDiskSnapshot(ctx, request, mount)
		if err != nil {
			return nil, err
		}
		newest = seed
	}
	if newest == nil || newest.ManifestKey == "" {
		return nil, nil
	}

	var rows []*types.DiskSnapshot
	row := newest
	for {
		if row.Format != types.DiskSnapshotFormatQcowV1 {
			return nil, fmt.Errorf("disk %q has snapshot %s in format %q; the qcow driver cannot restore it",
				mount.DurableDisk.Name, row.ExternalId, row.Format)
		}
		rows = append(rows, row)
		if row.ParentSnapshotId == "" {
			break
		}
		if len(rows) > disk.DefaultMaxChainDepth {
			return nil, fmt.Errorf("disk %q snapshot chain exceeds %d generations", mount.DurableDisk.Name, disk.DefaultMaxChainDepth)
		}
		parentResp, err := handleGRPCResponse(s.backendRepoClient.GetDiskSnapshot(ctx, &pb.GetDiskSnapshotRequest{
			WorkspaceId: cacheRequestWorkspaceID(request),
			SnapshotId:  row.ParentSnapshotId,
		}))
		if err != nil {
			return nil, fmt.Errorf("resolve qcow snapshot parent %s: %w", row.ParentSnapshotId, err)
		}
		row = durableDiskSnapshotFromProto(parentResp.Snapshot)
		if row == nil || row.ManifestKey == "" {
			return nil, fmt.Errorf("qcow snapshot parent %s is missing", rows[len(rows)-1].ParentSnapshotId)
		}
	}
	// Reverse into base-first order.
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	return rows, nil
}

func qcowManifestLayer(manifest *types.DiskSnapshotManifest) (*types.DiskSnapshotFile, error) {
	if manifest == nil || manifest.Format != types.DiskSnapshotFormatQcowV1 {
		return nil, fmt.Errorf("manifest is not %s", types.DiskSnapshotFormatQcowV1)
	}
	if len(manifest.Files) != 1 || manifest.Files[0].Path != disk.LayerFileName {
		return nil, fmt.Errorf("manifest does not contain exactly one %s entry", disk.LayerFileName)
	}
	return &manifest.Files[0], nil
}

func qcowChainVirtualSize(manifests []*types.DiskSnapshotManifest) int64 {
	var size int64
	for _, manifest := range manifests {
		size = max(size, manifest.LogicalSizeBytes)
	}
	return size
}

// snapshotQcowDurableDiskMount seals the volume and publishes every pending
// layer, oldest first, as ordinary DiskSnapshot generations.
func (s *Worker) snapshotQcowDurableDiskMount(ctx context.Context, request *types.ContainerRequest, mount *types.Mount, mode durableDiskSyncMode) (*types.DiskSnapshot, error) {
	if mount.ReadOnly {
		return nil, nil
	}
	key := s.qcowVolumeKey(request, mount)
	volume, ok := s.diskManager.Volume(key)
	if !ok {
		return nil, fmt.Errorf("qcow durable disk %q is not attached", mount.DurableDisk.Name)
	}

	// Fold published layers into the base so a long-running machine can be
	// snapshotted indefinitely without hitting the local chain depth cap.
	if volume.Depth() > disk.DefaultFlattenDepth {
		if err := volume.Compact(ctx); err != nil {
			log.Warn().Err(err).Str("disk", mount.DurableDisk.Name).Msg("failed to compact qcow backing chain")
		}
	}

	if err := s.ensureDurableDiskSnapshotStorage(ctx, request); err != nil {
		return nil, err
	}
	store, err := newDurableDiskSnapshotWriteStore(ctx, request)
	if err != nil {
		return nil, err
	}
	latest, err := s.latestQcowSnapshotRow(ctx, request, mount)
	if err != nil {
		return nil, err
	}

	// The final sync is a durability boundary and always seals. A disk with
	// no published generation yet also always seals: callers pin machine
	// state to a snapshot ID, so the first snapshot must produce one even if
	// nothing was written.
	sealed, skipped, err := volume.Seal(ctx, mode == durableDiskSyncFinal || latest == nil)
	if err != nil {
		return nil, err
	}
	if skipped {
		log.Debug().Str("disk", mount.DurableDisk.Name).Msg("qcow durable disk is unchanged; keeping the last generation")
		return latest, nil
	}

	parentID := sealed[0].ParentSnapshotID
	var published *types.DiskSnapshot
	for _, layer := range sealed {
		row, manifest, err := s.publishQcowLayer(ctx, request, mount, volume, store, layer.Path, parentID, latest)
		if err != nil {
			return published, err
		}
		if err := volume.MarkPublished(layer.Path, row.ExternalId); err != nil {
			return published, err
		}
		s.reportQcowChainContent(request, s.appendQcowChain(key, qcowChainEntry{row: row, manifest: manifest}))
		published, latest, parentID = row, row, row.ExternalId
	}
	return published, nil
}

func (s *Worker) latestQcowSnapshotRow(ctx context.Context, request *types.ContainerRequest, mount *types.Mount) (*types.DiskSnapshot, error) {
	resp, err := handleGRPCResponse(s.backendRepoClient.GetLatestDiskSnapshot(ctx, &pb.GetLatestDiskSnapshotRequest{
		WorkspaceId: cacheRequestWorkspaceID(request),
		DiskName:    mount.DurableDisk.Name,
	}))
	if err != nil {
		if durableDiskSnapshotNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get latest qcow disk snapshot: %w", err)
	}
	if resp == nil {
		return nil, nil
	}
	return durableDiskSnapshotFromProto(resp.Snapshot), nil
}

// publishQcowLayer uploads one sealed layer (or its flattened chain when the
// published chain is deep) plus its manifest, then creates the repository row.
// The manifest upload is the durability boundary, mirroring the dir.v1 driver.
func (s *Worker) publishQcowLayer(ctx context.Context, request *types.ContainerRequest, mount *types.Mount, volume *disk.Volume, store *durableDiskSnapshotBucketStore, sealedPath, parentID string, latest *types.DiskSnapshot) (*types.DiskSnapshot, *types.DiskSnapshotManifest, error) {
	depth := len(s.qcowChain(s.qcowVolumeKey(request, mount)))

	uploadPath := sealedPath
	if parentID != "" && depth+1 >= disk.DefaultFlattenDepth {
		// Publish a parentless flattened generation so restore chains stay
		// short. The local chain is untouched; only the artifact differs.
		flatPath := sealedPath + ".flat"
		if err := volume.Flatten(ctx, sealedPath, flatPath); err != nil {
			return nil, nil, fmt.Errorf("flatten qcow chain: %w", err)
		}
		defer os.Remove(flatPath)
		uploadPath = flatPath
		parentID = ""
	}

	chunkPrefix := durableDiskChunkPrefix(mount)
	layer, err := disk.ScanLayer(uploadPath, func(digest string) string {
		return path.Join(chunkPrefix, strings.TrimPrefix(digest, "sha256:"))
	})
	if err != nil {
		return nil, nil, fmt.Errorf("scan qcow layer: %w", err)
	}

	upload := s.qcowUploadChunks(s.qcowVolumeKey(request, mount), layer)
	sink := &qcowChunkSink{ctx: ctx, store: store}
	if err := disk.UploadLayer(ctx, sink, uploadPath, upload); err != nil {
		return nil, nil, err
	}
	if skipped := len(layer.Chunks) - len(upload.Chunks); skipped > 0 {
		log.Info().Str("disk", mount.DurableDisk.Name).Int("skipped", skipped).Int("total", len(layer.Chunks)).
			Msg("skipped qcow chunks already present in the bucket")
	}

	generation, err := nextDurableDiskSnapshotGeneration(time.Now().UnixNano(), latest)
	if err != nil {
		return nil, nil, err
	}
	sizeBytes, _ := durableDiskSizeBytes(mount.DurableDisk.Size)
	manifest := &types.DiskSnapshotManifest{
		Version:          1,
		Format:           types.DiskSnapshotFormatQcowV1,
		DiskName:         mount.DurableDisk.Name,
		Filesystem:       types.DiskFilesystemExt4,
		Generation:       generation,
		ParentSnapshotId: parentID,
		LogicalSizeBytes: sizeBytes,
		StoredSizeBytes:  disk.StoredBytes(layer),
		Files:            []types.DiskSnapshotFile{*layer},
		CreatedAt:        time.Now().UTC(),
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, nil, err
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	objectPrefix := durableDiskSnapshotObjectPrefix(mount, generation)
	manifestKey := path.Join(objectPrefix, durableDiskManifestFileName)
	if err := store.Upload(ctx, manifestKey, manifestBytes); err != nil {
		return nil, nil, fmt.Errorf("upload qcow snapshot manifest: %w", err)
	}

	snapshot := &types.DiskSnapshot{
		DiskName:            mount.DurableDisk.Name,
		Format:              types.DiskSnapshotFormatQcowV1,
		Status:              types.DiskSnapshotStatusAvailable,
		ParentSnapshotId:    parentID,
		Generation:          generation,
		SizeBytes:           sizeBytes,
		Filesystem:          types.DiskFilesystemExt4,
		Driver:              types.DurableDiskDriverQcow,
		ManifestKey:         manifestKey,
		ManifestDigest:      "sha256:" + hex.EncodeToString(manifestDigest[:]),
		ManifestSizeBytes:   int64(len(manifestBytes)),
		ChunkCount:          int64(len(layer.Chunks)),
		LogicalSizeBytes:    sizeBytes,
		StoredSizeBytes:     disk.StoredBytes(layer),
		BucketName:          store.bucket,
		ObjectPrefix:        objectPrefix,
		SourcePool:          s.poolName,
		SourceWorkerId:      s.workerId,
		SourceStorageNodeId: s.storageNodeID(),
	}
	resp, err := handleGRPCResponse(s.backendRepoClient.CreateDiskSnapshot(ctx, &pb.CreateDiskSnapshotRequest{
		WorkspaceId: cacheRequestWorkspaceID(request),
		StubId:      cacheRequestStubID(request),
		Snapshot:    durableDiskSnapshotToProto(snapshot),
	}))
	if err != nil {
		return nil, nil, err
	}
	if created := durableDiskSnapshotFromProto(resp.Snapshot); created != nil {
		snapshot = created
	}
	reportDurableDiskProgress(ctx, durableDiskProgressEvent{})
	return snapshot, manifest, nil
}

// detachQcowDurableDiskMount takes the volume offline at the container's
// durability boundary. Local layers stay cached for the next attachment.
func (s *Worker) detachQcowDurableDiskMount(ctx context.Context, request *types.ContainerRequest, mount *types.Mount) error {
	if s.diskManager == nil {
		return nil
	}
	return s.diskManager.Detach(ctx, s.qcowVolumeKey(request, mount))
}

// qcowChunkSink adapts the workspace bucket store, reporting progress so the
// sync inactivity watchdog sees upload activity.
type qcowChunkSink struct {
	ctx   context.Context
	store *durableDiskSnapshotBucketStore
}

func (s *qcowChunkSink) WriteChunk(ctx context.Context, key string, data []byte) error {
	if err := s.store.Upload(ctx, key, data); err != nil {
		return err
	}
	reportDurableDiskProgress(s.ctx, durableDiskProgressEvent{chunks: 1, logicalBytes: int64(len(data))})
	return nil
}

// qcowChunkSource reads chunks through the node-local content cache when
// possible and falls back to the snapshot stores otherwise. This is the same
// required-content sync path the directory driver uses, so warm nodes restore
// and fork without touching object storage.
type qcowChunkSource struct {
	cacheReader durableDiskSnapshotCacheReader
	stores      []durableDiskSnapshotStore
}

func (s *qcowChunkSource) ReadChunk(ctx context.Context, chunk types.DiskSnapshotChunk, dest []byte) error {
	hash := strings.TrimPrefix(chunk.Digest, "sha256:")
	if s.cacheReader != nil && hash != "" {
		read, err := s.cacheReader.ReadContentInto(ctx, hash, 0, dest, cache.ClientOptions{})
		if err == nil && read == int64(len(dest)) {
			return nil
		}
	}
	var lastErr error = fmt.Errorf("no stores available for chunk %s", chunk.ObjectKey)
	for _, store := range s.stores {
		reader, err := store.DownloadWithReader(ctx, chunk.ObjectKey)
		if err != nil {
			lastErr = err
			continue
		}
		_, err = io.ReadFull(reader, dest)
		reader.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}
