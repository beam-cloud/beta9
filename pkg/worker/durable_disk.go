package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	durableDiskDriverEnv  = "BETA9_DURABLE_DISK_DRIVER"
	durableDiskMarkerFile = ".beta9-durable-disk"
	durableDiskLockDir    = ".beta9-durable-disk-locks"
	durableDiskLockWait   = 10 * time.Minute

	durableDiskSnapshotInactivityTimeout = 3 * time.Minute
	durableDiskPhaseHeartbeatInterval    = 45 * time.Second

	durableDiskStateClean = "clean"
	durableDiskStateDirty = "dirty"
)

type durableDiskSyncMode uint8

const (
	// Final cleanup is the durable handoff fence: everything written since the
	// last publish is durable once it returns. The directory driver always
	// publishes a fresh generation, even when every content chunk is reused,
	// because its change detection is a heuristic. The qcow driver skips an
	// unchanged head (see snapshotQcowDurableDiskMount): the daemon's write
	// counter proves nothing changed, so the latest generation stays the
	// disk's pinned state instead of an empty layer deepening the chain.
	durableDiskSyncFinal durableDiskSyncMode = iota
	// An explicit snapshot may return the latest generation when nothing changed.
	durableDiskSyncExplicit
)

type durableDiskProgressEvent struct {
	logicalBytes int64
	files        int64
	chunks       int64
}

type durableDiskProgressReporterKey struct{}

func withDurableDiskProgressReporter(ctx context.Context, report func(durableDiskProgressEvent)) context.Context {
	if report == nil {
		return ctx
	}
	return context.WithValue(ctx, durableDiskProgressReporterKey{}, report)
}

func reportDurableDiskProgress(ctx context.Context, progress durableDiskProgressEvent) {
	if ctx == nil {
		return
	}
	if report, ok := ctx.Value(durableDiskProgressReporterKey{}).(func(durableDiskProgressEvent)); ok {
		report(progress)
	}
}

type durableDiskMarker struct {
	Driver         string
	State          string
	SnapshotID     string
	ManifestDigest string
	Generation     int64
}

// prepareDurableDiskMounts brings every durable disk on the request online.
// It runs during container startup alongside the image pull and workspace
// mount: a qcow attach costs ~45ms inside the kernel's NBD connect alone, so
// paying it serially inside spec generation put it squarely on the critical
// path. Disks attach concurrently with each other for the same reason.
//
// ctx is the startup context: when a sibling startup task fails or startup is
// canceled, in-flight attaches abort instead of finishing after the container
// has already been failed. Disks that did attach are detached by clearContainer.
func (s *Worker) prepareDurableDiskMounts(ctx context.Context, request *types.ContainerRequest) error {
	disks, ctx := errgroup.WithContext(ctx)
	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount.MountType != types.StorageModeDurableDisk {
			continue
		}
		disks.Go(func() error {
			if err := s.prepareDurableDiskMount(ctx, request, mount); err != nil {
				return fmt.Errorf("failed to prepare durable disk mount: %w", err)
			}
			return nil
		})
	}
	return disks.Wait()
}

func (s *Worker) prepareDurableDiskMount(ctx context.Context, request *types.ContainerRequest, mount *types.Mount) error {
	if mount == nil || mount.DurableDisk == nil {
		return fmt.Errorf("durable disk mount is missing metadata")
	}
	if mount.LocalPath == "" {
		return fmt.Errorf("durable disk %q has no local path", mount.DurableDisk.Name)
	}
	// Recover source metadata from the stub config when the mount omits it.
	if mount.DurableDisk.SourceSnapshotId == "" {
		mount.DurableDisk.SourceSnapshotId = durableDiskSourceSnapshotFromStub(request, mount.DurableDisk.Name)
	}

	ctx = s.durableDiskContext(ctx)
	driver := durableDiskDriver(mount.DurableDisk.Driver)
	switch driver {
	case types.DurableDiskDriverQcow:
		return withDurableDiskLock(ctx, mount, func() error {
			return s.prepareQcowDurableDiskMount(ctx, request, mount)
		})
	case types.DurableDiskDriverSnapshot:
		return withDurableDiskLock(ctx, mount, func() error {
			if s != nil {
				if err := s.restoreDurableDiskSnapshot(ctx, request, mount); err != nil {
					return err
				}
			}
			if err := prepareSnapshotDurableDiskMount(mount); err != nil {
				return err
			}
			if mount.ReadOnly {
				return nil
			}
			marker := readDurableDiskMarker(mount.LocalPath)
			return writeDurableDiskMarker(mount.LocalPath, durableDiskMarker{
				Driver:         types.DurableDiskDriverSnapshot,
				State:          durableDiskStateDirty,
				Generation:     marker.Generation,
				SnapshotID:     marker.SnapshotID,
				ManifestDigest: marker.ManifestDigest,
			})
		})
	default:
		return fmt.Errorf("durable disk %q requested unsupported driver %q", mount.DurableDisk.Name, driver)
	}
}

func durableDiskSourceSnapshotFromStub(request *types.ContainerRequest, diskName string) string {
	if request == nil || strings.TrimSpace(request.Stub.Config) == "" {
		return ""
	}
	config, err := request.Stub.UnmarshalConfig()
	if err != nil || config == nil {
		return ""
	}
	name := types.SafeDurableDiskName(diskName)
	for _, disk := range config.Disks {
		if disk != nil && types.SafeDurableDiskName(disk.Name) == name {
			return strings.TrimSpace(disk.SourceSnapshotId)
		}
	}
	return ""
}

func (s *Worker) durableDiskContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	if s != nil && s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func durableDiskDriver(configured string) string {
	if driver := types.NormalizeDurableDiskDriver(configured); driver != "" {
		return driver
	}
	if driver := types.NormalizeDurableDiskDriver(os.Getenv(durableDiskDriverEnv)); driver != "" {
		return driver
	}
	return types.DurableDiskDriverSnapshot
}

func prepareSnapshotDurableDiskMount(mount *types.Mount) error {
	if err := cleanDurableDiskRuntimeFiles(mount, mount.LocalPath); err != nil {
		return err
	}
	if durableDiskHasPayload(mount.LocalPath) && !durableDiskHasRestorablePayload(mount, mount.LocalPath) {
		return fmt.Errorf("durable disk %q has an active or incomplete local payload", mount.DurableDisk.Name)
	}
	if err := os.MkdirAll(mount.LocalPath, 0755); err != nil {
		return fmt.Errorf("create durable disk path %s: %w", mount.LocalPath, err)
	}
	if _, err := os.Stat(filepath.Join(mount.LocalPath, durableDiskMarkerFile)); os.IsNotExist(err) {
		return writeDurableDiskMarker(mount.LocalPath, durableDiskMarker{Driver: types.DurableDiskDriverSnapshot, State: durableDiskStateClean})
	}
	return nil
}

// syncDurableDiskMounts snapshots all durable mounts according to the caller's
// persistence boundary.
func (s *Worker) syncDurableDiskMounts(ctx context.Context, request *types.ContainerRequest, mode durableDiskSyncMode) ([]*types.DiskSnapshot, error) {
	if request == nil {
		return nil, nil
	}
	ctx = s.durableDiskContext(ctx)
	ctx, stopInactivityWatchdog := withDurableDiskInactivityWatchdog(ctx, durableDiskSnapshotInactivityTimeout)

	var syncErrs []error
	var snapshots []*types.DiskSnapshot
	for i := range request.Mounts {
		if err := ctx.Err(); err != nil {
			syncErrs = append(syncErrs, err)
			break
		}
		mount := &request.Mounts[i]
		if mount == nil || mount.DurableDisk == nil {
			continue
		}
		switch durableDiskDriver(mount.DurableDisk.Driver) {
		case types.DurableDiskDriverSnapshot:
			err := withDurableDiskLock(ctx, mount, func() error {
				snapshot, err := s.snapshotDurableDiskMount(ctx, request, mount, mode)
				if err != nil {
					return fmt.Errorf("snapshot: %w", err)
				}
				if snapshot != nil {
					snapshots = append(snapshots, snapshot)
				}
				return nil
			})
			if err != nil {
				log.Warn().
					Str("container_id", request.ContainerId).
					Str("disk", mount.DurableDisk.Name).
					Err(err).
					Msg("failed to sync durable disk")
				syncErrs = append(syncErrs, err)
			}
		case types.DurableDiskDriverQcow:
			err := withDurableDiskLock(ctx, mount, func() error {
				snapshot, snapErr := s.snapshotQcowDurableDiskMount(ctx, request, mount, mode)
				if snapshot != nil {
					snapshots = append(snapshots, snapshot)
				}
				if snapErr != nil {
					snapErr = fmt.Errorf("snapshot: %w", snapErr)
				}
				// The final sync is the container's durability boundary; the
				// volume comes offline afterwards even if publishing failed,
				// leaving sealed layers cached for the next attachment.
				if mode == durableDiskSyncFinal {
					if detachErr := s.detachQcowDurableDiskMount(ctx, request, mount); detachErr != nil {
						return errors.Join(snapErr, fmt.Errorf("detach: %w", detachErr))
					}
				}
				return snapErr
			})
			if err != nil {
				log.Warn().
					Str("container_id", request.ContainerId).
					Str("disk", mount.DurableDisk.Name).
					Err(err).
					Msg("failed to sync qcow durable disk")
				syncErrs = append(syncErrs, err)
			}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			if len(syncErrs) == 0 || !errors.Is(syncErrs[len(syncErrs)-1], ctxErr) {
				syncErrs = append(syncErrs, ctxErr)
			}
			break
		}
	}

	cause := context.Cause(ctx)
	stopInactivityWatchdog()
	if errors.Is(cause, errDurableDiskSnapshotInactive) {
		syncErrs = append(syncErrs, cause)
	}
	return snapshots, errors.Join(syncErrs...)
}

func withDurableDiskLock(ctx context.Context, mount *types.Mount, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if mount == nil || mount.LocalPath == "" {
		return fn()
	}

	cleanPath := filepath.Clean(mount.LocalPath)
	lockDir := filepath.Join(filepath.Dir(cleanPath), durableDiskLockDir)
	if err := os.MkdirAll(lockDir, 0755); err != nil {
		return fmt.Errorf("create durable disk lock dir: %w", err)
	}

	lock := NewFileLock(filepath.Join(lockDir, filepath.Base(cleanPath)+".lock"))
	start := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := lock.Acquire(); err == nil {
			break
		} else if time.Since(start) > durableDiskLockWait {
			return fmt.Errorf("acquire durable disk lock %s: %w", cleanPath, err)
		}
		// Contention can be another active snapshot and is bounded by durableDiskLockWait.
		reportDurableDiskProgress(ctx, durableDiskProgressEvent{})
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer func() {
		if err := lock.Release(); err != nil {
			log.Warn().Str("path", cleanPath).Err(err).Msg("failed to release durable disk lock")
		}
	}()

	return fn()
}

func (s *Worker) snapshotDurableDiskMount(ctx context.Context, request *types.ContainerRequest, mount *types.Mount, mode durableDiskSyncMode) (*types.DiskSnapshot, error) {
	if s == nil || s.backendRepoClient == nil || request == nil || mount == nil || mount.DurableDisk == nil {
		return nil, nil
	}
	if err := cleanDurableDiskRuntimeFiles(mount, mount.LocalPath); err != nil {
		return nil, err
	}
	if durableDiskHasPayload(mount.LocalPath) && !durableDiskHasRestorablePayload(mount, mount.LocalPath) {
		return nil, fmt.Errorf("durable disk %q is not ready to snapshot", mount.DurableDisk.Name)
	}

	ctx = s.durableDiskContext(ctx)
	sizeBytes, _ := durableDiskSizeBytes(mount.DurableDisk.Size)

	if err := s.ensureDurableDiskSnapshotStorage(ctx, request); err != nil {
		return nil, err
	}

	store, err := newDurableDiskSnapshotWriteStore(ctx, request)
	if err != nil {
		return nil, err
	}
	parentSnapshot, previousManifest, err := s.latestDurableDiskSnapshotManifest(ctx, request, mount, store)
	if err != nil {
		return nil, err
	}

	generation, err := nextDurableDiskSnapshotGeneration(time.Now().UnixNano(), parentSnapshot)
	if err != nil {
		return nil, err
	}
	snapshot, manifest, err := createDurableDiskDirectorySnapshot(
		ctx,
		store,
		mount.LocalPath,
		durableDiskSnapshotObjectPrefix(mount, generation),
		types.DiskSnapshot{
			DiskName:            mount.DurableDisk.Name,
			Format:              durableDiskSnapshotFormatForMount(request, mount),
			ParentSnapshotId:    durableDiskSnapshotExternalID(parentSnapshot),
			Generation:          generation,
			SizeBytes:           sizeBytes,
			Filesystem:          mount.DurableDisk.Filesystem,
			Driver:              durableDiskDriver(mount.DurableDisk.Driver),
			BucketName:          store.bucket,
			SourcePool:          s.poolName,
			SourceWorkerId:      s.workerId,
			SourceStorageNodeId: s.storageNodeID(),
		},
		defaultDurableDiskSnapshotChunkSize,
		previousManifest,
		mode == durableDiskSyncExplicit,
	)
	if err != nil {
		return nil, err
	}

	if snapshot == nil {
		log.Debug().Str("disk", mount.DurableDisk.Name).Msg("durable disk is unchanged; keeping the last generation")
		return parentSnapshot, nil
	}

	// The manifest upload above is the durability boundary. Publish the
	// generation before marking the local tree clean so observers never see a
	// clean marker for a generation that is absent from the repository.
	resp, err := handleGRPCResponse(s.backendRepoClient.CreateDiskSnapshot(ctx, &pb.CreateDiskSnapshotRequest{
		WorkspaceId: cacheRequestWorkspaceID(request),
		StubId:      cacheRequestStubID(request),
		Snapshot:    durableDiskSnapshotToProto(snapshot),
	}))
	if err != nil {
		return nil, err
	}
	if created := durableDiskSnapshotFromProto(resp.Snapshot); created != nil {
		snapshot = created
	}
	reportDurableDiskProgress(ctx, durableDiskProgressEvent{})

	recordPublishedDurableDiskMarker(mount, snapshot)

	s.reportDurableDiskSnapshotContent(request, snapshot, manifest, 0)
	return snapshot, nil
}

// recordPublishedDurableDiskMarker is deliberately best effort: the remote
// manifest and repository row are the durability fence. A missing local marker
// merely forces the next sync to scan and hash conservatively.
func recordPublishedDurableDiskMarker(mount *types.Mount, snapshot *types.DiskSnapshot) {
	if mount == nil || mount.DurableDisk == nil || snapshot == nil {
		return
	}
	if err := writeDurableDiskMarker(mount.LocalPath, durableDiskMarker{
		Driver:         types.DurableDiskDriverSnapshot,
		State:          durableDiskStateClean,
		SnapshotID:     snapshot.ExternalId,
		ManifestDigest: snapshot.ManifestDigest,
		Generation:     snapshot.Generation,
	}); err != nil {
		log.Warn().
			Str("disk", mount.DurableDisk.Name).
			Int64("generation", snapshot.Generation).
			Err(err).
			Msg("failed to record published durable disk generation locally")
	}
}

func (s *Worker) latestDurableDiskSnapshotManifest(ctx context.Context, request *types.ContainerRequest, mount *types.Mount, store durableDiskSnapshotStore) (*types.DiskSnapshot, *types.DiskSnapshotManifest, error) {
	resp, err := handleGRPCResponse(s.backendRepoClient.GetLatestDiskSnapshot(ctx, &pb.GetLatestDiskSnapshotRequest{
		WorkspaceId: cacheRequestWorkspaceID(request),
		DiskName:    mount.DurableDisk.Name,
	}))
	if err != nil {
		if durableDiskSnapshotNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("get latest durable disk snapshot: %w", err)
	}
	if resp == nil || resp.Snapshot == nil {
		return nil, nil, nil
	}
	snapshot := durableDiskSnapshotFromProto(resp.Snapshot)
	manifest, err := loadDurableDiskSnapshotManifest(ctx, store, s.durableDiskSnapshotCacheReader(), snapshot)
	if err != nil {
		return nil, nil, fmt.Errorf("load latest durable disk snapshot manifest: %w", err)
	}
	if manifest == nil {
		return nil, nil, fmt.Errorf("latest durable disk snapshot %q has no manifest", snapshot.ExternalId)
	}
	return snapshot, manifest, nil
}

func durableDiskSnapshotNotFound(err error) bool {
	if err == nil {
		return false
	}
	var notFound *types.ErrDiskSnapshotNotFound
	return errors.As(err, &notFound) || strings.HasPrefix(err.Error(), "disk snapshot not found:")
}

func durableDiskSnapshotExternalID(snapshot *types.DiskSnapshot) string {
	if snapshot == nil {
		return ""
	}
	return snapshot.ExternalId
}

func nextDurableDiskSnapshotGeneration(now int64, parent *types.DiskSnapshot) (int64, error) {
	if parent == nil || now > parent.Generation {
		return now, nil
	}
	if parent.Generation == math.MaxInt64 {
		return 0, fmt.Errorf("durable disk snapshot generation is exhausted")
	}
	return parent.Generation + 1, nil
}

func (s *Worker) restoreDurableDiskSnapshot(ctx context.Context, request *types.ContainerRequest, mount *types.Mount) error {
	if s == nil || s.backendRepoClient == nil || request == nil || mount == nil || mount.DurableDisk == nil {
		return nil
	}
	ctx = s.durableDiskContext(ctx)

	resp, err := handleGRPCResponse(s.backendRepoClient.GetLatestDiskSnapshot(ctx, &pb.GetLatestDiskSnapshotRequest{
		WorkspaceId: cacheRequestWorkspaceID(request),
		DiskName:    mount.DurableDisk.Name,
	}))
	if err != nil {
		return fmt.Errorf("get latest durable disk snapshot: %w", err)
	}
	snapshot := durableDiskSnapshotFromProto(resp.Snapshot)
	if snapshot == nil || snapshot.ManifestKey == "" {
		// Seed only disks without their own snapshot history.
		seed, err := s.seedDurableDiskSnapshot(ctx, request, mount)
		if err != nil {
			return err
		}
		snapshot = seed
	}
	if snapshot == nil || snapshot.ManifestKey == "" {
		if durableDiskHasRestorablePayload(mount, mount.LocalPath) {
			return nil
		}
		if durableDiskHasPayload(mount.LocalPath) {
			return fmt.Errorf("durable disk %q has an active or incomplete local payload", mount.DurableDisk.Name)
		}
		return nil
	}
	if !types.IsDiskSnapshotFilesystemFormat(snapshot.Format) {
		return fmt.Errorf("durable disk snapshot %s has unsupported filesystem format %q", snapshot.ExternalId, snapshot.Format)
	}
	if durableDiskHasRestorablePayload(mount, mount.LocalPath) {
		if durableDiskShouldKeepLocalPayload(readDurableDiskMarker(mount.LocalPath), snapshot) {
			return nil
		}
	} else if durableDiskHasPayload(mount.LocalPath) {
		return fmt.Errorf("durable disk %q has an active or incomplete local payload", mount.DurableDisk.Name)
	}

	ctx, cancel := context.WithTimeout(ctx, durableDiskTransferTimeout(max(snapshot.LogicalSizeBytes, snapshot.StoredSizeBytes)))
	defer cancel()

	if !snapshot.Public {
		if err := s.ensureDurableDiskSnapshotStorage(ctx, request); err != nil {
			return err
		}
	}
	store, err := newDurableDiskSnapshotReadStore(ctx, request, snapshot, s.backendRepoClient)
	if err != nil {
		return err
	}
	startedAt := time.Now()
	manifest, err := restoreDurableDiskDirectorySnapshotWithCache(ctx, store, s.durableDiskSnapshotCacheReader(), snapshot.ManifestKey, snapshot.ManifestDigest, snapshot.ManifestSizeBytes, mount.LocalPath)
	if err != nil {
		return fmt.Errorf("restore durable disk snapshot %s: %w", snapshot.ExternalId, err)
	}
	if len(manifest.Files) > 0 && !durableDiskHasRestorablePayload(mount, mount.LocalPath) {
		_ = os.RemoveAll(mount.LocalPath)
		return fmt.Errorf("restore durable disk snapshot %s produced an invalid payload", snapshot.ExternalId)
	}
	s.reportDurableDiskSnapshotContent(request, snapshot, manifest, 0)
	log.Info().
		Str("disk", mount.DurableDisk.Name).
		Int64("bytes", snapshot.LogicalSizeBytes).
		Dur("elapsed", time.Since(startedAt)).
		Msg("restored durable disk snapshot")
	return writeDurableDiskMarker(mount.LocalPath, durableDiskMarker{
		Driver:         types.DurableDiskDriverSnapshot,
		State:          durableDiskStateClean,
		SnapshotID:     snapshot.ExternalId,
		ManifestDigest: snapshot.ManifestDigest,
		Generation:     snapshot.Generation,
	})
}

func (s *Worker) seedDurableDiskSnapshot(ctx context.Context, request *types.ContainerRequest, mount *types.Mount) (*types.DiskSnapshot, error) {
	sourceID := mount.DurableDisk.SourceSnapshotId
	if sourceID == "" {
		return nil, nil
	}

	resp, err := handleGRPCResponse(s.backendRepoClient.GetDiskSnapshot(ctx, &pb.GetDiskSnapshotRequest{
		WorkspaceId: cacheRequestWorkspaceID(request),
		SnapshotId:  sourceID,
	}))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if durableDiskHasRestorablePayload(mount, mount.LocalPath) {
			log.Warn().
				Str("disk", mount.DurableDisk.Name).
				Str("source_snapshot_id", sourceID).
				Err(err).
				Msg("unable to resolve durable disk source snapshot; keeping existing local state")
			return nil, nil
		}
		log.Warn().
			Str("disk", mount.DurableDisk.Name).
			Str("source_snapshot_id", sourceID).
			Err(err).
			Msg("unable to resolve durable disk source snapshot")
		return nil, fmt.Errorf("resolve durable disk source snapshot %s: %w", sourceID, err)
	}

	seed := durableDiskSnapshotFromProto(resp.Snapshot)
	if seed == nil || seed.ManifestKey == "" {
		log.Warn().
			Str("disk", mount.DurableDisk.Name).
			Str("source_snapshot_id", sourceID).
			Msg("durable disk source snapshot is missing")
		return nil, fmt.Errorf("durable disk source snapshot %s is missing", sourceID)
	}

	log.Info().
		Str("disk", mount.DurableDisk.Name).
		Str("source_snapshot_id", sourceID).
		Str("source_disk", seed.DiskName).
		Msg("seeding durable disk from another disk's snapshot")
	return seed, nil
}

func durableDiskTransferTimeout(sizeBytes int64) time.Duration {
	// Allow two minutes of overhead plus one second per 16 MiB.
	seconds := min(max(sizeBytes, 0)/(16<<20), int64((time.Hour-2*time.Minute)/time.Second))
	return max(5*time.Minute, 2*time.Minute+time.Duration(seconds)*time.Second)
}

func (s *Worker) ensureDurableDiskSnapshotStorage(ctx context.Context, request *types.ContainerRequest) error {
	if request == nil || workspaceStorageDownloadAvailable(request.Workspace.Storage) {
		return nil
	}
	if !request.Workspace.StorageAvailable() {
		return fmt.Errorf("workspace storage is required for durable disk snapshots")
	}
	if s == nil || s.workerRepoClient == nil {
		return fmt.Errorf("worker repository client is required for durable disk snapshot credentials")
	}

	resp, err := handleGRPCResponse(s.workerRepoClient.GetContainerRuntimeCredentials(ctx, &pb.GetContainerRuntimeCredentialsRequest{
		WorkspaceId:      cacheRequestWorkspaceID(request),
		StubId:           cacheRequestStubID(request),
		ContainerId:      request.ContainerId,
		WorkspaceStorage: true,
	}))
	if err != nil {
		return fmt.Errorf("hydrate durable disk snapshot storage credentials: %w", err)
	}
	applyRuntimeCredentials(request, resp)
	if !workspaceStorageDownloadAvailable(request.Workspace.Storage) {
		return fmt.Errorf("workspace storage credentials are required for durable disk snapshots")
	}
	return nil
}

func durableDiskSnapshotFormatForMount(request *types.ContainerRequest, mount *types.Mount) string {
	config := requestStubConfig(request)
	if config == nil || config.EffectiveDatabaseConfig() == nil {
		return types.DiskSnapshotFormatDirV1
	}

	database := config.EffectiveDatabaseConfig()
	switch {
	case database.IsPostgres():
		return types.DiskSnapshotFormatPostgresWalV1
	case database.IsRedisCompatible():
		return types.DiskSnapshotFormatRedisAOFV1
	default:
		return types.DiskSnapshotFormatDirV1
	}
}

func (s *Worker) durableDiskSnapshotCacheReader() durableDiskSnapshotCacheReader {
	if s == nil || s.cacheManager == nil || s.cacheManager.client == nil {
		return nil
	}
	return s.cacheManager.client
}

// The workspace-bucket layout for durable disk artifacts:
// durable-disks/<disk>/snapshots/<generation>/<attempt>/manifest.json for
// manifests and durable-disks/<disk>/chunks/<hash> for content-addressed
// chunks.
const (
	durableDiskObjectRoot       = "durable-disks"
	durableDiskManifestFileName = "manifest.json"
)

func durableDiskSnapshotObjectPrefix(mount *types.Mount, generation int64) string {
	// Generations order repository rows, but two workers can legitimately choose
	// the same one after reading the same parent. Give each publish attempt its
	// own manifest directory; chunks remain content-addressed and shared.
	return path.Join(
		durableDiskObjectRoot,
		types.SafeDurableDiskName(mount.DurableDisk.Name),
		"snapshots",
		strconv.FormatInt(generation, 10),
		uuid.NewString(),
	)
}

// durableDiskChunkPrefix is where a disk's content-addressed chunks live,
// shared by every generation of the disk.
func durableDiskChunkPrefix(mount *types.Mount) string {
	return path.Join(durableDiskObjectRoot, types.SafeDurableDiskName(mount.DurableDisk.Name), "chunks")
}

// reportDurableDiskSnapshotContent registers a snapshot's manifest and chunks
// as required content. A non-zero chainGeneration overrides the generation the
// items are tagged with (see reportQcowChainContent).
func (s *Worker) reportDurableDiskSnapshotContent(request *types.ContainerRequest, snapshot *types.DiskSnapshot, manifest *types.DiskSnapshotManifest, chainGeneration int64) {
	if s == nil || s.cacheManager == nil || request == nil || snapshot == nil || manifest == nil {
		return
	}
	reporter := s.cacheManager.ContentReporter()
	if reporter == nil {
		return
	}
	items := durableDiskSnapshotRequiredContentItems(snapshot, manifest)
	if len(items) == 0 {
		return
	}
	if chainGeneration > 0 {
		for i := range items {
			items[i].SnapshotGeneration = chainGeneration
		}
	}
	reporter.reportItems(cacheRequestWorkspaceID(request), cacheRequestStubID(request), types.CacheContentKindDiskSnapshot, items)
}

func durableDiskSnapshotRequiredContentItems(snapshot *types.DiskSnapshot, manifest *types.DiskSnapshotManifest) []types.CacheRequiredContentItem {
	if snapshot == nil || manifest == nil || snapshot.BucketName == "" {
		return nil
	}
	items := make([]types.CacheRequiredContentItem, 0, len(manifest.Files)+1)
	add := func(hash, key string, size int64) {
		hash = strings.TrimPrefix(hash, "sha256:")
		if hash == "" || key == "" || size <= 0 {
			return
		}
		items = append(items, types.CacheRequiredContentItem{
			Hash:               hash,
			RoutingKey:         hash,
			ExpectedHash:       hash,
			SizeBytes:          size,
			Source:             key,
			SourceBucket:       snapshot.BucketName,
			Kind:               types.CacheContentKindDiskSnapshot,
			DiskName:           snapshot.DiskName,
			SnapshotGeneration: snapshot.Generation,
		})
	}
	add(snapshot.ManifestDigest, snapshot.ManifestKey, snapshot.ManifestSizeBytes)
	for _, file := range manifest.Files {
		for _, chunk := range file.Chunks {
			add(chunk.Digest, chunk.ObjectKey, chunk.SizeBytes)
		}
	}
	return items
}

func durableDiskHasPayload(path string) bool {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if entry.Name() == durableDiskMarkerFile {
			continue
		}
		return true
	}
	return false
}

func durableDiskHasRestorablePayload(mount *types.Mount, diskPath string) bool {
	if !durableDiskHasPayload(diskPath) {
		return false
	}
	return !durableDiskHasIncompletePostgresPayload(mount, diskPath)
}

func durableDiskHasIncompletePostgresPayload(mount *types.Mount, diskPath string) bool {
	if mount == nil || mount.MountPath != types.PostgresDataMountPath {
		return false
	}
	pgDataPath := filepath.Join(diskPath, "pgdata")
	if _, err := os.Stat(filepath.Join(pgDataPath, "PG_VERSION")); err != nil {
		return false
	}
	if _, err := os.Stat(filepath.Join(pgDataPath, "global", "pg_control")); err != nil {
		return true
	}
	return false
}

func cleanDurableDiskRuntimeFiles(mount *types.Mount, diskPath string) error {
	if mount == nil || mount.MountPath != types.PostgresDataMountPath {
		return nil
	}
	if err := os.Remove(filepath.Join(diskPath, "pgdata", "postmaster.pid")); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale postgres pid file: %w", err)
	}
	return nil
}

func readDurableDiskMarker(path string) durableDiskMarker {
	data, err := os.ReadFile(filepath.Join(path, durableDiskMarkerFile))
	if err != nil {
		return durableDiskMarker{}
	}
	marker := durableDiskMarker{}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		switch key {
		case "driver":
			marker.Driver = value
		case "state":
			marker.State = value
		case "snapshot_id":
			marker.SnapshotID = value
		case "manifest_digest":
			marker.ManifestDigest = value
		case "generation":
			marker.Generation, _ = strconv.ParseInt(value, 10, 64)
		}
	}
	return marker
}

func durableDiskShouldKeepLocalPayload(marker durableDiskMarker, snapshot *types.DiskSnapshot) bool {
	if snapshot == nil {
		return true
	}
	switch marker.State {
	case durableDiskStateDirty:
		return marker.Generation >= snapshot.Generation
	case durableDiskStateClean:
		return marker.Generation == snapshot.Generation && marker.ManifestDigest == snapshot.ManifestDigest
	default:
		return false
	}
}

func writeDurableDiskMarker(path string, marker durableDiskMarker) error {
	if marker.Driver == "" {
		marker.Driver = types.DurableDiskDriverSnapshot
	}
	var b strings.Builder
	fmt.Fprintf(&b, "driver=%s\nstate=%s\ngeneration=%d\n", marker.Driver, marker.State, marker.Generation)
	if marker.SnapshotID != "" {
		fmt.Fprintf(&b, "snapshot_id=%s\n", marker.SnapshotID)
	}
	if marker.ManifestDigest != "" {
		fmt.Fprintf(&b, "manifest_digest=%s\n", marker.ManifestDigest)
	}
	if err := os.WriteFile(filepath.Join(path, durableDiskMarkerFile), []byte(b.String()), 0644); err != nil {
		return fmt.Errorf("write durable disk marker %s: %w", path, err)
	}
	return nil
}

func durableDiskSizeBytes(size string) (int64, error) {
	size = strings.TrimSpace(size)
	if size == "" {
		return 0, fmt.Errorf("size is required")
	}

	units := []struct {
		suffix string
		factor int64
	}{
		{"Ti", 1 << 40},
		{"Gi", 1 << 30},
		{"Mi", 1 << 20},
		{"Ki", 1 << 10},
		{"T", 1000 * 1000 * 1000 * 1000},
		{"G", 1000 * 1000 * 1000},
		{"M", 1000 * 1000},
		{"K", 1000},
	}

	for _, unit := range units {
		if strings.HasSuffix(size, unit.suffix) {
			n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(size, unit.suffix)), 10, 64)
			if err != nil || n <= 0 {
				return 0, fmt.Errorf("invalid size")
			}
			return n * unit.factor, nil
		}
	}

	n, err := strconv.ParseInt(size, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid size")
	}
	return n, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
