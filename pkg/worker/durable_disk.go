package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

const (
	durableDiskDriverEnv  = "BETA9_DURABLE_DISK_DRIVER"
	durableDiskMarkerFile = ".beta9-durable-disk"
	durableDiskLockDir    = ".beta9-durable-disk-locks"
	durableDiskLockWait   = 10 * time.Minute

	durableDiskStateClean = "clean"
	durableDiskStateDirty = "dirty"
)

type durableDiskMarker struct {
	Driver         string
	State          string
	SnapshotID     string
	ManifestDigest string
	Generation     int64
}

func (s *Worker) prepareDurableDiskMount(request *types.ContainerRequest, mount *types.Mount) error {
	if mount == nil || mount.DurableDisk == nil {
		return fmt.Errorf("durable disk mount is missing metadata")
	}
	if mount.LocalPath == "" {
		return fmt.Errorf("durable disk %q has no local path", mount.DurableDisk.Name)
	}

	driver := durableDiskDriver(mount.DurableDisk.Driver)
	switch driver {
	case types.DurableDiskDriverSnapshot:
		return withDurableDiskLock(mount, func() error {
			if s != nil {
				if err := s.restoreDurableDiskSnapshot(request, mount); err != nil {
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

func (s *Worker) syncDurableDiskMounts(request *types.ContainerRequest) error {
	if request == nil {
		return nil
	}

	var syncErrs []error
	for i := range request.Mounts {
		mount := &request.Mounts[i]
		if mount == nil || mount.DurableDisk == nil {
			continue
		}
		switch durableDiskDriver(mount.DurableDisk.Driver) {
		case types.DurableDiskDriverSnapshot:
			err := withDurableDiskLock(mount, func() error {
				if err := s.snapshotDurableDiskMount(request, mount); err != nil {
					return fmt.Errorf("snapshot: %w", err)
				}
				return nil
			})
			if err != nil {
				log.Warn().Str("container_id", request.ContainerId).Str("disk", mount.DurableDisk.Name).Err(err).Msg("failed to sync durable disk")
				syncErrs = append(syncErrs, err)
			}
		}
	}

	return errors.Join(syncErrs...)
}

func withDurableDiskLock(mount *types.Mount, fn func() error) error {
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
		if err := lock.Acquire(); err == nil {
			break
		} else if time.Since(start) > durableDiskLockWait {
			return fmt.Errorf("acquire durable disk lock %s: %w", cleanPath, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	defer func() {
		if err := lock.Release(); err != nil {
			log.Warn().Str("path", cleanPath).Err(err).Msg("failed to release durable disk lock")
		}
	}()

	return fn()
}

func (s *Worker) snapshotDurableDiskMount(request *types.ContainerRequest, mount *types.Mount) error {
	if s == nil || s.backendRepoClient == nil || request == nil || mount == nil || mount.DurableDisk == nil {
		return nil
	}
	if err := cleanDurableDiskRuntimeFiles(mount, mount.LocalPath); err != nil {
		return err
	}
	if durableDiskHasPayload(mount.LocalPath) && !durableDiskHasRestorablePayload(mount, mount.LocalPath) {
		return fmt.Errorf("durable disk %q is not ready to snapshot", mount.DurableDisk.Name)
	}

	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	if err := s.ensureDurableDiskSnapshotStorage(ctx, request); err != nil {
		return err
	}

	store, err := newDurableDiskSnapshotBucketStore(ctx, request, mount.DurableDisk.Name, "", true)
	if err != nil {
		return err
	}
	parentSnapshot, previousManifest := s.latestDurableDiskSnapshotManifest(ctx, request, mount, store)

	generation := time.Now().UnixNano()
	sizeBytes, _ := durableDiskSizeBytes(mount.DurableDisk.Size)
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
	)
	if err != nil {
		return err
	}

	resp, err := handleGRPCResponse(s.backendRepoClient.CreateDiskSnapshot(ctx, &pb.CreateDiskSnapshotRequest{
		WorkspaceId: cacheRequestWorkspaceID(request),
		StubId:      cacheRequestStubID(request),
		Snapshot:    durableDiskSnapshotToProto(snapshot),
	}))
	if err != nil {
		return err
	}
	if created := durableDiskSnapshotFromProto(resp.Snapshot); created != nil {
		snapshot = created
	}

	if err := writeDurableDiskMarker(mount.LocalPath, durableDiskMarker{
		Driver:         types.DurableDiskDriverSnapshot,
		State:          durableDiskStateClean,
		SnapshotID:     snapshot.ExternalId,
		ManifestDigest: snapshot.ManifestDigest,
		Generation:     snapshot.Generation,
	}); err != nil {
		return err
	}

	s.reportDurableDiskSnapshotContent(request, snapshot, manifest)
	return nil
}

func (s *Worker) latestDurableDiskSnapshotManifest(ctx context.Context, request *types.ContainerRequest, mount *types.Mount, store durableDiskSnapshotStore) (*types.DiskSnapshot, *types.DiskSnapshotManifest) {
	resp, err := handleGRPCResponse(s.backendRepoClient.GetLatestDiskSnapshot(ctx, &pb.GetLatestDiskSnapshotRequest{
		WorkspaceId: cacheRequestWorkspaceID(request),
		DiskName:    mount.DurableDisk.Name,
	}))
	if err != nil || resp == nil || resp.Snapshot == nil {
		return nil, nil
	}
	snapshot := durableDiskSnapshotFromProto(resp.Snapshot)
	manifest, err := loadDurableDiskSnapshotManifest(ctx, store, s.durableDiskSnapshotCacheReader(), snapshot)
	if err != nil || manifest == nil || len(manifest.Files) == 0 {
		return snapshot, nil
	}
	return snapshot, manifest
}

func durableDiskSnapshotExternalID(snapshot *types.DiskSnapshot) string {
	if snapshot == nil {
		return ""
	}
	return snapshot.ExternalId
}

func (s *Worker) restoreDurableDiskSnapshot(request *types.ContainerRequest, mount *types.Mount) error {
	if s == nil || s.backendRepoClient == nil || request == nil || mount == nil || mount.DurableDisk == nil {
		return nil
	}
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	if err := s.ensureDurableDiskSnapshotStorage(ctx, request); err != nil {
		return err
	}

	resp, err := handleGRPCResponse(s.backendRepoClient.GetLatestDiskSnapshot(ctx, &pb.GetLatestDiskSnapshotRequest{
		WorkspaceId: cacheRequestWorkspaceID(request),
		DiskName:    mount.DurableDisk.Name,
	}))
	if err != nil {
		return fmt.Errorf("get latest durable disk snapshot: %w", err)
	}
	snapshot := durableDiskSnapshotFromProto(resp.Snapshot)
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

	store, err := newDurableDiskSnapshotBucketStore(ctx, request, mount.DurableDisk.Name, snapshot.BucketName, false)
	if err != nil {
		return err
	}
	manifest, err := restoreDurableDiskDirectorySnapshotWithCache(ctx, store, s.durableDiskSnapshotCacheReader(), snapshot.ManifestKey, snapshot.ManifestDigest, snapshot.ManifestSizeBytes, mount.LocalPath)
	if err != nil {
		return fmt.Errorf("restore durable disk snapshot %s: %w", snapshot.ExternalId, err)
	}
	if len(manifest.Files) > 0 && !durableDiskHasRestorablePayload(mount, mount.LocalPath) {
		_ = os.RemoveAll(mount.LocalPath)
		return fmt.Errorf("restore durable disk snapshot %s produced an invalid payload", snapshot.ExternalId)
	}
	return writeDurableDiskMarker(mount.LocalPath, durableDiskMarker{
		Driver:         types.DurableDiskDriverSnapshot,
		State:          durableDiskStateClean,
		SnapshotID:     snapshot.ExternalId,
		ManifestDigest: snapshot.ManifestDigest,
		Generation:     snapshot.Generation,
	})
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

func durableDiskSnapshotObjectPrefix(mount *types.Mount, generation int64) string {
	return path.Join(
		"durable-disks",
		types.SafeDurableDiskName(mount.DurableDisk.Name),
		"snapshots",
		strconv.FormatInt(generation, 10),
	)
}

func (s *Worker) reportDurableDiskSnapshotContent(request *types.ContainerRequest, snapshot *types.DiskSnapshot, manifest *types.DiskSnapshotManifest) {
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
	reporter.reportItems(cacheRequestWorkspaceID(request), cacheRequestStubID(request), types.CacheContentKindDiskSnapshot, items)
	reporter.flush()
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
			Hash:         hash,
			RoutingKey:   hash,
			ExpectedHash: hash,
			SizeBytes:    size,
			Source:       key,
			SourceBucket: snapshot.BucketName,
			Kind:         types.CacheContentKindDiskSnapshot,
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
