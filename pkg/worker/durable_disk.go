package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	durableDiskDriverEnv    = "BETA9_DURABLE_DISK_DRIVER"
	durableDiskMarkerFile   = ".beta9-durable-disk"
	durableDiskLockDir      = ".beta9-durable-disk-locks"
	durableDiskReplicaDir   = ".beta9-durable-disk-replicas"
	defaultDevReplicaCopies = 3
	devDurableDiskLockWait  = 10 * time.Minute
)

func (s *Worker) prepareDurableDiskMount(request *types.ContainerRequest, mount *types.Mount) error {
	if mount == nil || mount.DurableDisk == nil {
		return fmt.Errorf("durable disk mount is missing metadata")
	}
	if mount.LocalPath == "" {
		return fmt.Errorf("durable disk %q has no local path", mount.DurableDisk.Name)
	}

	driver := durableDiskDriver(mount.DurableDisk.Driver)
	switch driver {
	case "", types.DurableDiskDriverDev:
		return withDevDurableDiskLock(mount, func() error {
			if s != nil {
				if err := s.restoreDevDurableDiskSnapshot(request, mount); err != nil {
					return err
				}
			}
			return prepareDevDurableDiskMount(mount)
		})
	default:
		return fmt.Errorf("durable disk %q requested unsupported driver %q", mount.DurableDisk.Name, driver)
	}
}

func durableDiskDriver(configured string) string {
	if driver := types.NormalizeDurableDiskDriver(configured); driver != "" {
		return driver
	}
	return types.NormalizeDurableDiskDriver(os.Getenv(durableDiskDriverEnv))
}

func prepareDevDurableDiskMount(mount *types.Mount) error {
	if err := restoreDevDurableDiskPrimary(mount); err != nil {
		return err
	}
	if err := cleanDevDurableDiskRuntimeFiles(mount, mount.LocalPath); err != nil {
		return err
	}
	if err := os.MkdirAll(mount.LocalPath, 0755); err != nil {
		return fmt.Errorf("create dev durable disk path %s: %w", mount.LocalPath, err)
	}
	if err := writeDevDurableDiskMarker(mount.LocalPath, mount); err != nil {
		return err
	}

	return ensureDevDurableDiskReplicas(mount)
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
		case "", types.DurableDiskDriverDev:
			err := withDevDurableDiskLock(mount, func() error {
				if err := s.snapshotDevDurableDiskMount(request, mount); err != nil {
					if s != nil && s.backendRepoClient != nil {
						return fmt.Errorf("snapshot: %w", err)
					}
					log.Warn().Str("container_id", request.ContainerId).Str("disk", mount.DurableDisk.Name).Err(err).Msg("failed to snapshot dev durable disk")
				} else if s != nil && s.backendRepoClient != nil {
					return nil
				}
				if err := syncDevDurableDiskMount(mount); err != nil {
					return fmt.Errorf("sync replicas: %w", err)
				}
				return nil
			})
			if err != nil {
				log.Warn().Str("container_id", request.ContainerId).Str("disk", mount.DurableDisk.Name).Err(err).Msg("failed to sync dev durable disk")
				syncErrs = append(syncErrs, err)
			}
		}
	}

	return errors.Join(syncErrs...)
}

func withDevDurableDiskLock(mount *types.Mount, fn func() error) error {
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
		} else if time.Since(start) > devDurableDiskLockWait {
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

func restoreDevDurableDiskPrimary(mount *types.Mount) error {
	if devDurableDiskHasRestorablePayload(mount, mount.LocalPath) {
		return nil
	}
	if devDurableDiskHasPayload(mount.LocalPath) {
		return fmt.Errorf("durable disk %q has an active or incomplete local payload", mount.DurableDisk.Name)
	}

	for _, replicaPath := range devDurableDiskReplicaPaths(mount) {
		if !devDurableDiskHasRestorablePayload(mount, replicaPath) {
			continue
		}

		if err := os.RemoveAll(mount.LocalPath); err != nil {
			return fmt.Errorf("clear dev durable disk primary %s: %w", mount.LocalPath, err)
		}
		if err := copyDurableDiskDir(replicaPath, mount.LocalPath); err != nil {
			return fmt.Errorf("restore dev durable disk primary %s from replica %s: %w", mount.LocalPath, replicaPath, err)
		}
		return writeDevDurableDiskMarker(mount.LocalPath, mount)
	}

	return nil
}

func (s *Worker) snapshotDevDurableDiskMount(request *types.ContainerRequest, mount *types.Mount) error {
	if s == nil || s.backendRepoClient == nil || request == nil || mount == nil || mount.DurableDisk == nil {
		return nil
	}
	if !devDurableDiskHasPayload(mount.LocalPath) {
		return nil
	}
	if err := cleanDevDurableDiskRuntimeFiles(mount, mount.LocalPath); err != nil {
		return err
	}
	if !devDurableDiskHasRestorablePayload(mount, mount.LocalPath) {
		return fmt.Errorf("durable disk %q is not ready to snapshot", mount.DurableDisk.Name)
	}

	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

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

	_, err = handleGRPCResponse(s.backendRepoClient.CreateDiskSnapshot(ctx, &pb.CreateDiskSnapshotRequest{
		WorkspaceId: cacheRequestWorkspaceID(request),
		StubId:      cacheRequestStubID(request),
		Snapshot:    durableDiskSnapshotToProto(snapshot),
	}))
	if err != nil {
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

func (s *Worker) restoreDevDurableDiskSnapshot(request *types.ContainerRequest, mount *types.Mount) error {
	if s == nil || s.backendRepoClient == nil || request == nil || mount == nil || mount.DurableDisk == nil {
		return nil
	}
	if devDurableDiskHasRestorablePayload(mount, mount.LocalPath) || devDurableDiskReplicaHasRestorablePayload(mount) {
		return nil
	}
	if devDurableDiskHasPayload(mount.LocalPath) {
		return fmt.Errorf("durable disk %q has an active or incomplete local payload", mount.DurableDisk.Name)
	}

	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	resp, err := handleGRPCResponse(s.backendRepoClient.GetLatestDiskSnapshot(ctx, &pb.GetLatestDiskSnapshotRequest{
		WorkspaceId: cacheRequestWorkspaceID(request),
		DiskName:    mount.DurableDisk.Name,
	}))
	if err != nil {
		return fmt.Errorf("get latest durable disk snapshot: %w", err)
	}
	snapshot := durableDiskSnapshotFromProto(resp.Snapshot)
	if snapshot == nil || snapshot.ManifestKey == "" {
		return nil
	}
	if !types.IsDiskSnapshotFilesystemFormat(snapshot.Format) {
		return fmt.Errorf("durable disk snapshot %s has unsupported filesystem format %q", snapshot.ExternalId, snapshot.Format)
	}

	store, err := newDurableDiskSnapshotBucketStore(ctx, request, mount.DurableDisk.Name, snapshot.BucketName, false)
	if err != nil {
		return err
	}
	if _, err := restoreDurableDiskDirectorySnapshotWithCache(ctx, store, s.durableDiskSnapshotCacheReader(), snapshot.ManifestKey, snapshot.ManifestDigest, snapshot.ManifestSizeBytes, mount.LocalPath); err != nil {
		return fmt.Errorf("restore durable disk snapshot %s: %w", snapshot.ExternalId, err)
	}
	if !devDurableDiskHasRestorablePayload(mount, mount.LocalPath) {
		_ = os.RemoveAll(mount.LocalPath)
		return fmt.Errorf("restore durable disk snapshot %s produced an invalid payload", snapshot.ExternalId)
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

func devDurableDiskReplicaHasRestorablePayload(mount *types.Mount) bool {
	for _, replicaPath := range devDurableDiskReplicaPaths(mount) {
		if devDurableDiskHasRestorablePayload(mount, replicaPath) {
			return true
		}
	}
	return false
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

func ensureDevDurableDiskReplicas(mount *types.Mount) error {
	for _, replicaPath := range devDurableDiskReplicaPaths(mount) {
		if devDurableDiskHasPayload(mount.LocalPath) {
			if err := os.MkdirAll(replicaPath, 0755); err != nil {
				return fmt.Errorf("create dev durable disk replica %s: %w", replicaPath, err)
			}
			if err := writeDevDurableDiskMarker(replicaPath, mount); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(replicaPath, 0755); err != nil {
			return fmt.Errorf("create dev durable disk replica %s: %w", replicaPath, err)
		}
		if err := writeDevDurableDiskMarker(replicaPath, mount); err != nil {
			return err
		}
	}
	return nil
}

func syncDevDurableDiskMount(mount *types.Mount) error {
	if !devDurableDiskHasPayload(mount.LocalPath) {
		return nil
	}

	for _, replicaPath := range devDurableDiskReplicaPaths(mount) {
		if err := copyDurableDiskDirAtomic(mount.LocalPath, replicaPath); err != nil {
			return fmt.Errorf("sync dev durable disk replica %s: %w", replicaPath, err)
		}
	}
	return nil
}

func devDurableDiskReplicaPaths(mount *types.Mount) []string {
	if mount == nil || mount.DurableDisk == nil {
		return nil
	}

	replicas := int(mount.DurableDisk.Replicas)
	if replicas == 0 {
		replicas = defaultDevReplicaCopies
	}
	if replicas <= 1 {
		return nil
	}

	base := filepath.Base(filepath.Clean(mount.LocalPath))
	root := filepath.Join(filepath.Dir(filepath.Clean(mount.LocalPath)), durableDiskReplicaDir, base)
	paths := make([]string, 0, replicas-1)
	for replica := 1; replica < replicas; replica++ {
		paths = append(paths, filepath.Join(root, fmt.Sprintf("replica-%d", replica)))
	}
	return paths
}

func devDurableDiskHasPayload(path string) bool {
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

func devDurableDiskHasRestorablePayload(mount *types.Mount, diskPath string) bool {
	if !devDurableDiskHasPayload(diskPath) {
		return false
	}
	return !devDurableDiskHasIncompletePostgresPayload(mount, diskPath)
}

func devDurableDiskHasIncompletePostgresPayload(mount *types.Mount, diskPath string) bool {
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

func cleanDevDurableDiskRuntimeFiles(mount *types.Mount, diskPath string) error {
	if mount == nil || mount.MountPath != types.PostgresDataMountPath {
		return nil
	}
	if err := os.Remove(filepath.Join(diskPath, "pgdata", "postmaster.pid")); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale postgres pid file: %w", err)
	}
	return nil
}

func writeDevDurableDiskMarker(path string, mount *types.Mount) error {
	replicas := uint32(defaultDevReplicaCopies)
	if mount != nil && mount.DurableDisk != nil && mount.DurableDisk.Replicas > 0 {
		replicas = mount.DurableDisk.Replicas
	}

	marker := fmt.Sprintf("driver=dev\nreplicas=%d\n", replicas)
	if err := os.WriteFile(filepath.Join(path, durableDiskMarkerFile), []byte(marker), 0644); err != nil {
		return fmt.Errorf("write dev durable disk marker %s: %w", path, err)
	}
	return nil
}

func copyDurableDiskDirAtomic(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	suffix := fmt.Sprintf("%d.%d", os.Getpid(), time.Now().UnixNano())
	tmp := fmt.Sprintf("%s.tmp.%s", dst, suffix)
	backup := fmt.Sprintf("%s.old.%s", dst, suffix)
	movedExisting := false
	defer os.RemoveAll(tmp)

	if err := copyDurableDiskDir(src, tmp); err != nil {
		return err
	}

	if _, err := os.Lstat(dst); err == nil {
		if err := os.Rename(dst, backup); err != nil {
			return err
		}
		movedExisting = true
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.Rename(tmp, dst); err != nil {
		if movedExisting {
			_ = os.Rename(backup, dst)
		}
		return err
	}

	if movedExisting {
		_ = os.RemoveAll(backup)
	}
	return nil
}

func copyDurableDiskDir(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", src)
	}
	if err := os.MkdirAll(dst, info.Mode().Perm()); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		info, err := os.Lstat(srcPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(srcPath)
			if err != nil {
				return err
			}
			if err := os.Symlink(target, dstPath); err != nil {
				return err
			}
		case info.IsDir():
			if err := copyDurableDiskDir(srcPath, dstPath); err != nil {
				return err
			}
		case info.Mode().IsRegular():
			if err := copyDurableDiskFile(srcPath, dstPath, info.Mode().Perm()); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyDurableDiskFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
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
