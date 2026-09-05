package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	abstractionscommon "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

type ContainerMountManager struct {
	mountPointMounts *common.SafeMap[[]containerMountPoint]
	storageConfig    types.StorageConfig
	codeCacheRoot    string
	codeCacheGroup   singleflight.Group
	readyOutputDirs  sync.Map
}

type containerMountPoint struct {
	localPath string
	storage   storage.Storage
}

const (
	workspaceBindMountRetryBudget       = 3 * time.Minute
	workspaceBindMountRetryInitialDelay = 250 * time.Millisecond
	workspaceBindMountRetryMaxDelay     = 5 * time.Second
	slowBindMountSourceDirThreshold     = 250 * time.Millisecond
)

type bindMountSourceDirOps struct {
	mkdirAll    func(string, os.FileMode) error
	now         func() time.Time
	wait        func(context.Context, time.Duration) error
	jitter      func(time.Duration) time.Duration
	retryBudget time.Duration
}

func defaultBindMountSourceDirOps() bindMountSourceDirOps {
	return bindMountSourceDirOps{
		mkdirAll:    mkdirBindSource,
		now:         time.Now,
		wait:        waitForRetry,
		jitter:      jitterBindMountRetryDelay,
		retryBudget: workspaceBindMountRetryBudget,
	}
}

func NewContainerMountManager(config types.AppConfig, poolConfig ...types.WorkerPoolConfig) *ContainerMountManager {
	return &ContainerMountManager{
		mountPointMounts: common.NewSafeMap[[]containerMountPoint](),
		storageConfig:    config.Storage,
		codeCacheRoot:    stubCodeCacheRoot(config, poolConfig...),
	}
}

func stubCodeCacheRoot(config types.AppConfig, poolConfigs ...types.WorkerPoolConfig) string {
	cacheEnabled := config.Cache.Enabled && config.Worker.CacheEnabled
	diskEnabled := config.Cache.Disk.Enabled
	mountPath := config.Cache.Disk.MountPath

	if len(poolConfigs) > 0 {
		poolCache := poolConfigs[0].Cache
		if poolCache.Enabled != nil {
			cacheEnabled = cacheEnabled && *poolCache.Enabled
		}
		if poolCache.Disk.Enabled != nil {
			diskEnabled = *poolCache.Disk.Enabled
		}
		if poolCache.Disk.MountPath != "" {
			mountPath = poolCache.Disk.MountPath
		}
	}

	if cacheEnabled && diskEnabled && mountPath != "" {
		return filepath.Join(filepath.Clean(mountPath), "stub-code")
	}

	return filepath.Join(os.TempDir(), "beta9-stub-code-cache")
}

// SetupContainerMounts initializes any external storage for a container
func (c *ContainerMountManager) SetupContainerMounts(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger) error {
	for i, m := range request.Mounts {
		if m.MountPath == types.WorkerUserCodeVolume {
			localPath, err := c.setupUserCodeMount(ctx, request)
			if err != nil {
				return err
			}
			m.LocalPath = localPath
			request.Mounts[i].LocalPath = m.LocalPath
		}

		// NOTE: The following adjustments to local paths are part of a migration to use WorkspaceStorage and can be removed once all existing workspaces are migrated.
		if request.StorageAvailable() {
			switch {
			case strings.HasPrefix(m.MountPath, types.WorkerContainerVolumePath):
				m.LocalPath = strings.Replace(m.LocalPath, path.Join(types.DefaultVolumesPath, request.Workspace.Name), path.Join(c.storageConfig.WorkspaceStorage.BaseMountPath, request.Workspace.Name, types.DefaultVolumesPrefix), 1)
				request.Mounts[i].LocalPath = m.LocalPath

			case strings.HasPrefix(m.MountPath, types.WorkerUserOutputVolume):
				m.LocalPath = strings.Replace(m.LocalPath, path.Join(types.DefaultOutputsPath, request.Workspace.Name), path.Join(c.storageConfig.WorkspaceStorage.BaseMountPath, request.Workspace.Name, types.DefaultOutputsPrefix), 1)
				request.Mounts[i].LocalPath = m.LocalPath

			case strings.HasPrefix(m.LocalPath, types.DefaultVolumesPath):
				m.LocalPath = strings.Replace(m.LocalPath, path.Join(types.DefaultVolumesPath, request.Workspace.Name), path.Join(c.storageConfig.WorkspaceStorage.BaseMountPath, request.Workspace.Name, types.DefaultVolumesPrefix), 1)
				request.Mounts[i].LocalPath = m.LocalPath
			}
		}

		if m.MountType == storage.StorageModeMountPoint && m.MountPointConfig != nil {
			log.Info().
				Str("container_id", request.ContainerId).
				Str("mount_path", m.MountPath).
				Str("bucket", m.MountPointConfig.BucketName).
				Msg("setting up external s3 mount")

			// Add containerId to local mount path for mountpoint storage
			m.LocalPath = path.Join(m.LocalPath, request.ContainerId, m.MountPointConfig.BucketName)
			request.Mounts[i].LocalPath = m.LocalPath

			err := c.setupMountPointS3(request.ContainerId, m)
			if err != nil {
				outputLogger.Info(fmt.Sprintf("failed to setup s3 mount, error: %v\n", err))
				return err
			}
		}
	}

	return nil
}

func (c *ContainerMountManager) ensureBindMountSourceDirs(ctx context.Context, mounts []types.Mount) error {
	return c.ensureBindMountSourceDirsWithOps(ctx, mounts, defaultBindMountSourceDirOps())
}

// ensureBindMountSourceDirsWithOps creates the source directories in
// parallel: most live on network filesystems where each mkdir is a round
// trip, and the paths are independent.
func (c *ContainerMountManager) ensureBindMountSourceDirsWithOps(ctx context.Context, mounts []types.Mount, ops bindMountSourceDirOps) error {
	group, ctx := errgroup.WithContext(ctx)
	for _, mount := range mounts {
		if mount.MountType == storage.StorageModeMountPoint || mount.LocalPath == "" {
			continue
		}
		// Durable disks attach concurrently with this and create their own
		// mountpoints (prepareDurableDiskMounts).
		if mount.MountType == types.StorageModeDurableDisk {
			continue
		}
		if mount.MountPath == types.WorkerUserOutputVolume {
			if _, ready := c.readyOutputDirs.Load(mount.LocalPath); ready {
				continue
			}
		}
		retry := mount.MountType != storage.StorageModeLocal && mount.DurableDisk == nil &&
			pathWithinBase(c.storageConfig.WorkspaceStorage.BaseMountPath, mount.LocalPath)
		group.Go(func() error {
			if err := ensureBindMountSourceDir(ctx, mount.LocalPath, retry, ops); err != nil {
				return fmt.Errorf("create bind mount source %s for %s: %w", mount.LocalPath, mount.MountPath, err)
			}
			if mount.MountPath == types.WorkerUserOutputVolume {
				c.readyOutputDirs.Store(mount.LocalPath, struct{}{})
			}
			return nil
		})
	}
	return group.Wait()
}

// mkdirBindSource creates localPath in one round trip when its parent exists,
// which covers both an already present directory and a fresh one; sources sit
// on network storage, where os.MkdirAll would spend a stat per path element.
func mkdirBindSource(localPath string, perm os.FileMode) error {
	err := os.Mkdir(localPath, perm)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrExist):
		// EEXIST covers a regular file too, which cannot back a bind mount;
		// report ENOTDIR (what the mount itself would fail with) instead of
		// claiming success.
		info, statErr := os.Stat(localPath)
		if statErr != nil {
			return statErr
		}
		if !info.IsDir() {
			return &os.PathError{Op: "mkdir", Path: localPath, Err: syscall.ENOTDIR}
		}
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return os.MkdirAll(localPath, perm)
	default:
		return err
	}
}

func ensureBindMountSourceDir(ctx context.Context, localPath string, retry bool, ops bindMountSourceDirOps) error {
	startedAt := ops.now()
	deadline := startedAt.Add(ops.retryBudget)
	delay := workspaceBindMountRetryInitialDelay
	attempts := 0
	retrying := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		attempts++
		mkdirStart := ops.now()
		err := ops.mkdirAll(localPath, 0755)
		if elapsed := ops.now().Sub(mkdirStart); elapsed > slowBindMountSourceDirThreshold {
			log.Warn().Str("local_path", localPath).Dur("elapsed", elapsed).Err(err).Msg("slow bind mount source create")
		}
		if err == nil {
			if retrying {
				log.Info().Str("local_path", localPath).Int("attempts", attempts).Dur("elapsed", ops.now().Sub(startedAt)).Msg("workspace bind mount source recovered")
			}
			return nil
		}
		if !retry || !errors.Is(err, syscall.EAGAIN) {
			return err
		}

		remaining := deadline.Sub(ops.now())
		if remaining <= 0 {
			return err
		}
		if !retrying {
			retrying = true
			log.Warn().Str("local_path", localPath).Dur("retry_budget", ops.retryBudget).Msg("workspace bind mount source temporarily unavailable; retrying")
		}
		waitDelay := ops.jitter(delay)
		if waitDelay > remaining {
			waitDelay = remaining
		}
		if waitErr := ops.wait(ctx, waitDelay); waitErr != nil {
			return waitErr
		}
		if !ops.now().Before(deadline) {
			return err
		}
		delay = min(2*delay, workspaceBindMountRetryMaxDelay)
	}
}

func jitterBindMountRetryDelay(delay time.Duration) time.Duration {
	half := delay / 2
	return half + time.Duration(rand.Int63n(int64(delay-half)+1))
}

func pathWithinBase(basePath, localPath string) bool {
	if strings.TrimSpace(basePath) == "" {
		return false
	}
	rel, err := filepath.Rel(filepath.Clean(basePath), filepath.Clean(localPath))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func (c *ContainerMountManager) RequiresWorkspaceStorageMount(request *types.ContainerRequest) bool {
	if request == nil || !request.StorageAvailable() {
		return false
	}

	if request.IsBuildRequest() {
		return request.BuildOptions.Dockerfile != nil &&
			request.BuildOptions.BuildCtxObject != nil &&
			!workspaceStorageDownloadAvailable(request.Workspace.Storage)
	}

	directCodeDownload := workspaceStorageDownloadAvailable(request.Workspace.Storage)
	for _, mount := range request.Mounts {
		if mount.MountPath == types.WorkerUserCodeVolume {
			if !directCodeDownload {
				return true
			}
			continue
		}

		if mount.MountType == storage.StorageModeMountPoint {
			continue
		}

		if workspaceStorageMountPath(request, mount) {
			return true
		}
	}

	return false
}

func (c *ContainerMountManager) setupUserCodeMount(ctx context.Context, request *types.ContainerRequest) (string, error) {
	destPath := types.TempContainerWorkspace(request.ContainerId)
	readyPath := filepath.Join(filepath.Dir(destPath), ".workspace-ready")
	if pathExists(destPath) && pathExists(readyPath) {
		return destPath, nil
	}

	if request.Stub.Type.Kind() == types.StubTypeSandbox && abstractionscommon.IsEmptyStubObject(request.Stub.Object) {
		if err := createEmptyContainerWorkspace(destPath, readyPath); err != nil {
			return "", err
		}
		return destPath, nil
	}

	cachePath, err := c.ensureStubCodeCache(ctx, request)
	if err != nil {
		return "", err
	}

	if err := copyDirectoryContentsAtomic(cachePath, destPath, readyPath, request.ContainerId); err != nil {
		return "", err
	}

	return destPath, nil
}

func createEmptyContainerWorkspace(destPath, readyPath string) error {
	if err := os.RemoveAll(destPath); err != nil {
		return err
	}
	if err := os.Remove(readyPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(destPath, 0755); err != nil {
		return err
	}
	return os.WriteFile(readyPath, []byte("ok"), 0644)
}

func (c *ContainerMountManager) ensureStubCodeCache(ctx context.Context, request *types.ContainerRequest) (string, error) {
	cacheKey := stubCodeCacheKey(request.Workspace.Name, request.Stub.Object.ExternalId)
	cachePath := filepath.Join(c.codeCacheRoot, cacheKey)
	readyPath := filepath.Join(cachePath, ".beta9-cache-ready")
	markReadyUsed := func() {
		now := time.Now()
		_ = os.Chtimes(readyPath, now, now)
	}
	if pathExists(readyPath) {
		markReadyUsed()
		return cachePath, nil
	}

	value, err, _ := c.codeCacheGroup.Do(cacheKey, func() (any, error) {
		if pathExists(readyPath) {
			markReadyUsed()
			return cachePath, nil
		}

		tmpPath := fmt.Sprintf("%s.tmp.%s", cachePath, request.ContainerId)
		if err := os.RemoveAll(tmpPath); err != nil {
			return "", err
		}

		if err := c.extractStubCode(ctx, request, tmpPath); err != nil {
			_ = os.RemoveAll(tmpPath)
			return "", err
		}

		if err := os.WriteFile(filepath.Join(tmpPath, ".beta9-cache-ready"), []byte("ok"), 0644); err != nil {
			_ = os.RemoveAll(tmpPath)
			return "", err
		}

		if err := os.RemoveAll(cachePath); err != nil {
			_ = os.RemoveAll(tmpPath)
			return "", err
		}
		if err := os.Rename(tmpPath, cachePath); err != nil {
			_ = os.RemoveAll(tmpPath)
			return "", err
		}

		return cachePath, nil
	})
	if err != nil {
		return "", err
	}

	return value.(string), nil
}

func (c *ContainerMountManager) extractStubCode(ctx context.Context, request *types.ContainerRequest, destPath string) error {
	objectID := request.Stub.Object.ExternalId
	if request.StorageAvailable() {
		if workspaceStorageDownloadAvailable(request.Workspace.Storage) {
			log.Info().
				Str("container_id", request.ContainerId).
				Str("object_id", objectID).
				Msg("downloading stub code directly from workspace storage")
			return getAndExtractStubCodeToPath(ctx, request, destPath)
		}

		workspaceObjectPath := filepath.Join(c.storageConfig.WorkspaceStorage.BaseMountPath, request.Workspace.Name, types.DefaultObjectPrefix, objectID)
		if pathExists(workspaceObjectPath) {
			return common.ExtractObjectFile(ctx, workspaceObjectPath, destPath)
		}

		return fmt.Errorf("workspace object not available at %s and direct download is not configured", workspaceObjectPath)
	}

	objectPath := filepath.Join(types.DefaultObjectPath, request.Workspace.Name, objectID)
	return common.ExtractObjectFile(ctx, objectPath, destPath)
}

func workspaceStorageMountPath(request *types.ContainerRequest, mount types.Mount) bool {
	if strings.HasPrefix(mount.MountPath, types.WorkerContainerVolumePath) ||
		strings.HasPrefix(mount.MountPath, types.WorkerUserOutputVolume) {
		return true
	}

	workspaceVolumeRoot := path.Join(types.DefaultVolumesPath, request.Workspace.Name)
	workspaceOutputRoot := path.Join(types.DefaultOutputsPath, request.Workspace.Name)
	return strings.HasPrefix(mount.LocalPath, workspaceVolumeRoot) ||
		strings.HasPrefix(mount.LocalPath, workspaceOutputRoot)
}

func workspaceStorageDownloadAvailable(storage *types.WorkspaceStorage) bool {
	return storage != nil &&
		storage.BucketName != nil && *storage.BucketName != "" &&
		storage.AccessKey != nil && *storage.AccessKey != "" &&
		storage.SecretKey != nil && *storage.SecretKey != "" &&
		storage.Region != nil && *storage.Region != ""
}

func stubCodeCacheKey(workspaceName, objectID string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s:%d:%s", len(workspaceName), workspaceName, len(objectID), objectID)))
	return hex.EncodeToString(sum[:])
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func copyDirectoryContentsAtomic(src, dest, readyPath, containerID string) error {
	tmpPath := fmt.Sprintf("%s.tmp.%s", dest, containerID)
	if err := os.RemoveAll(tmpPath); err != nil {
		return err
	}
	if err := os.RemoveAll(dest); err != nil {
		return err
	}
	if err := os.Remove(readyPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := copyDirectoryContents(src, tmpPath); err != nil {
		_ = os.RemoveAll(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, dest); err != nil {
		_ = os.RemoveAll(tmpPath)
		return err
	}
	if err := os.WriteFile(readyPath, []byte("ok"), 0644); err != nil {
		return err
	}

	return nil
}

func copyDirectoryContents(src, dest string) error {
	if err := os.MkdirAll(dest, 0755); err != nil {
		return err
	}

	return filepath.WalkDir(src, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(src, path)
		if err != nil || rel == "." {
			return err
		}
		if rel == ".beta9-cache-ready" {
			return nil
		}

		target := filepath.Join(dest, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}

		switch {
		case entry.Type()&os.ModeSymlink != 0:
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(linkTarget, target)
		case entry.IsDir():
			return os.MkdirAll(target, info.Mode())
		default:
			return copyRegularFile(path, target, info.Mode())
		}
	})
}

func copyRegularFile(src, dest string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return err
	}

	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destFile, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	_, copyErr := io.Copy(destFile, srcFile)
	closeErr := destFile.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}

	return os.Chmod(dest, mode)
}

// RemoveContainerMounts removes all mounts for a container
func (c *ContainerMountManager) RemoveContainerMounts(containerId string) {
	mountPoints, ok := c.mountPointMounts.Get(containerId)
	if !ok {
		return
	}

	for _, mountPoint := range mountPoints {
		if mountPoint.storage == nil {
			continue
		}
		if err := mountPoint.storage.Unmount(mountPoint.localPath); err != nil {
			log.Error().Str("container_id", containerId).Str("local_path", mountPoint.localPath).Err(err).Msg("failed to unmount external s3 bucket")
		}
	}

	c.mountPointMounts.Delete(containerId)
}

func (c *ContainerMountManager) setupMountPointS3(containerId string, m types.Mount) error {
	mountPointS3, _ := storage.NewMountPointStorage(*m.MountPointConfig)

	err := mountPointS3.Mount(m.LocalPath)
	if err != nil {
		return err
	}

	mountPoints, ok := c.mountPointMounts.Get(containerId)
	if !ok {
		mountPoints = []containerMountPoint{{localPath: m.LocalPath, storage: mountPointS3}}
	} else {
		mountPoints = append(mountPoints, containerMountPoint{localPath: m.LocalPath, storage: mountPointS3})
	}

	c.mountPointMounts.Set(containerId, mountPoints)

	return nil
}

const (
	checkpointSignalFileName            = "READY_FOR_CHECKPOINT"
	checkpointCompleteFileName          = "CHECKPOINT_COMPLETE"
	checkpointContainerIdFileName       = "CONTAINER_ID"
	checkpointContainerHostnameFileName = "CONTAINER_HOSTNAME"
)

func checkpointSignalDir(containerId string) string {
	return containerSignalDir(containerId, "criu")
}

func runnerSignalDir(containerId string) string {
	return containerSignalDir(containerId, "runner")
}

func containerSignalDir(containerId, name string) string {
	return filepath.Join(types.AgentTmpPath, containerId, name)
}

func getAndExtractStubCodeToPath(ctx context.Context, request *types.ContainerRequest, destPath string) error {
	storageClient, err := clients.NewWorkspaceStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Str("workspace_id", request.Workspace.ExternalId).Err(err).Msg("unable to instantiate storage client")
		return err
	}

	objBytes, err := storageClient.Download(ctx, fmt.Sprintf("objects/%s", request.Stub.Object.ExternalId))
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Str("workspace_id", request.Workspace.ExternalId).Err(err).Msg("unable to download object")
		return err
	}

	return common.UnzipBytesToPath(destPath, objBytes, request)
}
