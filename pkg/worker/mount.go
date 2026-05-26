package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

type ContainerMountManager struct {
	mountPointPaths *common.SafeMap[[]string]
	storageConfig   types.StorageConfig
	codeCacheRoot   string
	codeCacheGroup  singleflight.Group
}

func NewContainerMountManager(config types.AppConfig) *ContainerMountManager {
	return &ContainerMountManager{
		mountPointPaths: common.NewSafeMap[[]string](),
		storageConfig:   config.Storage,
		codeCacheRoot:   filepath.Join(os.TempDir(), "beta9-stub-code-cache"),
	}
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
			log.Info().Interface("mount", m).Interface("config", m.MountPointConfig).Msg("setting up container mounts")

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

func (c *ContainerMountManager) setupUserCodeMount(ctx context.Context, request *types.ContainerRequest) (string, error) {
	destPath := types.TempContainerWorkspace(request.ContainerId)
	readyPath := filepath.Join(filepath.Dir(destPath), ".workspace-ready")
	if pathExists(destPath) && pathExists(readyPath) {
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

func (c *ContainerMountManager) ensureStubCodeCache(ctx context.Context, request *types.ContainerRequest) (string, error) {
	cacheKey := stubCodeCacheKey(request.Workspace.Name, request.Stub.Object.ExternalId)
	cachePath := filepath.Join(c.codeCacheRoot, cacheKey)
	readyPath := filepath.Join(cachePath, ".beta9-cache-ready")
	if pathExists(readyPath) {
		return cachePath, nil
	}

	value, err, _ := c.codeCacheGroup.Do(cacheKey, func() (any, error) {
		if pathExists(readyPath) {
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
		workspaceObjectPath := filepath.Join(c.storageConfig.WorkspaceStorage.BaseMountPath, request.Workspace.Name, types.DefaultObjectPrefix, objectID)
		if pathExists(workspaceObjectPath) {
			return common.ExtractObjectFile(ctx, workspaceObjectPath, destPath)
		}

		return getAndExtractStubCodeToPath(ctx, request, destPath)
	}

	objectPath := filepath.Join(types.DefaultObjectPath, request.Workspace.Name, objectID)
	return common.ExtractObjectFile(ctx, objectPath, destPath)
}

func stubCodeCacheKey(workspaceName, objectID string) string {
	return safeCachePathComponent(workspaceName) + "-" + safeCachePathComponent(objectID)
}

func safeCachePathComponent(value string) string {
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
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
	mountPointPaths, ok := c.mountPointPaths.Get(containerId)
	if !ok {
		return
	}

	mountPointS3, _ := storage.NewMountPointStorage(types.MountPointConfig{})
	for _, localPath := range mountPointPaths {
		if err := mountPointS3.Unmount(localPath); err != nil {
			log.Error().Str("container_id", containerId).Err(err).Msg("failed to unmount external s3 bucket")
		}
	}

	c.mountPointPaths.Delete(containerId)
}

func (c *ContainerMountManager) setupMountPointS3(containerId string, m types.Mount) error {
	mountPointS3, _ := storage.NewMountPointStorage(*m.MountPointConfig)

	err := mountPointS3.Mount(m.LocalPath)
	if err != nil {
		return err
	}

	mountPointPaths, ok := c.mountPointPaths.Get(containerId)
	if !ok {
		mountPointPaths = []string{m.LocalPath}
	} else {
		mountPointPaths = append(mountPointPaths, m.LocalPath)
	}

	c.mountPointPaths.Set(containerId, mountPointPaths)

	return nil
}

const (
	checkpointSignalFileName            = "READY_FOR_CHECKPOINT"
	checkpointCompleteFileName          = "CHECKPOINT_COMPLETE"
	checkpointContainerIdFileName       = "CONTAINER_ID"
	checkpointContainerHostnameFileName = "CONTAINER_HOSTNAME"
)

func checkpointSignalDir(containerId string) string {
	return fmt.Sprintf("/tmp/%s/criu", containerId)
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
