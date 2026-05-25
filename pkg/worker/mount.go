package worker

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
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
	mountPointPaths   *common.SafeMap[[]string]
	storageConfig     types.StorageConfig
	stubExtractGroup  singleflight.Group
	stubCodeCacheRoot string
}

func NewContainerMountManager(config types.AppConfig) *ContainerMountManager {
	return &ContainerMountManager{
		mountPointPaths:   common.NewSafeMap[[]string](),
		storageConfig:     config.Storage,
		stubCodeCacheRoot: filepath.Join(types.DefaultExtractedObjectPath, "_stub-cache"),
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
			request.Mounts[i].LocalPath = localPath
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
	if directoryExists(destPath) {
		return destPath, nil
	}

	cachePath := c.stubCodeCachePath(request)
	_, err, _ := c.stubExtractGroup.Do(cachePath, func() (any, error) {
		return nil, c.ensureStubCodeCache(ctx, request, cachePath)
	})
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(destPath, 0755); err != nil {
		return "", err
	}
	if err := copyDirectory(cachePath, destPath, nil); err != nil {
		return "", err
	}
	return destPath, nil
}

func (c *ContainerMountManager) ensureStubCodeCache(ctx context.Context, request *types.ContainerRequest, cachePath string) error {
	if directoryExists(cachePath) {
		return nil
	}

	parent := filepath.Dir(cachePath)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}

	tmpPath, err := os.MkdirTemp(parent, ".extract-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpPath)

	if !request.StorageAvailable() {
		objectPath := path.Join(types.DefaultObjectPath, request.Workspace.Name, request.Stub.Object.ExternalId)
		if err := common.ExtractObjectFile(ctx, objectPath, tmpPath); err != nil {
			return err
		}
	} else if err := downloadAndExtractStubCode(ctx, request, tmpPath); err != nil {
		return err
	}

	if err := os.RemoveAll(cachePath); err != nil {
		return err
	}
	return os.Rename(tmpPath, cachePath)
}

func (c *ContainerMountManager) stubCodeCachePath(request *types.ContainerRequest) string {
	cacheID := strings.Join([]string{
		request.Workspace.Name,
		request.Stub.Object.ExternalId,
		request.Stub.Object.Hash,
		fmt.Sprintf("%d", request.Stub.Object.Size),
	}, ":")
	sum := sha1.Sum([]byte(cacheID))
	objectID := request.Stub.Object.ExternalId
	if objectID == "" {
		objectID = "object"
	}
	return filepath.Join(c.stubCodeCacheRoot, request.Workspace.Name, fmt.Sprintf("%s-%s", objectID, hex.EncodeToString(sum[:8])))
}

func directoryExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
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

// downloadAndExtractStubCode downloads the object from storage and extracts it to the target path.
func downloadAndExtractStubCode(ctx context.Context, request *types.ContainerRequest, destPath string) error {
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
