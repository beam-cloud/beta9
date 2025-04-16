package worker

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

type ContainerMountManager struct {
	mountPointPaths *common.SafeMap[[]string]
	storageConfig   types.StorageConfig
}

func NewContainerMountManager(config types.AppConfig) *ContainerMountManager {
	return &ContainerMountManager{
		mountPointPaths: common.NewSafeMap[[]string](),
		storageConfig:   config.Storage,
	}
}

// SetupContainerMounts initializes any external storage for a container
func (c *ContainerMountManager) SetupContainerMounts(ctx context.Context, request *types.ContainerRequest, outputLogger *slog.Logger) error {
	for i, m := range request.Mounts {
		if m.MountPath == types.WorkerUserCodeVolume {
			m.LocalPath = types.TempContainerWorkspace(request.ContainerId)

			if !request.StorageAvailable() {
				objectPath := path.Join(types.DefaultObjectPath, request.Workspace.Name, request.Stub.Object.ExternalId)
				err := common.ExtractObjectFile(ctx, objectPath, m.LocalPath)
				if err != nil {
					return err
				}
			} else {
				if err := getMntCodeAndExtract(ctx, request); err != nil {
					return err
				}
			}

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

// getMntCodeAndExtract downloads the object from storage and extracts it to the temp location that will be mounted
func getMntCodeAndExtract(ctx context.Context, request *types.ContainerRequest) error {
	storageClient, err := clients.NewStorageClient(ctx, request.Workspace.Name, request.Workspace.Storage)
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Str("workspace_id", request.Workspace.ExternalId).Err(err).Msg("unable to instantiate storage client")
		return err
	}

	objBytes, err := storageClient.Download(ctx, fmt.Sprintf("objects/%s", request.Stub.Object.ExternalId))
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Str("workspace_id", request.Workspace.ExternalId).Err(err).Msg("unable to download object")
		return err
	}

	destPath := types.TempContainerWorkspace(request.ContainerId)

	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		return nil
	}

	if err := os.MkdirAll(destPath, 0755); err != nil {
		return err
	}

	zipReader, err := zip.NewReader(bytes.NewReader(objBytes), int64(len(objBytes)))
	if err != nil {
		log.Error().Str("container_id", request.ContainerId).Err(err).Msg("error creating zip reader")
		return err
	}

	// Extract each file
	for _, zipFile := range zipReader.File {
		filePath := filepath.Join(destPath, zipFile.Name)

		if !strings.HasPrefix(filePath, filepath.Clean(destPath)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path: %s", filePath)
		}

		if zipFile.FileInfo().IsDir() {
			if err := os.MkdirAll(filePath, zipFile.Mode()); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			return err
		}

		destFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, zipFile.Mode())
		if err != nil {
			return err
		}

		srcFile, err := zipFile.Open()
		if err != nil {
			destFile.Close()
			return err
		}

		_, err = io.Copy(destFile, srcFile)
		srcFile.Close()
		destFile.Close()

		if err != nil {
			return err
		}
	}

	return nil
}
