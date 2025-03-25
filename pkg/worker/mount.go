package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"strings"

	"github.com/rs/zerolog/log"

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
func (c *ContainerMountManager) SetupContainerMounts(request *types.ContainerRequest, outputLogger *slog.Logger) error {
	for i, m := range request.Mounts {
		if m.MountPath == types.WorkerUserCodeVolume {
			objectPath := path.Join(types.DefaultObjectPath, request.Workspace.Name, request.Stub.Object.ExternalId)

			if request.StorageAvailable() {
				objectPath = path.Join(c.storageConfig.WorkspaceStorage.BaseMountPath, request.Workspace.Name, types.DefaultObjectPrefix, request.Stub.Object.ExternalId)
			}

			err := common.ExtractObjectFile(context.TODO(), objectPath, types.TempContainerWorkspace(request.ContainerId))
			if err != nil {
				return err
			}

			m.LocalPath = types.TempContainerWorkspace(request.ContainerId)
			request.Mounts[i].LocalPath = m.LocalPath

			// NOTE: The following two adjustments to local paths are part of a migration to use WorkspaceStorage and can be removed once all existing workspaces are migrated.
		} else if strings.HasPrefix(m.MountPath, types.WorkerContainerVolumePath) {
			if request.StorageAvailable() {
				m.LocalPath = strings.Replace(m.LocalPath, path.Join(types.DefaultVolumesPath, request.Workspace.Name), path.Join(c.storageConfig.WorkspaceStorage.BaseMountPath, request.Workspace.Name, types.DefaultVolumesPrefix), 1)
				request.Mounts[i].LocalPath = m.LocalPath
				os.MkdirAll(m.LocalPath, os.FileMode(0755)) // TODO: remove this hack once we're sure it's created elsewhere
			}
		} else if strings.HasPrefix(m.LocalPath, types.WorkerUserOutputVolume) {
			if request.StorageAvailable() {
				m.LocalPath = strings.Replace(m.LocalPath, path.Join(types.DefaultOutputsPath, request.Workspace.Name), path.Join(c.storageConfig.WorkspaceStorage.BaseMountPath, request.Workspace.Name, types.DefaultOutputsPrefix), 1)
				request.Mounts[i].LocalPath = m.LocalPath
				os.MkdirAll(m.LocalPath, os.FileMode(0755)) // TODO: remove this hack once we're sure it's created elsewhere
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
	for _, m := range mountPointPaths {
		if err := mountPointS3.Unmount(m); err != nil {
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
