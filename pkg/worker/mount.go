package worker

import (
	"fmt"
	"log"
	"os"
	"path"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

type ContainerMountManager struct {
	mountPointPaths *common.SafeMap[[]string]
}

func NewContainerMountManager() *ContainerMountManager {
	return &ContainerMountManager{
		mountPointPaths: common.NewSafeMap[[]string](),
	}
}

// SetupContainerMounts initializes any external storage for a container
func (c *ContainerMountManager) SetupContainerMounts(request *types.ContainerRequest) error {
	// Create local workspace path so we can symlink volumes before the container starts
	os.MkdirAll(defaultContainerDirectory, os.FileMode(0755))

	for i, m := range request.Mounts {
		if m.MountPath == defaultContainerDirectory && !request.Stub.Type.IsServe() {
			source := m.LocalPath
			localUserSource := tempUserCodeDir(request.ContainerId)
			err := copyDirectory(source, localUserSource)
			if err != nil {
				log.Printf("<%s> - failed to eagerly copy remote user code to local /mnt/code: %v\n", request.ContainerId, err)
			} else {
				request.Mounts[i].LocalPath = localUserSource
			}
		}

		if m.MountType == storage.StorageModeMountPoint && m.MountPointConfig != nil {
			// Add containerId to local mount path for mountpoint storage
			m.LocalPath = path.Join(m.LocalPath, request.ContainerId)
			request.Mounts[i].LocalPath = m.LocalPath

			err := c.setupMountPointS3(request.ContainerId, m)
			if err != nil {
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
			log.Printf("<%s> - failed to unmount external s3 bucket: %v\n", containerId, err)
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

func tempUserCodeDir(containerId string) string {
	return fmt.Sprintf("/tmp/%s/code", containerId)
}
