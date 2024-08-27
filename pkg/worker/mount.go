package worker

import (
	"log"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

type containerMountManager struct {
	mountPointPaths *common.SafeMap[[]string]
}

func NewContainerMountManager() *containerMountManager {
	return &containerMountManager{
		mountPointPaths: common.NewSafeMap[[]string](),
	}
}

// SetupContainerMounts initializes any external storage for a container
func (c *containerMountManager) SetupContainerMounts(containerId string, mounts []types.Mount) error {
	for _, m := range mounts {
		if m.MountType == storage.StorageModeMountPoint && m.MountPointConfig != nil {
			err := c.setupMountPointS3(containerId, m)
			if err != nil {
				log.Printf("<%s> failed to mount s3 bucket with mountpoint-s3: %v", containerId, err)
				return err
			}
		}
	}

	return nil
}

// RemoveContainerMounts removes all mounts for a container
func (c *containerMountManager) RemoveContainerMounts(containerId string) {
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

func (c *containerMountManager) setupMountPointS3(containerId string, m types.Mount) error {
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
