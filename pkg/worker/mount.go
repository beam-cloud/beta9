package worker

import (
	"fmt"
	"log"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

type StorageManager struct {
	mountPointPaths *common.SafeMap[[]string]
	workerStorage   storage.Storage
	config          types.StorageConfig
}

func NewStorageManager(config types.StorageConfig) (*StorageManager, error) {
	workerStorage, err := storage.NewStorage(config)
	if err != nil {
		return nil, err
	}

	return &StorageManager{
		mountPointPaths: common.NewSafeMap[[]string](),
		workerStorage:   workerStorage,
		config:          config,
	}, nil
}

// SetupContainerStorage initializes any external storage for a container
func (smm *StorageManager) SetupContainerStorage(containerId string, mounts []types.Mount) error {
	for _, m := range mounts {
		if m.MountType == storage.StorageModeMountPoint && m.MountPointConfig != nil {
			err := smm.setupMountPointS3(containerId, m)
			if err != nil {
				log.Printf("<%s> failed to mount s3 bucket with mountpoint-s3: %v", containerId, err)
				return err
			}
		}
	}

	return nil
}

// RemoveContainerStorage removes all mounts for a container
func (smm *StorageManager) RemoveContainerStorage(containerId string) {
	mountPointPaths, ok := smm.mountPointPaths.Get(containerId)
	if !ok {
		return
	}

	mountPointS3, _ := storage.NewMountPointStorage(types.MountPointConfig{})
	for _, m := range mountPointPaths {
		if err := mountPointS3.Unmount(m); err != nil {
			log.Printf("<%s> - failed to unmount external s3 bucket: %v\n", containerId, err)
		}
	}

	smm.mountPointPaths.Delete(containerId)
}

// RemoveWorkerStorage removes all mounts for a worker
func (smm *StorageManager) RemoveWorkerStorage() error {

	err := smm.workerStorage.Unmount(smm.config.FilesystemPath)
	if err != nil {
		return fmt.Errorf("failed to unmount storage: %v", err)
	}

	return nil
}

func (smm *StorageManager) setupMountPointS3(containerId string, m types.Mount) error {
	mountPointS3, _ := storage.NewMountPointStorage(*m.MountPointConfig)

	err := mountPointS3.Mount(m.LocalPath)
	if err != nil {
		return err
	}

	mountPointPaths, ok := smm.mountPointPaths.Get(containerId)
	if !ok {
		mountPointPaths = []string{m.LocalPath}
	} else {
		mountPointPaths = append(mountPointPaths, m.LocalPath)
	}
	smm.mountPointPaths.Set(containerId, mountPointPaths)

	return nil
}
