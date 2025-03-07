package storage

import (
	"os"
	"path"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	rootMountPath = "/userdata"
)

type StorageManager struct {
	storage *common.SafeMap[Storage]
	config  types.StorageConfig
}

func NewStorageManager(config types.StorageConfig) (*StorageManager, error) {
	return &StorageManager{
		storage: common.NewSafeMap[Storage](),
		config:  config,
	}, nil
}

func (s *StorageManager) Create(name string, storage Storage) {
	s.storage.Set(name, storage)
}

func (s *StorageManager) Mount(workspaceId string, workspaceStorage *types.WorkspaceStorage) (Storage, error) {
	storage, ok := s.storage.Get(workspaceId)
	if ok {
		return storage, nil
	}

	mountPath := path.Join(rootMountPath, workspaceId)
	os.MkdirAll(mountPath, 0755)

	storage, err := NewStorage(types.StorageConfig{
		Mode:           StorageModeGeese,
		FilesystemName: workspaceId,
		FilesystemPath: mountPath,
		Geese: types.GeeseConfig{
			// Workspace settings
			EndpointUrl: workspaceStorage.EndpointUrl,
			BucketName:  workspaceStorage.BucketName,
			AccessKey:   workspaceStorage.AccessKey,
			SecretKey:   workspaceStorage.SecretKey,
			Region:      workspaceStorage.Region,

			// Global settings
			Debug:            s.config.WorkspaceStorage.Geese.Debug,
			Force:            s.config.WorkspaceStorage.Geese.Force,
			FsyncOnClose:     s.config.WorkspaceStorage.Geese.FsyncOnClose,
			MemoryLimit:      s.config.WorkspaceStorage.Geese.MemoryLimit,
			MaxFlushers:      s.config.WorkspaceStorage.Geese.MaxFlushers,
			MaxParallelParts: s.config.WorkspaceStorage.Geese.MaxParallelParts,
			PartSizes:        s.config.WorkspaceStorage.Geese.PartSizes,
			DirMode:          s.config.WorkspaceStorage.Geese.DirMode,
			FileMode:         s.config.WorkspaceStorage.Geese.FileMode,
			ListType:         s.config.WorkspaceStorage.Geese.ListType,
		},
	})
	if err != nil {
		return nil, err
	}

	err = storage.Mount(mountPath)
	if err != nil {
		return nil, err
	}

	return storage, nil
}

func (s *StorageManager) Unmount(workspaceId string, workspaceStorage *types.WorkspaceStorage) error {
	storage, ok := s.storage.Get(workspaceId)
	if !ok {
		return nil
	}

	mountPath := path.Join(rootMountPath, workspaceId)
	err := storage.Unmount(mountPath)
	if err != nil {
		return err
	}

	s.storage.Delete(workspaceId)

	return nil
}
