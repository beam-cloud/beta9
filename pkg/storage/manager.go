package storage

import (
	"os"
	"path"
	"sync"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type StorageManager struct {
	mounts *common.SafeMap[Storage]
	config types.StorageConfig
	mu     sync.Mutex
}

func NewStorageManager(config types.StorageConfig) (*StorageManager, error) {
	return &StorageManager{
		mounts: common.NewSafeMap[Storage](),
		config: config,
		mu:     sync.Mutex{},
	}, nil
}

func (s *StorageManager) Create(name string, storage Storage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mounts.Set(name, storage)
}

func (s *StorageManager) Mount(workspaceId string, workspaceStorage *types.WorkspaceStorage) (Storage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	mount, ok := s.mounts.Get(workspaceId)
	if ok {
		return mount, nil
	}

	mountPath := path.Join(s.config.WorkspaceStorage.BaseMountPath, workspaceId)
	os.MkdirAll(mountPath, 0755)

	mount, err := NewStorage(types.StorageConfig{
		Mode:           StorageModeGeese,
		FilesystemName: workspaceId,
		FilesystemPath: mountPath,
		Geese: types.GeeseConfig{
			// Workspace specific config
			EndpointUrl: workspaceStorage.EndpointUrl,
			BucketName:  workspaceStorage.BucketName,
			AccessKey:   workspaceStorage.AccessKey,
			SecretKey:   workspaceStorage.SecretKey,
			Region:      workspaceStorage.Region,

			// Global config
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

	err = mount.Mount(mountPath)
	if err != nil {
		return nil, err
	}

	s.mounts.Set(workspaceId, mount)

	return mount, nil
}

func (s *StorageManager) Unmount(workspaceId string, workspaceStorage *types.WorkspaceStorage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	mount, ok := s.mounts.Get(workspaceId)
	if !ok {
		return nil
	}

	mountPath := path.Join(s.config.WorkspaceStorage.BaseMountPath, workspaceId)
	err := mount.Unmount(mountPath)
	if err != nil {
		return err
	}

	s.mounts.Delete(workspaceId)

	return nil
}

func (s *StorageManager) Cleanup() error {
	s.mounts.Range(func(key string, value Storage) bool {
		value.Unmount(path.Join(s.config.WorkspaceStorage.BaseMountPath, key))
		return true
	})

	return nil
}
