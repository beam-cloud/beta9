package storage

import (
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type StorageManager struct {
	storage   *common.SafeMap[Storage]
	appConfig types.AppConfig
}

func NewStorageManager(appConfig types.AppConfig) *StorageManager {
	return &StorageManager{
		storage:   common.NewSafeMap[Storage](),
		appConfig: appConfig,
	}
}

func (s *StorageManager) Create(name string, storage Storage) {
	s.storage.Set(name, storage)
}

func (s *StorageManager) Mount(name string) Storage {
	storage, ok := s.storage.Get(name)
	if !ok {
		return nil
	}

	storage.Mount("/mock")

	return storage
}

func (s *StorageManager) Unmount(name string) {
	storage, ok := s.storage.Get(name)
	if !ok {
		return
	}

	storage.Unmount("/mock")
	s.storage.Delete(name)
}
