package storage

type StorageManager struct {
	storage map[string]Storage
}

func NewStorageManager() *StorageManager {
	return &StorageManager{
		storage: make(map[string]Storage),
	}
}

func (s *StorageManager) CreateWorkspaceStorage(name string, storage Storage) {
	s.storage[name] = storage
}

func (s *StorageManager) MountWorkspaceStorage(name string) Storage {
	return s.storage[name]
}
