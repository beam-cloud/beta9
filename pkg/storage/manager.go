package storage

type StorageManager struct {
	storage map[string]Storage
}

func NewStorageManager() *StorageManager {
	return &StorageManager{
		storage: make(map[string]Storage),
	}
}
