package storage

import "os"

type LocalStorage struct{}

func NewLocalStorage() *LocalStorage {
	return &LocalStorage{}
}

func (s *LocalStorage) Mount(localPath string) error {
	if localPath == "" {
		return nil
	}
	return os.MkdirAll(localPath, 0755)
}

func (s *LocalStorage) Unmount(localPath string) error {
	return nil
}

func (s *LocalStorage) Format(fsName string) error {
	return nil
}

func (s *LocalStorage) Mode() string {
	return StorageModeLocal
}
