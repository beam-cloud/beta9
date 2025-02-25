package storage

import (
	"github.com/beam-cloud/beta9/pkg/types"
)

type GeeseStorage struct {
	config types.GeeseConfig
}

func NewGeeseStorage(config types.GeeseConfig) (Storage, error) {
	return &GeeseStorage{
		config: config,
	}, nil
}

func (s *GeeseStorage) Mount(localPath string) error {
	return nil
}

func (s *GeeseStorage) Format(fsName string) error {
	return nil
}

func (s *GeeseStorage) Unmount(localPath string) error {
	return nil
}
