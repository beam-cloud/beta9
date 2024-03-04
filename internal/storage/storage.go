package storage

import (
	"errors"
	"log"

	"github.com/beam-cloud/beta9/internal/types"
)

const (
	StorageModeJuiceFS string = "juicefs"
)

type Storage interface {
	Mount(localPath string) error
	Format(fsName string) error
	Unmount(localPath string) error
}

func NewStorage(config types.StorageConfig) (Storage, error) {
	switch config.Mode {
	case StorageModeJuiceFS:
		s, err := NewJuiceFsStorage(config.JuiceFS)
		if err != nil {
			return nil, err
		}

		// Format filesystem
		// NOTE: this is a no-op if already formatted
		err = s.Format(config.FilesystemName)
		if err != nil {
			log.Fatalf("Unable to format filesystem: %+v\n", err)
		}

		// Mount filesystem
		err = s.Mount(config.FilesystemPath)
		if err != nil {
			log.Fatalf("Unable to mount filesystem: %+v\n", err)
		}

		return s, nil
	}

	return nil, errors.New("invalid storage mode")
}
