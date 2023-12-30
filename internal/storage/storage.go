package storage

import (
	"errors"
	"log"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/types"
)

const (
	DefaultFilesystemName string = types.DefaultFilesystemName
	DefaultFilesystemPath string = types.DefaultFilesystemPath
	DefaultObjectPath     string = types.DefaultObjectPath
)

const (
	StorageModeJuiceFs string = "JUICEFS"
)

type Storage interface {
	Mount(localPath string) error
	Format(fsName string) error
	Unmount(localPath string) error
}

func NewStorage() (Storage, error) {
	storageMode := common.Secrets().GetWithDefault("BEAM_STORAGE_MODE", StorageModeJuiceFs)

	switch storageMode {
	case StorageModeJuiceFs:
		s, err := NewJuiceFsStorage()
		if err != nil {
			return nil, err
		}

		// Format filesystem
		err = s.Format(DefaultFilesystemName)
		if err != nil {
			log.Fatalf("Unable to format filesystem: %+v\n", err)
		}

		// Mount filesystem
		err = s.Mount(DefaultFilesystemPath)
		if err != nil {
			log.Fatalf("Unable to mount filesystem: %+v\n", err)
		}

		return s, nil
	}

	return nil, errors.New("invalid storage mode")
}
