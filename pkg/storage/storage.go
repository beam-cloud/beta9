package storage

import (
	"errors"
	"log"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	StorageModeJuiceFS    string = "juicefs"
	StorageModeMountPoint string = "mountpoint"
)

type Storage interface {
	Mount(localPath string) error
	Format(fsName string) error
	Unmount(localPath string) error
}

// isMounted uses stat to check if the specified mount point is available
func isMounted(mountPoint string) bool {
	var stat syscall.Stat_t
	err := syscall.Stat(mountPoint, &stat)
	if err != nil {
		return false
	}

	return stat.Ino == 1
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
	case StorageModeMountPoint:
		s, err := NewMountPointStorage(config.MountPoint)
		if err != nil {
			return nil, err
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
