package storage

import (
	"errors"
	"log"

	"github.com/beam-cloud/beta9/pkg/types"
	"golang.org/x/sys/unix"
)

const (
	StorageModeJuiceFS    string = "juicefs"
	StorageModeCunoFS     string = "cunofs"
	StorageModeMountPoint string = "mountpoint"
)

type Storage interface {
	Mount(localPath string) error
	Format(fsName string) error
	Unmount(localPath string) error
}

// isMounted uses stat to check if the specified FUSE mount point is available
func isMounted(mountPoint string) bool {
	var statfs unix.Statfs_t
	if err := unix.Statfs(mountPoint, &statfs); err != nil {
		return false
	}

	// FUSE filesystems usually have a magic number 0x65735546 (FUSE_SUPER_MAGIC)
	const FUSE_SUPER_MAGIC = 0x65735546
	return statfs.Type == FUSE_SUPER_MAGIC
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
	case StorageModeCunoFS:
		s, err := NewCunoFsStorage(config.CunoFS)
		if err != nil {
			return nil, err
		}

		// Setup credentials and load buckets
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
