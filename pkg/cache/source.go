package cache

import (
	"errors"
	"fmt"
	"syscall"
)

const (
	SourceModeMountPoint string = "mountpoint"
)

type Source interface {
	Mount(localPath string) error
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

func NewSource(config SourceConfig) (Source, error) {
	switch config.Mode {
	case SourceModeMountPoint:
		s, err := NewMountPointSource(config.MountPoint)
		if err != nil {
			return nil, err
		}

		// Mount filesystem
		err = s.Mount(config.FilesystemPath)
		if err != nil {
			return nil, fmt.Errorf("unable to mount filesystem: %w", err)
		}

		return s, nil
	}

	return nil, errors.New("invalid storage mode")
}
