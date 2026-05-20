package cache

import (
	"errors"
	"syscall"
)

const (
	SourceModeJuiceFS    string = "juicefs"
	SourceModeMountPoint string = "mountpoint"
)

type Source interface {
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

func NewSource(config SourceConfig) (Source, error) {
	switch config.Mode {
	case SourceModeJuiceFS:
		s, err := NewJuiceFsSource(config.JuiceFS)
		if err != nil {
			return nil, err
		}

		// Format filesystem
		// NOTE: this is a no-op if already formatted
		err = s.Format(config.FilesystemName)
		if err != nil {
			Logger.Fatalf("Unable to format filesystem: %+v", err)
		}

		// Mount filesystem
		err = s.Mount(config.FilesystemPath)
		if err != nil {
			Logger.Fatalf("Unable to mount filesystem: %+v", err)
		}

		return s, nil
	case SourceModeMountPoint:
		s, err := NewMountPointSource(config.MountPoint)
		if err != nil {
			return nil, err
		}

		// Mount filesystem
		err = s.Mount(config.FilesystemPath)
		if err != nil {
			Logger.Fatalf("Unable to mount filesystem: %+v", err)
		}

		return s, nil
	}

	return nil, errors.New("invalid storage mode")
}
