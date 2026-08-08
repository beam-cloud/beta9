package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	StorageModeJuiceFS    string = types.StorageModeJuiceFS
	StorageModeMountPoint string = types.StorageModeMountPoint
	StorageModeGeese      string = types.StorageModeGeese
	StorageModeAlluxio    string = types.StorageModeAlluxio
	StorageModeLocal      string = types.StorageModeLocal
)

type Storage interface {
	Mount(localPath string) error
	Unmount(localPath string) error
	Format(fsName string) error
	Mode() string
}

// IsMounted reports whether mountPoint is present in mountinfo without touching
// the mounted filesystem. FUSE calls like statfs can block if the daemon is wedged.
func IsMounted(mountPoint string) bool {
	return isMounted(mountPoint)
}

func isMounted(mountPoint string) bool {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	defer file.Close()

	return mountInfoContains(file, mountPoint)
}

func mountInfoContains(reader io.Reader, mountPoint string) bool {
	target := cleanMountInfoPath(mountPoint)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}

		if cleanMountInfoPath(unescapeMountInfoPath(fields[4])) == target {
			return true
		}
	}
	return false
}

func cleanMountInfoPath(path string) string {
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	return filepath.Clean(path)
}

func unescapeMountInfoPath(path string) string {
	replacer := strings.NewReplacer(
		`\\`, `\`,
		`\040`, " ",
		`\011`, "\t",
		`\012`, "\n",
		`\134`, `\`,
	)
	return replacer.Replace(path)
}

// NewStorage mounts one filesystem and hands it back.
//
// Every failure here is returned rather than fatal. Workers call this per
// workspace on the container path, so exiting on a bad mount would take down
// every unrelated container already running on that worker for the sake of the
// one request that could not be served. Boot-time callers pass the error up and
// still refuse to start.
func NewStorage(config types.StorageConfig, cacheClient *cache.Client) (Storage, error) {
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
			return nil, fmt.Errorf("unable to format filesystem: %w", err)
		}

		// Mount filesystem
		err = s.Mount(config.FilesystemPath)
		if err != nil {
			return nil, fmt.Errorf("unable to mount filesystem: %w", err)
		}

		return s, nil
	case StorageModeGeese:
		s, err := NewGeeseStorage(config.Geese, cacheClient)
		if err != nil {
			return nil, err
		}

		// Mount filesystem
		err = s.Mount(config.FilesystemPath)
		if err != nil {
			return nil, fmt.Errorf("unable to mount filesystem: %w", err)
		}

		return s, nil
	case StorageModeAlluxio:
		s, err := NewAlluxioStorage(config.Alluxio)
		if err != nil {
			return nil, err
		}

		// Mount filesystem
		err = s.Mount(config.FilesystemPath)
		if err != nil {
			return nil, fmt.Errorf("unable to mount filesystem: %w", err)
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
			return nil, fmt.Errorf("unable to mount filesystem: %w", err)
		}

		return s, nil
	case StorageModeLocal:
		s := NewLocalStorage()
		if err := s.Mount(config.FilesystemPath); err != nil {
			return nil, err
		}

		return s, nil
	}

	return nil, errors.New("invalid storage mode")
}
