package storage

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	StorageModeJuiceFS    string = "juicefs"
	StorageModeMountPoint string = "mountpoint"
	StorageModeGeese      string = "geese"
	StorageModeAlluxio    string = "alluxio"
	StorageModeLocal      string = "local"
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
			log.Fatal().Err(err).Msg("unable to format filesystem")
		}

		// Mount filesystem
		err = s.Mount(config.FilesystemPath)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to mount filesystem")
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
			log.Fatal().Err(err).Msg("unable to mount filesystem")
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
			log.Fatal().Err(err).Msg("unable to mount filesystem")
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
			log.Fatal().Err(err).Msg("unable to mount filesystem")
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
