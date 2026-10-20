package storage

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	StorageModeMountPoint string = types.StorageModeMountPoint
	StorageModeGeese      string = types.StorageModeGeese
	StorageModeAlluxio    string = types.StorageModeAlluxio
	StorageModeLocal      string = types.StorageModeLocal
)

type Storage interface {
	Mount(localPath string) error
	Unmount(localPath string) error
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
