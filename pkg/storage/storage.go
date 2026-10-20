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
	StorageModeMountPoint string = types.StorageModeMountPoint
	StorageModeGeese      string = types.StorageModeGeese
	StorageModeLocal      string = types.StorageModeLocal
)

type Storage interface {
	Mount(localPath string) error
	Unmount(localPath string) error
	Mode() string
}

// NewStorage constructs the selected workspace filesystem using workspace credentials.
// The caller owns mounting and unmounting the returned backend.
func NewStorage(mode string, config types.WorkspaceStorageConfig, workspaceStorage *types.WorkspaceStorage, cacheClient *cache.Client) (Storage, error) {
	if mode != StorageModeGeese && mode != StorageModeMountPoint {
		return nil, fmt.Errorf("workspace storage mode %q is invalid: workspace storage mounts request-scoped credentials through a FUSE backend", mode)
	}
	if err := validateWorkspaceStorage(workspaceStorage); err != nil {
		return nil, err
	}

	switch mode {
	case StorageModeGeese:
		geeseConfig := config.Geese
		geeseConfig.EndpointUrl = *workspaceStorage.EndpointUrl
		geeseConfig.BucketName = *workspaceStorage.BucketName
		geeseConfig.AccessKey = *workspaceStorage.AccessKey
		geeseConfig.SecretKey = *workspaceStorage.SecretKey
		geeseConfig.Region = *workspaceStorage.Region
		return NewGeeseStorage(geeseConfig, cacheClient)
	default:
		return NewMountPointStorage(types.MountPointConfig{
			BucketName:  *workspaceStorage.BucketName,
			AccessKey:   *workspaceStorage.AccessKey,
			SecretKey:   *workspaceStorage.SecretKey,
			EndpointURL: *workspaceStorage.EndpointUrl,
			Region:      *workspaceStorage.Region,
		})
	}
}

func validateWorkspaceStorage(workspaceStorage *types.WorkspaceStorage) error {
	if workspaceStorage == nil {
		return errors.New("workspace storage metadata is required")
	}
	if workspaceStorage.EndpointUrl == nil || workspaceStorage.BucketName == nil || workspaceStorage.AccessKey == nil || workspaceStorage.SecretKey == nil || workspaceStorage.Region == nil {
		return errors.New("workspace storage metadata is incomplete")
	}
	return nil
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
