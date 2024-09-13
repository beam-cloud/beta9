package worker

import (
	"log"
	"os"
	"path/filepath"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/types"
	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
)

const (
	baseFileCachePath string = "/cache"
)

type FileCacheManager struct {
	config types.AppConfig
	client *blobcache.BlobCacheClient
}

func NewFileCacheManager(config types.AppConfig, client *blobcache.BlobCacheClient) *FileCacheManager {
	return &FileCacheManager{
		config: config,
		client: client,
	}
}

// CacheFilesInPath caches files from a specified source path
func (cm *FileCacheManager) CacheFilesInPath(sourcePath string) {
	filepath.Walk(sourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			_, err := cm.client.StoreContentFromSource(path, 0)
			if err == nil {
				log.Printf("File cached successfully: %q\n", path)
			}
		}

		return nil
	})
}

// GetClient returns the blobcache client instance.
func (cm *FileCacheManager) GetClient() *blobcache.BlobCacheClient {
	if !cm.CacheAvailable() {
		return nil
	}

	return cm.client
}

// CacheAvailable checks if the file cache is available
func (cm *FileCacheManager) CacheAvailable() bool {
	if !cm.config.Worker.BlobCacheEnabled {
		return false
	}

	if _, err := os.Stat(baseFileCachePath); os.IsNotExist(err) {
		return false
	}

	// Check if it's a valid mount point
	var stat syscall.Statfs_t
	if err := syscall.Statfs(baseFileCachePath, &stat); err != nil {
		return false
	}

	return stat.Type != 0
}
