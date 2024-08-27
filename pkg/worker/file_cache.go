package worker

import (
	"os"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/types"
	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
)

const (
	BaseFileCachePath string = "/cache"
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

func (cm *FileCacheManager) GetClient() *blobcache.BlobCacheClient {
	return cm.client
}

func (cm *FileCacheManager) CacheFiles() {
	if cm.client == nil {
		return
	}

	if !fileCacheAvailable() {
		return
	}

}

func fileCacheAvailable() bool {
	if _, err := os.Stat(BaseFileCachePath); os.IsNotExist(err) {
		return false
	}

	// Check if it's a valid mount point
	var stat syscall.Statfs_t
	if err := syscall.Statfs(BaseFileCachePath, &stat); err != nil {
		return false
	}

	return stat.Type != 0
}
