package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/types"
	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	"github.com/opencontainers/runtime-spec/specs-go"
)

const (
	baseFileCachePath     string = "/cache"
	additionalPreloadPath        = "/usr/local/lib/ipbind.so"
	specialPreloadPath           = "/usr/local/lib/volume_cache.so"
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
			_, err := cm.client.StoreContentFromFUSE(struct {
				Path string
			}{
				Path: path,
			}, struct {
				RoutingKey string
				Lock       bool
			}{
				RoutingKey: path,
			})
			if err != nil {
				log.Error().Str("path", path).Err(err).Msg("failed to cache file")
			}
		}

		return nil
	})
}

func (cm *FileCacheManager) EnableVolumeCaching(workspaceName string, volumeCacheMap map[string]string, spec *specs.Spec) error {
	if !cm.CacheAvailable() || !cm.client.HostsAvailable() {
		return blobcache.ErrHostNotFound
	}

	volumeCacheMapStr := "{}"
	volumeCacheMapBytes, err := json.Marshal(volumeCacheMap)
	if err != nil {
		return err
	}
	volumeCacheMapStr = string(volumeCacheMapBytes)

	workspaceVolumePath, err := cm.initWorkspace(workspaceName)
	if err != nil {
		return err
	}

	cacheMount := specs.Mount{
		Type:        "none",
		Source:      filepath.Join(baseFileCachePath, workspaceVolumePath),
		Destination: "/cache",
		Options: []string{"ro",
			"rbind",
			"rprivate",
			"nosuid",
			"noexec",
			"nodev"},
	}

	interceptMount := specs.Mount{
		Type:        "none",
		Source:      "/usr/local/lib/volume_cache.so",
		Destination: "/usr/local/lib/volume_cache.so",
		Options: []string{"ro",
			"rbind",
			"rprivate",
			"nosuid",
			"nodev"},
	}

	spec.Mounts = append(spec.Mounts, cacheMount)
	spec.Mounts = append(spec.Mounts, interceptMount)

	for i, envVar := range spec.Process.Env {
		if strings.HasPrefix(envVar, "LD_PRELOAD=") {
			spec.Process.Env[i] = fmt.Sprintf("%s:%s", envVar, "/usr/local/lib/volume_cache.so")
			break
		}
	}

	spec.Process.Env = append(spec.Process.Env, []string{fmt.Sprintf("VOLUME_CACHE_MAP=%s", volumeCacheMapStr)}...)
	return nil
}

func (cm *FileCacheManager) initWorkspace(workspaceName string) (string, error) {
	workspaceVolumePath := filepath.Join(types.DefaultVolumesPath, workspaceName)
	fileName := fmt.Sprintf("%s/.cache", workspaceVolumePath)

	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		file, err := os.Create(fileName)
		if err != nil {
			return "", err
		}
		defer file.Close()
	} else if cm.CacheAvailable() && cm.client.IsPathCachedNearby(context.Background(), workspaceVolumePath) {
		return workspaceVolumePath, nil
	}

	_, err = cm.client.StoreContentFromFUSE(struct {
		Path string
	}{
		Path: fileName,
	}, struct {
		RoutingKey string
		Lock       bool
	}{
		RoutingKey: fileName,
		Lock:       true,
	})
	if err != nil {
		return "", err
	}

	return workspaceVolumePath, nil
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

	if cm.client == nil {
		return false
	}

	return stat.Type != 0
}
