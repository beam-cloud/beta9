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

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

const (
	baseFileCachePath      string = "/cache"
	volumeCacheLibraryPath string = "/usr/local/lib/volume_cache.so"
	volumeCacheMapEnv      string = "VOLUME_CACHE_MAP"
	ldPreloadEnv           string = "LD_PRELOAD"
)

type FileCacheManager struct {
	config types.AppConfig
	client *cache.Client
}

func NewFileCacheManager(config types.AppConfig, client *cache.Client) *FileCacheManager {
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
			_, err := cm.client.StoreContentFromLocalFile(cache.LocalContentSource{
				Path:      path,
				CachePath: path,
			}, cache.StoreContentOptions{
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
		return cache.ErrHostNotFound
	}

	if spec.Process == nil {
		return fmt.Errorf("container spec missing process")
	}

	volumeCacheMapStr, err := encodeVolumeCacheMap(volumeCacheMap)
	if err != nil {
		return err
	}

	workspaceVolumePath, err := cm.initWorkspace(workspaceName)
	if err != nil {
		return err
	}

	spec.Mounts = append(spec.Mounts, volumeCacheMounts(workspaceVolumePath)...)
	spec.Process.Env = withVolumeCacheEnv(spec.Process.Env, volumeCacheMapStr)
	return nil
}

func encodeVolumeCacheMap(volumeCacheMap map[string]string) (string, error) {
	volumeCacheMapBytes, err := json.Marshal(volumeCacheMap)
	if err != nil {
		return "", err
	}
	return string(volumeCacheMapBytes), nil
}

func volumeCacheMounts(workspaceVolumePath string) []specs.Mount {
	return []specs.Mount{{
		Type:        "none",
		Source:      filepath.Join(baseFileCachePath, workspaceVolumePath),
		Destination: "/cache",
		Options:     []string{"ro", "rbind", "rprivate", "nosuid", "noexec", "nodev"},
	}, {
		Type:        "none",
		Source:      volumeCacheLibraryPath,
		Destination: volumeCacheLibraryPath,
		Options:     []string{"ro", "rbind", "rprivate", "nosuid", "nodev"},
	}}
}

func withVolumeCacheEnv(env []string, volumeCacheMapStr string) []string {
	env = withLDPreload(env, volumeCacheLibraryPath)
	return append(env, fmt.Sprintf("%s=%s", volumeCacheMapEnv, volumeCacheMapStr))
}

func withLDPreload(env []string, libraryPath string) []string {
	prefix := ldPreloadEnv + "="
	for i, envVar := range env {
		if !strings.HasPrefix(envVar, prefix) {
			continue
		}

		value := strings.TrimPrefix(envVar, prefix)
		if value == "" {
			env[i] = prefix + libraryPath
		} else if !envListContains(value, libraryPath) {
			env[i] = fmt.Sprintf("%s%s:%s", prefix, value, libraryPath)
		}
		return env
	}

	return append(env, prefix+libraryPath)
}

func envListContains(value string, item string) bool {
	for _, part := range strings.Split(value, ":") {
		if part == item {
			return true
		}
	}
	return false
}

func (cm *FileCacheManager) initWorkspace(workspaceName string) (string, error) {
	workspaceVolumePath := filepath.Join(types.DefaultVolumesPath, workspaceName)
	fileName := fmt.Sprintf("%s/.cache", workspaceVolumePath)

	if cm.CacheAvailable() && cm.client.IsPathCachedReachable(context.Background(), fileName) {
		return workspaceVolumePath, nil
	}

	_, err := cm.client.StoreContentAtPath([]byte{}, fileName, cache.StoreContentOptions{
		RoutingKey: fileName,
		Lock:       true,
	})
	if err != nil {
		return "", err
	}

	return workspaceVolumePath, nil
}

// GetClient returns the cache client instance.
func (cm *FileCacheManager) GetClient() *cache.Client {
	if !cm.CacheAvailable() {
		return nil
	}

	return cm.client
}

// CacheAvailable checks if the file cache is available
func (cm *FileCacheManager) CacheAvailable() bool {
	if !cm.config.Worker.CacheEnabled || !cm.config.Cache.Enabled {
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
