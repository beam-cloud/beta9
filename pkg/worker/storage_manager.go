package worker

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	mountCleanupInterval = 30 * time.Second

	workspaceStorageEndpointEnv             = "BEAM_WORKSPACE_STORAGE_ENDPOINT_URL"
	workspaceStorageEndpointRewriteHostsEnv = "BEAM_WORKSPACE_STORAGE_ENDPOINT_REWRITE_HOSTS"
	defaultWorkspaceStorageEndpointHosts    = "localstack,localstack.beta9,localstack.beta9.svc,localstack.beta9.svc.cluster.local"
)

type WorkspaceStorageManager struct {
	ctx                context.Context
	mounts             *common.SafeMap[storage.Storage]
	config             types.StorageConfig
	poolConfig         types.WorkerPoolConfig
	containerInstances *common.SafeMap[*ContainerInstance]
	mountLocks         map[string]*sync.Mutex
	mountLocksMu       sync.Mutex
	cacheClient        *cache.Client
}

func NewWorkspaceStorageManager(ctx context.Context, config types.StorageConfig, poolConfig types.WorkerPoolConfig, containerInstances *common.SafeMap[*ContainerInstance], cacheClient *cache.Client) (*WorkspaceStorageManager, error) {
	sm := &WorkspaceStorageManager{
		ctx:                ctx,
		mounts:             common.NewSafeMap[storage.Storage](),
		config:             config,
		poolConfig:         poolConfig,
		containerInstances: containerInstances,
		mountLocks:         make(map[string]*sync.Mutex),
		cacheClient:        cacheClient,
	}

	if sm.poolConfig.StorageMode == "" {
		sm.poolConfig.StorageMode = sm.config.WorkspaceStorage.DefaultStorageMode
	}

	log.Info().Str("storage_mode", sm.poolConfig.StorageMode).Msgf("using storage mode: '%s'", sm.poolConfig.StorageMode)

	go sm.cleanupUnusedMounts()
	return sm, nil
}

func (sm *WorkspaceStorageManager) Create(workspaceName string, storage storage.Storage) {
	unlock := sm.lockWorkspaceMount(workspaceName)
	defer unlock()

	sm.mounts.Set(workspaceName, storage)
}

func (sm *WorkspaceStorageManager) Mount(workspaceName string, workspaceStorage *types.WorkspaceStorage) (storage.Storage, error) {
	mountPath := path.Join(sm.config.WorkspaceStorage.BaseMountPath, workspaceName)
	mount, ok := sm.mounts.Get(workspaceName)
	if ok && workspaceMountHealthy(mount, mountPath) {
		return mount, nil
	}

	unlock := sm.lockWorkspaceMount(workspaceName)
	defer unlock()

	mount, ok = sm.mounts.Get(workspaceName)
	if ok {
		if workspaceMountHealthy(mount, mountPath) {
			return mount, nil
		}

		log.Warn().
			Str("workspace_name", workspaceName).
			Str("local_path", mountPath).
			Msg("workspace storage mount is registered but not mounted, remounting")
		if err := mount.Unmount(mountPath); err != nil {
			log.Warn().Err(err).Str("workspace_name", workspaceName).Str("local_path", mountPath).Msg("failed to unmount stale workspace storage")
		}
		sm.mounts.Delete(workspaceName)
		_ = os.RemoveAll(mountPath)
	}

	var err error
	workspaceStorage = workspaceStorageForMount(workspaceStorage)
	switch sm.poolConfig.StorageMode {
	case storage.StorageModeGeese:
		if err := validateWorkspaceStorage(workspaceStorage); err != nil {
			return nil, err
		}
		os.MkdirAll(mountPath, 0755)

		mount, err = storage.NewStorage(types.StorageConfig{
			Mode:           storage.StorageModeGeese,
			FilesystemName: workspaceName,
			FilesystemPath: mountPath,
			Geese: types.GeeseConfig{
				// Workspace specific config
				EndpointUrl: *workspaceStorage.EndpointUrl,
				BucketName:  *workspaceStorage.BucketName,
				AccessKey:   *workspaceStorage.AccessKey,
				SecretKey:   *workspaceStorage.SecretKey,
				Region:      *workspaceStorage.Region,

				// Global config
				Debug:                  sm.config.WorkspaceStorage.Geese.Debug,
				FsyncOnClose:           sm.config.WorkspaceStorage.Geese.FsyncOnClose,
				MemoryLimit:            sm.config.WorkspaceStorage.Geese.MemoryLimit,
				MaxFlushers:            sm.config.WorkspaceStorage.Geese.MaxFlushers,
				MaxParallelParts:       sm.config.WorkspaceStorage.Geese.MaxParallelParts,
				HTTPTimeout:            sm.config.WorkspaceStorage.Geese.HTTPTimeout,
				DirMode:                sm.config.WorkspaceStorage.Geese.DirMode,
				FileMode:               sm.config.WorkspaceStorage.Geese.FileMode,
				ListType:               sm.config.WorkspaceStorage.Geese.ListType,
				MountOptions:           sm.config.WorkspaceStorage.Geese.MountOptions,
				ReadAheadKB:            sm.config.WorkspaceStorage.Geese.ReadAheadKB,
				ReadAheadLargeKB:       sm.config.WorkspaceStorage.Geese.ReadAheadLargeKB,
				ReadAheadParallelKB:    sm.config.WorkspaceStorage.Geese.ReadAheadParallelKB,
				FuseReadAheadKB:        sm.config.WorkspaceStorage.Geese.FuseReadAheadKB,
				DisableVolumeCaching:   sm.config.WorkspaceStorage.Geese.DisableVolumeCaching,
				StagedWriteModeEnabled: sm.config.WorkspaceStorage.Geese.StagedWriteModeEnabled,
				StagedWritePath:        sm.config.WorkspaceStorage.Geese.StagedWritePath,
				StagedWriteDebounce:    sm.config.WorkspaceStorage.Geese.StagedWriteDebounce,
				CacheStreamingEnabled:  sm.config.WorkspaceStorage.Geese.CacheStreamingEnabled,
				CacheDirectIO:          sm.config.WorkspaceStorage.Geese.CacheDirectIO,
				CacheThroughEnabled:    sm.config.WorkspaceStorage.Geese.CacheThroughEnabled,
			},
		}, sm.cacheClient)
		if err != nil {
			return nil, err
		}

	case storage.StorageModeAlluxio:
		if err := validateWorkspaceStorage(workspaceStorage); err != nil {
			return nil, err
		}
		mount, err = storage.NewStorage(types.StorageConfig{
			Mode:           storage.StorageModeAlluxio,
			FilesystemName: workspaceName,
			FilesystemPath: mountPath,
			Alluxio: types.AlluxioConfig{
				// Global config
				Debug:          sm.config.WorkspaceStorage.Alluxio.Debug,
				ImageUrl:       sm.config.WorkspaceStorage.Alluxio.ImageUrl,
				EtcdEndpoint:   sm.config.WorkspaceStorage.Alluxio.EtcdEndpoint,
				EtcdUsername:   sm.config.WorkspaceStorage.Alluxio.EtcdUsername,
				EtcdPassword:   sm.config.WorkspaceStorage.Alluxio.EtcdPassword,
				EtcdTlsEnabled: sm.config.WorkspaceStorage.Alluxio.EtcdTlsEnabled,

				// Workspace specific config
				BucketName:     *workspaceStorage.BucketName,
				AccessKey:      *workspaceStorage.AccessKey,
				SecretKey:      *workspaceStorage.SecretKey,
				EndpointURL:    *workspaceStorage.EndpointUrl,
				Region:         *workspaceStorage.Region,
				ReadOnly:       false,
				ForcePathStyle: false,
			},
		}, sm.cacheClient)
		if err != nil {
			return nil, err
		}

	case storage.StorageModeLocal:
		mount, err = storage.NewStorage(types.StorageConfig{
			Mode:           storage.StorageModeLocal,
			FilesystemName: workspaceName,
			FilesystemPath: mountPath,
		}, sm.cacheClient)
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.New("invalid storage mode")
	}

	sm.mounts.Set(workspaceName, mount)

	return mount, nil
}

func workspaceStorageForMount(workspaceStorage *types.WorkspaceStorage) *types.WorkspaceStorage {
	if workspaceStorage == nil || workspaceStorage.EndpointUrl == nil {
		return workspaceStorage
	}

	endpoint := *workspaceStorage.EndpointUrl
	rewrittenEndpoint := rewriteWorkspaceStorageEndpoint(endpoint)
	if rewrittenEndpoint == endpoint {
		return workspaceStorage
	}

	out := *workspaceStorage
	out.EndpointUrl = &rewrittenEndpoint
	log.Info().
		Str("original_endpoint", endpoint).
		Str("endpoint", rewrittenEndpoint).
		Msg("rewrote workspace storage endpoint for agent worker")
	return &out
}

func rewriteWorkspaceStorageEndpoint(endpoint string) string {
	override := strings.TrimSpace(os.Getenv(workspaceStorageEndpointEnv))
	if override == "" {
		return endpoint
	}

	current, err := url.Parse(endpoint)
	if err != nil || current.Hostname() == "" {
		return endpoint
	}
	if !workspaceStorageEndpointHostRewriteAllowed(current.Hostname()) {
		return endpoint
	}

	next, err := url.Parse(override)
	if err != nil || next.Scheme == "" || next.Host == "" {
		return endpoint
	}

	current.Scheme = next.Scheme
	current.Host = next.Host
	if next.Path != "" && next.Path != "/" {
		current.Path = strings.TrimRight(next.Path, "/")
	}
	return strings.TrimRight(current.String(), "/")
}

func workspaceStorageEndpointHostRewriteAllowed(host string) bool {
	host = strings.ToLower(strings.Trim(host, "[]"))
	rewriteHosts := strings.TrimSpace(os.Getenv(workspaceStorageEndpointRewriteHostsEnv))
	if rewriteHosts == "" {
		rewriteHosts = defaultWorkspaceStorageEndpointHosts
	}
	for _, allowed := range strings.Split(rewriteHosts, ",") {
		allowed = strings.ToLower(strings.TrimSpace(strings.Trim(allowed, "[]")))
		if allowed == "" {
			continue
		}
		if host == allowed || strings.HasSuffix(host, "."+allowed) {
			return true
		}
	}
	return false
}

func validateWorkspaceStorage(workspaceStorage *types.WorkspaceStorage) error {
	if workspaceStorage == nil {
		return errors.New("workspace storage metadata is required")
	}
	if workspaceStorage.EndpointUrl == nil || workspaceStorage.BucketName == nil || workspaceStorage.AccessKey == nil || workspaceStorage.SecretKey == nil || workspaceStorage.Region == nil {
		return fmt.Errorf("workspace storage metadata is incomplete")
	}
	return nil
}

func (sm *WorkspaceStorageManager) Unmount(workspaceName string) error {
	mount, ok := sm.mounts.Get(workspaceName)
	if !ok {
		return nil
	}

	unlock := sm.lockWorkspaceMount(workspaceName)
	defer unlock()

	mount, ok = sm.mounts.Get(workspaceName)
	if !ok {
		return nil
	}

	localPath := path.Join(sm.config.WorkspaceStorage.BaseMountPath, workspaceName)

	switch mount.Mode() {
	case storage.StorageModeGeese:
		err := mount.Unmount(localPath)
		if err != nil {
			return err
		}

		os.RemoveAll(localPath)
	case storage.StorageModeLocal:
		if err := mount.Unmount(localPath); err != nil {
			return err
		}
	case storage.StorageModeAlluxio:
		fallthrough
	default:
	}

	sm.mounts.Delete(workspaceName)

	return nil
}

func (sm *WorkspaceStorageManager) lockWorkspaceMount(workspaceName string) func() {
	sm.mountLocksMu.Lock()
	lock, ok := sm.mountLocks[workspaceName]
	if !ok {
		lock = &sync.Mutex{}
		sm.mountLocks[workspaceName] = lock
	}
	sm.mountLocksMu.Unlock()

	lock.Lock()
	return lock.Unlock
}

func workspaceMountHealthy(mount storage.Storage, mountPath string) bool {
	switch mount.Mode() {
	case storage.StorageModeGeese, storage.StorageModeJuiceFS, storage.StorageModeMountPoint:
		return storage.IsMounted(mountPath)
	default:
		return true
	}
}

func (sm *WorkspaceStorageManager) Cleanup() error {
	mountsToDelete := []string{}
	activeWorkspaceNames := []string{}

	sm.containerInstances.Range(func(containerInstanceId string, value *ContainerInstance) bool {
		activeWorkspaceNames = append(activeWorkspaceNames, value.Request.Workspace.Name)
		return true
	})

	sm.mounts.Range(func(workspaceName string, value storage.Storage) bool {
		if !slices.Contains(activeWorkspaceNames, workspaceName) {
			mountsToDelete = append(mountsToDelete, workspaceName)
		}

		return true
	})

	for _, workspaceName := range mountsToDelete {
		log.Info().Str("workspace_name", workspaceName).Msg("unmounting storage")
		err := sm.Unmount(workspaceName)
		if err != nil {
			log.Error().Str("workspace_name", workspaceName).Err(err).Msg("failed to unmount storage")
			continue
		}
		log.Info().Str("workspace_name", workspaceName).Msg("unmounted storage")
	}

	return nil
}

func (sm *WorkspaceStorageManager) cleanupUnusedMounts() {
	ticker := time.NewTicker(mountCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case <-ticker.C:
			err := sm.Cleanup()
			if err != nil {
				log.Error().Err(err).Msg("failed to cleanup unused mounts")
			}
		}
	}
}
