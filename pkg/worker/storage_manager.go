package worker

import (
	"context"
	"errors"
	"os"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	"github.com/rs/zerolog/log"
)

const (
	mountCleanupInterval = 30 * time.Second
)

type WorkspaceStorageManager struct {
	ctx                context.Context
	mounts             *common.SafeMap[storage.Storage]
	config             types.StorageConfig
	poolConfig         types.WorkerPoolConfig
	containerInstances *common.SafeMap[*ContainerInstance]
	mu                 sync.Mutex
	cacheClient        *blobcache.BlobCacheClient
}

func NewWorkspaceStorageManager(ctx context.Context, config types.StorageConfig, poolConfig types.WorkerPoolConfig, containerInstances *common.SafeMap[*ContainerInstance], cacheClient *blobcache.BlobCacheClient) (*WorkspaceStorageManager, error) {
	sm := &WorkspaceStorageManager{
		ctx:                ctx,
		mounts:             common.NewSafeMap[storage.Storage](),
		config:             config,
		poolConfig:         poolConfig,
		containerInstances: containerInstances,
		mu:                 sync.Mutex{},
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
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.mounts.Set(workspaceName, storage)
}

func (sm *WorkspaceStorageManager) Mount(workspaceName string, workspaceStorage *types.WorkspaceStorage) (storage.Storage, error) {
	mount, ok := sm.mounts.Get(workspaceName)
	if ok {
		return mount, nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	mount, ok = sm.mounts.Get(workspaceName)
	if ok {
		return mount, nil
	}

	mountPath := path.Join(sm.config.WorkspaceStorage.BaseMountPath, workspaceName)

	var err error
	switch sm.poolConfig.StorageMode {
	case storage.StorageModeGeese:
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
			},
		}, sm.cacheClient)
		if err != nil {
			return nil, err
		}

	case storage.StorageModeAlluxio:
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

	default:
		return nil, errors.New("invalid storage mode")
	}

	sm.mounts.Set(workspaceName, mount)

	return mount, nil
}

func (sm *WorkspaceStorageManager) Unmount(workspaceName string) error {
	mount, ok := sm.mounts.Get(workspaceName)
	if !ok {
		return nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

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
	case storage.StorageModeAlluxio:
		fallthrough
	default:
	}

	sm.mounts.Delete(workspaceName)

	return nil
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
