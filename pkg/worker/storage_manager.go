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

	if poolConfig.StorageMode == "" {
		poolConfig.StorageMode = config.WorkspaceStorage.DefaultStorageMode
	}

	go sm.cleanupUnusedMounts()
	return sm, nil
}

func (s *WorkspaceStorageManager) Create(workspaceName string, storage storage.Storage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mounts.Set(workspaceName, storage)
}

func (s *WorkspaceStorageManager) Mount(workspaceName string, workspaceStorage *types.WorkspaceStorage) (storage.Storage, error) {
	mount, ok := s.mounts.Get(workspaceName)
	if ok {
		return mount, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	mount, ok = s.mounts.Get(workspaceName)
	if ok {
		return mount, nil
	}

	mountPath := path.Join(s.config.WorkspaceStorage.BaseMountPath, workspaceName)

	var err error
	if s.poolConfig.StorageMode == storage.StorageModeGeese {
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
				Debug:                  s.config.WorkspaceStorage.Geese.Debug,
				FsyncOnClose:           s.config.WorkspaceStorage.Geese.FsyncOnClose,
				MemoryLimit:            s.config.WorkspaceStorage.Geese.MemoryLimit,
				MaxFlushers:            s.config.WorkspaceStorage.Geese.MaxFlushers,
				MaxParallelParts:       s.config.WorkspaceStorage.Geese.MaxParallelParts,
				DirMode:                s.config.WorkspaceStorage.Geese.DirMode,
				FileMode:               s.config.WorkspaceStorage.Geese.FileMode,
				ListType:               s.config.WorkspaceStorage.Geese.ListType,
				MountOptions:           s.config.WorkspaceStorage.Geese.MountOptions,
				ReadAheadKB:            s.config.WorkspaceStorage.Geese.ReadAheadKB,
				ReadAheadLargeKB:       s.config.WorkspaceStorage.Geese.ReadAheadLargeKB,
				ReadAheadParallelKB:    s.config.WorkspaceStorage.Geese.ReadAheadParallelKB,
				FuseReadAheadKB:        s.config.WorkspaceStorage.Geese.FuseReadAheadKB,
				DisableVolumeCaching:   s.config.WorkspaceStorage.Geese.DisableVolumeCaching,
				StagedWriteModeEnabled: s.config.WorkspaceStorage.Geese.StagedWriteModeEnabled,
				StagedWritePath:        s.config.WorkspaceStorage.Geese.StagedWritePath,
				StagedWriteDebounce:    s.config.WorkspaceStorage.Geese.StagedWriteDebounce,
				CacheStreamingEnabled:  s.config.WorkspaceStorage.Geese.CacheStreamingEnabled,
			},
		}, s.cacheClient)
		if err != nil {
			return nil, err
		}
	} else if s.poolConfig.StorageMode == storage.StorageModeAlluxio {
		alluxioCoordinatorHostname := os.Getenv("ALLUXIO_COORDINATOR_HOSTNAME")
		if alluxioCoordinatorHostname == "" {
			alluxioCoordinatorHostname = s.config.WorkspaceStorage.Alluxio.CoordinatorHostname
		}

		mount, err = storage.NewStorage(types.StorageConfig{
			Mode:           storage.StorageModeAlluxio,
			FilesystemName: workspaceName,
			FilesystemPath: workspaceName,
			Alluxio: types.AlluxioConfig{
				// Global config
				Debug:               s.config.WorkspaceStorage.Alluxio.Debug,
				ImageUrl:            s.config.WorkspaceStorage.Alluxio.ImageUrl,
				EtcdEndpoint:        s.config.WorkspaceStorage.Alluxio.EtcdEndpoint,
				EtcdUsername:        s.config.WorkspaceStorage.Alluxio.EtcdUsername,
				EtcdPassword:        s.config.WorkspaceStorage.Alluxio.EtcdPassword,
				EtcdTlsEnabled:      s.config.WorkspaceStorage.Alluxio.EtcdTlsEnabled,
				CoordinatorHostname: alluxioCoordinatorHostname,

				// Workspace specific config
				BucketName:     *workspaceStorage.BucketName,
				AccessKey:      *workspaceStorage.AccessKey,
				SecretKey:      *workspaceStorage.SecretKey,
				EndpointURL:    *workspaceStorage.EndpointUrl,
				Region:         *workspaceStorage.Region,
				ReadOnly:       false,
				ForcePathStyle: false,
			},
		}, s.cacheClient)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("invalid storage mode")
	}

	s.mounts.Set(workspaceName, mount)

	return mount, nil
}

func (s *WorkspaceStorageManager) Unmount(workspaceName string) error {
	mount, ok := s.mounts.Get(workspaceName)
	if !ok {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	mount, ok = s.mounts.Get(workspaceName)
	if !ok {
		return nil
	}

	// TODO: correct these paths
	localPath := path.Join(s.config.WorkspaceStorage.BaseMountPath, workspaceName)

	err := mount.Unmount(localPath)
	if err != nil {
		return err
	}

	os.RemoveAll(localPath)
	s.mounts.Delete(workspaceName)

	return nil
}

func (s *WorkspaceStorageManager) Cleanup() error {
	mountsToDelete := []string{}
	activeWorkspaceNames := []string{}

	s.containerInstances.Range(func(containerInstanceId string, value *ContainerInstance) bool {
		activeWorkspaceNames = append(activeWorkspaceNames, value.Request.Workspace.Name)
		return true
	})

	s.mounts.Range(func(workspaceName string, value storage.Storage) bool {
		if !slices.Contains(activeWorkspaceNames, workspaceName) {
			mountsToDelete = append(mountsToDelete, workspaceName)
		}

		return true
	})

	for _, workspaceName := range mountsToDelete {
		log.Info().Str("workspace_name", workspaceName).Msg("unmounting storage")
		err := s.Unmount(workspaceName)
		if err != nil {
			log.Error().Str("workspace_name", workspaceName).Err(err).Msg("failed to unmount storage")
			continue
		}
		log.Info().Str("workspace_name", workspaceName).Msg("unmounted storage")
	}

	return nil
}

func (s *WorkspaceStorageManager) cleanupUnusedMounts() {
	ticker := time.NewTicker(mountCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			err := s.Cleanup()
			if err != nil {
				log.Error().Err(err).Msg("failed to cleanup unused mounts")
			}
		}
	}
}
