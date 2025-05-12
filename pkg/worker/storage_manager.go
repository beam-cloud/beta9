package worker

import (
	"context"
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
	containerInstances *common.SafeMap[*ContainerInstance]
	mu                 sync.Mutex
	cacheClient        *blobcache.BlobCacheClient
}

func NewWorkspaceStorageManager(ctx context.Context, config types.StorageConfig, containerInstances *common.SafeMap[*ContainerInstance], cacheClient *blobcache.BlobCacheClient) (*WorkspaceStorageManager, error) {
	sm := &WorkspaceStorageManager{
		ctx:                ctx,
		mounts:             common.NewSafeMap[storage.Storage](),
		config:             config,
		containerInstances: containerInstances,
		mu:                 sync.Mutex{},
		cacheClient:        cacheClient,
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
	os.MkdirAll(mountPath, 0755)

	mount, err := storage.NewStorage(types.StorageConfig{
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
			FuseReadAheadKB:        s.config.WorkspaceStorage.Geese.FuseReadAheadKB,
			DisableVolumeCaching:   s.config.WorkspaceStorage.Geese.DisableVolumeCaching,
			StagedWriteModeEnabled: s.config.WorkspaceStorage.Geese.StagedWriteModeEnabled,
			StagedWritePath:        s.config.WorkspaceStorage.Geese.StagedWritePath,
			StagedWriteDebounce:    s.config.WorkspaceStorage.Geese.StagedWriteDebounce,
		},
	}, s.cacheClient)
	if err != nil {
		return nil, err
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
	s.mounts.Range(func(workspaceName string, value storage.Storage) bool {
		s.Unmount(workspaceName)
		log.Info().Str("workspace_name", workspaceName).Msg("unmounted storage")
		return true
	})

	log.Info().Msg("cleaned up storage")
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
			mountsToDelete := []string{}
			activeWorkspaceNames := []string{}

			s.containerInstances.Range(func(containerInstanceId string, value *ContainerInstance) bool {
				activeWorkspaceNames = append(activeWorkspaceNames, value.Request.Workspace.Name)
				return true
			})

			s.mounts.Range(func(workspaceName string, value storage.Storage) bool {
				if !slices.Contains(activeWorkspaceNames, workspaceName) {
					log.Info().Str("workspace_name", workspaceName).Msg("unmounting storage")
					mountsToDelete = append(mountsToDelete, workspaceName)
				}

				return true
			})

			for _, workspaceName := range mountsToDelete {
				s.Unmount(workspaceName)
			}
		}
	}
}
