package storage

import (
	"context"
	"os"
	"path"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	mountCleanupInterval = 30 * time.Second
)

type StorageManager struct {
	ctx    context.Context
	mounts *common.SafeMap[Storage]
	config types.StorageConfig
	mu     sync.Mutex
}

func NewStorageManager(ctx context.Context, config types.StorageConfig) (*StorageManager, error) {
	sm := &StorageManager{
		ctx:    ctx,
		mounts: common.NewSafeMap[Storage](),
		config: config,
		mu:     sync.Mutex{},
	}

	go sm.cleanupUnusedMounts()

	return sm, nil
}

func (s *StorageManager) Create(workspaceName string, storage Storage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mounts.Set(workspaceName, storage)
}

func (s *StorageManager) Mount(workspaceName string, workspaceStorage *types.WorkspaceStorage) (Storage, error) {
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

	mount, err := NewStorage(types.StorageConfig{
		Mode:           StorageModeGeese,
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
			Debug:            s.config.WorkspaceStorage.Geese.Debug,
			Force:            s.config.WorkspaceStorage.Geese.Force,
			FsyncOnClose:     s.config.WorkspaceStorage.Geese.FsyncOnClose,
			MemoryLimit:      s.config.WorkspaceStorage.Geese.MemoryLimit,
			MaxFlushers:      s.config.WorkspaceStorage.Geese.MaxFlushers,
			MaxParallelParts: s.config.WorkspaceStorage.Geese.MaxParallelParts,
			PartSizes:        s.config.WorkspaceStorage.Geese.PartSizes,
			DirMode:          s.config.WorkspaceStorage.Geese.DirMode,
			FileMode:         s.config.WorkspaceStorage.Geese.FileMode,
			ListType:         s.config.WorkspaceStorage.Geese.ListType,
		},
	})
	if err != nil {
		return nil, err
	}

	s.mounts.Set(workspaceName, mount)

	return mount, nil
}

func (s *StorageManager) Unmount(workspaceName string) error {
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

	err := mount.Unmount(path.Join(s.config.WorkspaceStorage.BaseMountPath, workspaceName))
	if err != nil {
		return err
	}

	s.mounts.Delete(workspaceName)

	return nil
}

func (s *StorageManager) Cleanup() error {
	s.mounts.Range(func(workspaceName string, value Storage) bool {
		s.Unmount(workspaceName)
		return true
	})

	return nil
}

func (s *StorageManager) cleanupUnusedMounts() {
	ticker := time.NewTicker(mountCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			mountsToDelete := []string{}

			s.mounts.Range(func(workspaceName string, value Storage) bool {
				mountsToDelete = append(mountsToDelete, workspaceName)
				return true
			})

			for _, workspaceName := range mountsToDelete {
				s.Unmount(workspaceName)
			}
		}
	}
}
