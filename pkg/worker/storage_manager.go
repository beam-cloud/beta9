package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
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
	mountIdleTimeout     = 5 * time.Minute
)

type WorkspaceStorageManager struct {
	ctx                context.Context
	mounts             *common.SafeMap[storage.Storage]
	mountLastUsed      *common.SafeMap[time.Time]
	config             types.StorageConfig
	poolConfig         types.WorkerPoolConfig
	containerInstances *common.SafeMap[*ContainerInstance]
	mountLocks         map[string]*sync.RWMutex
	mountLocksMu       sync.Mutex
	cacheClient        *cache.Client
}

func NewWorkspaceStorageManager(ctx context.Context, config types.StorageConfig, poolConfig types.WorkerPoolConfig, containerInstances *common.SafeMap[*ContainerInstance], cacheClient *cache.Client) (*WorkspaceStorageManager, error) {
	sm := &WorkspaceStorageManager{
		ctx:                ctx,
		mounts:             common.NewSafeMap[storage.Storage](),
		mountLastUsed:      common.NewSafeMap[time.Time](),
		config:             config,
		poolConfig:         poolConfig,
		containerInstances: containerInstances,
		mountLocks:         make(map[string]*sync.RWMutex),
		cacheClient:        cacheClient,
	}

	if sm.poolConfig.StorageMode == "" {
		sm.poolConfig.StorageMode = sm.config.WorkspaceStorage.DefaultStorageMode
	}

	log.Info().Str("storage_mode", sm.poolConfig.StorageMode).Msgf("using storage mode: '%s'", sm.poolConfig.StorageMode)

	// Workspace mounts are reused for the worker lifetime and cleaned up after
	// scheduling is disabled during worker shutdown. Background unmounts can
	// spend up to a minute flushing GeeseFS while holding the workspace lock,
	// which puts an unrelated new container directly behind that delay.
	return sm, nil
}

func (sm *WorkspaceStorageManager) Create(workspaceName string, storage storage.Storage) {
	unlock := sm.lockWorkspaceMount(workspaceName)
	defer unlock()

	sm.mounts.Set(workspaceName, storage)
	sm.mountLastUsed.Set(workspaceName, time.Now())
}

func (sm *WorkspaceStorageManager) Mount(workspaceName string, workspaceStorage *types.WorkspaceStorage) (storage.Storage, error) {
	mountPath := path.Join(sm.config.WorkspaceStorage.BaseMountPath, workspaceName)
	readUnlock := sm.rlockWorkspaceMount(workspaceName)
	mount, ok := sm.mounts.Get(workspaceName)
	if ok && workspaceMountHealthy(mount, mountPath) {
		sm.mountLastUsed.Set(workspaceName, time.Now())
		readUnlock()
		return mount, nil
	}
	readUnlock()

	unlock := sm.lockWorkspaceMount(workspaceName)
	defer unlock()

	mount, ok = sm.mounts.Get(workspaceName)
	if ok {
		if workspaceMountHealthy(mount, mountPath) {
			sm.mountLastUsed.Set(workspaceName, time.Now())
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

	mount, err := sm.newWorkspaceMount(workspaceStorage)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return nil, err
	}
	if err := mount.Mount(mountPath); err != nil {
		return nil, fmt.Errorf("unable to mount filesystem: %w", err)
	}

	sm.mounts.Set(workspaceName, mount)
	sm.mountLastUsed.Set(workspaceName, time.Now())

	return mount, nil
}

func (sm *WorkspaceStorageManager) newWorkspaceMount(workspaceStorage *types.WorkspaceStorage) (storage.Storage, error) {
	mode := sm.poolConfig.StorageMode
	if mode != storage.StorageModeGeese && mode != storage.StorageModeMountPoint {
		return nil, fmt.Errorf("workspace storage mode %q is invalid: workspace storage mounts request-scoped credentials through a FUSE backend", mode)
	}
	if err := validateWorkspaceStorage(workspaceStorage); err != nil {
		return nil, err
	}

	switch mode {
	case storage.StorageModeGeese:
		config := sm.config.WorkspaceStorage.Geese
		config.EndpointUrl = *workspaceStorage.EndpointUrl
		config.BucketName = *workspaceStorage.BucketName
		config.AccessKey = *workspaceStorage.AccessKey
		config.SecretKey = *workspaceStorage.SecretKey
		config.Region = *workspaceStorage.Region
		return storage.NewGeeseStorage(config, sm.cacheClient)
	default:
		return storage.NewMountPointStorage(types.MountPointConfig{
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
		return fmt.Errorf("workspace storage metadata is incomplete")
	}
	return nil
}

func (sm *WorkspaceStorageManager) Unmount(workspaceName string) error {
	unlock := sm.lockWorkspaceMount(workspaceName)
	defer unlock()
	return sm.unmountLocked(workspaceName)
}

// unmountLocked requires the per-workspace mount lock. Keeping the lookup,
// unmount, path removal, and map deletion in one critical section prevents a
// task from receiving a mount that cleanup is about to tear down.
func (sm *WorkspaceStorageManager) unmountLocked(workspaceName string) error {
	mount, ok := sm.mounts.Get(workspaceName)
	if !ok {
		return nil
	}

	localPath := path.Join(sm.config.WorkspaceStorage.BaseMountPath, workspaceName)

	if err := mount.Unmount(localPath); err != nil {
		return err
	}
	if err := os.RemoveAll(localPath); err != nil {
		return err
	}

	sm.mounts.Delete(workspaceName)
	sm.mountLastUsed.Delete(workspaceName)

	return nil
}

func (sm *WorkspaceStorageManager) workspaceActive(workspaceName string) bool {
	if sm.containerInstances == nil {
		return false
	}
	active := false
	sm.containerInstances.Range(func(_ string, value *ContainerInstance) bool {
		if value != nil && value.Request != nil && value.Request.Workspace.Name == workspaceName {
			active = true
			return false
		}
		return true
	})
	return active
}

// unmountIfIdle revalidates the cleanup decision while holding the same lock
// used by Mount. A stale cleanup snapshot can therefore neither unmount a mount
// just handed to a new task nor replace its FUSE source with an empty directory.
func (sm *WorkspaceStorageManager) unmountIfIdle(workspaceName string, lastUsedBefore time.Time) (bool, error) {
	unlock := sm.lockWorkspaceMount(workspaceName)
	defer unlock()

	if _, ok := sm.mounts.Get(workspaceName); !ok {
		return false, nil
	}
	if sm.workspaceActive(workspaceName) {
		return false, nil
	}
	if lastUsed, ok := sm.mountLastUsed.Get(workspaceName); ok && lastUsed.After(lastUsedBefore) {
		return false, nil
	}
	return true, sm.unmountLocked(workspaceName)
}

func (sm *WorkspaceStorageManager) lockWorkspaceMount(workspaceName string) func() {
	lock := sm.workspaceMountLock(workspaceName)
	lock.Lock()
	return lock.Unlock
}

func (sm *WorkspaceStorageManager) rlockWorkspaceMount(workspaceName string) func() {
	lock := sm.workspaceMountLock(workspaceName)
	lock.RLock()
	return lock.RUnlock
}

func (sm *WorkspaceStorageManager) workspaceMountLock(workspaceName string) *sync.RWMutex {
	sm.mountLocksMu.Lock()
	lock, ok := sm.mountLocks[workspaceName]
	if !ok {
		lock = &sync.RWMutex{}
		sm.mountLocks[workspaceName] = lock
	}
	sm.mountLocksMu.Unlock()
	return lock
}

func workspaceMountHealthy(mount storage.Storage, mountPath string) bool {
	switch mount.Mode() {
	case storage.StorageModeGeese, storage.StorageModeMountPoint:
		return storage.IsMounted(mountPath)
	default:
		return true
	}
}

func (sm *WorkspaceStorageManager) Cleanup() error {
	return sm.cleanupUnused(time.Now())
}

func (sm *WorkspaceStorageManager) cleanupUnused(lastUsedBefore time.Time) error {
	var candidates []string
	sm.mounts.Range(func(workspaceName string, _ storage.Storage) bool {
		candidates = append(candidates, workspaceName)
		return true
	})

	for _, workspaceName := range candidates {
		unmounted, err := sm.unmountIfIdle(workspaceName, lastUsedBefore)
		if err != nil {
			log.Error().Str("workspace_name", workspaceName).Err(err).Msg("failed to unmount storage")
			continue
		}
		if unmounted {
			log.Info().Str("workspace_name", workspaceName).Msg("unmounted idle workspace storage")
		}
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
			err := sm.cleanupUnused(time.Now().Add(-mountIdleTimeout))
			if err != nil {
				log.Error().Err(err).Msg("failed to cleanup unused mounts")
			}
		}
	}
}
