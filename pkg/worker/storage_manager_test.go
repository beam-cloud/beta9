package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type trackedStorage struct {
	unmounted  bool
	mode       string
	unmountErr error
}

func (*trackedStorage) Mount(string) error { return nil }
func (s *trackedStorage) Unmount(string) error {
	s.unmounted = true
	return s.unmountErr
}
func (s *trackedStorage) Mode() string {
	if s.mode != "" {
		return s.mode
	}
	return storage.StorageModeLocal
}

func TestWorkspaceStorageMountHotPathAllowsConcurrentHealthChecks(t *testing.T) {
	mount := &trackedStorage{}
	manager := &WorkspaceStorageManager{
		mounts:        common.NewSafeMap[storage.Storage](),
		mountLastUsed: common.NewSafeMap[time.Time](),
		mountLocks:    make(map[string]*sync.RWMutex),
		config: types.StorageConfig{WorkspaceStorage: types.WorkspaceStorageConfig{
			BaseMountPath: t.TempDir(),
		}},
	}
	manager.mounts.Set("workspace-a", mount)

	unlock := manager.rlockWorkspaceMount("workspace-a")
	defer unlock()

	done := make(chan error, 1)
	go func() {
		_, err := manager.Mount("workspace-a", nil)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("healthy workspace mount waited behind another health check")
	}
}

func TestWorkspaceStorageManagerRejectsLocalStorage(t *testing.T) {
	base := t.TempDir()
	manager, err := NewWorkspaceStorageManager(
		context.Background(),
		types.StorageConfig{
			WorkspaceStorage: types.WorkspaceStorageConfig{
				BaseMountPath:      base,
				DefaultStorageMode: storage.StorageModeLocal,
			},
		},
		types.WorkerPoolConfig{},
		common.NewSafeMap[*ContainerInstance](),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = manager.Mount("workspace-a", nil)
	if err == nil {
		t.Fatal("expected local workspace storage to be rejected")
	}
	if !strings.Contains(err.Error(), "request-scoped credentials") {
		t.Fatalf("expected error to describe workspace storage credentials, got %q", err.Error())
	}
}

func TestWorkspaceStorageBackendSelection(t *testing.T) {
	for _, override := range []string{"", storage.StorageModeMountPoint} {
		t.Run("pool="+override, func(t *testing.T) {
			manager, err := NewWorkspaceStorageManager(context.Background(), types.StorageConfig{
				WorkspaceStorage: types.WorkspaceStorageConfig{DefaultStorageMode: storage.StorageModeGeese},
			}, types.WorkerPoolConfig{StorageMode: override}, nil, nil)
			require.NoError(t, err)
			want := storage.StorageModeGeese
			if override != "" {
				want = override
			}
			require.Equal(t, want, manager.poolConfig.StorageMode)
		})
	}
}

func TestWorkspaceStorageUnmountRetainsFailedMount(t *testing.T) {
	for _, mode := range []string{storage.StorageModeGeese, storage.StorageModeMountPoint} {
		t.Run(mode, func(t *testing.T) {
			manager, err := NewWorkspaceStorageManager(context.Background(), types.StorageConfig{
				WorkspaceStorage: types.WorkspaceStorageConfig{BaseMountPath: t.TempDir()},
			}, types.WorkerPoolConfig{}, nil, nil)
			require.NoError(t, err)
			mount := &trackedStorage{mode: mode, unmountErr: errors.New("unmount failed")}
			manager.Create("workspace", mount)
			require.ErrorIs(t, manager.Unmount("workspace"), mount.unmountErr)
			_, registered := manager.mounts.Get("workspace")
			require.True(t, registered)
			mount.unmountErr = nil
			require.NoError(t, manager.Unmount("workspace"))
			require.True(t, mount.unmounted)
			_, registered = manager.mounts.Get("workspace")
			require.False(t, registered)
		})
	}
}

func TestWorkspaceStorageMountRemainsWarmUntilIdle(t *testing.T) {
	mount := &trackedStorage{}
	manager := &WorkspaceStorageManager{
		mounts:             common.NewSafeMap[storage.Storage](),
		mountLastUsed:      common.NewSafeMap[time.Time](),
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		mountLocks:         make(map[string]*sync.RWMutex),
		config: types.StorageConfig{WorkspaceStorage: types.WorkspaceStorageConfig{
			BaseMountPath: t.TempDir(),
		}},
	}
	manager.mounts.Set("workspace-a", mount)
	manager.mountLastUsed.Set("workspace-a", time.Now())

	if err := manager.cleanupUnused(time.Now().Add(-mountIdleTimeout)); err != nil {
		t.Fatal(err)
	}
	if mount.unmounted {
		t.Fatal("recent workspace mount was unmounted")
	}

	manager.mountLastUsed.Set("workspace-a", time.Now().Add(-mountIdleTimeout-time.Second))
	if err := manager.cleanupUnused(time.Now().Add(-mountIdleTimeout)); err != nil {
		t.Fatal(err)
	}
	if !mount.unmounted {
		t.Fatal("idle workspace mount was not unmounted")
	}
}
