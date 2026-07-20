package worker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

type trackedStorage struct{ unmounted bool }

func (*trackedStorage) Mount(string) error { return nil }
func (s *trackedStorage) Unmount(string) error {
	s.unmounted = true
	return nil
}
func (*trackedStorage) Format(string) error { return nil }
func (*trackedStorage) Mode() string        { return storage.StorageModeLocal }

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

func TestWorkspaceStorageMountRemainsWarmUntilIdle(t *testing.T) {
	mount := &trackedStorage{}
	manager := &WorkspaceStorageManager{
		mounts:             common.NewSafeMap[storage.Storage](),
		mountLastUsed:      common.NewSafeMap[time.Time](),
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		mountLocks:         make(map[string]*sync.Mutex),
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

func TestWorkspaceStorageMountReuseWaitsForWorkspaceLock(t *testing.T) {
	mount := &trackedStorage{}
	manager := &WorkspaceStorageManager{
		mounts:             common.NewSafeMap[storage.Storage](),
		mountLastUsed:      common.NewSafeMap[time.Time](),
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		mountLocks:         make(map[string]*sync.Mutex),
		config: types.StorageConfig{WorkspaceStorage: types.WorkspaceStorageConfig{
			BaseMountPath: t.TempDir(),
		}},
	}
	manager.mounts.Set("workspace-a", mount)
	manager.mountLastUsed.Set("workspace-a", time.Now())

	unlock := manager.lockWorkspaceMount("workspace-a")
	result := make(chan storage.Storage, 1)
	errResult := make(chan error, 1)
	go func() {
		got, err := manager.Mount("workspace-a", nil)
		result <- got
		errResult <- err
	}()

	select {
	case <-result:
		t.Fatal("Mount reused a workspace filesystem while its teardown lock was held")
	case <-time.After(50 * time.Millisecond):
	}
	unlock()

	select {
	case got := <-result:
		if got != mount {
			t.Fatalf("Mount returned %T, want existing tracked mount", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Mount did not resume after workspace lock release")
	}
	if err := <-errResult; err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceStorageCleanupRechecksActiveContainers(t *testing.T) {
	mount := &trackedStorage{}
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set("container-a", &ContainerInstance{Request: &types.ContainerRequest{
		Workspace: types.Workspace{Name: "workspace-a"},
	}})
	manager := &WorkspaceStorageManager{
		mounts:             common.NewSafeMap[storage.Storage](),
		mountLastUsed:      common.NewSafeMap[time.Time](),
		containerInstances: instances,
		mountLocks:         make(map[string]*sync.Mutex),
		config: types.StorageConfig{WorkspaceStorage: types.WorkspaceStorageConfig{
			BaseMountPath: t.TempDir(),
		}},
	}
	manager.mounts.Set("workspace-a", mount)
	manager.mountLastUsed.Set("workspace-a", time.Now().Add(-2*mountIdleTimeout))

	if err := manager.cleanupUnused(time.Now().Add(-mountIdleTimeout)); err != nil {
		t.Fatal(err)
	}
	if mount.unmounted {
		t.Fatal("cleanup unmounted workspace storage used by an active container")
	}
}
