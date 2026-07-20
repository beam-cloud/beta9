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
