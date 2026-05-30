package worker

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestWorkspaceStorageManagerMountsLocalStorage(t *testing.T) {
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

	mount, err := manager.Mount("workspace-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if mount.Mode() != storage.StorageModeLocal {
		t.Fatalf("expected local storage, got %q", mount.Mode())
	}
	if !pathExists(filepath.Join(base, "workspace-a")) {
		t.Fatal("expected local workspace mount directory to exist")
	}
}
