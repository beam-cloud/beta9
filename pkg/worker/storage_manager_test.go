package worker

import (
	"context"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
)

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
