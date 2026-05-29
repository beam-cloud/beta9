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

func TestRewriteWorkspaceStorageEndpointForAgentWorker(t *testing.T) {
	t.Setenv(workspaceStorageEndpointEnv, "http://host.docker.internal:4566")

	got := rewriteWorkspaceStorageEndpoint("http://localstack:4566")
	if got != "http://host.docker.internal:4566" {
		t.Fatalf("endpoint = %q, want Docker host endpoint", got)
	}
}

func TestRewriteWorkspaceStorageEndpointKeepsExternalStorage(t *testing.T) {
	t.Setenv(workspaceStorageEndpointEnv, "http://host.docker.internal:4566")

	got := rewriteWorkspaceStorageEndpoint("https://s3.amazonaws.com")
	if got != "https://s3.amazonaws.com" {
		t.Fatalf("external endpoint was rewritten: %q", got)
	}
}

func TestWorkspaceStorageForMountCopiesEndpointOverride(t *testing.T) {
	t.Setenv(workspaceStorageEndpointEnv, "http://host.docker.internal:4566")

	endpoint := "http://localstack:4566"
	workspaceStorage := &types.WorkspaceStorage{EndpointUrl: &endpoint}
	got := workspaceStorageForMount(workspaceStorage)

	if got == workspaceStorage {
		t.Fatal("expected rewritten workspace storage copy")
	}
	if got.EndpointUrl == nil || *got.EndpointUrl != "http://host.docker.internal:4566" {
		t.Fatalf("endpoint = %#v, want rewritten endpoint", got.EndpointUrl)
	}
	if *workspaceStorage.EndpointUrl != endpoint {
		t.Fatalf("original workspace storage endpoint changed to %q", *workspaceStorage.EndpointUrl)
	}
}
