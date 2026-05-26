package worker

import (
	"archive/zip"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestSetupContainerMountsCachesStubCodeWithoutSharingContainerWorkspaces(t *testing.T) {
	manager := NewContainerMountManager(types.AppConfig{
		Storage: types.StorageConfig{
			WorkspaceStorage: types.WorkspaceStorageConfig{
				BaseMountPath: t.TempDir(),
			},
		},
	})
	manager.codeCacheRoot = t.TempDir()

	workspace := "workspace-1"
	objectID := "object-1"
	objectPath := filepath.Join(manager.storageConfig.WorkspaceStorage.BaseMountPath, workspace, types.DefaultObjectPrefix, objectID)
	require.NoError(t, writeZipObject(objectPath, map[string]string{
		"main.py":         "print('hello')\n",
		"pkg/__init__.py": "",
	}))

	request1 := stubCodeMountRequest("container-cache-1", workspace, objectID)
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(types.TempContainerWorkspace(request1.ContainerId))) })
	require.NoError(t, manager.SetupContainerMounts(context.Background(), request1, discardLogger()))

	workspace1 := request1.Mounts[0].LocalPath
	require.FileExists(t, filepath.Join(workspace1, "main.py"))
	require.NoError(t, os.WriteFile(filepath.Join(workspace1, "main.py"), []byte("mutated\n"), 0644))

	request2 := stubCodeMountRequest("container-cache-2", workspace, objectID)
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(types.TempContainerWorkspace(request2.ContainerId))) })
	require.NoError(t, manager.SetupContainerMounts(context.Background(), request2, discardLogger()))

	workspace2 := request2.Mounts[0].LocalPath
	require.NotEqual(t, workspace1, workspace2)

	cachePath := filepath.Join(manager.codeCacheRoot, stubCodeCacheKey(workspace, objectID))
	require.FileExists(t, filepath.Join(cachePath, ".beta9-cache-ready"))

	cacheBytes, err := os.ReadFile(filepath.Join(cachePath, "main.py"))
	require.NoError(t, err)
	require.Equal(t, "print('hello')\n", string(cacheBytes))

	workspace2Bytes, err := os.ReadFile(filepath.Join(workspace2, "main.py"))
	require.NoError(t, err)
	require.Equal(t, "print('hello')\n", string(workspace2Bytes))
}

func TestStubCodeCacheKeyDoesNotCollideAcrossWorkspaceObjectPairs(t *testing.T) {
	key1 := stubCodeCacheKey("workspace-a", "b-c")
	key2 := stubCodeCacheKey("workspace-a-b", "c")

	require.NotEqual(t, key1, key2)
	require.NotContains(t, key1, string(filepath.Separator))
	require.NotContains(t, key2, string(filepath.Separator))
}

func stubCodeMountRequest(containerID, workspaceName, objectID string) *types.ContainerRequest {
	storageID := uint(1)
	return &types.ContainerRequest{
		ContainerId: containerID,
		Workspace: types.Workspace{
			Name: workspaceName,
			Storage: &types.WorkspaceStorage{
				Id: &storageID,
			},
		},
		Stub: types.StubWithRelated{Object: types.Object{ExternalId: objectID}},
		Mounts: []types.Mount{{
			MountPath: types.WorkerUserCodeVolume,
		}},
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func writeZipObject(path string, files map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := zip.NewWriter(file)
	defer writer.Close()

	for name, contents := range files {
		entry, err := writer.Create(name)
		if err != nil {
			return err
		}
		if _, err := entry.Write([]byte(contents)); err != nil {
			return err
		}
	}

	return nil
}
