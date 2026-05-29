package worker

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/beam-cloud/beta9/pkg/storage"
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

func TestSetupContainerMountsPrefersDirectWorkspaceStorageForStubCode(t *testing.T) {
	manager := NewContainerMountManager(types.AppConfig{
		Storage: types.StorageConfig{
			WorkspaceStorage: types.WorkspaceStorageConfig{
				BaseMountPath: t.TempDir(),
			},
		},
	})
	manager.codeCacheRoot = t.TempDir()

	workspace := "workspace-direct"
	objectID := "object-direct"
	mountedObjectPath := filepath.Join(manager.storageConfig.WorkspaceStorage.BaseMountPath, workspace, types.DefaultObjectPrefix, objectID)
	require.NoError(t, writeZipObject(mountedObjectPath, map[string]string{
		"main.py": "mounted\n",
	}))

	directObject, err := zipObjectBytes(map[string]string{
		"main.py": "direct\n",
	})
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(directObject)
	}))
	t.Cleanup(server.Close)

	request := stubCodeMountRequest("container-direct", workspace, objectID)
	bucket := "bucket"
	accessKey := "access"
	secretKey := "secret"
	region := "us-east-1"
	endpoint := server.URL
	storageID := uint(1)
	request.Workspace.Storage = &types.WorkspaceStorage{
		Id:          &storageID,
		BucketName:  &bucket,
		AccessKey:   &accessKey,
		SecretKey:   &secretKey,
		Region:      &region,
		EndpointUrl: &endpoint,
	}
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(types.TempContainerWorkspace(request.ContainerId))) })

	require.NoError(t, manager.SetupContainerMounts(context.Background(), request, discardLogger()))

	workspacePath := request.Mounts[0].LocalPath
	workspaceBytes, err := os.ReadFile(filepath.Join(workspacePath, "main.py"))
	require.NoError(t, err)
	require.Equal(t, "direct\n", string(workspaceBytes))
}

func TestSetupContainerMountsRewritesDirectWorkspaceStorageEndpoint(t *testing.T) {
	manager := NewContainerMountManager(types.AppConfig{
		Storage: types.StorageConfig{
			WorkspaceStorage: types.WorkspaceStorageConfig{
				BaseMountPath: t.TempDir(),
			},
		},
	})
	manager.codeCacheRoot = t.TempDir()

	directObject, err := zipObjectBytes(map[string]string{
		"main.py": "rewritten\n",
	})
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/bucket/objects/object-direct", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(directObject)
	}))
	t.Cleanup(server.Close)
	t.Setenv(workspaceStorageEndpointEnv, server.URL)

	request := stubCodeMountRequest("container-direct-rewrite", "workspace-direct", "object-direct")
	bucket := "bucket"
	accessKey := "access"
	secretKey := "secret"
	region := "us-east-1"
	endpoint := "http://localstack:4566"
	storageID := uint(1)
	request.Workspace.Storage = &types.WorkspaceStorage{
		Id:          &storageID,
		BucketName:  &bucket,
		AccessKey:   &accessKey,
		SecretKey:   &secretKey,
		Region:      &region,
		EndpointUrl: &endpoint,
	}
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(types.TempContainerWorkspace(request.ContainerId))) })

	require.NoError(t, manager.SetupContainerMounts(context.Background(), request, discardLogger()))
	require.FileExists(t, filepath.Join(request.Mounts[0].LocalPath, "main.py"))
}

func TestRequiresWorkspaceStorageMount(t *testing.T) {
	manager := NewContainerMountManager(types.AppConfig{})

	t.Run("direct storage user code only", func(t *testing.T) {
		request := stubCodeMountRequest("container-direct-code", "workspace", "object")
		request.Workspace.Storage = directWorkspaceStorage()

		require.False(t, manager.RequiresWorkspaceStorageMount(request))
	})

	t.Run("legacy storage user code", func(t *testing.T) {
		request := stubCodeMountRequest("container-legacy-code", "workspace", "object")

		require.True(t, manager.RequiresWorkspaceStorageMount(request))
	})

	t.Run("workspace volume", func(t *testing.T) {
		request := stubCodeMountRequest("container-volume", "workspace", "object")
		request.Workspace.Storage = directWorkspaceStorage()
		request.Mounts = append(request.Mounts, types.Mount{
			MountPath: types.WorkerContainerVolumePath + "/data",
			LocalPath: filepath.Join(types.DefaultVolumesPath, request.Workspace.Name, "data"),
		})

		require.True(t, manager.RequiresWorkspaceStorageMount(request))
	})

	t.Run("mountpoint storage", func(t *testing.T) {
		request := stubCodeMountRequest("container-mountpoint", "workspace", "object")
		request.Workspace.Storage = directWorkspaceStorage()
		request.Mounts = []types.Mount{{
			MountPath: "/mnt/s3",
			MountType: storage.StorageModeMountPoint,
		}}

		require.False(t, manager.RequiresWorkspaceStorageMount(request))
	})

	t.Run("build request", func(t *testing.T) {
		sourceImage := "alpine"
		request := stubCodeMountRequest("container-build", "workspace", "object")
		request.Workspace.Storage = directWorkspaceStorage()
		request.BuildOptions.SourceImage = &sourceImage

		require.True(t, manager.RequiresWorkspaceStorageMount(request))
	})
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

func directWorkspaceStorage() *types.WorkspaceStorage {
	storageID := uint(1)
	bucket := "bucket"
	accessKey := "access"
	secretKey := "secret"
	region := "us-east-1"
	return &types.WorkspaceStorage{
		Id:         &storageID,
		BucketName: &bucket,
		AccessKey:  &accessKey,
		SecretKey:  &secretKey,
		Region:     &region,
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

func zipObjectBytes(files map[string]string) ([]byte, error) {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for name, contents := range files {
		entry, err := writer.Create(name)
		if err != nil {
			_ = writer.Close()
			return nil, err
		}
		if _, err := entry.Write([]byte(contents)); err != nil {
			_ = writer.Close()
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
