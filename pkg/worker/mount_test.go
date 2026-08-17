package worker

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	abstractionscommon "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type bindMountRetryTestClock struct {
	current time.Time
	waits   []time.Duration
}

func (c *bindMountRetryTestClock) now() time.Time {
	return c.current
}

func (c *bindMountRetryTestClock) wait(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.waits = append(c.waits, delay)
	c.current = c.current.Add(delay)
	return nil
}

func bindMountRetryTestOps(clock *bindMountRetryTestClock, mkdirAll func(string, os.FileMode) error) bindMountSourceDirOps {
	ops := defaultBindMountSourceDirOps()
	ops.mkdirAll = mkdirAll
	ops.now = clock.now
	ops.wait = clock.wait
	ops.jitter = func(delay time.Duration) time.Duration { return delay }
	return ops
}

func TestSetupContainerMountsUsesLocalWorkspaceForEmptySandbox(t *testing.T) {
	manager := NewContainerMountManager(types.AppConfig{})
	manager.codeCacheRoot = filepath.Join(t.TempDir(), "code-cache")

	request := stubCodeMountRequest("sandbox-empty-code", "workspace-empty", "object-empty")
	request.Stub.Type = types.StubType(types.StubTypeSandbox)
	request.Stub.Object.Hash = abstractionscommon.EmptyStubObjectHash()
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(types.TempContainerWorkspace(request.ContainerId))) })

	require.NoError(t, manager.SetupContainerMounts(context.Background(), request, discardLogger()))
	require.DirExists(t, request.Mounts[0].LocalPath)
	require.FileExists(t, filepath.Join(filepath.Dir(request.Mounts[0].LocalPath), ".workspace-ready"))
	require.NoDirExists(t, manager.codeCacheRoot)
}

func TestEnsureBindMountSourceDirsRetriesWorkspaceEAGAIN(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "workspace-storage")
	localPath := filepath.Join(basePath, "workspace", "outputs", "stub")
	manager := NewContainerMountManager(types.AppConfig{Storage: types.StorageConfig{
		WorkspaceStorage: types.WorkspaceStorageConfig{BaseMountPath: basePath},
	}})
	transientErr := &os.PathError{Op: "mkdir", Path: localPath, Err: syscall.EAGAIN}
	attempts := 0
	clock := &bindMountRetryTestClock{current: time.Unix(1, 0)}
	ops := bindMountRetryTestOps(clock, func(string, os.FileMode) error {
		attempts++
		if attempts < 3 {
			return transientErr
		}
		return nil
	})

	err := manager.ensureBindMountSourceDirsWithOps(context.Background(), []types.Mount{{
		LocalPath: localPath,
		MountPath: "/data",
	}}, ops)

	require.NoError(t, err)
	require.Equal(t, 3, attempts)
	require.Equal(t, []time.Duration{250 * time.Millisecond, 500 * time.Millisecond}, clock.waits)
}

func TestEnsureBindMountSourceDirsStopsAtWorkspaceRetryBudget(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "workspace-storage")
	localPath := filepath.Join(basePath, "workspace", "volumes", "data")
	manager := NewContainerMountManager(types.AppConfig{Storage: types.StorageConfig{
		WorkspaceStorage: types.WorkspaceStorageConfig{BaseMountPath: basePath},
	}})
	transientErr := &os.PathError{Op: "mkdir", Path: localPath, Err: syscall.EAGAIN}
	attempts := 0
	clock := &bindMountRetryTestClock{current: time.Unix(1, 0)}
	ops := bindMountRetryTestOps(clock, func(string, os.FileMode) error {
		attempts++
		return transientErr
	})
	ops.retryBudget = 2 * time.Second

	err := manager.ensureBindMountSourceDirsWithOps(context.Background(), []types.Mount{{
		LocalPath: localPath,
		MountPath: "/data",
	}}, ops)

	require.ErrorIs(t, err, transientErr)
	require.Equal(t, 4, attempts)
	require.Equal(t, []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 250 * time.Millisecond}, clock.waits)
}

func TestEnsureBindMountSourceDirsStopsWhenContextCanceled(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "workspace-storage")
	localPath := filepath.Join(basePath, "workspace", "outputs", "stub")
	manager := NewContainerMountManager(types.AppConfig{Storage: types.StorageConfig{
		WorkspaceStorage: types.WorkspaceStorageConfig{BaseMountPath: basePath},
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts := 0
	waits := 0
	clock := &bindMountRetryTestClock{current: time.Unix(1, 0)}
	ops := bindMountRetryTestOps(clock, func(string, os.FileMode) error {
		attempts++
		return syscall.EAGAIN
	})
	ops.wait = func(ctx context.Context, _ time.Duration) error {
		waits++
		cancel()
		return ctx.Err()
	}

	err := manager.ensureBindMountSourceDirsWithOps(ctx, []types.Mount{{
		LocalPath: localPath,
		MountPath: "/data",
	}}, ops)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
	require.Equal(t, 1, waits)
}

func TestEnsureBindMountSourceDirsRetriesOnlyWorkspaceFusePaths(t *testing.T) {
	root := t.TempDir()
	basePath := filepath.Join(root, "workspace-storage")
	workspacePath := filepath.Join(basePath, "workspace", "volumes", "data")
	transientErr := &os.PathError{Op: "mkdir", Path: workspacePath, Err: syscall.EAGAIN}
	permanentErr := errors.New("permission denied")

	tests := []struct {
		name  string
		mount types.Mount
		err   error
	}{
		{
			name:  "healthy workspace path",
			mount: types.Mount{LocalPath: workspacePath, MountPath: "/data"},
		},
		{
			name:  "workspace non-EAGAIN",
			mount: types.Mount{LocalPath: workspacePath, MountPath: "/data"},
			err:   permanentErr,
		},
		{
			name:  "sibling prefix",
			mount: types.Mount{LocalPath: filepath.Join(basePath+"-sibling", "workspace"), MountPath: "/data"},
			err:   transientErr,
		},
		{
			name:  "local path",
			mount: types.Mount{LocalPath: filepath.Join(root, "local", "data"), MountPath: "/data"},
			err:   transientErr,
		},
		{
			name:  "local mount under workspace root",
			mount: types.Mount{LocalPath: workspacePath, MountPath: "/data", MountType: storage.StorageModeLocal},
			err:   transientErr,
		},
		{
			name: "durable disk under workspace root",
			mount: types.Mount{
				LocalPath: workspacePath,
				MountPath: "/data",
				MountType: types.StorageModeDurableDisk,
			},
			err: transientErr,
		},
		{
			name: "durable disk metadata under workspace root",
			mount: types.Mount{
				LocalPath:   workspacePath,
				MountPath:   "/data",
				DurableDisk: &types.DurableDiskMountConfig{Name: "data"},
			},
			err: transientErr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := NewContainerMountManager(types.AppConfig{Storage: types.StorageConfig{
				WorkspaceStorage: types.WorkspaceStorageConfig{BaseMountPath: basePath},
			}})
			attempts := 0
			clock := &bindMountRetryTestClock{current: time.Unix(1, 0)}
			ops := bindMountRetryTestOps(clock, func(string, os.FileMode) error {
				attempts++
				return test.err
			})

			err := manager.ensureBindMountSourceDirsWithOps(context.Background(), []types.Mount{test.mount}, ops)

			if test.err == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, test.err)
			}
			require.Equal(t, 1, attempts)
			require.Empty(t, clock.waits)
		})
	}
}

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

func TestSetupContainerMountsReusesDurableStubCodeCacheAcrossManagers(t *testing.T) {
	cacheRoot := t.TempDir()
	storageRoot := t.TempDir()
	config := types.AppConfig{
		Worker: types.WorkerConfig{CacheEnabled: true},
		Storage: types.StorageConfig{
			WorkspaceStorage: types.WorkspaceStorageConfig{
				BaseMountPath: storageRoot,
			},
		},
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:   true,
				MountPath: cacheRoot,
			},
		},
	}

	workspace := "workspace-durable"
	objectID := "object-durable"
	objectPath := filepath.Join(storageRoot, workspace, types.DefaultObjectPrefix, objectID)
	require.NoError(t, writeZipObject(objectPath, map[string]string{
		"main.py": "print('durable')\n",
	}))

	request1 := stubCodeMountRequest("container-durable-1", workspace, objectID)
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(types.TempContainerWorkspace(request1.ContainerId))) })
	require.NoError(t, NewContainerMountManager(config).SetupContainerMounts(context.Background(), request1, discardLogger()))
	require.NoError(t, os.Remove(objectPath))

	request2 := stubCodeMountRequest("container-durable-2", workspace, objectID)
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(types.TempContainerWorkspace(request2.ContainerId))) })
	require.NoError(t, NewContainerMountManager(config).SetupContainerMounts(context.Background(), request2, discardLogger()))

	workspaceBytes, err := os.ReadFile(filepath.Join(request2.Mounts[0].LocalPath, "main.py"))
	require.NoError(t, err)
	require.Equal(t, "print('durable')\n", string(workspaceBytes))
	require.FileExists(t, filepath.Join(cacheRoot, "stub-code", stubCodeCacheKey(workspace, objectID), ".beta9-cache-ready"))
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

func TestStubCodeCacheRootUsesDiskCacheWhenEnabled(t *testing.T) {
	cacheRoot := t.TempDir()
	config := types.AppConfig{
		Worker: types.WorkerConfig{CacheEnabled: true},
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:   true,
				MountPath: cacheRoot,
			},
		},
	}

	require.Equal(t, filepath.Join(cacheRoot, "stub-code"), stubCodeCacheRoot(config))
}

func TestStubCodeCacheRootFallsBackToTempWhenDiskCacheDisabled(t *testing.T) {
	config := types.AppConfig{
		Worker: types.WorkerConfig{CacheEnabled: true},
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:   false,
				MountPath: t.TempDir(),
			},
		},
	}

	require.Equal(t, filepath.Join(os.TempDir(), "beta9-stub-code-cache"), stubCodeCacheRoot(config))
}

func TestStubCodeCacheRootRespectsPoolDiskCacheOverride(t *testing.T) {
	globalCacheRoot := t.TempDir()
	poolCacheRoot := t.TempDir()
	config := types.AppConfig{
		Worker: types.WorkerConfig{CacheEnabled: true},
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:   true,
				MountPath: globalCacheRoot,
			},
		},
	}
	poolConfig := types.WorkerPoolConfig{
		Cache: types.WorkerPoolCacheConfig{
			Disk: types.WorkerPoolCacheDiskConfig{
				MountPath: poolCacheRoot,
			},
		},
	}

	require.Equal(t, filepath.Join(poolCacheRoot, "stub-code"), stubCodeCacheRoot(config, poolConfig))
}

func TestStubCodeCacheRootRespectsPoolDiskCacheDisable(t *testing.T) {
	disabled := false
	config := types.AppConfig{
		Worker: types.WorkerConfig{CacheEnabled: true},
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:   true,
				MountPath: t.TempDir(),
			},
		},
	}
	poolConfig := types.WorkerPoolConfig{
		Cache: types.WorkerPoolCacheConfig{
			Disk: types.WorkerPoolCacheDiskConfig{
				Enabled: &disabled,
			},
		},
	}

	require.Equal(t, filepath.Join(os.TempDir(), "beta9-stub-code-cache"), stubCodeCacheRoot(config, poolConfig))
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
		dockerfile := "FROM alpine"
		request := stubCodeMountRequest("container-build", "workspace", "object")
		request.Workspace.Storage = directWorkspaceStorage()
		request.BuildOptions.Dockerfile = &dockerfile
		request.BuildOptions.BuildCtxObject = &request.Stub.Object.ExternalId

		require.False(t, manager.RequiresWorkspaceStorageMount(request))
	})

	t.Run("build request with incomplete direct storage", func(t *testing.T) {
		dockerfile := "FROM alpine"
		request := stubCodeMountRequest("container-build-incomplete", "workspace", "object")
		request.BuildOptions.Dockerfile = &dockerfile
		request.BuildOptions.BuildCtxObject = &request.Stub.Object.ExternalId

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
