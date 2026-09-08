package worker

import (
	"context"
	"path"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/storage"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type abortableTrackedStorage struct {
	trackedStorage
	abortPath string
	calls     []string
}

func (s *abortableTrackedStorage) AbortPendingOperations(localPath string) error {
	s.abortPath = localPath
	s.calls = append(s.calls, "abort")
	return nil
}

func (s *abortableTrackedStorage) Unmount(localPath string) error {
	s.calls = append(s.calls, "unmount")
	return s.trackedStorage.Unmount(localPath)
}

func TestStartedRuntimeStuckAfterKillSchedulesWorkspaceRecovery(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "stopping"
		if force {
			name = "orphaned"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request := &types.ContainerRequest{
				ContainerId: "stuck-container",
				Workspace:   types.Workspace{Name: "stuck-workspace"},
			}
			rt := &shutdownSignalRuntime{mockRuntime: mockRuntime{
				state: func(context.Context, string) (runtime.State, error) {
					// SIGKILL succeeds, but a blocked filesystem operation keeps
					// the runtime alive until its workspace mount is recovered.
					return runtime.State{Status: types.RuncContainerStatusRunning}, nil
				},
			}}
			instance := &ContainerInstance{
				Id: request.ContainerId, ExitCode: -1, StopReason: types.StopContainerReasonUser,
				Request: request, Runtime: rt,
			}
			instance.markRuntimeStarted(1234)
			instance.StopEscalationStarted.Store(true)
			instances := common.NewSafeMap[*ContainerInstance]()
			instances.Set(instance.Id, instance)
			mount := &abortableTrackedStorage{trackedStorage: trackedStorage{mode: storage.StorageModeGeese}}
			otherMount := &abortableTrackedStorage{trackedStorage: trackedStorage{mode: storage.StorageModeGeese}}
			manager := &WorkspaceStorageManager{
				mounts: common.NewSafeMap[storage.Storage](), mountLastUsed: common.NewSafeMap[time.Time](),
				containerInstances: instances, mountLocks: make(map[string]*sync.RWMutex),
				poolConfig: types.WorkerPoolConfig{StorageMode: storage.StorageModeGeese},
				config:     types.StorageConfig{WorkspaceStorage: types.WorkspaceStorageConfig{BaseMountPath: t.TempDir()}},
			}
			manager.mounts.Set(request.Workspace.Name, mount)
			manager.mounts.Set("other-workspace", otherMount)
			worker := &Worker{ctx: ctx, containerInstances: instances, storageManager: manager}

			worker.stopObservedContainer(instance.Id, request, types.EventSourceWorkerStatusHeartbeat, force)

			require.False(t, instance.StopEscalationStarted.Load(), "runtime stop must remain retryable")
			require.True(t, instance.StuckMountRecoveryStarted.Load(), "a started runtime blocked after SIGKILL needs mount recovery")
			expected := []syscall.Signal{syscall.SIGKILL}
			if !force {
				expected = append([]syscall.Signal{syscall.SIGTERM}, expected...)
			}
			require.Equal(t, expected, rt.recordedSignals())

			// A new container can arrive before the recovery timer fires. The
			// callback must recheck the shared workspace before unmounting it.
			instances.Set("sibling", &ContainerInstance{ExitCode: -1, Request: request})
			worker.abortStuckWorkspaceMount(request)
			require.False(t, mount.unmounted, "a running sibling must keep its mount")
			require.Empty(t, mount.calls, "a running sibling must protect pending operations too")
			instances.Delete("sibling")
			worker.abortStuckWorkspaceMount(request)
			require.True(t, mount.unmounted, "a workspace with only stopping containers can recover")
			require.Equal(t, []string{"abort", "unmount"}, mount.calls)
			require.Equal(t, path.Join(manager.config.WorkspaceStorage.BaseMountPath, request.Workspace.Name), mount.abortPath)
			require.False(t, otherMount.unmounted, "recovery must not touch another workspace")
			require.Empty(t, otherMount.calls)
		})
	}
}
