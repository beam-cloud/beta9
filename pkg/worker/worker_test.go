package worker

import (
	"context"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type shutdownSignalRuntime struct {
	mockRuntime
	mu      sync.Mutex
	signals []syscall.Signal
	onKill  func(syscall.Signal)
}

func (m *shutdownSignalRuntime) Kill(ctx context.Context, containerID string, sig syscall.Signal, opts *runtime.KillOpts) error {
	m.mu.Lock()
	m.signals = append(m.signals, sig)
	m.mu.Unlock()
	if m.onKill != nil {
		m.onKill(sig)
	}
	return nil
}

func (m *shutdownSignalRuntime) recordedSignals() []syscall.Signal {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]syscall.Signal(nil), m.signals...)
}

func TestCalculateCPUShares(t *testing.T) {
	tests := []struct {
		name       string
		millicores int64
		wantShares uint64
		wantQuota  int64
	}{
		{
			name:       "100m",
			millicores: 100,
			wantShares: 102,
			wantQuota:  10_000,
		},
		{
			name:       "250m",
			millicores: 250,
			wantShares: 256,
			wantQuota:  25_000,
		},
		{
			name:       "1000m",
			millicores: 1000,
			wantShares: 1024,
			wantQuota:  100_000,
		},
		{
			name:       "2000m",
			millicores: 2000,
			wantShares: 2048,
			wantQuota:  200_000,
		},
		{
			name:       "32000m",
			millicores: 32_000,
			wantShares: 32_768,
			wantQuota:  3_200_000,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := calculateCPUShares(test.millicores)
			if got != test.wantShares {
				t.Errorf("calculateCPUShares(%d) = %d, want %d", test.millicores, got, test.wantShares)
			}

			gotQuota := calculateCPUQuota(test.millicores)
			if gotQuota != test.wantQuota {
				t.Errorf("calculateCPUQuota(%d) = %d, want %d", test.millicores, gotQuota, test.wantQuota)
			}
		})
	}
}

func TestContainerStartLimitForRuntimeUsesRuntimeName(t *testing.T) {
	t.Setenv("WORKER_CONTAINER_START_CONCURRENCY", "")

	require.Equal(t, 16, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeRunc.String(), 16, 2))
	require.Equal(t, 2, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeGvisor.String(), 16, 2))
	require.Equal(t, 16, containerStartLimitForRuntimeWithDefaults("unknown", 16, 2))
}

func TestContainerStartLimitForRuntimeAllowsExplicitOverride(t *testing.T) {
	t.Setenv("WORKER_CONTAINER_START_CONCURRENCY", "4")

	require.Equal(t, 4, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeRunc.String(), 16, 2))
	require.Equal(t, 4, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeGvisor.String(), 16, 2))
}

func TestContainerStartLimitForPoolRuntimeUsesPoolConfig(t *testing.T) {
	t.Setenv("WORKER_CONTAINER_START_CONCURRENCY", "")

	poolConfig := types.WorkerPoolConfig{ContainerStartConcurrency: 64}

	require.Equal(t, 64, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeGvisor.String(), 0))
	require.Equal(t, 64, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeRunc.String(), 0))
}

func TestContainerStartLimitForPoolRuntimeCapsByWorkerCPU(t *testing.T) {
	t.Setenv("WORKER_CONTAINER_START_CONCURRENCY", "")

	poolConfig := types.WorkerPoolConfig{
		ContainerStartConcurrency: 128,
		PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{
			DefaultWorkerCPU: "1000m",
		},
	}

	require.Equal(t, 2, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeRunc.String(), 1000))
	require.Equal(t, 4, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeGvisor.String(), 1000))
}

func TestContainerStartLimitForPoolRuntimeScalesWithWorkerCPU(t *testing.T) {
	t.Setenv("WORKER_CONTAINER_START_CONCURRENCY", "")

	poolConfig := types.WorkerPoolConfig{
		ContainerStartConcurrency: 128,
		PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{
			DefaultWorkerCPU: "8000m",
		},
	}

	require.Equal(t, 16, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeRunc.String(), 8000))
	require.Equal(t, 32, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeGvisor.String(), 8000))
}

func TestContainerStartLimitForPoolRuntimeAllowsEnvOverride(t *testing.T) {
	t.Setenv("WORKER_CONTAINER_START_CONCURRENCY", "8")

	poolConfig := types.WorkerPoolConfig{ContainerStartConcurrency: 64}

	require.Equal(t, 8, containerStartLimitForPoolRuntime(poolConfig, "", types.ContainerRuntimeGvisor.String(), 0))
}

func TestUpdateContainerStatusOnceStopsHeartbeatForExitedInstance(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{
			ContainerId: "container-1",
			Status:      string(types.ContainerStatusRunning),
		},
	}
	worker := &Worker{
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerRepoClient: repoClient,
		stopContainerChan:   make(chan stopContainerEvent, 1),
	}
	worker.containerInstances.Set("container-1", &ContainerInstance{ExitCode: 0})

	done, err := worker.updateContainerStatusOnce(&types.ContainerRequest{
		ContainerId: "container-1",
		ImageId:     "image-1",
	})

	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, 0, repoClient.getStateCalls)
	require.Equal(t, 0, repoClient.updateStatusCalls)
}

func TestUpdateContainerStatusOnceReconcilesStartedPendingContainer(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{
			ContainerId: "container-1",
			Status:      string(types.ContainerStatusPending),
		},
	}
	worker := &Worker{
		containerInstances:  common.NewSafeMap[*ContainerInstance](),
		containerRepoClient: repoClient,
		stopContainerChan:   make(chan stopContainerEvent, 1),
	}
	worker.containerInstances.Set("container-1", &ContainerInstance{
		ExitCode:       -1,
		RuntimeStarted: true,
		RuntimePid:     1234,
	})

	done, err := worker.updateContainerStatusOnce(&types.ContainerRequest{
		ContainerId: "container-1",
		ImageId:     "image-1",
	})

	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, 1, repoClient.getStateCalls)
	require.Equal(t, 1, repoClient.updateStatusCalls)
	require.Equal(t, string(types.ContainerStatusRunning), repoClient.lastUpdateStatus.Status)
	require.Equal(t, int64(types.ContainerStateTtlS), repoClient.lastUpdateStatus.ExpirySeconds)
}

func TestShutdownWaitDrainsWithoutStoppingActiveContainer(t *testing.T) {
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		containerWg:        sync.WaitGroup{},
	}
	rt := &shutdownSignalRuntime{}
	worker.containerInstances.Set("container-1", &ContainerInstance{
		Id:      "container-1",
		Runtime: rt,
	})

	done := make(chan struct{})
	go func() {
		worker.waitForActiveContainersBeforeShutdown()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("shutdown wait returned before active instance drained")
	case <-time.After(25 * time.Millisecond):
	}
	require.Empty(t, rt.recordedSignals())

	worker.containerInstances.Delete("container-1")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown wait did not return after active instance drained")
	}
	require.Empty(t, rt.recordedSignals())
}

func TestStopActiveContainersForShutdownStopsNestedRuntimeBeforeWorkerExit(t *testing.T) {
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		containerWg:        sync.WaitGroup{},
		config: types.AppConfig{
			Worker: types.WorkerConfig{TerminationGracePeriod: 30},
		},
	}
	rt := &shutdownSignalRuntime{}
	rt.onKill = func(sig syscall.Signal) {
		worker.containerInstances.Delete("container-1")
	}
	worker.containerInstances.Set("container-1", &ContainerInstance{
		Id:      "container-1",
		Runtime: rt,
	})

	worker.stopActiveContainersForShutdown()

	require.Empty(t, worker.activeContainerIDs())
	require.Equal(t, []syscall.Signal{syscall.SIGTERM}, rt.recordedSignals())
}

func TestStopActiveContainersForShutdownForceKillsStuckRuntime(t *testing.T) {
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		containerWg:        sync.WaitGroup{},
		config: types.AppConfig{
			Worker: types.WorkerConfig{TerminationGracePeriod: 1},
		},
	}
	rt := &shutdownSignalRuntime{}
	rt.onKill = func(sig syscall.Signal) {
		if sig == syscall.SIGKILL {
			worker.containerInstances.Delete("container-1")
		}
	}
	worker.containerInstances.Set("container-1", &ContainerInstance{
		Id:      "container-1",
		Runtime: rt,
	})

	start := time.Now()
	worker.stopActiveContainersForShutdown()

	require.GreaterOrEqual(t, time.Since(start), time.Second)
	require.Empty(t, worker.activeContainerIDs())
	require.Equal(t, []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}, rt.recordedSignals())
}

func TestMarkContainerStoppingUsesStoppingTTL(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	worker := &Worker{containerRepoClient: repoClient}

	worker.markContainerStopping("container-1")

	require.Equal(t, 1, repoClient.updateStatusCalls)
	require.Equal(t, "container-1", repoClient.lastUpdateStatus.ContainerId)
	require.Equal(t, string(types.ContainerStatusStopping), repoClient.lastUpdateStatus.Status)
	require.Equal(t, int64(types.ContainerStateTtlSWhilePending), repoClient.lastUpdateStatus.ExpirySeconds)
}

func TestDropCancelledContainerRequestDeletesStoppingStateAndReleasesCapacity(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{
			ContainerId: "build-1",
			Status:      string(types.ContainerStatusStopping),
		},
	}
	request := &types.ContainerRequest{ContainerId: "build-1"}
	worker := &Worker{
		containerRepoClient: repoClient,
		completedRequests:   make(chan *types.ContainerRequest, 1),
	}

	require.True(t, worker.dropCancelledContainerRequest(request))
	require.Equal(t, 1, repoClient.getStateCalls)
	require.Equal(t, 1, repoClient.deleteStateCalls)
	require.Equal(t, "build-1", repoClient.lastDeleteContainerID)

	select {
	case got := <-worker.completedRequests:
		require.Equal(t, request, got)
	default:
		t.Fatal("expected skipped request to release capacity")
	}
}

func TestDropCancelledContainerRequestReleasesCapacityForMissingState(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		getStateErrorMsg: (&types.ErrContainerStateNotFound{ContainerId: "build-1"}).Error(),
	}
	request := &types.ContainerRequest{ContainerId: "build-1"}
	worker := &Worker{
		containerRepoClient: repoClient,
		completedRequests:   make(chan *types.ContainerRequest, 1),
	}

	require.True(t, worker.dropCancelledContainerRequest(request))
	require.Equal(t, 1, repoClient.getStateCalls)
	require.Equal(t, 0, repoClient.deleteStateCalls)

	select {
	case got := <-worker.completedRequests:
		require.Equal(t, request, got)
	default:
		t.Fatal("expected skipped request to release capacity")
	}
}

type fakeContainerRepoClient struct {
	state                 *pb.ContainerState
	getStateErrorMsg      string
	getStateCalls         int
	deleteStateCalls      int
	lastDeleteContainerID string
	updateStatusCalls     int
	lastUpdateStatus      *pb.UpdateContainerStatusRequest
	addressMap            map[int32]string
	setAddressCalls       int
	lastSetAddress        *pb.SetContainerAddressRequest
	setAddressMapCalls    int
	lastSetAddressMap     *pb.SetContainerAddressMapRequest
}

func (f *fakeContainerRepoClient) GetContainerState(ctx context.Context, in *pb.GetContainerStateRequest, opts ...grpc.CallOption) (*pb.GetContainerStateResponse, error) {
	f.getStateCalls++
	if f.getStateErrorMsg != "" {
		return &pb.GetContainerStateResponse{
			Ok:       false,
			ErrorMsg: f.getStateErrorMsg,
		}, nil
	}

	return &pb.GetContainerStateResponse{
		Ok:          true,
		ContainerId: in.ContainerId,
		State:       f.state,
	}, nil
}

func (f *fakeContainerRepoClient) DeleteContainerState(ctx context.Context, in *pb.DeleteContainerStateRequest, opts ...grpc.CallOption) (*pb.DeleteContainerStateResponse, error) {
	f.deleteStateCalls++
	f.lastDeleteContainerID = in.ContainerId
	return &pb.DeleteContainerStateResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) UpdateContainerStatus(ctx context.Context, in *pb.UpdateContainerStatusRequest, opts ...grpc.CallOption) (*pb.UpdateContainerStatusResponse, error) {
	f.updateStatusCalls++
	f.lastUpdateStatus = in
	return &pb.UpdateContainerStatusResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) SetContainerExitCode(ctx context.Context, in *pb.SetContainerExitCodeRequest, opts ...grpc.CallOption) (*pb.SetContainerExitCodeResponse, error) {
	return &pb.SetContainerExitCodeResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) SetContainerAddress(ctx context.Context, in *pb.SetContainerAddressRequest, opts ...grpc.CallOption) (*pb.SetContainerAddressResponse, error) {
	f.setAddressCalls++
	f.lastSetAddress = in
	return &pb.SetContainerAddressResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) SetContainerAddressMap(ctx context.Context, in *pb.SetContainerAddressMapRequest, opts ...grpc.CallOption) (*pb.SetContainerAddressMapResponse, error) {
	f.setAddressMapCalls++
	f.lastSetAddressMap = in
	return &pb.SetContainerAddressMapResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) GetContainerAddressMap(ctx context.Context, in *pb.GetContainerAddressMapRequest, opts ...grpc.CallOption) (*pb.GetContainerAddressMapResponse, error) {
	return &pb.GetContainerAddressMapResponse{Ok: true, AddressMap: f.addressMap}, nil
}

func (f *fakeContainerRepoClient) SetWorkerAddress(ctx context.Context, in *pb.SetWorkerAddressRequest, opts ...grpc.CallOption) (*pb.SetWorkerAddressResponse, error) {
	return &pb.SetWorkerAddressResponse{Ok: true}, nil
}
