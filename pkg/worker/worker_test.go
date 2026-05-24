package worker

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

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

	require.Equal(t, 64, containerStartLimitForPoolRuntime(poolConfig, types.ContainerRuntimeGvisor.String()))
	require.Equal(t, 64, containerStartLimitForPoolRuntime(poolConfig, types.ContainerRuntimeRunc.String()))
}

func TestContainerStartLimitForPoolRuntimeAllowsEnvOverride(t *testing.T) {
	t.Setenv("WORKER_CONTAINER_START_CONCURRENCY", "8")

	poolConfig := types.WorkerPoolConfig{ContainerStartConcurrency: 64}

	require.Equal(t, 8, containerStartLimitForPoolRuntime(poolConfig, types.ContainerRuntimeGvisor.String()))
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

func TestMarkContainerStoppingUsesStoppingTTL(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	worker := &Worker{containerRepoClient: repoClient}

	worker.markContainerStopping("container-1")

	require.Equal(t, 1, repoClient.updateStatusCalls)
	require.Equal(t, "container-1", repoClient.lastUpdateStatus.ContainerId)
	require.Equal(t, string(types.ContainerStatusStopping), repoClient.lastUpdateStatus.Status)
	require.Equal(t, int64(types.ContainerStateTtlSWhilePending), repoClient.lastUpdateStatus.ExpirySeconds)
}

type fakeContainerRepoClient struct {
	state             *pb.ContainerState
	getStateCalls     int
	updateStatusCalls int
	lastUpdateStatus  *pb.UpdateContainerStatusRequest
}

func (f *fakeContainerRepoClient) GetContainerState(ctx context.Context, in *pb.GetContainerStateRequest, opts ...grpc.CallOption) (*pb.GetContainerStateResponse, error) {
	f.getStateCalls++
	return &pb.GetContainerStateResponse{
		Ok:          true,
		ContainerId: in.ContainerId,
		State:       f.state,
	}, nil
}

func (f *fakeContainerRepoClient) DeleteContainerState(ctx context.Context, in *pb.DeleteContainerStateRequest, opts ...grpc.CallOption) (*pb.DeleteContainerStateResponse, error) {
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
	return &pb.SetContainerAddressResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) SetContainerAddressMap(ctx context.Context, in *pb.SetContainerAddressMapRequest, opts ...grpc.CallOption) (*pb.SetContainerAddressMapResponse, error) {
	return &pb.SetContainerAddressMapResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) GetContainerAddressMap(ctx context.Context, in *pb.GetContainerAddressMapRequest, opts ...grpc.CallOption) (*pb.GetContainerAddressMapResponse, error) {
	return &pb.GetContainerAddressMapResponse{Ok: true}, nil
}

func (f *fakeContainerRepoClient) SetWorkerAddress(ctx context.Context, in *pb.SetWorkerAddressRequest, opts ...grpc.CallOption) (*pb.SetWorkerAddressResponse, error) {
	return &pb.SetWorkerAddressResponse{Ok: true}, nil
}
