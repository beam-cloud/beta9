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
