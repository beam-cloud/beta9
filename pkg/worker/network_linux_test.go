//go:build linux

package worker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// slotPoolWorkerRepoClient records the pool lock and reservation RPCs in order.
type slotPoolWorkerRepoClient struct {
	pb.WorkerRepositoryServiceClient
	assignments []*pb.ContainerIpAssignment
	removeDelay time.Duration

	mu       sync.Mutex
	calls    []string
	inflight int
	peak     int
}

func (c *slotPoolWorkerRepoClient) record(call string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
}

func (c *slotPoolWorkerRepoClient) SetNetworkLock(context.Context, *pb.SetNetworkLockRequest, ...grpc.CallOption) (*pb.SetNetworkLockResponse, error) {
	c.record("lock")
	return &pb.SetNetworkLockResponse{Ok: true, Token: "token"}, nil
}

func (c *slotPoolWorkerRepoClient) RemoveNetworkLock(context.Context, *pb.RemoveNetworkLockRequest, ...grpc.CallOption) (*pb.RemoveNetworkLockResponse, error) {
	c.record("unlock")
	return &pb.RemoveNetworkLockResponse{Ok: true}, nil
}

func (c *slotPoolWorkerRepoClient) GetContainerIpAssignments(context.Context, *pb.GetContainerIpAssignmentsRequest, ...grpc.CallOption) (*pb.GetContainerIpAssignmentsResponse, error) {
	c.record("list")
	return &pb.GetContainerIpAssignmentsResponse{Ok: true, Assignments: c.assignments}, nil
}

func (c *slotPoolWorkerRepoClient) RemoveContainerIp(ctx context.Context, in *pb.RemoveContainerIpRequest, _ ...grpc.CallOption) (*pb.RemoveContainerIpResponse, error) {
	c.mu.Lock()
	c.inflight++
	c.peak = max(c.peak, c.inflight)
	c.mu.Unlock()
	time.Sleep(c.removeDelay)
	c.mu.Lock()
	c.inflight--
	c.mu.Unlock()
	c.record("remove " + in.ContainerId)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &pb.RemoveContainerIpResponse{Ok: true}, nil
}

func TestCloseReleasesPooledSlotsTogether(t *testing.T) {
	repoClient := &slotPoolWorkerRepoClient{removeDelay: 10 * time.Millisecond}
	manager := &ContainerNetworkManager{
		ctx:                context.Background(),
		workerId:           "worker-a",
		workerRepoClient:   repoClient,
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		allocatedIPs:       map[string]struct{}{},
		containerIPs:       map[string]string{},
		containerSlots:     map[string]*containerNetworkSlot{},
		slotDiscards:       make(chan pendingSlotDiscard, 1),
	}
	for i := 0; i < 2*networkSlotCleanupConcurrency; i++ {
		manager.freeSlots = append(manager.freeSlots, &containerNetworkSlot{id: fmt.Sprintf("slot-%d", i), namespace: "missing-netns", vethHost: "missing-veth", ip: fmt.Sprintf("192.168.0.%d", 10+i)})
	}
	manager.totalSlots = len(manager.freeSlots)
	manager.slotDiscards <- pendingSlotDiscard{slot: &containerNetworkSlot{id: "slot-used", namespace: "missing-netns", vethHost: "missing-veth", ip: "192.168.0.9"}}

	require.NoError(t, manager.Close())

	released := 0
	for _, call := range repoClient.calls {
		if strings.HasPrefix(call, "remove ") {
			released++
		}
	}
	require.Equal(t, 2*networkSlotCleanupConcurrency+1, released, "every free and pending slot is released")
	require.Greater(t, repoClient.peak, 1, "releases run concurrently")
	require.True(t, manager.slotPoolClosed)
	require.Zero(t, manager.totalSlots)
}

func TestCleanupStaleNetworkSlotsReleasesTheLockBeforeDestroyingSlots(t *testing.T) {
	reservation := containerNetworkSlotReservationIDForWorker("worker-a", "slot-stale")
	repoClient := &slotPoolWorkerRepoClient{assignments: []*pb.ContainerIpAssignment{{ContainerId: reservation, IpAddress: "192.168.0.44"}}}
	manager := &ContainerNetworkManager{
		ctx:              context.Background(),
		workerId:         "worker-a",
		networkPrefix:    "node",
		workerRepoClient: repoClient,
		allocatedIPs:     map[string]struct{}{},
		containerIPs:     map[string]string{},
	}

	require.NoError(t, manager.cleanupStaleNetworkSlots())
	require.Equal(t, []string{"lock", "list", "unlock", "remove " + reservation}, repoClient.calls)
}
