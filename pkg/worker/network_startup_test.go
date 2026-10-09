package worker

import (
	"context"
	"errors"
	"github.com/opencontainers/runtime-spec/specs-go"
	"sync"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type startupClaimClient struct {
	pb.WorkerRepositoryServiceClient
	mu                sync.Mutex
	ips               map[string]string
	claims, moves     int
	response          *pb.ClaimContainerResponse
	lostReply         bool
	moveBeforeFailure bool
	cancelAfterMove   context.CancelFunc
}

func (r *startupClaimClient) move(from, to, ip string) error {
	if r.ips[to] == ip {
		return nil
	}
	if r.ips[from] != ip || r.ips[to] != "" {
		return errors.New("IP ownership conflict")
	}
	delete(r.ips, from)
	r.ips[to] = ip
	return nil
}

func (r *startupClaimClient) ClaimContainer(_ context.Context, req *pb.ClaimContainerRequest, _ ...grpc.CallOption) (*pb.ClaimContainerResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claims++
	if r.response.StartupPrepared || r.moveBeforeFailure || r.lostReply {
		if err := r.move(req.Network.FromContainerId, req.Network.ToContainerId, req.Network.IpAddress); err != nil {
			return nil, err
		}
	}
	if r.cancelAfterMove != nil {
		r.cancelAfterMove()
		return nil, status.Error(codes.Unavailable, "lost reply on shutdown")
	}
	if r.lostReply && r.claims == 1 {
		return nil, status.Error(codes.Unavailable, "lost accepted claim reply")
	}
	return r.response, nil
}

func (r *startupClaimClient) GetContainerIp(_ context.Context, req *pb.GetContainerIpRequest, _ ...grpc.CallOption) (*pb.GetContainerIpResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ip := r.ips[req.ContainerId]
	return &pb.GetContainerIpResponse{Ok: ip != "", IpAddress: ip}, nil
}

func (r *startupClaimClient) MoveContainerIp(_ context.Context, req *pb.MoveContainerIpRequest, _ ...grpc.CallOption) (*pb.MoveContainerIpResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.moves++
	err := r.move(req.FromContainerId, req.ToContainerId, req.IpAddress)
	if err != nil {
		return &pb.MoveContainerIpResponse{ErrorMsg: err.Error()}, nil
	}
	return &pb.MoveContainerIpResponse{Ok: true}, nil
}

func TestClaimNetworkOwnershipAndGatewayCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name                                               string
		response                                           *pb.ClaimContainerResponse
		lostReply, moveBeforeFailure                       bool
		wantClaimed, wantError, wantCommitted, wantAddress bool
		wantMoves, wantClaims                              int
	}{
		{name: "prepared", response: &pb.ClaimContainerResponse{Ok: true, Claimed: true, StartupPrepared: true}, wantClaimed: true, wantCommitted: true, wantAddress: true, wantClaims: 1},
		{name: "old gateway", response: &pb.ClaimContainerResponse{Ok: true, Claimed: true}, wantClaimed: true, wantCommitted: true, wantMoves: 1, wantClaims: 1},
		{name: "rejected", response: &pb.ClaimContainerResponse{ErrorMsg: "another owner"}, wantError: true, wantClaims: 1},
		{name: "lost reply", response: &pb.ClaimContainerResponse{Ok: true, Claimed: true, StartupPrepared: true}, lostReply: true, wantClaimed: true, wantCommitted: true, wantAddress: true, wantClaims: 2},
		{name: "failure after move", response: &pb.ClaimContainerResponse{Claimed: true, ErrorMsg: "address failed"}, moveBeforeFailure: true, wantClaimed: true, wantError: true, wantMoves: 1, wantClaims: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slot := &containerNetworkSlot{id: "s1", ip: "192.168.0.2", netnsPath: t.TempDir()}
			reservation := containerNetworkSlotReservationIDForWorker("w1", slot.id)
			repo := &startupClaimClient{ips: map[string]string{reservation: slot.ip}, response: tc.response, lostReply: tc.lostReply, moveBeforeFailure: tc.moveBeforeFailure}
			instances := common.NewSafeMap[*ContainerInstance]()
			instances.Set("c1", &ContainerInstance{Id: "c1"})
			m := &ContainerNetworkManager{ctx: context.Background(), workerId: "w1", workerRepoClient: repo, networkPrefix: "node1", containerInstances: instances,
				freeSlots: []*containerNetworkSlot{slot}, containerSlots: map[string]*containerNetworkSlot{}, containerIPs: map[string]string{reservation: slot.ip}, allocatedIPs: map[string]struct{}{slot.ip: {}}}
			worker := &Worker{workerId: "w1", workerRepoClient: repo, containerInstances: instances, podAddr: "worker", containerServer: &ContainerRuntimeServer{port: 1234}, containerNetworkManager: &localContainerNetwork{ContainerNetworkManager: m}}
			claimed, err := worker.claimContainer(context.Background(), &types.ContainerRequest{ContainerId: "c1", DeliveryToken: "t1", Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}}})
			require.Equal(t, tc.wantClaimed, claimed)
			require.Equal(t, tc.wantError, err != nil)
			require.Equal(t, tc.wantClaims, repo.claims)
			require.Equal(t, tc.wantMoves, repo.moves)
			instance, _ := instances.Get("c1")
			require.Equal(t, tc.wantAddress, instance.workerAddressPublished.Load())
			if tc.wantCommitted {
				require.Equal(t, slot.ip, repo.ips["c1"])
				require.NotContains(t, repo.ips, reservation)
				require.Same(t, slot, m.containerSlots["c1"])
				require.Empty(t, m.freeSlots)
			} else {
				require.Equal(t, slot.ip, repo.ips[reservation])
				require.NotContains(t, repo.ips, "c1")
				require.Empty(t, m.containerSlots)
				require.Equal(t, []*containerNetworkSlot{slot}, m.freeSlots)
			}
		})
	}
}

func TestClaimNetworkRecoversAcceptedClaimOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slot := &containerNetworkSlot{id: "s1", ip: "192.168.0.2", netnsPath: t.TempDir()}
	reservation := containerNetworkSlotReservationIDForWorker("w1", slot.id)
	repo := &startupClaimClient{ips: map[string]string{reservation: slot.ip}, response: &pb.ClaimContainerResponse{Ok: true, Claimed: true, StartupPrepared: true}, cancelAfterMove: cancel}
	m := &ContainerNetworkManager{ctx: ctx, workerId: "w1", workerRepoClient: repo, networkPrefix: "node1", freeSlots: []*containerNetworkSlot{slot}, containerSlots: map[string]*containerNetworkSlot{}, containerIPs: map[string]string{reservation: slot.ip}, allocatedIPs: map[string]struct{}{slot.ip: {}}}
	worker := &Worker{workerId: "w1", workerRepoClient: repo, containerNetworkManager: &localContainerNetwork{ContainerNetworkManager: m}}
	claimed, err := worker.claimContainer(ctx, &types.ContainerRequest{ContainerId: "c1", DeliveryToken: "t1", Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}}})
	require.False(t, claimed)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, repo.claims)
	require.Equal(t, 1, repo.moves)
	require.Equal(t, map[string]string{reservation: slot.ip}, repo.ips)
	require.Equal(t, []*containerNetworkSlot{slot}, m.freeSlots)
}

func TestClaimNetworkInstallsWithoutAnotherRepositoryCall(t *testing.T) {
	slot := &containerNetworkSlot{id: "s1", ip: "192.168.0.2", netnsPath: t.TempDir()}
	instances := common.NewSafeMap[*ContainerInstance]()
	instances.Set("c1", &ContainerInstance{Id: "c1"})
	// A nil client fails the test if setup tries to publish the IP again.
	m := &ContainerNetworkManager{containerSlots: map[string]*containerNetworkSlot{"c1": slot}, containerInstances: instances, slotPoolClosed: true}
	spec := &specs.Spec{Linux: &specs.Linux{}}
	used, err := m.setupPreallocatedNetworkSlot("c1", spec, nil)
	require.NoError(t, err)
	require.True(t, used)
	require.Equal(t, []specs.LinuxNamespace{{Type: specs.NetworkNamespace, Path: slot.netnsPath}}, spec.Linux.Namespaces)
	instance, _ := instances.Get("c1")
	require.Equal(t, slot.ip, instance.ContainerIp)
}

func TestClaimNetworkUnrecoverableIPIsNeverReleasedForReuse(t *testing.T) {
	slot := &containerNetworkSlot{id: "s1", ip: "192.168.0.2"}
	reservation := containerNetworkSlotReservationIDForWorker("w1", slot.id)
	m := &ContainerNetworkManager{workerId: "w1", containerIPs: map[string]string{reservation: slot.ip}, allocatedIPs: map[string]struct{}{slot.ip: {}}, containerSlots: map[string]*containerNetworkSlot{}, containerInstances: common.NewSafeMap[*ContainerInstance](), totalSlots: 1}
	require.NoError(t, m.finishNetworkSlotDiscard("", slot, false, nil))
	require.Contains(t, m.allocatedIPs, slot.ip)
	require.Empty(t, m.releasedIPs)
	require.Equal(t, slot.ip, m.containerIPs[reservation])
	require.Zero(t, m.totalSlots)
}
