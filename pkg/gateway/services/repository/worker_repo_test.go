package repository_services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestGetWorkerMeteringConfigRequiresAssignedManagedSlot(t *testing.T) {
	service := &WorkerRepositoryService{
		workerRepo: &lifecycleWorkerRepo{worker: &types.Worker{
			Id: "worker-1", WorkspaceId: "workspace-1", PoolName: "pool-1", MachineId: "machine-1", ControlPlaneManaged: true,
		}},
		computeRepo: &lifecycleComputeRepo{slots: []*compute.AgentWorkerSlotState{{
			WorkerID: "worker-1", WorkerTokenID: "token-1",
		}}},
		appConfig: types.AppConfig{Monitoring: types.MonitoringConfig{
			MetricsCollector:        string(types.MetricsCollectorOpenMeter),
			OpenMeter:               types.OpenMeterConfig{ServerUrl: "https://meter.example.com", ApiKey: "meter-key"},
			ContainerCostHookConfig: types.ContainerCostHookConfig{Endpoint: "https://billing.example.com/quote", Token: "quote-key"},
		}},
	}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token:     &types.Token{ExternalId: "token-1", TokenType: types.TokenTypeWorker},
		Workspace: &types.Workspace{ExternalId: "workspace-1"},
	})

	response, err := service.GetWorkerMeteringConfig(ctx, &pb.GetWorkerMeteringConfigRequest{WorkerId: "worker-1"})
	require.NoError(t, err)
	require.True(t, response.Ok)
	var metering types.MonitoringConfig
	require.NoError(t, json.Unmarshal([]byte(response.MeteringJson), &metering))
	require.Equal(t, service.appConfig.Monitoring, metering)

	service.computeRepo = &lifecycleComputeRepo{slots: []*compute.AgentWorkerSlotState{{WorkerID: "worker-1", WorkerTokenID: "another-token"}}}
	response, err = service.GetWorkerMeteringConfig(ctx, &pb.GetWorkerMeteringConfigRequest{WorkerId: "worker-1"})
	require.NoError(t, err)
	require.False(t, response.Ok)
}

type claimWorkerRepo struct {
	repository.WorkerRepository
	claimErr error
	moves    []*pb.MoveContainerIpRequest
	moveErr  error
}

func (r *claimWorkerRepo) MoveContainerIp(prefix, from, to, ip string) error {
	r.moves = append(r.moves, &pb.MoveContainerIpRequest{NetworkPrefix: prefix, FromContainerId: from, ToContainerId: to, IpAddress: ip})
	return r.moveErr
}

func (r *claimWorkerRepo) AddContainerToWorker(workerID, containerID, deliveryToken string) error {
	return r.claimErr
}

// claimContainerRepo mirrors the redis repository's transition rules: a
// renewal of PENDING is refused silently once the container is STOPPING.
type claimContainerRepo struct {
	repository.ContainerRepository
	state         *types.ContainerState
	beforeUpdate  func()
	updates       []types.ContainerStatus
	updateExpiry  []int64
	getStateCalls int
	address       string
	addressErr    error
}

func (r *claimContainerRepo) SetWorkerAddress(_ string, address string) error {
	r.address = address
	return r.addressErr
}

func (r *claimContainerRepo) UpdateContainerStatus(containerID string, status types.ContainerStatus, expirySeconds int64) error {
	if r.beforeUpdate != nil {
		r.beforeUpdate()
	}
	r.updates = append(r.updates, status)
	r.updateExpiry = append(r.updateExpiry, expirySeconds)
	if r.state == nil {
		return &types.ErrContainerStateNotFound{ContainerId: containerID}
	}
	if containerStatusTransitionAllowedForTest(types.ContainerStatus(r.state.Status), status) {
		r.state.Status = status
	}
	return nil
}

func (r *claimContainerRepo) GetContainerState(containerID string) (*types.ContainerState, error) {
	r.getStateCalls++
	if r.state == nil {
		return nil, &types.ErrContainerStateNotFound{ContainerId: containerID}
	}
	copied := *r.state
	return &copied, nil
}

func containerStatusTransitionAllowedForTest(stored, requested types.ContainerStatus) bool {
	switch stored {
	case types.ContainerStatusPending:
		return true
	case types.ContainerStatusRunning:
		return requested != types.ContainerStatusPending
	default:
		return requested == types.ContainerStatusStopping
	}
}

func TestClaimContainerReturnsStoppingWhenStopRacesTheRenewal(t *testing.T) {
	containerRepo := &claimContainerRepo{
		state: &types.ContainerState{ContainerId: "container-id", WorkspaceId: "workspace-id", StubId: "stub-id", Status: types.ContainerStatusPending},
	}
	// The scheduler stops the container after the worker's claim is accepted
	// but before the pending lease is renewed.
	containerRepo.beforeUpdate = func() { containerRepo.state.Status = types.ContainerStatusStopping }
	service := &WorkerRepositoryService{workerRepo: &claimWorkerRepo{}, containerRepo: containerRepo}

	resp, err := service.ClaimContainer(context.Background(), &pb.ClaimContainerRequest{WorkerId: "worker-1", ContainerId: "container-id", DeliveryToken: "token"})

	require.NoError(t, err)
	require.True(t, resp.Claimed)
	require.True(t, resp.Ok)
	require.Equal(t, []types.ContainerStatus{types.ContainerStatusPending}, containerRepo.updates)
	require.Equal(t, string(types.ContainerStatusStopping), resp.State.Status, "the claim must report the persisted status, not the pre-renewal snapshot")
}

func TestClaimContainerRenewsPendingLease(t *testing.T) {
	containerRepo := &claimContainerRepo{
		state: &types.ContainerState{ContainerId: "container-id", WorkspaceId: "workspace-id", StubId: "stub-id", Status: types.ContainerStatusPending},
	}
	service := &WorkerRepositoryService{workerRepo: &claimWorkerRepo{}, containerRepo: containerRepo}

	resp, err := service.ClaimContainer(context.Background(), &pb.ClaimContainerRequest{WorkerId: "worker-1", ContainerId: "container-id", DeliveryToken: "token"})

	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.Equal(t, string(types.ContainerStatusPending), resp.State.Status)
	require.Equal(t, []int64{int64(types.ContainerStateTtlSWhilePending)}, containerRepo.updateExpiry)
	require.Equal(t, 1, containerRepo.getStateCalls)
}

func TestClaimContainerReportsMissingStateAfterClaim(t *testing.T) {
	service := &WorkerRepositoryService{workerRepo: &claimWorkerRepo{}, containerRepo: &claimContainerRepo{}}

	resp, err := service.ClaimContainer(context.Background(), &pb.ClaimContainerRequest{WorkerId: "worker-1", ContainerId: "container-id", DeliveryToken: "token"})

	require.NoError(t, err)
	require.True(t, resp.Claimed)
	require.False(t, resp.Ok)
	require.True(t, (&types.ErrContainerStateNotFound{}).From(errors.New(resp.ErrorMsg)))
}

func TestClaimContainerPublishesStartupOnlyAfterAcceptance(t *testing.T) {
	for _, tc := range []struct {
		name                                              string
		claimErr, moveErr, addressErr                     error
		stopping                                          bool
		wantClaimed, wantMoved, wantAddress, wantPrepared bool
	}{
		{name: "accepted", wantClaimed: true, wantMoved: true, wantAddress: true, wantPrepared: true},
		{name: "rejected", claimErr: errors.New("delivery owned elsewhere")},
		{name: "stopping", stopping: true, wantClaimed: true},
		{name: "move failed", moveErr: errors.New("source ownership changed"), wantClaimed: true, wantMoved: true},
		{name: "address failed", addressErr: errors.New("route unavailable"), wantClaimed: true, wantMoved: true, wantAddress: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := &claimWorkerRepo{claimErr: tc.claimErr, moveErr: tc.moveErr}
			containers := &claimContainerRepo{state: &types.ContainerState{ContainerId: "c1", WorkspaceId: "ws1", Status: types.ContainerStatusPending}, addressErr: tc.addressErr}
			if tc.stopping {
				containers.state.Status = types.ContainerStatusStopping
			}
			service := &WorkerRepositoryService{workerRepo: worker, containerRepo: containers}
			resp, err := service.ClaimContainer(context.Background(), &pb.ClaimContainerRequest{
				WorkerId: "w1", ContainerId: "c1", DeliveryToken: "token",
				Network:       &pb.MoveContainerIpRequest{NetworkPrefix: "node1", FromContainerId: "network-slot:w1:s1", ToContainerId: "c1", IpAddress: "192.168.0.2"},
				WorkerAddress: &pb.SetWorkerAddressRequest{ContainerId: "c1", Address: "worker:1234"},
			})
			require.NoError(t, err)
			require.Equal(t, tc.wantClaimed, resp.Claimed)
			require.Equal(t, tc.wantMoved, len(worker.moves) == 1)
			require.Equal(t, tc.wantAddress, containers.address != "")
			require.Equal(t, tc.wantPrepared, resp.StartupPrepared)
		})
	}
}

func TestClaimContainerRefusesForeignStartupResources(t *testing.T) {
	for _, req := range []*pb.ClaimContainerRequest{
		{WorkerId: "w1", ContainerId: "c1", WorkerAddress: &pb.SetWorkerAddressRequest{ContainerId: "other"}},
		{WorkerId: "w1", ContainerId: "c1", Network: &pb.MoveContainerIpRequest{FromContainerId: "network-slot:w2:s1", ToContainerId: "c1"}},
		{WorkerId: "w1", ContainerId: "c1", Network: &pb.MoveContainerIpRequest{FromContainerId: "network-slot:w1:s1", ToContainerId: "other"}},
		{WorkerId: "w1", ContainerId: "c1", WorkerAddress: &pb.SetWorkerAddressRequest{ContainerId: "c1", Route: &pb.BackendRoute{WorkerId: "w2", Kind: types.BackendRouteKindWorker}}},
	} {
		// Missing repositories make any acceptance or publication fail the test.
		resp, err := (&WorkerRepositoryService{}).ClaimContainer(context.Background(), req)
		require.NoError(t, err)
		require.False(t, resp.Claimed)
		require.False(t, resp.Ok)
	}
}

type updateStatusContainerRepo struct {
	repository.ContainerRepository
	updates int
}

func (r *updateStatusContainerRepo) UpdateContainerStatus(string, types.ContainerStatus, int64) error {
	r.updates++
	return nil
}

func TestUpdateContainerStatusRejectsNonPositiveExpiryBeforeWriting(t *testing.T) {
	containerRepo := &updateStatusContainerRepo{}
	service := &ContainerRepositoryService{containerRepo: containerRepo}

	for _, expiry := range []int64{0, -5} {
		resp, err := service.UpdateContainerStatus(context.Background(), &pb.UpdateContainerStatusRequest{
			ContainerId:   "container-id",
			Status:        string(types.ContainerStatusRunning),
			ExpirySeconds: expiry,
		})
		require.NoError(t, err)
		require.False(t, resp.Ok)
		require.Contains(t, resp.ErrorMsg, "expiry_seconds")
	}
	require.Zero(t, containerRepo.updates)
}

// keepAliveWorkerRepo records how often the pool is consulted for a headroom
// answer and can fail the worker lookup.
type keepAliveWorkerRepo struct {
	repository.WorkerRepository
	worker        *types.Worker
	lookupErr     error
	poolScans     int
	poolScanError error
}

func (r *keepAliveWorkerRepo) SetWorkerKeepAlive(workerId string, keepAlive types.WorkerKeepAlive) error {
	return nil
}

func (r *keepAliveWorkerRepo) GetWorkerById(workerId string) (*types.Worker, error) {
	if r.lookupErr != nil {
		return nil, r.lookupErr
	}
	return r.worker, nil
}

func (r *keepAliveWorkerRepo) GetAllWorkersInPool(poolName string) ([]*types.Worker, error) {
	r.poolScans++
	if r.poolScanError != nil {
		return nil, r.poolScanError
	}
	return []*types.Worker{r.worker}, nil
}

func headroomTestConfig() types.AppConfig {
	return types.AppConfig{Worker: types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"default": {PoolSizing: types.WorkerPoolJobSpecPoolSizingConfig{MinFreeCPU: "1000m", MinFreeMemory: "1Gi", MinFreeGPU: "0"}},
	}}}
}

func TestSetWorkerKeepAliveOnlyScansThePoolForIdleWorkers(t *testing.T) {
	workerRepo := &keepAliveWorkerRepo{worker: &types.Worker{Id: "worker-1", PoolName: "default", Status: types.WorkerStatusAvailable, FreeCpu: 4000, FreeMemory: 8192}}
	service := &WorkerRepositoryService{workerRepo: workerRepo, appConfig: headroomTestConfig()}

	resp, err := service.SetWorkerKeepAlive(context.Background(), &pb.SetWorkerKeepAliveRequest{WorkerId: "worker-1"})
	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.False(t, resp.PoolHeadroom, "a busy worker cannot spin down, so it is not told it holds headroom")
	require.Equal(t, 0, workerRepo.poolScans, "busy keepalives must not scan the pool")

	resp, err = service.SetWorkerKeepAlive(context.Background(), &pb.SetWorkerKeepAliveRequest{WorkerId: "worker-1", Idle: true})
	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.True(t, resp.PoolHeadroom, "the only ready worker holds the pool's minimum")
	require.Equal(t, 1, workerRepo.poolScans)
}

func TestSetWorkerKeepAliveFailsClosedWhenHeadroomCannotBeDetermined(t *testing.T) {
	for name, repo := range map[string]*keepAliveWorkerRepo{
		"worker lookup fails": {lookupErr: errors.New("redis: connection refused")},
		"pool scan fails":     {worker: &types.Worker{Id: "worker-1", PoolName: "default", Status: types.WorkerStatusAvailable}, poolScanError: errors.New("redis: timeout")},
	} {
		t.Run(name, func(t *testing.T) {
			service := &WorkerRepositoryService{workerRepo: repo, appConfig: headroomTestConfig()}
			resp, err := service.SetWorkerKeepAlive(context.Background(), &pb.SetWorkerKeepAliveRequest{WorkerId: "worker-1", Idle: true})
			require.NoError(t, err)
			require.True(t, resp.Ok)
			require.True(t, resp.PoolHeadroom, "an idle worker must stay up until the pool can actually be read")
		})
	}
}

func TestClaimContainerRefusesForeignRouteWorkspaceAfterAcceptance(t *testing.T) {
	workers := &claimWorkerRepo{}
	containers := &claimContainerRepo{state: &types.ContainerState{ContainerId: "c1", WorkspaceId: "ws1", Status: types.ContainerStatusPending}}
	service := &WorkerRepositoryService{workerRepo: workers, containerRepo: containers}
	resp, err := service.ClaimContainer(context.Background(), &pb.ClaimContainerRequest{WorkerId: "w1", ContainerId: "c1", DeliveryToken: "token", Network: &pb.MoveContainerIpRequest{FromContainerId: "network-slot:w1:s1", ToContainerId: "c1", IpAddress: "192.168.0.2"}, WorkerAddress: &pb.SetWorkerAddressRequest{ContainerId: "c1", Route: &pb.BackendRoute{WorkerId: "w1", WorkspaceId: "other", Kind: types.BackendRouteKindWorker}}})
	require.NoError(t, err)
	require.True(t, resp.Claimed)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrorMsg, "workspace")
	require.Empty(t, workers.moves)
	require.Empty(t, containers.address)
}
