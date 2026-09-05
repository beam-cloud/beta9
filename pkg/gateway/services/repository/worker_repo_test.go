package repository_services

import (
	"context"
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type claimWorkerRepo struct {
	repository.WorkerRepository
	claimErr error
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
