package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type enforcementStubRepo struct {
	repo.BackendRepository
	calls map[string]int
}

func (r *enforcementStubRepo) GetStubByExternalId(_ context.Context, id string, _ ...types.QueryFilter) (*types.StubWithRelated, error) {
	r.calls[id]++
	if id == "unavailable" {
		return nil, errors.New("database unavailable")
	}
	kind := types.StubType("sandbox")
	if id == "gitops" {
		kind = types.StubType(types.StubTypePlatformDeployer)
	}
	if id == "platform" {
		kind = types.StubType(types.StubTypeManagedEndpointDeployment)
	}
	return &types.StubWithRelated{Stub: types.Stub{Type: kind}}, nil
}

func TestEnforcementExemptsPlatformReplicasButNotCustomerWorkInSameWorkspace(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	ctx := context.Background()
	rdb := s.requestBacklog.rdb
	backend := &enforcementStubRepo{calls: map[string]int{}}
	s.backendRepo = backend
	s.creditGate = newCreditGate(types.CreditGateConfig{}, &fakeCreditBackend{decision: creditDecision{OK: false}}, rdb)
	require.NoError(t, s.workerRepo.AddWorker(&types.Worker{Id: "worker", PoolName: "default", Status: types.WorkerStatusAvailable}))
	for _, fixture := range []struct {
		id, stub  string
		evictable bool
	}{
		{"endpoint-evictable", "platform", true},
		{"endpoint-protected", "platform", false},
		{"managed-spoofed-prefix", "customer", false},
		{"unknown", "unavailable", false},
		{"deployer", "gitops", false},
	} {
		require.NoError(t, s.containerRepo.SetContainerState(fixture.id, &types.ContainerState{
			ContainerId: fixture.id, StubId: fixture.stub, WorkspaceId: "admin", WorkerId: "worker",
			Status: types.ContainerStatusRunning, ScheduledAt: time.Now().Unix(), Evictable: fixture.evictable,
		}))
		require.NoError(t, rdb.SAdd(ctx, common.RedisKeys.SchedulerContainerWorkerIndex("worker"), common.RedisKeys.SchedulerContainerState(fixture.id)).Err())
	}
	require.NoError(t, s.enforceCredits(ctx))
	for _, id := range []string{"endpoint-evictable", "endpoint-protected", "unknown", "deployer"} {
		state, err := s.containerRepo.GetContainerState(id)
		require.NoError(t, err)
		require.Equal(t, types.ContainerStatusRunning, state.Status)
	}
	state, err := s.containerRepo.GetContainerState("managed-spoofed-prefix")
	require.NoError(t, err)
	require.Equal(t, types.ContainerStatusStopping, state.Status)
	require.Equal(t, map[string]int{"platform": 1, "customer": 1, "unavailable": 1, "gitops": 1}, backend.calls)
}
