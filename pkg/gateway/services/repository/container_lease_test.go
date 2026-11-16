package repository_services

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// leaseTestClient returns a client of a gateway that has these containers,
// and the Redis their state is in. Its worker tokens belong to the worker
// "worker", a private one to the workspace "own".
func leaseTestClient(t *testing.T, containers ...*types.ContainerState) (pb.ContainerRepositoryServiceClient, *common.RedisClient) {
	t.Helper()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	for _, state := range containers {
		require.NoError(t, repo.SetContainerState(state.ContainerId, state))
	}
	conn := repositoryAuthClient(t, &WorkerRepositoryService{}, &ContainerRepositoryService{containerRepo: repo})
	return pb.NewContainerRepositoryServiceClient(conn), rdb
}

func requireContainerStateTTL(t *testing.T, rdb *common.RedisClient, containerID string, seconds int64) {
	t.Helper()
	ttl, err := rdb.TTL(context.Background(), common.RedisKeys.SchedulerContainerState(containerID)).Result()
	require.NoError(t, err)
	require.InDelta(t, seconds, ttl.Seconds(), 1, "state TTL of %s", containerID)
}

func TestWorkerRenewsItsContainersInOneCall(t *testing.T) {
	client, rdb := leaseTestClient(t,
		&types.ContainerState{ContainerId: "container-running", WorkspaceId: "any", WorkerId: "worker", Status: types.ContainerStatusRunning},
		&types.ContainerState{ContainerId: "container-starting", WorkspaceId: "any", WorkerId: "worker", Status: types.ContainerStatusPending},
		&types.ContainerState{ContainerId: "container-evicted", WorkspaceId: "any", WorkerId: "worker", Status: types.ContainerStatusStopping, DrainSeconds: 30},
		&types.ContainerState{ContainerId: "container-foreign", WorkspaceId: "any", WorkerId: "other-worker", Status: types.ContainerStatusRunning},
	)
	ctx := context.Background()
	// What the scheduler writes when it evicts a container.
	require.NoError(t, rdb.HSet(ctx, common.RedisKeys.SchedulerContainerState("container-evicted"), "evicting", "true").Err())

	response, err := client.RenewContainerLeases(repositoryAuthContext(types.TokenTypeWorker), &pb.RenewContainerLeasesRequest{Containers: []*pb.ContainerLeaseRenewal{
		{ContainerId: "container-running"},
		{ContainerId: "container-starting", RuntimeStarted: true},
		{ContainerId: "container-evicted"},
		{ContainerId: "container-foreign"},
		{ContainerId: "container-gone"},
	}})

	require.NoError(t, err)
	want := &pb.RenewContainerLeasesResponse{Ok: true, Leases: []*pb.ContainerLease{
		{ContainerId: "container-running", Ok: true, Status: "RUNNING"},
		{ContainerId: "container-starting", Ok: true, Status: "RUNNING", Started: true},
		{ContainerId: "container-evicted", Ok: true, Status: "STOPPING", Evicting: true, DrainSeconds: 30},
		{ContainerId: "container-foreign", ErrorMsg: "container is not assigned to this worker"},
		{ContainerId: "container-gone", ErrorMsg: "container state not found: container-gone"},
	}}
	require.True(t, proto.Equal(want, response), "leases: %v", response)
	// The worker tells a container without state from any other failure by this.
	require.True(t, (&types.ErrContainerStateNotFound{}).From(errors.New(response.Leases[4].ErrorMsg)))

	// The containers created just now had the lease of a pending one.
	requireContainerStateTTL(t, rdb, "container-running", types.ContainerStateTtlS)
	requireContainerStateTTL(t, rdb, "container-starting", types.ContainerStateTtlS)
	requireContainerStateTTL(t, rdb, "container-evicted", types.ContainerStateTtlSWhilePending)
	requireContainerStateTTL(t, rdb, "container-foreign", types.ContainerStateTtlSWhilePending)
	startedAt, err := rdb.HGet(ctx, common.RedisKeys.SchedulerContainerState("container-starting"), "started_at").Int64()
	require.NoError(t, err)
	require.InDelta(t, time.Now().Unix(), startedAt, 2)
}

func TestPrivateWorkerRenewsOnlyContainersOfItsWorkspace(t *testing.T) {
	client, rdb := leaseTestClient(t,
		&types.ContainerState{ContainerId: "container-own", WorkspaceId: "own", WorkerId: "worker", Status: types.ContainerStatusRunning},
		&types.ContainerState{ContainerId: "container-other-workspace", WorkspaceId: "other", WorkerId: "worker", Status: types.ContainerStatusRunning},
	)

	response, err := client.RenewContainerLeases(repositoryAuthContext(types.TokenTypeWorkerPrivate), &pb.RenewContainerLeasesRequest{Containers: []*pb.ContainerLeaseRenewal{
		{ContainerId: "container-own"},
		{ContainerId: "container-other-workspace"},
		{ContainerId: "container-gone"},
	}})

	require.NoError(t, err)
	// A private worker is not told which containers exist outside its
	// workspace, so one that is gone reads like one that is not its own.
	want := &pb.RenewContainerLeasesResponse{Ok: true, Leases: []*pb.ContainerLease{
		{ContainerId: "container-own", Ok: true, Status: "RUNNING"},
		{ContainerId: "container-other-workspace", ErrorMsg: "container is not assigned to this worker"},
		{ContainerId: "container-gone", ErrorMsg: "container is not assigned to this worker"},
	}}
	require.True(t, proto.Equal(want, response), "leases: %v", response)
	requireContainerStateTTL(t, rdb, "container-own", types.ContainerStateTtlS)
	requireContainerStateTTL(t, rdb, "container-other-workspace", types.ContainerStateTtlSWhilePending)
}

func TestRenewContainerLeasesRefusesMoreContainersThanACallMayCarry(t *testing.T) {
	// No repository: a refused call must not reach it.
	conn := repositoryAuthClient(t, &WorkerRepositoryService{}, &ContainerRepositoryService{})
	request := &pb.RenewContainerLeasesRequest{}
	for n := range maxContainerLeasesPerCall + 1 {
		request.Containers = append(request.Containers, &pb.ContainerLeaseRenewal{ContainerId: fmt.Sprintf("container-%04d", n)})
	}

	response, err := pb.NewContainerRepositoryServiceClient(conn).RenewContainerLeases(repositoryAuthContext(types.TokenTypeWorker), request)

	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Contains(t, response.ErrorMsg, "at most 1000 containers")
	require.Empty(t, response.Leases)
}
