package pod

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type blockedInstanceRepository struct {
	repository.BackendRepository
	started chan struct{}
	release chan struct{}
}

func (r *blockedInstanceRepository) GetStubByExternalId(_ context.Context, id string, _ ...types.QueryFilter) (*types.StubWithRelated, error) {
	if id == "slow" {
		close(r.started)
		<-r.release
	}
	return nil, errors.New("stub unavailable")
}

func TestSlowInstanceLookupDoesNotBlockOtherVMsOrCleanup(t *testing.T) {
	repo := &blockedInstanceRepository{started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(repo.release) })
	service := &GenericPodService{ctx: context.Background(), backendRepo: repo, podInstances: common.NewSafeMap[*podInstance]()}
	slow := make(chan error, 1)
	go func() { _, err := service.getOrCreatePodInstance("slow"); slow <- err }()
	<-repo.started

	fast := make(chan error, 1)
	go func() { _, err := service.getOrCreatePodInstance("other"); fast <- err }()
	select {
	case err := <-fast:
		require.ErrorContains(t, err, "invalid stub id")
	case <-time.After(time.Second):
		t.Fatal("a different VM waited behind the slow database lookup")
	}
	instance := &podInstance{}
	service.podInstances.Set("finished", instance)
	cleaned := make(chan struct{})
	go func() { service.deletePodInstance("finished", instance); close(cleaned) }()
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("instance cleanup waited behind the slow database lookup")
	}
}

// redisTrips counts the round trips a client makes to Redis.
type redisTrips struct{ calls atomic.Int64 }

func (h *redisTrips) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *redisTrips) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.calls.Add(1)
		return next(ctx, cmd)
	}
}

func (h *redisTrips) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.calls.Add(1)
		return next(ctx, cmds)
	}
}

func TestChoosingContainersToStopUsesTheScalingEventsRead(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()

	instance := &podInstance{AutoscaledInstance: &abstractions.AutoscaledInstance{
		Ctx:           ctx,
		Rdb:           rdb,
		IsActive:      true,
		Workspace:     &types.Workspace{Name: "workspace", ExternalId: "workspace-id"},
		Stub:          &types.StubWithRelated{Stub: types.Stub{ExternalId: "stub", Type: types.StubType(types.StubTypeSandbox)}},
		StubConfig:    &types.StubConfigV1{},
		ContainerRepo: repo,
	}}

	// Forty sandboxes are running; every fourth is kept warm.
	want := []string{}
	for n := range 40 {
		containerID := fmt.Sprintf("sandbox-stub-%02d", n)
		require.NoError(t, repo.SetContainerState(containerID, &types.ContainerState{
			ContainerId: containerID,
			StubId:      "stub",
			WorkspaceId: "workspace-id",
			Status:      types.ContainerStatusRunning,
		}))
		if n%4 == 0 {
			require.NoError(t, repo.SetPodKeepWarmLock(ctx, "workspace", "stub", containerID, 60))
		} else {
			want = append(want, containerID)
		}
	}

	trips := &redisTrips{}
	rdb.AddHook(trips)

	// Asked on its own, it reads the stub's containers itself.
	alone, err := instance.stoppableContainers()
	require.NoError(t, err)
	require.ElementsMatch(t, want, alone)
	readingItself := trips.calls.Swap(0)

	// A scaling event has read the state a moment before it asks, and the
	// same answer then costs two round trips fewer: the stub's index and
	// the state of every container.
	_, err = instance.State()
	require.NoError(t, err)
	trips.calls.Store(0)
	afterState, err := instance.stoppableContainers()
	require.NoError(t, err)
	require.ElementsMatch(t, want, afterState)
	require.EqualValues(t, readingItself-2, trips.calls.Load(), "round trips after the state was just read")
}

func TestEventsOfContainersTheProxyDoesNotRouteToAreCheckedTogether(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	repo := repository.NewContainerRedisRepositoryForTest(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A burst of sandboxes is being placed: the proxy routes to none of them
	// yet. One is already stopping and one is gone by the time it is checked.
	containerIDs := []string{}
	for n := range 300 {
		containerID := fmt.Sprintf("sandbox-stub-%03d", n)
		containerIDs = append(containerIDs, containerID)
		require.NoError(t, repo.SetContainerState(containerID, &types.ContainerState{
			ContainerId: containerID,
			StubId:      "stub",
			WorkspaceId: "workspace",
			Status:      types.ContainerStatusRunning,
		}))
	}
	stopping, gone := containerIDs[7], containerIDs[8]
	require.NoError(t, repo.UpdateContainerStatus(stopping, types.ContainerStatusStopping, int64(types.ContainerStateTtlSWhileStopping)))
	require.NoError(t, repo.DeleteContainerState(gone))

	pb := &PodProxyBuffer{discoverReady: make(chan struct{}, 1)}
	instance := &podInstance{
		AutoscaledInstance: &abstractions.AutoscaledInstance{
			Ctx:                ctx,
			ContainerEventChan: make(chan types.ContainerEvent, 4096),
			ContainerRepo:      repo,
		},
		buffer: pb,
	}

	trips := &redisTrips{}
	rdb.AddHook(trips)

	// Each of them writes its state several times on the way to running.
	for range 3 {
		for _, containerID := range containerIDs {
			instance.ConsumeContainerEvent(types.ContainerEvent{ContainerId: containerID, Change: 1})
		}
	}

	// The stopping one is kept out of the rotation, as if it had been read
	// on its own, and the one that is gone starts a discovery pass.
	require.Eventually(t, func() bool {
		pb.availableContainersLock.RLock()
		defer pb.availableContainersLock.RUnlock()
		_, retiring := pb.retiringContainers[stopping]
		return retiring
	}, 2*time.Second, 5*time.Millisecond)
	select {
	case <-pb.discoverReady:
	default:
		t.Fatal("a container that is gone did not start a discovery pass")
	}
	pb.availableContainersLock.RLock()
	require.Len(t, pb.retiringContainers, 1, "only the stopping container is retired")
	pb.availableContainersLock.RUnlock()

	// 900 events used to be 900 round trips, one after another.
	require.LessOrEqual(t, trips.calls.Load(), int64(3), "round trips for 900 events of 300 containers")
}
