package scheduler

import (
	"testing"
	"time"

	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// pendingOf is what the container says it waits for.
func pendingOf(t *testing.T, s *Scheduler, containerID string) (types.ContainerPendingPhase, types.ContainerPendingReason) {
	t.Helper()
	state, err := s.containerRepo.GetContainerState(containerID)
	require.NoError(t, err)
	return state.PendingPhase, state.PendingReason
}

// A request that may run on one machine only waits for room there. No pool
// is asked for a worker, and the request says which of the two it is.
func TestRequestPinnedToAMachineWithNoRoomSaysItsMachineIsBusy(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	require.NoError(t, err)
	request := &types.ContainerRequest{Cpu: 1000, Memory: 1024, MachineId: "machine-1", Timestamp: time.Now()}
	setPendingSchedulerRequests(t, scheduler, request)

	newSchedulingAttempt(scheduler, request, nil).run()

	phase, reason := pendingOf(t, scheduler, request.ContainerId)
	require.Equal(t, types.ContainerPendingWaitingForCapacity, phase)
	require.Equal(t, types.ContainerPendingMachineBusy, reason)
	require.Equal(t, int64(1), scheduler.requestBacklog.Len())
}

// A waiting request passes several times a second. It stores what it waits
// for when that changes, and not on every pass.
func TestWaitingRequestStoresWhatItWaitsForOnlyWhenItChanges(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	require.NoError(t, err)
	stores := &countPendingStores{ContainerRepository: scheduler.containerRepo}
	scheduler.containerRepo = stores
	request := &types.ContainerRequest{Cpu: 1000, Memory: 1024, MachineId: "machine-1", Timestamp: time.Now()}
	setPendingSchedulerRequests(t, scheduler, request)

	for range 5 {
		newSchedulingAttempt(scheduler, request, nil).run()
	}
	require.Equal(t, 1, stores.n)

	attempt := newSchedulingAttempt(scheduler, request, nil)
	attempt.waitsFor(types.ContainerPendingWaitingForCapacity, types.ContainerPendingNoCapacity)
	attempt.waitsFor(types.ContainerPendingWaitingForCapacity, types.ContainerPendingNoCapacity)
	require.Equal(t, 2, stores.n)

	// The request a worker is given carries nothing of it: one that comes
	// back to wait says what for again.
	worker := &types.Worker{Id: "worker-1"}
	require.Empty(t, scheduler.prepareWorkerRequest(worker, request).PendingReported)
	require.NotEmpty(t, request.PendingReported)
}

// countPendingStores counts the times a container's pending phase is stored.
type countPendingStores struct {
	repo.ContainerRepository
	n int
}

func (c *countPendingStores) SetContainerPending(containerId string, phase types.ContainerPendingPhase, reason types.ContainerPendingReason, since time.Time) (bool, error) {
	c.n++
	return c.ContainerRepository.SetContainerPending(containerId, phase, reason, since)
}

func TestRouteBackup(t *testing.T) {
	pool := func(name, gpu string, priority int32, alsoServes ...string) *WorkerPool {
		return &WorkerPool{Name: name, Config: types.WorkerPoolConfig{GPUType: gpu, Priority: priority, AlsoServes: alsoServes}}
	}
	r := newRoute(&types.ContainerRequest{Gpu: "A10G", GpuCount: 1}, []*WorkerPool{
		pool("a10g-a", "A10G", 10),
		pool("a10g-b", "A10G", 10),
		pool("a10g-low", "A10G", 1),
		pool("l40s", "L40S", 50, "A10G"),
		pool("t4", "T4", 50),
	}, "")

	require.False(t, r.backup("a10g-a"))
	require.False(t, r.backup("a10g-b"), "differs from the first pool by name alone")
	require.True(t, r.backup("a10g-low"), "tried after the pools of a higher priority")
	require.True(t, r.backup("l40s"), "another GPU type that also serves the request")
	require.False(t, r.backup("t4"), "not in the route")
	require.False(t, route{}.backup("a10g-a"))
}
