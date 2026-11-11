package worker

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// heartbeatTestWorker returns a worker that sends no heartbeat by itself:
// the test runs an interval's heartbeat, or the loop, when it wants one.
func heartbeatTestWorker(t *testing.T, repoClient *fakeContainerRepoClient) (*Worker, *evictionRuntime) {
	t.Helper()
	worker, rt := evictionWorkerForTest(false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	worker.ctx = ctx
	worker.containerRepoClient = repoClient
	worker.statusHeartbeats.running = true
	return worker, rt
}

// addHeartbeatContainer adds a running container with a status heartbeat.
func addHeartbeatContainer(worker *Worker, rt runtime.Runtime, containerID string) *ContainerInstance {
	instance := addRunningInstance(worker, rt, containerID)
	worker.startContainerStatusHeartbeat(instance.Request)
	return instance
}

// heartbeatContainers waits for the heartbeats under way for single
// containers, then returns the containers that still have a heartbeat.
func heartbeatContainers(t *testing.T, worker *Worker) []string {
	t.Helper()
	var containerIDs []string
	require.Eventually(t, func() bool {
		worker.statusHeartbeats.mu.Lock()
		defer worker.statusHeartbeats.mu.Unlock()
		containerIDs = nil
		for containerID, heartbeat := range worker.statusHeartbeats.containers {
			if heartbeat.alone {
				return false
			}
			containerIDs = append(containerIDs, containerID)
		}
		return true
	}, 5*time.Second, time.Millisecond)
	slices.Sort(containerIDs)
	return containerIDs
}

// renewedContainers returns the containers a RenewContainerLeases call carried.
func renewedContainers(renewal *pb.RenewContainerLeasesRequest) []string {
	containerIDs := make([]string, len(renewal.Containers))
	for i, container := range renewal.Containers {
		containerIDs[i] = container.ContainerId
	}
	slices.Sort(containerIDs)
	return containerIDs
}

func TestRunningContainersShareOneHeartbeatCallEachInterval(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	worker, rt := heartbeatTestWorker(t, repoClient)
	const containers = 200
	for n := range containers {
		addHeartbeatContainer(worker, rt, fmt.Sprintf("container-%03d", n))
	}

	// With a loop each, they made 200 x 2 calls every interval.
	const interval = 20 * time.Millisecond
	started := time.Now()
	go worker.runContainerStatusHeartbeats(interval)
	time.Sleep(10 * interval)
	renewals := repoClient.containerLeaseRenewals()
	intervals := int(time.Since(started) / interval)

	require.GreaterOrEqual(t, len(renewals), 2, "the heartbeat did not keep going")
	require.LessOrEqual(t, len(renewals), intervals+1, "calls for %d containers over %d intervals", containers, intervals)
	for _, renewal := range renewals {
		require.Len(t, renewal.Containers, containers)
	}
	require.Zero(t, repoClient.getContainerStateCalls())
	require.Empty(t, repoClient.containerStatusUpdates())
}

func TestHeartbeatOfManyContainersIsSplitIntoBoundedCalls(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	worker, rt := heartbeatTestWorker(t, repoClient)
	const containers = 2*containerLeasesPerCall + 200
	for n := range containers {
		addHeartbeatContainer(worker, rt, fmt.Sprintf("container-%04d", n))
	}

	worker.updateContainerStatuses(worker.ctx)

	renewals := repoClient.containerLeaseRenewals()
	require.Len(t, renewals, 3)
	renewed := make(map[string]int)
	for _, renewal := range renewals {
		require.LessOrEqual(t, len(renewal.Containers), containerLeasesPerCall)
		for _, container := range renewal.Containers {
			renewed[container.ContainerId]++
		}
	}
	require.Len(t, renewed, containers)
	for containerID, times := range renewed {
		require.Equal(t, 1, times, containerID)
	}
}

func TestHeartbeatTellsTheGatewayWhichContainersHaveStarted(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	worker, rt := heartbeatTestWorker(t, repoClient)
	addHeartbeatContainer(worker, rt, "container-starting")
	addHeartbeatContainer(worker, rt, "container-started").markRuntimeStarted(1234)

	worker.updateContainerStatuses(worker.ctx)

	renewals := repoClient.containerLeaseRenewals()
	require.Len(t, renewals, 1)
	started := make(map[string]bool)
	for _, container := range renewals[0].Containers {
		started[container.ContainerId] = container.RuntimeStarted
	}
	require.Equal(t, map[string]bool{"container-starting": false, "container-started": true}, started)
}

func TestHeartbeatActsOnWhatTheGatewayFoundForEachContainer(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		// What the heartbeat of a single stopping container reads.
		state: &pb.ContainerState{Status: string(types.ContainerStatusStopping)},
		leases: map[string]*pb.ContainerLease{
			"container-orphan":   {ContainerId: "container-orphan", ErrorMsg: (&types.ErrContainerStateNotFound{ContainerId: "container-orphan"}).Error()},
			"container-stopping": {ContainerId: "container-stopping", Ok: true, Status: string(types.ContainerStatusStopping)},
			"container-evicted":  {ContainerId: "container-evicted", Ok: true, Status: string(types.ContainerStatusStopping), Evicting: true},
			"container-foreign":  {ContainerId: "container-foreign", ErrorMsg: (&types.ErrContainerNotAssigned{}).Error()},
		},
	}
	worker, rt := heartbeatTestWorker(t, repoClient)
	instances := make(map[string]*ContainerInstance)
	for _, containerID := range []string{"container-running", "container-orphan", "container-stopping", "container-evicted", "container-foreign"} {
		instances[containerID] = addHeartbeatContainer(worker, rt, containerID)
	}
	stopReason := func(containerID string) types.StopContainerReason {
		_, reason := instances[containerID].lifecycleState()
		return reason
	}
	signalled := func(containerID string, signals ...syscall.Signal) {
		t.Helper()
		require.Eventually(t, func() bool { return slices.Equal(rt.observed(containerID), signals) }, 5*time.Second, time.Millisecond, containerID)
	}

	worker.updateContainerStatuses(worker.ctx)
	heartbeatContainers(t, worker)

	// A container the gateway has no state for is an orphan, killed at once.
	require.Equal(t, types.StopContainerReasonUnknown, stopReason("container-orphan"))
	signalled("container-orphan", syscall.SIGKILL)
	// A STOPPING container is asked to stop and given its grace period.
	require.Equal(t, types.StopContainerReasonUnknown, stopReason("container-stopping"))
	signalled("container-stopping", syscall.SIGTERM)
	// An eviction victim goes on its own drain window, none here.
	require.Equal(t, types.StopContainerReasonEvicted, stopReason("container-evicted"))
	signalled("container-evicted", syscall.SIGKILL)
	// The others keep running: one was renewed, one is not ours to judge.
	for _, containerID := range []string{"container-running", "container-foreign"} {
		require.Empty(t, stopReason(containerID), containerID)
		require.Empty(t, rt.observed(containerID), containerID)
	}
	// All of that came from one call.
	require.Len(t, repoClient.containerLeaseRenewals(), 1)
	require.Zero(t, repoClient.getContainerStateCalls())
	require.Empty(t, repoClient.containerStatusUpdates())

	// The next interval renews only the containers still running. The
	// stopping one has its own heartbeat until it exits, and the two that
	// are gone have none.
	worker.updateContainerStatuses(worker.ctx)

	require.Equal(t, []string{"container-foreign", "container-running", "container-stopping"}, heartbeatContainers(t, worker))
	renewals := repoClient.containerLeaseRenewals()
	require.Len(t, renewals, 2)
	require.Equal(t, []string{"container-foreign", "container-running"}, renewedContainers(renewals[1]))
	require.Equal(t, 1, repoClient.getContainerStateCalls())
}

// containerEventRecorder keeps the container events the worker records.
type containerEventRecorder struct {
	repo.EventRepository
	mu     sync.Mutex
	events []types.EventContainerEventSchema
}

func (r *containerEventRecorder) PushContainerEvent(event types.EventContainerEventSchema) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *containerEventRecorder) recorded() map[string]types.ContainerEventID {
	r.mu.Lock()
	defer r.mu.Unlock()
	events := make(map[string]types.ContainerEventID)
	for _, event := range r.events {
		events[event.ContainerID] = event.ID
	}
	return events
}

func TestHeartbeatRecordsWhatBecameOfAContainer(t *testing.T) {
	repoClient := &fakeContainerRepoClient{leases: map[string]*pb.ContainerLease{
		"container-started": {ContainerId: "container-started", Ok: true, Status: string(types.ContainerStatusRunning), Started: true},
		"container-orphan":  {ContainerId: "container-orphan", ErrorMsg: (&types.ErrContainerStateNotFound{ContainerId: "container-orphan"}).Error()},
	}}
	worker, rt := heartbeatTestWorker(t, repoClient)
	events := &containerEventRecorder{}
	worker.eventRepo = events
	addHeartbeatContainer(worker, rt, "container-running")
	started := addHeartbeatContainer(worker, rt, "container-started")
	started.markRuntimeStarted(1234)
	addHeartbeatContainer(worker, rt, "container-orphan")

	worker.updateContainerStatuses(worker.ctx)
	heartbeatContainers(t, worker)

	// A container the gateway moved from PENDING to RUNNING on this heartbeat
	// is recorded as reconciled, and keeps running.
	want := map[string]types.ContainerEventID{
		"container-started": types.ContainerEventWorkerPendingReconciled,
		"container-orphan":  types.ContainerEventWorkerOrphanStateMissing,
	}
	require.Eventually(t, func() bool { return len(events.recorded()) == len(want) }, 5*time.Second, time.Millisecond)
	require.Equal(t, want, events.recorded())
	_, stopReason := started.lifecycleState()
	require.Empty(t, stopReason)
	require.Empty(t, rt.observed("container-started"))
}

func TestExitedContainerLeavesTheHeartbeat(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	worker, rt := heartbeatTestWorker(t, repoClient)
	addHeartbeatContainer(worker, rt, "container-running")
	addHeartbeatContainer(worker, rt, "container-exited").setExitCode(0)
	addHeartbeatContainer(worker, rt, "container-removed")
	worker.containerInstances.Delete("container-removed")

	worker.updateContainerStatuses(worker.ctx)

	require.Equal(t, []string{"container-running"}, heartbeatContainers(t, worker))
	renewals := repoClient.containerLeaseRenewals()
	require.Len(t, renewals, 1)
	require.Equal(t, []string{"container-running"}, renewedContainers(renewals[0]))
	require.Zero(t, repoClient.getContainerStateCalls())
}

func TestStoppingAndManagedContainersKeepTheirOwnHeartbeat(t *testing.T) {
	repoClient := &fakeContainerRepoClient{state: &pb.ContainerState{Status: string(types.ContainerStatusRunning)}}
	worker, rt := heartbeatTestWorker(t, repoClient)
	addHeartbeatContainer(worker, rt, "container-running")
	// The gateway gives a managed endpoint its longer lease on UpdateContainerStatus.
	addHeartbeatContainer(worker, rt, "managed-model-replica")
	// Only UpdateContainerStatus publishes a stop the worker decided on.
	addHeartbeatContainer(worker, rt, "container-stopping").setStopReason(types.StopContainerReasonUser)

	worker.updateContainerStatuses(worker.ctx)

	require.Len(t, heartbeatContainers(t, worker), 3)
	renewals := repoClient.containerLeaseRenewals()
	require.Len(t, renewals, 1)
	require.Equal(t, []string{"container-running"}, renewedContainers(renewals[0]))
	updates := make(map[string]string)
	for _, update := range repoClient.containerStatusUpdates() {
		updates[update.ContainerId] = fmt.Sprintf("%s for %ds", update.Status, update.ExpirySeconds)
	}
	require.Equal(t, map[string]string{
		"managed-model-replica": fmt.Sprintf("RUNNING for %ds", types.ContainerStateTtlS),
		"container-stopping":    fmt.Sprintf("STOPPING for %ds", types.ContainerStateTtlSWhileStopping),
	}, updates)
}

func TestHeartbeatUsesSingleContainerCallsWithAGatewayThatHasNoSharedOne(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		state:            &pb.ContainerState{Status: string(types.ContainerStatusRunning)},
		renewLeasesError: status.Error(codes.Unimplemented, "unknown method RenewContainerLeases for service ContainerRepositoryService"),
	}
	worker, rt := heartbeatTestWorker(t, repoClient)
	containerIDs := []string{"container-a", "container-b", "container-c"}
	for _, containerID := range containerIDs {
		addHeartbeatContainer(worker, rt, containerID)
	}

	// An older gateway: every container is renewed the way it was before.
	worker.updateContainerStatuses(worker.ctx)

	require.Equal(t, containerIDs, heartbeatContainers(t, worker))
	require.Equal(t, len(containerIDs), repoClient.getContainerStateCalls())
	var updated []string
	for _, update := range repoClient.containerStatusUpdates() {
		require.Equal(t, string(types.ContainerStatusRunning), update.Status)
		require.Equal(t, int64(types.ContainerStateTtlS), update.ExpirySeconds)
		updated = append(updated, update.ContainerId)
	}
	slices.Sort(updated)
	require.Equal(t, containerIDs, updated)

	// Once the gateway is upgraded, they share a call again.
	repoClient.mu.Lock()
	repoClient.renewLeasesError = nil
	repoClient.mu.Unlock()
	worker.updateContainerStatuses(worker.ctx)

	require.Equal(t, containerIDs, heartbeatContainers(t, worker))
	renewals := repoClient.containerLeaseRenewals()
	require.Len(t, renewals, 2)
	require.Equal(t, containerIDs, renewedContainers(renewals[1]))
	require.Equal(t, len(containerIDs), repoClient.getContainerStateCalls())
	require.Len(t, repoClient.containerStatusUpdates(), len(containerIDs))
}

func TestHeartbeatKeepsItsContainersWhenTheGatewayIsUnreachable(t *testing.T) {
	repoClient := &fakeContainerRepoClient{renewLeasesError: status.Error(codes.Unavailable, "connection refused")}
	worker, rt := heartbeatTestWorker(t, repoClient)
	instance := addHeartbeatContainer(worker, rt, "container-running")

	worker.updateContainerStatuses(worker.ctx)

	// Nothing is concluded about the container, and it is tried again.
	require.Equal(t, []string{"container-running"}, heartbeatContainers(t, worker))
	_, stopReason := instance.lifecycleState()
	require.Empty(t, stopReason)
	require.Zero(t, repoClient.getContainerStateCalls())
	worker.updateContainerStatuses(worker.ctx)
	require.Len(t, repoClient.containerLeaseRenewals(), 2)
}

func TestContainerBeingFinalizedIsNotRenewed(t *testing.T) {
	repoClient := &fakeContainerRepoClient{}
	worker, rt := heartbeatTestWorker(t, repoClient)
	addHeartbeatContainer(worker, rt, "container-running")
	finalizing := addHeartbeatContainer(worker, rt, "container-finalizing")

	// clearContainer holds this while it publishes STOPPING and records the exit.
	finalizing.statusHeartbeatMu.Lock()
	worker.updateContainerStatuses(worker.ctx)
	finalizing.setExitCode(0)
	finalizing.statusHeartbeatMu.Unlock()
	worker.updateContainerStatuses(worker.ctx)

	require.Equal(t, []string{"container-running"}, heartbeatContainers(t, worker))
	renewals := repoClient.containerLeaseRenewals()
	require.Len(t, renewals, 2)
	for _, renewal := range renewals {
		require.Equal(t, []string{"container-running"}, renewedContainers(renewal))
	}
}

func TestClearContainerWaitsForARenewalAlreadyOnItsWay(t *testing.T) {
	fixture := newLockedDurableDiskFinalizationFixture(t, "container-renewal-on-its-way")
	request := fixture.request
	renewalStarted := make(chan struct{}, 1)
	renewalRelease := make(chan struct{})
	leaseStarted := make(chan struct{}, 1)
	repoClient := &fakeContainerRepoClient{
		renewLeasesStarted:  renewalStarted,
		renewLeasesRelease:  renewalRelease,
		updateStatusStarted: leaseStarted,
		updateStatusErrorForExpiry: map[int64]error{
			types.ContainerStateTtlSWhileStopping: context.Canceled,
		},
	}
	worker := newContainerFinalizationTestWorker(request, repoClient, nil)
	worker.statusHeartbeats.running = true
	worker.startContainerStatusHeartbeat(request)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		worker.updateContainerStatuses(context.Background())
	}()
	select {
	case <-renewalStarted:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not send the renewal")
	}

	// A renewal arriving after the exit was recorded would keep a finished
	// container RUNNING, so finalization starts once the renewal is back.
	clearDone := clearContainerAsync(worker, request, 7)
	select {
	case <-leaseStarted:
		t.Fatal("finalization began while a renewal was on its way")
	case <-time.After(50 * time.Millisecond):
	}
	instance, ok := worker.containerInstances.Get(request.ContainerId)
	require.True(t, ok)
	exitCode, _ := instance.lifecycleState()
	require.Equal(t, -1, exitCode)

	close(renewalRelease)
	<-heartbeatDone
	requireLocalContainerExitCode(t, worker, request.ContainerId, 7)

	// The next interval finds the container exited and renews nothing.
	worker.updateContainerStatuses(context.Background())
	require.Empty(t, heartbeatContainers(t, worker))
	require.Len(t, repoClient.containerLeaseRenewals(), 1)
	require.Zero(t, repoClient.getContainerStateCalls())

	fixture.releaseAndWaitForClear(t, clearDone)
}
