package worker

import (
	"context"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

// evictionRuntime records signals; SIGTERM exits the container only when
// drainExits is set, SIGKILL does unless ignoreKill is set (a victim wedged
// in the kernel or a slow device release).
type evictionRuntime struct {
	mockRuntime
	mu         sync.Mutex
	worker     *Worker
	drainExits bool
	ignoreKill bool
	signals    map[string][]syscall.Signal
}

func (r *evictionRuntime) Kill(_ context.Context, containerID string, signal syscall.Signal, _ *runtime.KillOpts) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.signals == nil {
		r.signals = map[string][]syscall.Signal{}
	}
	r.signals[containerID] = append(r.signals[containerID], signal)
	if (signal == syscall.SIGKILL && !r.ignoreKill) || r.drainExits {
		r.worker.containerInstances.Delete(containerID)
	}
	return nil
}

// shortenEvictionKillTimeout overrides the kill window for one test.
func shortenEvictionKillTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	previous := evictionKillTimeout
	evictionKillTimeout = timeout
	t.Cleanup(func() { evictionKillTimeout = previous })
}

func (r *evictionRuntime) observed(containerID string) []syscall.Signal {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]syscall.Signal(nil), r.signals[containerID]...)
}

func evictionWorkerForTest(drainExits bool) (*Worker, *evictionRuntime) {
	worker := &Worker{
		ctx:                context.Background(),
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		containerCancels:   common.NewSafeMap[context.CancelFunc](),
		config:             types.AppConfig{Worker: types.WorkerConfig{TerminationGracePeriod: 300}},
	}
	rt := &evictionRuntime{worker: worker, drainExits: drainExits}
	return worker, rt
}

func addRunningInstance(worker *Worker, rt runtime.Runtime, containerID string) *ContainerInstance {
	instance := &ContainerInstance{
		Id:       containerID,
		ExitCode: -1,
		Request:  &types.ContainerRequest{ContainerId: containerID},
		Runtime:  rt,
	}
	worker.containerInstances.Set(containerID, instance)
	return instance
}

func TestEvictForRequestDrainsThenStartsWhenVictimsExit(t *testing.T) {
	worker, rt := evictionWorkerForTest(true)
	victim := addRunningInstance(worker, rt, "victim-1")
	addRunningInstance(worker, rt, "survivor")

	started := time.Now()
	require.NoError(t, worker.evictForRequest(context.Background(), &types.ContainerRequest{
		ContainerId:       "incoming",
		EvictContainerIds: []string{"victim-1", "already-gone"},
		EvictDrainSeconds: 30,
	}))

	// The victim left on SIGTERM, so the request proceeds well before the
	// drain window and with the eviction exit reason recorded.
	require.Less(t, time.Since(started), 5*time.Second)
	require.Equal(t, []syscall.Signal{syscall.SIGTERM}, rt.observed("victim-1"))
	_, reason := victim.lifecycleState()
	require.Equal(t, types.StopContainerReasonEvicted, reason)
	_, exists := worker.containerInstances.Get("survivor")
	require.True(t, exists)
	require.Empty(t, rt.observed("survivor"))
}

func TestEvictForRequestKillsAfterDrainWindow(t *testing.T) {
	worker, rt := evictionWorkerForTest(false)
	addRunningInstance(worker, rt, "victim-1")

	require.NoError(t, worker.evictForRequest(context.Background(), &types.ContainerRequest{
		ContainerId:       "incoming",
		EvictContainerIds: []string{"victim-1"},
		EvictDrainSeconds: 0,
	}))

	require.Eventually(t, func() bool {
		_, exists := worker.containerInstances.Get("victim-1")
		return !exists
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, []syscall.Signal{syscall.SIGKILL}, rt.observed("victim-1"))
}

func TestEvictForRequestWaitsForVictimAlreadyFinalizing(t *testing.T) {
	worker, rt := evictionWorkerForTest(false)
	// The victim has exited (exit code recorded) but clearContainer has not yet
	// released its GPU or dropped the instance: it must still be waited for,
	// and must not be signalled again.
	victim := addRunningInstance(worker, rt, "victim-1")
	victim.setExitCode(0)

	require.True(t, worker.evictContainer("victim-1", time.Hour, "incoming"))
	require.Empty(t, rt.observed("victim-1"))
	require.False(t, victim.StopEscalationStarted.Load())

	released := make(chan struct{})
	go func() {
		time.Sleep(300 * time.Millisecond)
		worker.containerInstances.Delete("victim-1")
		close(released)
	}()
	require.NoError(t, worker.evictForRequest(context.Background(), &types.ContainerRequest{
		ContainerId:       "incoming",
		EvictContainerIds: []string{"victim-1"},
		EvictDrainSeconds: 30,
	}))
	select {
	case <-released:
	default:
		t.Fatal("request proceeded before the finalizing victim was removed")
	}
	require.Empty(t, rt.observed("victim-1"))
}

func TestEvictForRequestFailsWhenVictimsOutliveKillWindow(t *testing.T) {
	shortenEvictionKillTimeout(t, 300*time.Millisecond)
	worker, rt := evictionWorkerForTest(false)
	rt.ignoreKill = true
	addRunningInstance(worker, rt, "victim-1")

	// Mirror runContainerRequestWithRunner: the incoming request's startup
	// context is registered so a failed eviction can abort it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker.registerContainerCancel("incoming", cancel)

	err := worker.evictForRequest(ctx, &types.ContainerRequest{
		ContainerId:       "incoming",
		EvictContainerIds: []string{"victim-1"},
		EvictDrainSeconds: 0,
	})
	require.ErrorIs(t, err, ErrEvictionIncomplete)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	// The victim received an immediate SIGKILL; it just never
	// went away, so the request cannot claim its resources.
	require.Equal(t, []syscall.Signal{syscall.SIGKILL}, rt.observed("victim-1"))
	_, exists := worker.containerInstances.Get("victim-1")
	require.True(t, exists)
}

func TestRunContainerRequestFailsInsteadOfStartingOnHeldResources(t *testing.T) {
	shortenEvictionKillTimeout(t, 300*time.Millisecond)
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	repoClient := &fakeContainerRepoClient{}
	// The incoming container never reaches the runtime, so its state lookup
	// during cleanup reports it absent.
	incomingRuntime := &mockRuntime{name: types.ContainerRuntimeRunc.String(), state: func(_ context.Context, containerID string) (runtime.State, error) {
		return runtime.State{}, runtime.ErrContainerNotFound{ContainerID: containerID}
	}}
	worker := &Worker{
		ctx:                     workerCtx,
		workerId:                "worker-1",
		runtime:                 incomingRuntime,
		workerRepoClient:        &fakeWorkerRepoClient{},
		containerRepoClient:     repoClient,
		containerInstances:      common.NewSafeMap[*ContainerInstance](),
		containerCancels:        common.NewSafeMap[context.CancelFunc](),
		containerNetworkManager: &fakeContainerNetworkController{},
		completedRequests:       make(chan *types.ContainerRequest, 1),
	}
	rt := &evictionRuntime{worker: worker, ignoreKill: true}
	addRunningInstance(worker, rt, "victim-1")

	request := &types.ContainerRequest{
		ContainerId:       "incoming",
		DeliveryToken:     "delivery-1",
		EvictContainerIds: []string{"victim-1"},
		EvictDrainSeconds: 0,
	}
	require.True(t, worker.reserveContainerInstance(request))

	runnerStarted := make(chan struct{}, 1)
	preparationStarted := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		worker.runContainerRequestWithRunner(request, func(_ context.Context, _ *types.ContainerRequest, waitForEviction func() error) error {
			preparationStarted <- struct{}{}
			if err := waitForEviction(); err != nil {
				return err
			}
			runnerStarted <- struct{}{}
			return nil
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("container request did not complete")
	}

	// The runtime never started; the request took the same pre-start failure
	// path as a container that fails to run: a failed exit code is recorded
	// and the instance is dropped so the worker's accounting is released.
	require.Empty(t, runnerStarted)
	require.Len(t, preparationStarted, 1, "preparation overlaps reclamation even when the GPU cannot be released")
	require.Equal(t, 1, repoClient.setExitCodeCalls)
	require.Equal(t, "incoming", repoClient.lastSetExitCode.ContainerId)
	require.Equal(t, int32(1), repoClient.lastSetExitCode.ExitCode)
	_, exists := worker.containerInstances.Get("incoming")
	require.False(t, exists)
	require.Len(t, worker.completedRequests, 1)
	_, cancelRegistered := worker.containerCancels.Get("incoming")
	require.False(t, cancelRegistered)
}

func TestEvictContainerIsIdempotentAndOwnsEscalation(t *testing.T) {
	worker, rt := evictionWorkerForTest(false)
	instance := addRunningInstance(worker, rt, "victim-1")

	require.True(t, worker.evictContainer("victim-1", time.Hour, "incoming"))
	require.True(t, worker.evictContainer("victim-1", time.Hour, "incoming"))
	require.False(t, worker.evictContainer("missing", time.Hour, "incoming"))
	require.True(t, instance.StopEscalationStarted.Load())
	// One SIGTERM despite two calls; the heartbeat-observed STOPPING path
	// also defers to the in-flight eviction instead of applying its own grace.
	require.Equal(t, []syscall.Signal{syscall.SIGTERM}, rt.observed("victim-1"))
	worker.handleObservedStoppingContainer("victim-1", types.EventSourceWorkerStatusHeartbeat)
	require.Equal(t, []syscall.Signal{syscall.SIGTERM}, rt.observed("victim-1"))
}

func TestStatusHeartbeatEvictsMarkedVictimOnItsOwnDrainWindow(t *testing.T) {
	const containerID = "victim-heartbeat"
	worker, rt := evictionWorkerForTest(false)
	worker.containerRepoClient = &fakeContainerRepoClient{state: &pb.ContainerState{
		ContainerId:  containerID,
		Status:       string(types.ContainerStatusStopping),
		Evicting:     true,
		DrainSeconds: 0,
	}}
	instance := addRunningInstance(worker, rt, containerID)

	done, err := worker.updateContainerStatusOnce(context.Background(), instance.Request)
	require.NoError(t, err)
	require.False(t, done)
	_, reason := instance.lifecycleState()
	require.Equal(t, types.StopContainerReasonEvicted, reason)
	require.Eventually(t, func() bool {
		_, exists := worker.containerInstances.Get(containerID)
		return !exists
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, []syscall.Signal{syscall.SIGKILL}, rt.observed(containerID))
}
