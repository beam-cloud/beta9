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
// drainExits is set, SIGKILL always does.
type evictionRuntime struct {
	mockRuntime
	mu         sync.Mutex
	worker     *Worker
	drainExits bool
	signals    map[string][]syscall.Signal
}

func (r *evictionRuntime) Kill(_ context.Context, containerID string, signal syscall.Signal, _ *runtime.KillOpts) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.signals == nil {
		r.signals = map[string][]syscall.Signal{}
	}
	r.signals[containerID] = append(r.signals[containerID], signal)
	if signal == syscall.SIGKILL || r.drainExits {
		r.worker.containerInstances.Delete(containerID)
	}
	return nil
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
	worker.evictForRequest(context.Background(), &types.ContainerRequest{
		ContainerId:       "incoming",
		EvictContainerIds: []string{"victim-1", "already-gone"},
		EvictDrainSeconds: 30,
	})

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

	worker.evictForRequest(context.Background(), &types.ContainerRequest{
		ContainerId:       "incoming",
		EvictContainerIds: []string{"victim-1"},
		EvictDrainSeconds: 0,
	})

	require.Eventually(t, func() bool {
		_, exists := worker.containerInstances.Get("victim-1")
		return !exists
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}, rt.observed("victim-1"))
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
	require.Equal(t, []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}, rt.observed(containerID))
}

func TestNormalizeContainerExitCodeEvicted(t *testing.T) {
	require.Equal(t, int(types.ContainerExitCodeEvicted), normalizeContainerExitCode(137, types.StopContainerReasonEvicted, false))
	require.False(t, types.ContainerExitCodeEvicted.IsFailed())
}
