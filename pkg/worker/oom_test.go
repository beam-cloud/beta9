package worker

import (
	"bytes"
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestSetupOOMWatcherSkipsGvisorWhenMemoryIsNotEnforced(t *testing.T) {
	worker := &Worker{
		runtime:            &mockRuntime{name: types.ContainerRuntimeGvisor.String()},
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set("container-id", &ContainerInstance{})

	worker.setupOOMWatcher(
		context.Background(),
		"container-id",
		123,
		nil,
		&types.ContainerRequest{Memory: 128},
		slog.Default(),
		&atomic.Bool{},
	)

	instance, exists := worker.containerInstances.Get("container-id")
	require.True(t, exists)
	require.False(t, instance.hasOOMWatcher())
}

type guestOOMRuntime struct {
	*MockRuntime
	events chan runtime.Event
	killed chan syscall.Signal
}

type guestOOMEventRepo struct {
	repository.EventRepository
	events chan types.EventContainerEventSchema
	logs   []types.EventContainerLogSchema
}

func (r *guestOOMEventRepo) PushContainerEvent(event types.EventContainerEventSchema) {
	r.events <- event
}

func (r *guestOOMEventRepo) PushContainerRequestLogLine(_ string, request *types.ContainerRequest, _ string, stream, line string) {
	r.logs = append(r.logs, types.EventContainerLogSchema{ContainerID: request.ContainerId, Stream: stream, Line: line})
}

func (*guestOOMEventRepo) PushContainerLogFirstByte(string, *types.ContainerRequest, string) {}
func (*guestOOMEventRepo) PushContainerLogLastByte(string, *types.ContainerRequest)          {}
func (*guestOOMEventRepo) PushContainerLogFlushCompleted(string, *types.ContainerRequest)    {}

func (r *guestOOMRuntime) Events(context.Context, string) (<-chan runtime.Event, error) {
	return r.events, nil
}

func (r *guestOOMRuntime) Kill(_ context.Context, _ string, signal syscall.Signal, _ *runtime.KillOpts) error {
	r.killed <- signal
	return nil
}

type oomTestWorker struct {
	*Worker
	ctx      context.Context
	instance *ContainerInstance
	rt       *guestOOMRuntime
	repo     *guestOOMEventRepo
	logs     chan common.LogRecord
	oom      atomic.Bool
}

func newOOMTestWorker(t *testing.T, kind string) *oomTestWorker {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	rt := &guestOOMRuntime{
		MockRuntime: NewMockRuntime("gvisor", runtime.Capabilities{}),
		events:      make(chan runtime.Event, 2), killed: make(chan syscall.Signal, 1),
	}
	request := &types.ContainerRequest{ContainerId: "container", Memory: 2048,
		Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(kind), Config: `{"_beta9_force_resource_limits":true}`}},
	}
	repo := &guestOOMEventRepo{events: make(chan types.EventContainerEventSchema, 4)}
	worker := &Worker{ctx: t.Context(), runtime: rt, eventRepo: repo,
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		containerCancels:   common.NewSafeMap[context.CancelFunc](),
		stopContainerChan:  make(chan stopContainerEvent, 1),
	}
	instance := &ContainerInstance{Id: request.ContainerId, Request: request, Runtime: rt, ExitCode: -1, LogBuffer: common.NewLogBuffer()}
	worker.containerInstances.Set(instance.Id, instance)
	worker.containerCancels.Set(instance.Id, cancel)
	w := &oomTestWorker{Worker: worker, ctx: ctx, instance: instance, rt: rt, repo: repo, logs: make(chan common.LogRecord, 4)}
	worker.setupOOMWatcher(ctx, instance.Id, 123, nil, request, slog.New(common.NewChannelHandler(w.logs)), &w.oom)
	t.Cleanup(func() {
		instance.stopOOMWatcher()
		cancel()
		instance.LogBuffer.Dispose()
	})
	return w
}

func guestOOM(kills uint64) runtime.Event {
	return runtime.Event{Type: "application_oom", ApplicationOOM: &microvm.ApplicationOOM{Kills: kills, MemoryLimit: 2048 << 20}}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value, ok := <-ch:
		require.True(t, ok, "channel closed before notification")
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for notification")
		var zero T
		return zero
	}
}

func TestGuestOOMTerminatesWorkloadsButPreservesSandboxes(t *testing.T) {
	for _, kind := range []string{
		types.StubTypeEndpointDeployment, types.StubTypeTaskQueueDeployment,
		types.StubTypePodDeployment, types.StubTypeFunction, types.StubTypeSandbox, types.StubTypeVM,
	} {
		t.Run(kind, func(t *testing.T) {
			w := newOOMTestWorker(t, kind)
			if w.instance.Request.Stub.Type.IsSandbox() {
				for count := 1; count <= 2; count++ {
					w.rt.events <- guestOOM(uint64(count))
					event := receive(t, w.repo.events)
					require.Equal(t, types.ContainerEventApplicationOOMKilled, event.ID)
					require.Equal(t, "OOM", event.Reason)
					require.Equal(t, "137", event.Attrs[types.EventAttrExitCode])
					require.Equal(t, strconv.Itoa(count), event.Attrs["oom_kills"])
				}
				require.False(t, w.oom.Load())
				exitCode, reason := w.instance.lifecycleState()
				require.Equal(t, -1, exitCode)
				require.Empty(t, reason)
				require.Empty(t, w.rt.killed)
				return
			}
			w.rt.events <- guestOOM(1)
			require.Equal(t, syscall.SIGKILL, receive(t, w.rt.killed))
			require.True(t, w.oom.Load())
			require.Equal(t, 137, normalizeContainerExitCode(-1, types.StopContainerReasonUnknown, w.oom.Load()))
			event := receive(t, w.repo.events)
			require.Equal(t, types.ContainerEventRuntimeOOMKilled, event.ID)
			require.Equal(t, "OOM", event.Reason)
			require.Equal(t, "true", event.Attrs[types.EventAttrOOMKilled])
		})
	}
}

func TestOOMLogsBypassQuotaAndReachAllSinks(t *testing.T) {
	for _, test := range []struct{ kind, message string }{
		{types.StubTypeEndpointDeployment, types.WorkerContainerExitCodeOomKillMessage},
		{types.StubTypeSandbox, types.EventMessageApplicationOOMKilled.String()},
	} {
		t.Run(test.kind, func(t *testing.T) {
			w := newOOMTestWorker(t, test.kind)
			w.rt.events <- guestOOM(1)
			receive(t, w.repo.events)
			w.instance.stopOOMWatcher()
			close(w.logs)
			logger := &ContainerLogger{containerInstances: w.containerInstances, eventRepo: w.repo, logLinesPerHour: 0}
			var persisted bytes.Buffer
			require.NoError(t, logger.captureLogs(w.instance.Request, w.logs, &persisted))
			require.Len(t, w.repo.logs, 1)
			line := w.repo.logs[0]
			require.Equal(t, "system", line.Stream)
			require.Equal(t, test.message, line.Line)
			require.Contains(t, persisted.String(), line.Line)
			buffered := make([]byte, len(line.Line))
			require.Eventually(t, func() bool {
				n, _ := w.instance.LogBuffer.ReadAt(buffered, 0)
				return n == len(buffered)
			}, time.Second, time.Millisecond)
			require.Equal(t, line.Line, string(buffered))
		})
	}
}

func TestForcedResourceLimitsEnableGvisorOOMPolicy(t *testing.T) {
	worker := &Worker{}
	request := &types.ContainerRequest{Stub: types.StubWithRelated{Stub: types.Stub{
		Config: `{"_beta9_force_resource_limits":true}`,
	}}}

	require.True(t, worker.memoryLimitsEnforced(request))
}

func TestExplicitStopsNeverBecomeOOMs(t *testing.T) {
	for _, reason := range []types.StopContainerReason{
		types.StopContainerReasonUser, types.StopContainerReasonScheduler,
		types.StopContainerReasonTtl, types.StopContainerReasonAdmin,
		types.StopContainerReasonInsufficientCredits, types.StopContainerReasonEvicted,
	} {
		t.Run(string(reason), func(t *testing.T) {
			w := newOOMTestWorker(t, types.StubTypeEndpointDeployment)
			require.True(t, w.handleStopContainerArgs(types.StopContainerArgs{ContainerId: w.instance.Id, Reason: reason, Force: true}, types.EventSourceSchedulerStop))
			stop := receive(t, w.stopContainerChan)
			require.NoError(t, w.stopContainerWithoutCheckpointDeferral(t.Context(), stop.ContainerId, stop.Kill))
			require.Equal(t, syscall.SIGKILL, receive(t, w.rt.killed))
			// A late notification from a cancelled observer must be ignored.
			w.rt.events <- guestOOM(1)
			require.ErrorIs(t, w.ctx.Err(), context.Canceled)
			require.Never(t, w.oom.Load, 20*time.Millisecond, time.Millisecond)
			_, recordedReason := w.instance.lifecycleState()
			want, ok := reason.ExitCode()
			require.True(t, ok)
			exitCode := normalizeContainerExitCode(137, recordedReason, w.oom.Load())
			require.Equal(t, int(want), exitCode)
			require.Equal(t, string(reason), containerExitReason(exitCode, recordedReason, w.oom.Load()))
			require.False(t, types.ContainerExitCode(exitCode).IsFailed())
			for len(w.repo.events) > 0 {
				require.NotEqual(t, types.ContainerEventRuntimeOOMKilled, receive(t, w.repo.events).ID)
			}
		})
	}
}

func TestFinalOOMReportDeduplicatesWatcherEvidence(t *testing.T) {
	w := newOOMTestWorker(t, types.StubTypeSandbox)
	logger := slog.New(common.NewChannelHandler(w.logs))
	for range 2 {
		w.reportOOMKill(t.Context(), w.instance.Request, logger, &w.oom)
	}
	require.True(t, w.oom.Load())
	require.Equal(t, types.ContainerEventRuntimeOOMKilled, receive(t, w.repo.events).ID)
	require.Empty(t, w.repo.events)
	require.Len(t, w.logs, 1)
	require.Empty(t, w.rt.killed, "final evidence reports an already-exited runtime")
}
