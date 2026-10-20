package worker

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
	events      chan runtime.Event
	eventsCalls atomic.Int32
	killed      chan syscall.Signal
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
	r.eventsCalls.Add(1)
	return r.events, nil
}

func (r *guestOOMRuntime) Kill(_ context.Context, _ string, signal syscall.Signal, _ *runtime.KillOpts) error {
	r.killed <- signal
	return nil
}

func TestGuestOOMTerminatesWorkloadsButPreservesSandboxes(t *testing.T) {
	for _, kind := range []string{types.StubTypeEndpointDeployment, types.StubTypeTaskQueueDeployment, types.StubTypePodDeployment, types.StubTypeFunction, types.StubTypeSandbox, types.StubTypeVM} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rt := &guestOOMRuntime{
				MockRuntime: NewMockRuntime(types.ContainerRuntimeGvisor.String(), runtime.Capabilities{}),
				events:      make(chan runtime.Event, 1),
				killed:      make(chan syscall.Signal, 1),
			}
			request := &types.ContainerRequest{ContainerId: "guest-oom-container", Memory: 2048,
				Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(kind), Config: `{"_beta9_force_resource_limits":true}`}},
			}
			eventRepo := &guestOOMEventRepo{events: make(chan types.EventContainerEventSchema, 2)}
			worker := &Worker{ctx: ctx, runtime: rt, eventRepo: eventRepo, containerInstances: common.NewSafeMap[*ContainerInstance]()}
			instance := &ContainerInstance{Id: request.ContainerId, Request: request, Runtime: rt, ExitCode: -1, LogBuffer: common.NewLogBuffer()}
			t.Cleanup(instance.LogBuffer.Dispose)
			worker.containerInstances.Set(request.ContainerId, instance)
			var oom atomic.Bool
			logs := make(chan common.LogRecord, 4)
			worker.setupOOMWatcher(ctx, request.ContainerId, 123, nil, request, slog.New(common.NewChannelHandler(logs)), &oom)
			defer instance.stopOOMWatcher()
			require.EqualValues(t, 1, rt.eventsCalls.Load())
			rt.events <- runtime.Event{Type: "application_oom", ApplicationOOM: &microvm.ApplicationOOM{Kills: 1, MemoryLimit: 2048 << 20}}
			wantLog, wantLogCount := types.WorkerContainerExitCodeOomKillMessage, 1
			if request.Stub.Type.IsSandbox() {
				wantLog, wantLogCount = types.EventMessageApplicationOOMKilled.String(), 2
				for count := 1; count <= 2; count++ {
					if count == 2 {
						rt.events <- runtime.Event{Type: "application_oom", ApplicationOOM: &microvm.ApplicationOOM{Kills: 2, MemoryLimit: 2048 << 20}}
					}
					select {
					case event := <-eventRepo.events:
						require.Equal(t, types.ContainerEventApplicationOOMKilled, event.ID)
						require.Equal(t, "OOM", event.Reason)
						require.Equal(t, "137", event.Attrs[types.EventAttrExitCode])
						require.Equal(t, strconv.Itoa(count), event.Attrs["oom_kills"])
					case <-time.After(time.Second):
						t.Fatal("sandbox process OOM was not reported")
					}
				}
				require.False(t, oom.Load())
				exitCode, reason := instance.lifecycleState()
				require.Equal(t, -1, exitCode)
				require.Empty(t, reason)
				attempted, _ := instance.oomTerminationResult()
				require.False(t, attempted)
				select {
				case <-rt.killed:
					t.Fatal("guest OOM terminated an interactive sandbox")
				default:
				}
			} else {
				select {
				case signal := <-rt.killed:
					require.Equal(t, syscall.SIGKILL, signal)
				case <-time.After(time.Second):
					t.Fatal("guest OOM did not terminate the supervisor")
				}
				require.True(t, oom.Load())
				require.Equal(t, 137, normalizeContainerExitCode(-1, types.StopContainerReasonUnknown, oom.Load()))
				require.Equal(t, "OOM", containerExitReason(137, types.StopContainerReasonUnknown, oom.Load()))
				select {
				case event := <-eventRepo.events:
					require.Equal(t, types.ContainerEventRuntimeOOMKilled, event.ID)
					require.Equal(t, "OOM", event.Reason)
					require.Equal(t, "true", event.Attrs[types.EventAttrOOMKilled])
				case <-time.After(time.Second):
					t.Fatal("guest OOM was not surfaced as a container event")
				}
			}
			// Exercise the real user-log capture path, including persistence and
			// the replay buffer used by container log streams.
			instance.stopOOMWatcher()
			close(logs)
			// An exhausted application-log quota must not hide the OOM.
			logger := &ContainerLogger{containerInstances: worker.containerInstances, eventRepo: eventRepo, logLinesPerHour: 0}
			logPath := filepath.Join(t.TempDir(), "container.log")
			logFile, err := os.Create(logPath)
			require.NoError(t, err)
			defer logFile.Close()
			require.NoError(t, logger.captureLogs(request, logs, logFile))
			require.Len(t, eventRepo.logs, wantLogCount)
			for _, line := range eventRepo.logs {
				require.Equal(t, "system", line.Stream)
				require.Equal(t, wantLog, line.Line)
			}
			persisted, err := os.ReadFile(logPath)
			require.NoError(t, err)
			require.Contains(t, string(persisted), wantLog)
			buffered := make([]byte, len(wantLog)*wantLogCount)
			require.Eventually(t, func() bool {
				n, _ := instance.LogBuffer.ReadAt(buffered, 0)
				return n == len(buffered)
			}, time.Second, time.Millisecond)
			require.Contains(t, string(buffered), wantLog)
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rt := &guestOOMRuntime{MockRuntime: NewMockRuntime("gvisor", runtime.Capabilities{}), events: make(chan runtime.Event, 1), killed: make(chan syscall.Signal, 1)}
			req := &types.ContainerRequest{ContainerId: "stopped-container", Memory: 2048,
				Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeEndpointDeployment), Config: `{"_beta9_force_resource_limits":true}`}},
			}
			events := &guestOOMEventRepo{events: make(chan types.EventContainerEventSchema, 4)}
			worker := &Worker{ctx: context.Background(), runtime: rt, eventRepo: events,
				containerInstances: common.NewSafeMap[*ContainerInstance](),
				containerCancels:   common.NewSafeMap[context.CancelFunc](),
				stopContainerChan:  make(chan stopContainerEvent, 1),
			}
			instance := &ContainerInstance{Id: req.ContainerId, Request: req, Runtime: rt, ExitCode: -1}
			worker.containerInstances.Set(req.ContainerId, instance)
			worker.containerCancels.Set(req.ContainerId, cancel)
			var oom atomic.Bool
			worker.setupOOMWatcher(ctx, req.ContainerId, 123, nil, req, slog.New(slog.NewTextHandler(io.Discard, nil)), &oom)
			defer instance.stopOOMWatcher()
			require.True(t, worker.handleStopContainerArgs(types.StopContainerArgs{ContainerId: req.ContainerId, Reason: reason, Force: true}, types.EventSourceSchedulerStop))
			stop := <-worker.stopContainerChan
			require.NoError(t, worker.stopContainerWithoutCheckpointDeferral(context.Background(), stop.ContainerId, stop.Kill))
			require.Equal(t, syscall.SIGKILL, <-rt.killed)
			// Even a late notification from a cancelled observer must be ignored.
			rt.events <- runtime.Event{Type: "application_oom", ApplicationOOM: &microvm.ApplicationOOM{Kills: 1}}
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.False(t, oom.Load())
			_, recordedReason := instance.lifecycleState()
			want, ok := reason.ExitCode()
			require.True(t, ok)
			exitCode := normalizeContainerExitCode(137, recordedReason, oom.Load())
			require.Equal(t, int(want), exitCode)
			require.Equal(t, string(reason), containerExitReason(exitCode, recordedReason, oom.Load()))
			require.False(t, types.ContainerExitCode(exitCode).IsFailed())
			for len(events.events) > 0 {
				require.NotEqual(t, types.ContainerEventRuntimeOOMKilled, (<-events.events).ID)
			}
		})
	}
}
