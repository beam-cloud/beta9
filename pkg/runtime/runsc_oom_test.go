package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/stretchr/testify/require"
)

func TestRunscGuestOOMEventsUseActualKillsOnly(t *testing.T) {
	runscPath := filepath.Join(t.TempDir(), "runsc")
	require.NoError(t, os.WriteFile(runscPath, []byte(`#!/bin/sh
cat <<'EOF'
{"type":"stats","id":"container","data":{"memory":{"usage":{"usage":9999999999}}}}
{"type":"stats","id":"container","data":{"memory":{"raw":{"guest_oom_kill":0}}}}
{"type":"stats","id":"other","data":{"memory":{"raw":{"guest_oom_kill":99}}}}
{"type":"stats","id":"container","data":{"memory":{"raw":{"guest_oom_kill":1,"guest_memory_limit":67108864},"usage":{"usage":60000000}}}}
{"type":"stats","id":"container","data":{"memory":{"raw":{"guest_oom_kill":1}}}}
{"type":"stats","id":"container","data":{"memory":{"raw":{"guest_oom_kill":2}}}}
EOF
`), 0o755))
	rt := &Runsc{cfg: Config{RunscPath: runscPath}}
	events, err := rt.Events(context.Background(), "container")
	require.NoError(t, err)
	var kills int
	for event := range events {
		require.NoError(t, event.Err)
		require.Equal(t, "application_oom", event.Type)
		kills++
		require.Equal(t, uint64(kills), event.ApplicationOOM.Kills)
		if kills == 1 {
			require.Equal(t, int64(64<<20), event.ApplicationOOM.MemoryLimit)
			require.Equal(t, uint64(60000000), event.ApplicationOOM.MemoryUsage)
		}
	}
	require.Equal(t, 2, kills)
}

func TestRunscGuestOOMEventsStopOnCancellationAndInvalidJSON(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "invalid JSON"}[invalid], func(t *testing.T) {
			runscPath := filepath.Join(t.TempDir(), "runsc")
			script := "#!/bin/sh\n"
			if invalid {
				script += "echo invalid-json\n"
			}
			// exec avoids leaving a child behind when the stream is cancelled.
			script += "exec sleep 60\n"
			require.NoError(t, os.WriteFile(runscPath, []byte(script), 0o755))
			rt := &Runsc{cfg: Config{RunscPath: runscPath}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			events, err := rt.Events(ctx, "container")
			require.NoError(t, err)
			if invalid {
				select {
				case event := <-events:
					require.ErrorContains(t, event.Err, "decode gVisor OOM")
				case <-time.After(time.Second):
					t.Fatal("invalid JSON did not terminate the stream")
				}
			} else {
				cancel()
			}
			select {
			case _, ok := <-events:
				require.False(t, ok)
			case <-time.After(time.Second):
				t.Fatal("event process did not stop")
			}
		})
	}
}

type eventTestRuntime struct {
	Runtime
	events chan Event
	err    error
}

func (r *eventTestRuntime) Events(context.Context, string) (<-chan Event, error) {
	return r.events, r.err
}

type eventTestFallback struct {
	notify  func()
	stopped atomic.Bool
}

func (w *eventTestFallback) Watch(notify func()) error { w.notify = notify; return nil }
func (w *eventTestFallback) Stop()                     { w.stopped.Store(true) }

func TestRuntimeOOMWatcherDeduplicatesNativeAndHostKills(t *testing.T) {
	fallback := &eventTestFallback{}
	rt := &eventTestRuntime{events: make(chan Event, 2)}
	watcher := NewRuntimeOOMWatcher(context.Background(), rt, "container", fallback, nil)
	defer watcher.Stop()
	var calls atomic.Int32
	require.NoError(t, watcher.Watch(func() { calls.Add(1) }))
	rt.events <- Event{Type: "application_oom", ApplicationOOM: &microvm.ApplicationOOM{Kills: 1}}
	require.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, time.Millisecond)
	fallback.notify()
	require.EqualValues(t, 1, calls.Load())
}

func TestRuntimeOOMWatcherRejectsEventsAfterStop(t *testing.T) {
	fallback := &eventTestFallback{}
	rt := &eventTestRuntime{events: make(chan Event, 1)}
	watcher := NewRuntimeOOMWatcher(context.Background(), rt, "container", fallback, nil)
	var calls atomic.Int32
	require.NoError(t, watcher.Watch(func() { calls.Add(1) }))
	watcher.Stop()
	fallback.notify()
	rt.events <- Event{Type: "oom"}
	close(rt.events)
	require.True(t, fallback.stopped.Load())
	require.Never(t, func() bool { return calls.Load() != 0 }, 20*time.Millisecond, time.Millisecond)
}

func TestRuntimeOOMWatcherPreservesHostDetectionWhenNativeStreamFails(t *testing.T) {
	fallback := &eventTestFallback{}
	rt := &eventTestRuntime{err: errors.New("runsc events unavailable")}
	watcher := NewRuntimeOOMWatcher(context.Background(), rt, "container", fallback, nil)
	defer watcher.Stop()
	var calls atomic.Int32
	require.NoError(t, watcher.Watch(func() { calls.Add(1) }))
	require.False(t, fallback.stopped.Load())
	fallback.notify()
	require.EqualValues(t, 1, calls.Load())
}

func TestRuntimeOOMWatcherReportsRepeatedSandboxOOMsWithoutTerminating(t *testing.T) {
	fallback := &eventTestFallback{}
	rt := &eventTestRuntime{events: make(chan Event, 1)}
	reported := make(chan uint64, 2)
	watcher := NewRuntimeOOMWatcher(context.Background(), rt, "sandbox", fallback, func(oom *microvm.ApplicationOOM) {
		reported <- oom.Kills
	})
	defer watcher.Stop()
	var terminalCalls atomic.Int32
	require.NoError(t, watcher.Watch(func() { terminalCalls.Add(1) }))
	for count := uint64(1); count <= 2; count++ {
		rt.events <- Event{Type: "application_oom", ApplicationOOM: &microvm.ApplicationOOM{Kills: count}}
		select {
		case got := <-reported:
			require.Equal(t, count, got)
		case <-time.After(time.Second):
			t.Fatal("sandbox OOM was not reported")
		}
		require.Zero(t, terminalCalls.Load())
	}
	// A real host OOM is still terminal for the sandbox runtime.
	fallback.notify()
	require.EqualValues(t, 1, terminalCalls.Load())
	watcher.Stop()
	rt.events <- Event{Type: "application_oom", ApplicationOOM: &microvm.ApplicationOOM{Kills: 3}}
	close(rt.events)
	require.Never(t, func() bool { return len(reported) != 0 }, 20*time.Millisecond, time.Millisecond)
}
