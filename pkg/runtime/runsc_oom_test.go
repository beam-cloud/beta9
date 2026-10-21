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
	events := runscEvents(t, t.Context(), `cat <<'EOF'
{"type":"stats","id":"container","data":null}
{"type":"stats","id":"container","data":{"memory":{"usage":{"usage":9999999999}}}}
{"type":"stats","id":"container","data":{"memory":{"raw":{"guest_oom_kill":0}}}}
{"type":"stats","id":"other","data":{"memory":{"raw":{"guest_oom_kill":99}}}}
{"type":"stats","id":"container","data":{"memory":{"raw":{"guest_oom_kill":1,"guest_memory_limit":67108864},"usage":{"usage":60000000}}}}
{"type":"stats","id":"container","data":{"memory":{"raw":{"guest_oom_kill":1}}}}
{"type":"stats","id":"container","data":{"memory":{"raw":{"guest_oom_kill":2}}}}
EOF
`)
	var got []Event
	for event := range events {
		got = append(got, event)
	}
	require.Equal(t, []Event{
		{Type: "application_oom", ApplicationOOM: &microvm.ApplicationOOM{Kills: 1, MemoryLimit: 64 << 20, MemoryUsage: 60000000}},
		{Type: "application_oom", ApplicationOOM: &microvm.ApplicationOOM{Kills: 2}},
	}, got)
}

func runscEvents(t *testing.T, ctx context.Context, script string) <-chan Event {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runsc")
	require.NoError(t, os.WriteFile(path, []byte(`#!/bin/sh
case " $* " in
  *" state "*) echo '{"id":"container","pid":1,"status":"running"}'; exit 0 ;;
esac
`+script), 0o755))
	rt := &Runsc{cfg: Config{RunscPath: path}}
	events, err := rt.Events(ctx, "container")
	require.NoError(t, err)
	return events
}

func TestRunscGuestOOMEventsStopOnCancellationAndInvalidJSON(t *testing.T) {
	for _, test := range []struct{ name, script, error string }{
		{"cancel", "exec sleep 60", ""},
		{"invalid JSON", "echo invalid-json; exec sleep 60", "decode gVisor OOM"},
		{"failed command", "echo event-stream-failed >&2; exit 1", "event-stream-failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			// exec keeps cancellation from leaving a shell child behind.
			events := runscEvents(t, ctx, test.script)
			if test.error != "" {
				require.ErrorContains(t, receive(t, events).Err, test.error)
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

func TestRunscGuestOOMEventsWaitForSandboxStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runsc")
	require.NoError(t, os.WriteFile(path, []byte(`#!/bin/sh
case " $* " in
  *" state "*)
    if [ ! -f "$0.ready" ]; then
      touch "$0.ready"
      echo 'loading container: file does not exist' >&2
      exit 128
    fi
    echo '{"id":"container","pid":1,"status":"running"}' ;;
  *" events "*)
    test -f "$0.ready" || exit 128
    echo '{"type":"stats","id":"container","data":{"memory":{"raw":{"guest_oom_kill":1}}}}' ;;
esac
`), 0o755))
	rt := &Runsc{cfg: Config{RunscPath: path}}
	events, err := rt.Events(t.Context(), "container")
	require.NoError(t, err)
	require.Equal(t, &microvm.ApplicationOOM{Kills: 1}, receive(t, events).ApplicationOOM)
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
		require.Equal(t, count, receive(t, reported))
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
