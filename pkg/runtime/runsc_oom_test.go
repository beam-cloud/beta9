package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

func TestRunscFinalExitRequiresNewKernelOOMKill(t *testing.T) {
	for _, test := range []struct {
		name  string
		count int
		oom   bool
	}{
		{"plain SIGKILL", 4, false}, {"host OOM", 5, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "runsc")
			require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf(`#!/bin/sh
printf 'oom_kill %d\n' > '%s/memory.events'
echo '{"exitStatus":137}'
`, test.count, root)), 0o755))
			rt := &Runsc{cfg: Config{RunscPath: path}}
			code, err := rt.waitForExit(t.Context(), "container", false, &CgroupOOMWatcher{cgroupRoot: root}, 4)
			require.Equal(t, 137, code)
			if test.oom {
				require.ErrorIs(t, err, ErrOOMKilled)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRunscFinalMetricsSurviveLostWaitRPC(t *testing.T) {
	for _, test := range []struct {
		name                      string
		status, guest, init, want int
		err                       error
	}{
		{"success", 0, 0, 0, 0, nil},
		{"boot failure", 42 << 8, 0, 0, 42, nil},
		{"exit 255", 255 << 8, 0, 0, 255, nil},
		{"SIGKILL", 9, 0, 0, 137, nil},
		{"init OOM", 9, 1, 1, 137, ErrOOMKilled},
		{"child OOM then SIGKILL", 9, 1, 0, 137, ErrGuestOOM},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "runsc")
			require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\necho 'sandbox no longer running' >&2; exit 128\n"), 0o755))
			rt := &Runsc{cfg: Config{RunscPath: path, RunscRoot: root}}
			metrics := fmt.Sprintf("runsc_task_init_exit_status %d 123\nrunsc_memory_guest_oom_kills %d 123\nrunsc_memory_guest_init_oom_kills %d 123\n", test.status, test.guest, test.init)
			require.NoError(t, os.WriteFile(rt.finalMetricsPath("container"), []byte(metrics), 0o600))
			code, err := rt.Wait(t.Context(), "container")
			require.ErrorIs(t, err, test.err)
			require.Equal(t, test.want, code)
			require.NoFileExists(t, rt.finalMetricsPath("container"))
		})
	}
}

func TestRunscDetachedRunDrainsOutputAndReturnsContainerExit(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "runsc")
	require.NoError(t, os.WriteFile(path, []byte(`#!/bin/sh
case " $* " in
  *" run "*)
    case " $* " in *" --detach "*) ;; *) exit 1 ;; esac
    (sleep .1; echo application-output; echo application-error >&2) & ;;
  *" state "*) echo '{"id":"container","pid":4321,"status":"running"}' ;;
  *" wait "*) echo '{"exitStatus":137}' ;;
  *" delete "*) touch "$0.deleted" ;;
  *) exit 1 ;;
esac
`), 0o755))
	rt := &Runsc{cfg: Config{RunscPath: path}}
	var stdout, stderr bytes.Buffer
	started := make(chan int, 1)
	code, err := rt.Run(t.Context(), "container", writeRunscBundle(t, root, false), &RunOpts{OutputWriter: &stdout, ErrorWriter: &stderr, Started: started})
	require.NoError(t, err)
	require.Equal(t, 137, code)
	require.Equal(t, "application-output\n", stdout.String())
	require.Equal(t, "application-error\n", stderr.String())
	require.FileExists(t, path+".deleted")
	require.Equal(t, 4321, receive(t, started))
}

func TestRunscDetachedRunCleansUpAfterCancellation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "runsc")
	require.NoError(t, os.WriteFile(path, []byte(`#!/bin/sh
case " $* " in
  *" run "*)
    sleep 60 &
    echo $! > "$0.child" ;;
  *" state "*) echo '{"pid":4321,"status":"running"}' ;;
  *" wait "*) exec sleep 60 ;;
  *" delete "*) kill "$(cat "$0.child")"; touch "$0.deleted" ;;
esac
`), 0o755))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan int, 1)
	done := make(chan error, 1)
	rt := &Runsc{cfg: Config{RunscPath: path}}
	bundle := writeRunscBundle(t, root, false)
	go func() {
		_, err := rt.Run(ctx, "container", bundle, &RunOpts{OutputWriter: &bytes.Buffer{}, Started: started})
		done <- err
	}()
	require.Equal(t, 4321, receive(t, started))
	cancel()
	require.Error(t, receive(t, done))
	require.FileExists(t, path+".deleted")
}

type gatedRestoreOutput struct {
	buffer bytes.Buffer
	gate   <-chan struct{}
}

func (w *gatedRestoreOutput) Write(p []byte) (int, error) {
	<-w.gate
	return w.buffer.Write(p)
}

func TestRunscWaitDrainsRestoredOutputBeforeReturning(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "runsc")
	require.NoError(t, os.WriteFile(path, []byte(`#!/bin/sh
case " $* " in
 *" restore "*) echo first-line; sleep .1; echo final-line ;;
 *" state "*) echo '{"pid":4321,"status":"running"}' ;;
 *" wait --restore "*) exit 0 ;;
 *" wait "*) sleep .2; echo '{"exitStatus":42}' ;;
 *" delete "*) exit 0 ;;
esac
`), 0o755))
	gate := make(chan struct{})
	output := &gatedRestoreOutput{gate: gate}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rt := &Runsc{cfg: Config{RunscPath: path, RunscRoot: root}}
	_, err := rt.Restore(ctx, "container", &RestoreOpts{BundlePath: writeRunscBundle(t, root, false), OutputWriter: output})
	require.NoError(t, err)
	done := make(chan int, 1)
	go func() { code, _ := rt.Wait(ctx, "container"); done <- code }()
	select {
	case <-done:
		close(gate)
		t.Fatal("restore completed before stdout drained")
	case <-time.After(350 * time.Millisecond):
	}
	close(gate)
	require.Equal(t, 42, receive(t, done))
	require.Equal(t, "first-line\nfinal-line\n", output.buffer.String())
}
