package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunscRestoreSignalsStartedAfterStateRunning(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "runsc.log")
	readyPath := filepath.Join(dir, "restore-ready")
	runscPath := filepath.Join(dir, "runsc")
	require.NoError(t, os.WriteFile(runscPath, []byte(`#!/bin/sh
set -eu
cmd=""
for arg in "$@"; do
  case "$arg" in
    flags|restore|state|delete)
      cmd="$arg"
      break
      ;;
  esac
done
case "$cmd" in
  flags)
    echo "-TESTONLY-allow-packet-endpoint-write"
    ;;
  restore)
    echo restore-start >> "$RUNSC_FAKE_LOG"
    sleep 0.2
    touch "$RUNSC_FAKE_READY"
    echo restore-ready >> "$RUNSC_FAKE_LOG"
    sleep 5
    echo restore-done >> "$RUNSC_FAKE_LOG"
    ;;
  state)
    echo state >> "$RUNSC_FAKE_LOG"
    if [ ! -f "$RUNSC_FAKE_READY" ]; then
      exit 1
    fi
    printf '{"id":"container-1","pid":4321,"status":"running"}'
    ;;
  delete)
    echo delete >> "$RUNSC_FAKE_LOG"
    ;;
  *)
    echo "unexpected args: $*" >&2
    exit 1
    ;;
esac
`), 0o755))
	t.Setenv("RUNSC_FAKE_LOG", logPath)
	t.Setenv("RUNSC_FAKE_READY", readyPath)

	rt, err := NewRunsc(Config{RunscPath: runscPath, RunscRoot: filepath.Join(dir, "root")})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan int, 1)
	result := make(chan error, 1)
	go func() {
		_, err := rt.Restore(ctx, "container-1", &RestoreOpts{
			ImagePath:  filepath.Join(dir, "checkpoint"),
			BundlePath: filepath.Join(dir, "bundle"),
			Started:    started,
		})
		result <- err
	}()

	select {
	case pid := <-started:
		t.Fatalf("restore signaled started before restored runtime state was available, pid=%d", pid)
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case pid := <-started:
		require.Equal(t, 4321, pid)
	case <-time.After(time.Second):
		t.Fatal("restore did not signal started from restored runtime state")
	}

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("restore did not return after restored runtime state was available")
	}

	logData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(logData), "restore-start\n")
	require.Contains(t, string(logData), "restore-ready\n")
	require.NotContains(t, string(logData), "restore-done\n")
}

// newFakeRunsc writes a fake runsc script whose `exec` subcommand runs the given
// shell body, and returns a Runsc wired to it.
func newFakeRunsc(t *testing.T, execBody string) *Runsc {
	t.Helper()

	dir := t.TempDir()
	runscPath := filepath.Join(dir, "runsc")
	require.NoError(t, os.WriteFile(runscPath, []byte(`#!/bin/sh
set -u
for arg in "$@"; do
  if [ "$arg" = "exec" ]; then
`+execBody+`
    exit 0
  fi
done
echo "unexpected args: $*" >&2
exit 1
`), 0o755))

	return &Runsc{cfg: Config{RunscPath: runscPath, RunscRoot: filepath.Join(dir, "root")}}
}

func TestFindCUDAProcessesReturnsPIDs(t *testing.T) {
	rt := newFakeRunsc(t, `    printf '12\n345\n'
	    exit 0`)

	pids, err := rt.findCUDAProcesses(context.Background(), "container-1")
	require.NoError(t, err)
	require.Equal(t, []int{12, 345}, pids)
}

func TestFindCUDAProcessesExecBodyMayFallThrough(t *testing.T) {
	rt := newFakeRunsc(t, `    printf '7\n'`)

	pids, err := rt.findCUDAProcesses(context.Background(), "container-1")
	require.NoError(t, err)
	require.Equal(t, []int{7}, pids)
}

func TestFindCUDAProcessesEmptyOutputIsNotAnError(t *testing.T) {
	rt := newFakeRunsc(t, `    exit 0`)

	pids, err := rt.findCUDAProcesses(context.Background(), "container-1")
	require.NoError(t, err)
	require.Empty(t, pids)
}

func TestFindCUDAProcessesNoMatchExitOneIsNotAnError(t *testing.T) {
	rt := newFakeRunsc(t, `    exit 1`)

	pids, err := rt.findCUDAProcesses(context.Background(), "container-1")
	require.NoError(t, err)
	require.Empty(t, pids)
}

func TestFindCUDAProcessesExitOneWithPIDsReturnsPIDs(t *testing.T) {
	rt := newFakeRunsc(t, `    printf '12\n'
	    exit 1`)

	pids, err := rt.findCUDAProcesses(context.Background(), "container-1")
	require.NoError(t, err)
	require.Equal(t, []int{12}, pids)
}

func TestFindCUDAProcessesExecFailureReturnsError(t *testing.T) {
	rt := newFakeRunsc(t, `    echo boom >&2
	    exit 1`)

	pids, err := rt.findCUDAProcesses(context.Background(), "container-1")
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to enumerate CUDA processes")
	require.ErrorContains(t, err, "boom")
	require.Nil(t, pids)
}
