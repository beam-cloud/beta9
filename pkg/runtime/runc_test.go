package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRestoreArgs(t *testing.T) {
	rt := &Runc{}
	args := rt.restoreArgs("container-1", &RestoreOpts{
		ImagePath:  "/checkpoints/container-1",
		WorkDir:    "/tmp/restore-work",
		BundlePath: "/tmp/bundle",
		TCPClose:   true,
	})

	require.Equal(t, []string{
		"restore",
		"--image-path", "/checkpoints/container-1",
		"--work-path", "/tmp/restore-work",
		"--link-remap",
		"--manage-cgroups-mode", "soft",
		"--tcp-close",
		"--bundle", "/tmp/bundle",
		"container-1",
	}, args)
}

func TestPollRestoredContainerPIDWaitsForState(t *testing.T) {
	attempts := 0
	pid, result, err := pollRestoredContainerPID(
		context.Background(),
		"container-1",
		make(chan runcCommandResult),
		time.Second,
		time.Millisecond,
		func(context.Context, string) (State, error) {
			attempts++
			if attempts < 3 {
				return State{}, errors.New("state unavailable")
			}
			return State{Pid: 4321, Status: types.RuncContainerStatusRunning}, nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, 4321, pid)
	require.Nil(t, result)
	require.Equal(t, 3, attempts)
}

func TestPollRestoredContainerPIDFailsWhenStateUnavailable(t *testing.T) {
	_, result, err := pollRestoredContainerPID(
		context.Background(),
		"container-1",
		make(chan runcCommandResult),
		10*time.Millisecond,
		time.Millisecond,
		func(context.Context, string) (State, error) {
			return State{}, ErrContainerNotFound{ContainerID: "container-1"}
		},
	)

	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "restore succeeded but restored container state was unavailable")
}

func TestRuncRestoreReturnsAfterStateRunning(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "runc.log")
	readyPath := filepath.Join(dir, "restore-ready")
	runcPath := filepath.Join(dir, "runc")
	require.NoError(t, os.WriteFile(runcPath, []byte(`#!/bin/sh
set -eu
cmd=""
for arg in "$@"; do
  case "$arg" in
    restore|state)
      cmd="$arg"
      break
      ;;
  esac
done
case "$cmd" in
  restore)
    echo restore-start >> "$RUNC_FAKE_LOG"
    touch "$RUNC_FAKE_READY"
    sleep 1
    echo restore-done >> "$RUNC_FAKE_LOG"
    ;;
  state)
    echo state >> "$RUNC_FAKE_LOG"
    if [ ! -f "$RUNC_FAKE_READY" ]; then
      exit 1
    fi
    printf '{"id":"container-1","pid":4321,"status":"running"}'
    ;;
  *)
    echo "unexpected args: $*" >&2
    exit 1
    ;;
esac
`), 0o755))
	t.Setenv("RUNC_FAKE_LOG", logPath)
	t.Setenv("RUNC_FAKE_READY", readyPath)

	rt, err := NewRunc(Config{RuncPath: runcPath})
	require.NoError(t, err)

	started := make(chan int, 1)
	result := make(chan error, 1)
	go func() {
		_, err := rt.Restore(context.Background(), "container-1", &RestoreOpts{
			ImagePath:  filepath.Join(dir, "checkpoint"),
			BundlePath: filepath.Join(dir, "bundle"),
			Started:    started,
		})
		result <- err
	}()

	select {
	case pid := <-started:
		require.Equal(t, 4321, pid)
	case <-time.After(time.Second):
		t.Fatal("restore did not signal started from restored runtime state")
	}

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("restore waited for the restored process to exit")
	}

	logData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(logData), "restore-start\n")
	require.Contains(t, string(logData), "state\n")
	require.NotContains(t, string(logData), "restore-done\n")
}
