package runtime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type synchronizedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

type gatedWriter struct {
	once    sync.Once
	started chan struct{}
	release <-chan struct{}
	buffer  synchronizedBuffer
}

func (w *gatedWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return w.buffer.Write(p)
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestRestoreArgs(t *testing.T) {
	tests := []struct {
		name         string
		allowOpenTCP bool
		tcpClose     bool
		wantTCPFlag  string
	}{
		{name: "close TCP", tcpClose: true, wantTCPFlag: "--tcp-close"},
		{name: "open TCP", allowOpenTCP: true, tcpClose: true, wantTCPFlag: "--tcp-established"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := &Runc{}
			args := rt.restoreArgs("container-1", &RestoreOpts{
				ImagePath:    "/checkpoints/container-1",
				WorkDir:      "/tmp/restore-work",
				BundlePath:   "/tmp/bundle",
				AllowOpenTCP: tt.allowOpenTCP,
				TCPClose:     tt.tcpClose,
			})

			require.Equal(t, []string{
				"restore",
				"--detach",
				"--image-path", "/checkpoints/container-1",
				"--work-path", "/tmp/restore-work",
				"--link-remap",
				"--manage-cgroups-mode", "soft",
				tt.wantTCPFlag,
				"--bundle", "/tmp/bundle",
				"container-1",
			}, args)
		})
	}
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

func TestRuncRestoreSignalsStartedAfterRestoreCompletes(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "runc.log")
	readyPath := filepath.Join(dir, "restore-ready")
	releasePath := filepath.Join(dir, "restore-release")
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
		echo restore-output-start
    touch "$RUNC_FAKE_READY"
		while [ ! -f "$RUNC_FAKE_RELEASE" ]; do sleep 0.01; done
		(sleep 0.25; echo container-output) &
    echo restore-done >> "$RUNC_FAKE_LOG"
		echo restore-output-done
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
	t.Setenv("RUNC_FAKE_RELEASE", releasePath)

	rt, err := NewRunc(Config{RuncPath: runcPath})
	require.NoError(t, err)

	started := make(chan int, 1)
	result := make(chan error, 1)
	output := &synchronizedBuffer{}
	go func() {
		_, err := rt.Restore(context.Background(), "container-1", &RestoreOpts{
			ImagePath:    filepath.Join(dir, "checkpoint"),
			BundlePath:   filepath.Join(dir, "bundle"),
			Started:      started,
			OutputWriter: output,
		})
		result <- err
	}()

	require.Eventually(t, func() bool {
		data, err := os.ReadFile(logPath)
		return err == nil && bytes.Contains(data, []byte("restore-start\n"))
	}, 5*time.Second, 10*time.Millisecond)
	select {
	case pid := <-started:
		t.Fatalf("restore signaled PID %d before runc restore completed", pid)
	default:
	}
	require.NoError(t, os.WriteFile(releasePath, nil, 0o644))

	select {
	case pid := <-started:
		require.Equal(t, 4321, pid)
	case <-time.After(3 * time.Second):
		select {
		case err := <-result:
			t.Fatalf("restore returned before signaling started: %v", err)
		default:
			t.Fatal("restore did not signal started from restored runtime state")
		}
	}

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("restore did not return after detached restore completed")
	}
	require.Contains(t, output.String(), "restore-output-done\n")
	require.NotContains(t, output.String(), "container-output\n")
	require.Eventually(t, func() bool {
		return bytes.Contains([]byte(output.String()), []byte("container-output\n"))
	}, time.Second, 10*time.Millisecond, "restored container output did not remain connected")

	logData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(logData), "restore-start\n")
	require.Contains(t, string(logData), "restore-done\n")
	require.Contains(t, string(logData), "state\n")
}

func TestRuncRestoreStopsWhenContextIsCanceled(t *testing.T) {
	dir := t.TempDir()
	runcPath := filepath.Join(dir, "runc")
	require.NoError(t, os.WriteFile(runcPath, []byte(`#!/bin/sh
set -eu
sleep 10
`), 0o755))

	rt, err := NewRunc(Config{RuncPath: runcPath})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = rt.Restore(ctx, "container-1", &RestoreOpts{
		ImagePath:  filepath.Join(dir, "checkpoint"),
		BundlePath: filepath.Join(dir, "bundle"),
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestRuncRestoreDrainsFailedCommandOutput(t *testing.T) {
	dir := t.TempDir()
	runcPath := filepath.Join(dir, "runc")
	require.NoError(t, os.WriteFile(runcPath, []byte(`#!/bin/sh
set -eu
echo 'criu failed: type RESTORE' >&2
exit 1
`), 0o755))

	rt, err := NewRunc(Config{RuncPath: runcPath})
	require.NoError(t, err)

	release := make(chan struct{})
	output := &gatedWriter{started: make(chan struct{}), release: release}
	result := make(chan error, 1)
	go func() {
		_, err := rt.Restore(context.Background(), "container-1", &RestoreOpts{
			ImagePath:    filepath.Join(dir, "checkpoint"),
			BundlePath:   filepath.Join(dir, "bundle"),
			OutputWriter: output,
		})
		result <- err
	}()

	select {
	case <-output.started:
	case <-time.After(time.Second):
		t.Fatal("restore output was not forwarded")
	}
	select {
	case err := <-result:
		t.Fatalf("restore returned before failed command output drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("restore did not return after failed command output drained")
	}
	require.Contains(t, output.buffer.String(), "criu failed: type RESTORE")
}
