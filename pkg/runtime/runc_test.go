package runtime

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

const runcTestOperationTimeout = 5 * time.Second

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

func TestRuncRunSignalsContainerInitPID(t *testing.T) {
	dir := t.TempDir()
	bundlePath := filepath.Join(dir, "bundle")
	require.NoError(t, os.Mkdir(bundlePath, 0o755))

	bundlePIDFile := filepath.Join(bundlePath, runcInitPIDFileName)
	require.NoError(t, os.WriteFile(bundlePIDFile, []byte("image-content"), 0o644))

	launchedPath := filepath.Join(dir, "launched")
	pidPathFile := filepath.Join(dir, "pid-path")
	pidRemovedPath := filepath.Join(dir, "pid-removed")
	writePIDPath := filepath.Join(dir, "write-pid")
	exitPath := filepath.Join(dir, "exit")
	runcPath := filepath.Join(dir, "runc")
	require.NoError(t, os.WriteFile(runcPath, []byte(`#!/bin/sh
set -eu
pid_file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --pid-file)
      pid_file="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
test -n "$pid_file"
printf '%s' "$pid_file" > "$RUNC_FAKE_PID_PATH"
touch "$RUNC_FAKE_LAUNCHED"
while [ ! -f "$RUNC_FAKE_WRITE_PID" ]; do sleep 0.01; done
printf '4321' > "$pid_file"
while [ -e "$pid_file" ]; do sleep 0.01; done
touch "$RUNC_FAKE_PID_REMOVED"
while [ ! -f "$RUNC_FAKE_EXIT" ]; do sleep 0.01; done
`), 0o755))
	t.Setenv("RUNC_FAKE_LAUNCHED", launchedPath)
	t.Setenv("RUNC_FAKE_PID_PATH", pidPathFile)
	t.Setenv("RUNC_FAKE_PID_REMOVED", pidRemovedPath)
	t.Setenv("RUNC_FAKE_WRITE_PID", writePIDPath)
	t.Setenv("RUNC_FAKE_EXIT", exitPath)

	rt, err := NewRunc(Config{RuncPath: runcPath})
	require.NoError(t, err)

	started := make(chan int, 1)
	result := make(chan error, 1)
	go func() {
		_, err := rt.Run(context.Background(), "container-1", bundlePath, &RunOpts{
			Started: started,
		})
		result <- err
	}()

	require.Eventually(t, func() bool {
		_, err := os.Stat(launchedPath)
		return err == nil
	}, runcTestOperationTimeout, 10*time.Millisecond)
	pidPathBytes, err := os.ReadFile(pidPathFile)
	require.NoError(t, err)
	pidFile := string(pidPathBytes)
	require.False(t, strings.HasPrefix(pidFile, bundlePath+string(os.PathSeparator)))
	require.DirExists(t, filepath.Dir(pidFile))
	require.FileExists(t, bundlePIDFile)
	select {
	case pid := <-started:
		t.Fatalf("run signaled PID %d before runc created the init pid file", pid)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, os.WriteFile(writePIDPath, nil, 0o644))
	select {
	case pid := <-started:
		require.Equal(t, 4321, pid)
	case <-time.After(runcTestOperationTimeout):
		t.Fatal("run did not signal the container init PID")
	}
	require.Eventually(t, func() bool {
		_, err := os.Stat(pidRemovedPath)
		return err == nil
	}, runcTestOperationTimeout, 10*time.Millisecond)
	require.NoFileExists(t, pidFile)
	require.NoDirExists(t, filepath.Dir(pidFile))

	require.NoError(t, os.WriteFile(exitPath, nil, 0o644))
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(runcTestOperationTimeout):
		t.Fatal("run did not return after runc exited")
	}
	require.NoDirExists(t, filepath.Dir(pidFile))
	require.FileExists(t, bundlePIDFile)
	require.Equal(t, "image-content", string(requireFileContents(t, bundlePIDFile)))
}

func TestRuncRunFailureDoesNotSignalStarted(t *testing.T) {
	dir := t.TempDir()
	bundlePath := filepath.Join(dir, "bundle")
	require.NoError(t, os.Mkdir(bundlePath, 0o755))

	pidPathFile := filepath.Join(dir, "pid-path")
	runcPath := filepath.Join(dir, "runc")
	require.NoError(t, os.WriteFile(runcPath, []byte(`#!/bin/sh
set -eu
pid_file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --pid-file)
      pid_file="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
test -n "$pid_file"
printf '%s' "$pid_file" > "$RUNC_FAKE_PID_PATH"
exit 17
`), 0o755))
	t.Setenv("RUNC_FAKE_PID_PATH", pidPathFile)

	rt, err := NewRunc(Config{RuncPath: runcPath})
	require.NoError(t, err)

	started := make(chan int, 1)
	_, err = rt.Run(context.Background(), "container-1", bundlePath, &RunOpts{
		Started: started,
	})
	require.Error(t, err)
	select {
	case pid := <-started:
		t.Fatalf("failed run signaled PID %d", pid)
	default:
	}
	pidPathBytes, readErr := os.ReadFile(pidPathFile)
	require.NoError(t, readErr)
	pidFile := string(pidPathBytes)
	require.False(t, strings.HasPrefix(pidFile, bundlePath+string(os.PathSeparator)))
	require.NoDirExists(t, filepath.Dir(pidFile))
}

func TestRuncRunSignalsInitPIDWhenCommandExitsImmediately(t *testing.T) {
	dir := t.TempDir()
	bundlePath := filepath.Join(dir, "bundle")
	require.NoError(t, os.Mkdir(bundlePath, 0o755))

	runcPath := filepath.Join(dir, "runc")
	require.NoError(t, os.WriteFile(runcPath, []byte(`#!/bin/sh
set -eu
pid_file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --pid-file)
      pid_file="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
test -n "$pid_file"
printf '4321' > "$pid_file"
`), 0o755))

	rt, err := NewRunc(Config{RuncPath: runcPath})
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		started := make(chan int, 1)
		_, err := rt.Run(context.Background(), "container-1", bundlePath, &RunOpts{Started: started})
		require.NoError(t, err)
		select {
		case pid := <-started:
			require.Equal(t, 4321, pid)
		default:
			t.Fatal("run returned without signaling the container init PID")
		}
	}
}

func TestRuncRunReturnsWhenStartedIsUnconsumed(t *testing.T) {
	dir := t.TempDir()
	bundlePath := filepath.Join(dir, "bundle")
	require.NoError(t, os.Mkdir(bundlePath, 0o755))

	runcPath := filepath.Join(dir, "runc")
	require.NoError(t, os.WriteFile(runcPath, []byte(`#!/bin/sh
set -eu
pid_file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --pid-file)
      pid_file="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
test -n "$pid_file"
printf '4321' > "$pid_file"
`), 0o755))

	rt, err := NewRunc(Config{RuncPath: runcPath})
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		_, err := rt.Run(context.Background(), "container-1", bundlePath, &RunOpts{
			Started: make(chan int),
		})
		result <- err
	}()

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(runcTestOperationTimeout):
		t.Fatal("run blocked on an unconsumed Started channel")
	}
}

func TestWatchRuncInitPIDDeliversWhenCommandAndStopAreAlreadyReady(t *testing.T) {
	for i := 0; i < 100; i++ {
		pidFile := filepath.Join(t.TempDir(), runcInitPIDFileName)
		require.NoError(t, os.WriteFile(pidFile, []byte("4321"), 0o644))
		commandStarted := make(chan int, 1)
		commandStarted <- 9999
		stop := make(chan struct{})
		close(stop)
		done := make(chan struct{})
		started := make(chan int, 1)

		watchRuncInitPID(context.Background(), pidFile, commandStarted, stop, done, started)

		select {
		case pid := <-started:
			require.Equal(t, 4321, pid)
		default:
			t.Fatal("watcher returned without delivering the init PID")
		}
		require.NoFileExists(t, pidFile)
		select {
		case <-done:
		default:
			t.Fatal("watcher did not close its done channel")
		}
	}
}

func requireFileContents(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
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
