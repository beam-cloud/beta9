package main

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerInitReapsDetachedChildren(t *testing.T) {
	if os.Getenv("BEAM_TEST_WORKER_INIT") == "1" {
		require.NoError(t, ensureInit())
		require.NotEqual(t, 1, os.Getpid(), "worker must run under init")
		file := os.Getenv("BEAM_TEST_ORPHAN_PID")
		cmd := exec.Command("sh", "-c", `sleep .1 & echo $! > "$BEAM_TEST_ORPHAN_PID"`)
		require.NoError(t, cmd.Run())
		pid, err := os.ReadFile(file)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join("/proc", strings.TrimSpace(string(pid))))
			return os.IsNotExist(err)
		}, time.Second, 10*time.Millisecond, "init must reap the detached child")
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM)
		require.NoError(t, syscall.Kill(1, syscall.SIGTERM))
		select {
		case <-signals:
		case <-time.After(time.Second):
			t.Fatal("init did not forward SIGTERM")
		}
		os.Exit(42) // Init must return the worker's exit code.
	}
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux PID namespaces")
	}
	if _, err := os.Stat("/usr/bin/tini"); err != nil {
		t.Skip("requires the worker image's tini")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "unshare", "--fork", "--pid", "--mount-proc", os.Args[0], "-test.run=^TestWorkerInitReapsDetachedChildren$")
	cmd.Env = append(os.Environ(), "BEAM_TEST_WORKER_INIT=1", "BEAM_TEST_ORPHAN_PID="+filepath.Join(t.TempDir(), "pid"))
	output, err := cmd.CombinedOutput()
	if strings.Contains(string(output), "Operation not permitted") && os.Geteuid() != 0 {
		t.Skip("requires permission to create a PID namespace")
	}
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, "%s", output)
	require.Equal(t, 42, exit.ExitCode(), "%s", output)
}
