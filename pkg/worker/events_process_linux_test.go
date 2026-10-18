//go:build linux

package worker

import (
	"os"
	"os/exec"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessMonitorFindsChildFromAnotherThread(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	cmd := exec.Command("sleep", "60")
	started := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		started <- cmd.Start()
	}()
	require.NoError(t, <-started)
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	monitor := NewProcessMonitor(os.Getpid(), nil, nil)
	processes, err := monitor.findProcesses()
	require.NoError(t, err)
	for _, p := range processes {
		if p.Pid == int32(cmd.Process.Pid) {
			return
		}
	}
	t.Fatal("process tree omitted the child")
}

func BenchmarkFindProcesses(b *testing.B) {
	monitor := NewProcessMonitor(os.Getpid(), nil, nil)
	for b.Loop() {
		if _, err := monitor.findProcesses(); err != nil {
			b.Fatal(err)
		}
	}
}
