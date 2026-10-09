//go:build linux

package disk

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExitedDaemonDoesNotWaitForReaping(t *testing.T) {
	cmd := exec.Command("sleep", "0.05")
	require.NoError(t, cmd.Start())
	defer cmd.Wait()
	pid := cmd.Process.Pid
	comm, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/comm")
	require.NoError(t, err)
	require.True(t, processAlive(pid, strings.TrimSpace(string(comm))))
	require.NoError(t, waitFor(context.Background(), time.Second, func() bool {
		return !processAlive(pid, strings.TrimSpace(string(comm)))
	}))
	// Wait has deliberately not reaped the child yet.
	_, err = os.Stat("/proc/" + strconv.Itoa(pid))
	require.NoError(t, err)
}
