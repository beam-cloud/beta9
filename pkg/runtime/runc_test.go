package runtime

import (
	"context"
	"errors"
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
