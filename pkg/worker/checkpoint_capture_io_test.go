package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCheckpointCaptureIOCancellationAndFailureRelease(t *testing.T) {
	slots := newMicroVMSnapshotIOSlots("1")
	entered, finish, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	want := errors.New("capture failed")
	go func() {
		_, err := withCheckpointCaptureIO(context.Background(), types.ContainerRuntimeMicroVM.String(), slots, func(admit func(context.Context) error) (string, error) {
			if err := admit(context.Background()); err != nil {
				return "", err
			}
			// A duplicated paused callback must not acquire a second slot.
			if err := admit(context.Background()); err != nil {
				return "", err
			}
			close(entered)
			<-finish
			return "", want
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("capture did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	paused := false
	_, err := withCheckpointCaptureIO(ctx, types.ContainerRuntimeMicroVM.String(), slots, func(admit func(context.Context) error) (string, error) {
		paused = true
		if err := admit(ctx); err != nil {
			return "", err
		}
		t.Fatal("cancelled queued capture wrote memory")
		return "", nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, paused, "guest must pause before waiting for an IO slot")
	close(finish)
	require.ErrorIs(t, <-done, want)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	path, err := withCheckpointCaptureIO(ctx, types.ContainerRuntimeMicroVM.String(), slots, func(admit func(context.Context) error) (string, error) {
		return "captured", admit(ctx)
	})
	require.NoError(t, err)
	require.Equal(t, "captured", path)
	_, err = withCheckpointCaptureIO(ctx, types.ContainerRuntimeMicroVM.String(), slots, func(func(context.Context) error) (string, error) { return "", want })
	require.ErrorIs(t, err, want)
	require.Empty(t, slots, "failure before pause must not retain a slot")
	for _, value := range []string{"", "0", "129", "bad"} {
		require.Nil(t, newMicroVMSnapshotIOSlots(value))
	}
}
