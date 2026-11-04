package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func TestMicroVMBootAdmissionWeightsCancellationAndReady(t *testing.T) {
	a := &microVMBootAdmission{2, semaphore.NewWeighted(2)}
	ready := make(chan struct{})
	release, err := a.acquire(context.Background(), 8, ready)
	require.NoError(t, err, "a large guest must be admitted alone rather than deadlocking")
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = a.acquire(ctx, 1, nil)
	require.ErrorIs(t, err, context.Canceled)
	close(ready)
	deadline, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	other, err := a.acquire(deadline, 2, nil)
	require.NoError(t, err, "guest-control connection must release the boot budget")
	release() // Runtime teardown must not release another guest's admission.
	blocked, done := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer done()
	_, err = a.acquire(blocked, 1, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	other()
	last, err := a.acquire(deadline, 2, nil)
	require.NoError(t, err)
	last()
}

func TestMicroVMBootAdmissionConfiguration(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "257", "invalid"} {
		t.Setenv("BETA9_MICROVM_BOOT_CONCURRENCY", value)
		require.Nil(t, newMicroVMBootAdmission())
	}
	t.Setenv("BETA9_MICROVM_BOOT_CONCURRENCY", "32")
	require.EqualValues(t, 32, newMicroVMBootAdmission().capacity)
}
