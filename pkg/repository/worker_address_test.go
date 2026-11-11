package repository

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func workerAddressLoopRunning(repo *ContainerRedisRepository) bool {
	repo.workerAddresses.mu.Lock()
	defer repo.workerAddresses.mu.Unlock()
	return repo.workerAddresses.running
}

func TestCallersWaitingForWorkerAddressesShareTheirReads(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewContainerRedisRepositoryForTest(rdb).(*ContainerRedisRepository)
	trips := &redisRoundTrips{}
	rdb.AddHook(trips)

	// A burst: 200 sandboxes created, none placed yet, a caller waiting on each.
	const callers = 200
	type outcome struct {
		containerID string
		address     string
		err         error
	}
	outcomes := make(chan outcome, callers)
	for n := range callers {
		containerID := fmt.Sprintf("sandbox-stub-%03d", n)
		go func() {
			address, err := repo.GetWorkerAddress(context.Background(), containerID)
			outcomes <- outcome{containerID, address, err}
		}()
	}
	require.Eventually(t, func() bool { return repo.workerAddresses.waiting() == callers }, 5*time.Second, time.Millisecond)

	// Polling on their own, they made 200 x 2 round trips every interval.
	const intervals = 10
	trips.enabled.Store(true)
	time.Sleep(intervals * workerAddressPollInterval)
	trips.enabled.Store(false)
	got := trips.calls.Load()
	require.Positive(t, got, "nothing read the addresses being waited for")
	require.LessOrEqual(t, got, int64(intervals+2), "round trips for 200 waiting callers over %d intervals", intervals)

	for n := range callers {
		require.NoError(t, repo.SetWorkerAddress(fmt.Sprintf("sandbox-stub-%03d", n), fmt.Sprintf("10.0.0.1:%d", 20000+n)))
	}
	for range callers {
		select {
		case outcome := <-outcomes:
			require.NoError(t, outcome.err)
			port := strings.TrimPrefix(outcome.containerID, "sandbox-stub-")
			require.Equal(t, "10.0.0.1:20"+port, outcome.address, outcome.containerID)
		case <-time.After(5 * time.Second):
			t.Fatal("a caller was not given its container's worker address")
		}
	}

	// With nobody waiting, nothing is read.
	require.Eventually(t, func() bool { return !workerAddressLoopRunning(repo) }, 5*time.Second, time.Millisecond)
	require.Zero(t, repo.workerAddresses.waiting())
}

func TestCallerWaitingForAWorkerAddressLearnsThatSchedulingFailed(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewContainerRedisRepositoryForTest(rdb).(*ContainerRedisRepository)

	failed := make(chan error, 1)
	go func() {
		_, err := repo.GetWorkerAddress(context.Background(), "sandbox-stub-unplaceable")
		failed <- err
	}()
	require.Eventually(t, func() bool { return repo.workerAddresses.waiting() == 1 }, 5*time.Second, time.Millisecond)

	require.NoError(t, repo.SetContainerRequestStatus("sandbox-stub-unplaceable", types.ContainerRequestStatusFailed))
	select {
	case err := <-failed:
		require.ErrorContains(t, err, "failed to schedule")
	case <-time.After(5 * time.Second):
		t.Fatal("the caller kept waiting for a container the scheduler gave up on")
	}
}

func TestCallerThatStopsWaitingForAWorkerAddressIsForgotten(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewContainerRedisRepositoryForTest(rdb).(*ContainerRedisRepository)

	// Two callers wait for the same container; one has a short deadline.
	patient := make(chan string, 1)
	go func() {
		address, _ := repo.GetWorkerAddress(context.Background(), "sandbox-stub-shared")
		patient <- address
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*workerAddressPollInterval)
	defer cancel()
	_, err = repo.GetWorkerAddress(ctx, "sandbox-stub-shared")
	require.ErrorContains(t, err, "context cancelled")

	// The other is still served.
	require.Eventually(t, func() bool { return repo.workerAddresses.waiting() == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, repo.SetWorkerAddress("sandbox-stub-shared", "10.0.0.1:20000"))
	select {
	case address := <-patient:
		require.Equal(t, "10.0.0.1:20000", address)
	case <-time.After(5 * time.Second):
		t.Fatal("the remaining caller was not given the worker address")
	}
	require.Eventually(t, func() bool { return !workerAddressLoopRunning(repo) }, 5*time.Second, time.Millisecond)
}

func TestWorkerAddressReadIntervalGrowsWithALargeBurst(t *testing.T) {
	for waiting, want := range map[int]time.Duration{
		0:                           workerAddressPollInterval,
		1:                           workerAddressPollInterval,
		workerAddressesPerPoll:      workerAddressPollInterval,
		workerAddressesPerPoll + 1:  2 * workerAddressPollInterval,
		10 * workerAddressesPerPoll: 10 * workerAddressPollInterval,
	} {
		require.Equal(t, want, workerAddressReadInterval(waiting), "interval with %d containers waited for", waiting)
	}
}
