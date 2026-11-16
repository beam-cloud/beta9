package repository

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
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

	// Polling on their own, they made 200 x 2 round trips every 25 ms.
	const intervals = 4
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

// Redis announces an address as it is set, and the caller is told then, not
// at the next read of everything being waited for.
func TestCallerWaitingForAWorkerAddressIsToldWhenItIsSet(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewContainerRedisRepositoryForTest(rdb).(*ContainerRedisRepository)
	trips := &redisRoundTrips{}
	rdb.AddHook(trips)

	told := make(chan string, 1)
	go func() {
		address, _ := repo.GetWorkerAddress(context.Background(), "sandbox-stub-placed")
		told <- address
	}()
	require.Eventually(t, func() bool { return repo.workerAddresses.waiting() == 1 }, 5*time.Second, time.Millisecond)

	// Right after a read of everything, the next is an interval away.
	trips.enabled.Store(true)
	read := trips.calls.Load()
	require.Eventually(t, func() bool { return trips.calls.Load() > read }, 5*time.Second, time.Millisecond)

	set := time.Now()
	require.NoError(t, repo.SetWorkerAddress("sandbox-stub-placed", "10.0.0.1:20000"))
	// The test Redis does not announce keys; this is what a real one sends.
	key := common.RedisKeys.SchedulerWorkerAddress("sandbox-stub-placed")
	require.NoError(t, rdb.Publish(context.Background(), "__keyspace@0__:"+key, common.KeyOperationSet).Err())

	select {
	case address := <-told:
		require.Equal(t, "10.0.0.1:20000", address)
		require.Less(t, time.Since(set), workerAddressPollInterval/2)
	case <-time.After(5 * time.Second):
		t.Fatal("the caller was not given the worker address")
	}
}

func TestReadingWorkerAddressesTellsPlacedFromFailedFromWaiting(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewContainerRedisRepositoryForTest(rdb)
	require.NoError(t, repo.SetWorkerAddress("sandbox-stub-placed", "10.0.0.1:20000"))
	require.NoError(t, repo.SetContainerRequestStatus("sandbox-stub-unplaceable", types.ContainerRequestStatusFailed))

	results := readWorkerAddresses(context.Background(), rdb, []string{"sandbox-stub-placed", "sandbox-stub-unplaceable", "sandbox-stub-waiting"})

	require.Len(t, results, 2)
	require.Equal(t, workerAddressResult{address: "10.0.0.1:20000"}, results["sandbox-stub-placed"])
	require.ErrorContains(t, results["sandbox-stub-unplaceable"].err, "failed to schedule")
}

// A caller must hear that scheduling failed about as soon as it did before.
func TestCallerWaitingForAWorkerAddressLearnsSoonThatSchedulingFailed(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewContainerRedisRepositoryForTest(rdb).(*ContainerRedisRepository)

	failed := make(chan time.Time, 1)
	go func() {
		_, _ = repo.GetWorkerAddress(context.Background(), "sandbox-stub-unplaceable")
		failed <- time.Now()
	}()
	require.Eventually(t, func() bool { return repo.workerAddresses.waiting() == 1 }, 5*time.Second, time.Millisecond)

	require.NoError(t, repo.SetContainerRequestStatus("sandbox-stub-unplaceable", types.ContainerRequestStatusFailed))
	set := time.Now()
	select {
	case at := <-failed:
		require.Less(t, at.Sub(set), time.Second)
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
