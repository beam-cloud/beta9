package repository

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Admitting a container and updating its status no longer take the
// container's lock. These are the things the lock was there for.

func holdContainerLock(t *testing.T, rdb *common.RedisClient, containerID string) *redislock.Lock {
	t.Helper()
	lock, err := redislock.Obtain(context.Background(), rdb, common.RedisKeys.SchedulerContainerLock(containerID), time.Minute, nil)
	require.NoError(t, err)
	return lock
}

func TestStatusUpdateReturnsTheStateItLeft(t *testing.T) {
	repo, rdb, counts := countedContainerRepo(t)
	ctx := context.Background()
	create := func(containerID string) string {
		request := testContainerRequest(containerID, "workspace", 100)
		request.MachineId, request.Gpu, request.GpuCount, request.Evictable, request.DrainSeconds = "machine-1", "T4", 1, true, 30
		require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(nil, request))
		return containerID
	}
	stored := func(containerID string) *types.ContainerState {
		state, err := repo.GetContainerState(containerID)
		require.NoError(t, err)
		return state
	}
	create("sandbox-warm")
	_, err := repo.UpdateContainerStatusAndGetState("sandbox-warm", types.ContainerStatusPending, types.ContainerStateTtlSWhilePending)
	require.NoError(t, err)

	t.Run("a container that starts running", func(t *testing.T) {
		id := create("sandbox-starts")
		var state *types.ContainerState
		_, roundTrips := redisCost(counts, func() {
			state, err = repo.UpdateContainerStatusAndGetState(id, types.ContainerStatusRunning, types.ContainerStateTtlS)
		})
		require.NoError(t, err)
		require.Equal(t, 1, roundTrips, "the state came back with the update")
		require.Equal(t, types.ContainerStatusRunning, state.Status)
		require.InDelta(t, time.Now().Unix(), state.StartedAt, 2)
		require.Equal(t, "machine-1", state.MachineId)
		require.Equal(t, stored(id), state)
		requireLease(t, rdb, id, types.ContainerStateTtlS)
	})

	t.Run("a running container keeps the time it started", func(t *testing.T) {
		id := create("sandbox-runs")
		require.NoError(t, repo.UpdateContainerStatus(id, types.ContainerStatusRunning, types.ContainerStateTtlS))
		require.NoError(t, rdb.HSet(ctx, common.RedisKeys.SchedulerContainerState(id), "started_at", 1000).Err())
		state, err := repo.UpdateContainerStatusAndGetState(id, types.ContainerStatusRunning, types.ContainerStateTtlS)
		require.NoError(t, err)
		require.EqualValues(t, 1000, state.StartedAt)
		require.Equal(t, stored(id), state)
	})

	t.Run("a pending container that is renewed has not started", func(t *testing.T) {
		id := create("sandbox-waits")
		state, err := repo.UpdateContainerStatusAndGetState(id, types.ContainerStatusPending, types.ContainerStateTtlSWhilePending)
		require.NoError(t, err)
		require.Equal(t, types.ContainerStatusPending, state.Status)
		require.Zero(t, state.StartedAt)
		require.Equal(t, "0", rdb.HGet(ctx, common.RedisKeys.SchedulerContainerState(id), "started_at").Val())
		require.Equal(t, stored(id), state)
		requireLease(t, rdb, id, types.ContainerStateTtlSWhilePending)
	})

	t.Run("a refused transition returns the state as it is", func(t *testing.T) {
		id := create("sandbox-stopping")
		require.NoError(t, repo.UpdateContainerStatus(id, types.ContainerStatusStopping, types.ContainerStateTtlSWhileStopping))
		for _, status := range []types.ContainerStatus{types.ContainerStatusPending, types.ContainerStatusRunning} {
			state, err := repo.UpdateContainerStatusAndGetState(id, status, types.ContainerStateTtlS)
			require.NoError(t, err)
			require.Equal(t, types.ContainerStatusStopping, state.Status)
			require.Equal(t, stored(id), state)
		}
		requireLease(t, rdb, id, types.ContainerStateTtlSWhileStopping)

		id = create("sandbox-running")
		require.NoError(t, repo.UpdateContainerStatus(id, types.ContainerStatusRunning, types.ContainerStateTtlS))
		state, err := repo.UpdateContainerStatusAndGetState(id, types.ContainerStatusPending, types.ContainerStateTtlSWhilePending)
		require.NoError(t, err)
		require.Equal(t, types.ContainerStatusRunning, state.Status)
		requireLease(t, rdb, id, types.ContainerStateTtlS)
	})

	t.Run("a container that is stopped", func(t *testing.T) {
		id := create("sandbox-stops")
		state, err := repo.UpdateContainerStatusAndGetState(id, types.ContainerStatusStopping, types.ContainerStateTtlSWhileStopping)
		require.NoError(t, err)
		require.Equal(t, types.ContainerStatusStopping, state.Status)
		require.Equal(t, stored(id), state)
	})

	t.Run("a container without state", func(t *testing.T) {
		for _, status := range []types.ContainerStatus{types.ContainerStatusPending, types.ContainerStatusRunning, types.ContainerStatusStopping} {
			state, err := repo.UpdateContainerStatusAndGetState("sandbox-gone", status, types.ContainerStateTtlS)
			var notFound *types.ErrContainerStateNotFound
			require.ErrorAs(t, err, &notFound, status)
			require.Nil(t, state)
			require.Zero(t, rdb.Exists(ctx, common.RedisKeys.SchedulerContainerState("sandbox-gone")).Val(), "the update created the state")
		}
	})

	t.Run("a status that is not one", func(t *testing.T) {
		_, err := repo.UpdateContainerStatusAndGetState(create("sandbox-odd"), "PAUSED", types.ContainerStateTtlS)
		require.EqualError(t, err, "invalid status: PAUSED")
	})
}

// Gateways from before the update was one script take the container's lock
// for it, and deleting or stopping a container still does.
func TestStatusUpdateWaitsForWhoeverHoldsTheContainerLock(t *testing.T) {
	repo, rdb, _ := countedContainerRepo(t)
	require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(nil, testContainerRequest("sandbox-1", "workspace", 100)))
	lock := holdContainerLock(t, rdb, "sandbox-1")

	// Held for longer than an update waits: it fails as it always did.
	err := repo.UpdateContainerStatus("sandbox-1", types.ContainerStatusRunning, types.ContainerStateTtlS)
	require.ErrorIs(t, err, redislock.ErrNotObtained)
	requireContainerStatus(t, repo, "sandbox-1", types.ContainerStatusPending)

	updated := make(chan error, 1)
	go func() {
		updated <- repo.UpdateContainerStatus("sandbox-1", types.ContainerStatusRunning, types.ContainerStateTtlS)
	}()
	select {
	case err := <-updated:
		t.Fatalf("the update did not wait for the lock: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	requireContainerStatus(t, repo, "sandbox-1", types.ContainerStatusPending)

	require.NoError(t, lock.Release(context.Background()))
	select {
	case err := <-updated:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the update did not go ahead once the lock was released")
	}
	requireContainerStatus(t, repo, "sandbox-1", types.ContainerStatusRunning)
}

func TestStoppingAContainerAndRenewingAManagedEndpointStillTakeTheLock(t *testing.T) {
	repo, _, counts := countedContainerRepo(t)
	for _, test := range []struct {
		containerID string
		status      types.ContainerStatus
	}{
		{"sandbox-1", types.ContainerStatusStopping},
		{"managed-endpoint-1", types.ContainerStatusRunning},
	} {
		require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(nil, testContainerRequest(test.containerID, "workspace", 100)))
		commands, _ := redisCost(counts, func() {
			require.NoError(t, repo.UpdateContainerStatus(test.containerID, test.status, types.ContainerStateTtlS))
		})
		require.Equal(t, 1, commands["MSETNX"], "%s to %s: lock taken", test.containerID, test.status)
		require.Equal(t, 1, commands["DEL"], "%s to %s: lock released", test.containerID, test.status)
		requireContainerStatus(t, repo, test.containerID, test.status)
	}
}

// What the script writes must be what a gateway that takes the lock writes,
// because both kinds run side by side during a deploy.
func TestContainerIsAdmittedTheSameWithAndWithoutItsLock(t *testing.T) {
	repo, rdb, _ := countedContainerRepo(t)
	ctx := context.Background()
	quota := &types.ConcurrencyLimit{GPULimit: 10, CPUMillicoreLimit: 100_000}
	request := func(containerID string) *types.ContainerRequest {
		request := testContainerRequest(containerID, "workspace", 1500)
		request.MachineId, request.Gpu, request.GpuCount, request.Evictable, request.DrainSeconds, request.EvictOrder = "machine-1", "T4", 2, true, 30, 7
		return request
	}
	require.NoError(t, repo.createContainerState(quota, request("sandbox-unlocked")))
	require.NoError(t, repo.createLockedContainerState(quota, request("sandbox-locked")))

	hash := func(key string, without ...string) map[string]string {
		fields, err := rdb.HGetAll(ctx, key).Result()
		require.NoError(t, err)
		require.NotEmpty(t, fields, key)
		for _, field := range without {
			delete(fields, field)
		}
		return fields
	}
	unlocked := common.RedisKeys.SchedulerContainerState("sandbox-unlocked")
	locked := common.RedisKeys.SchedulerContainerState("sandbox-locked")

	// Only the script records which call admitted the container.
	require.NotEmpty(t, rdb.HGet(ctx, unlocked, "admission").Val())
	require.Equal(t, hash(locked, "container_id", "scheduled_at"), hash(unlocked, "container_id", "scheduled_at", "admission"))
	unlockedState, err := repo.GetContainerState("sandbox-unlocked")
	require.NoError(t, err)
	lockedState, err := repo.GetContainerState("sandbox-locked")
	require.NoError(t, err)
	require.Equal(t, "sandbox-unlocked", unlockedState.ContainerId)
	require.InDelta(t, lockedState.ScheduledAt, unlockedState.ScheduledAt, 2)
	require.InDelta(t, time.Now().Unix(), unlockedState.ScheduledAt, 2)

	for _, containerID := range []string{"sandbox-unlocked", "sandbox-locked"} {
		key := common.RedisKeys.SchedulerContainerState(containerID)
		requireLease(t, rdb, containerID, types.ContainerStateTtlSWhilePending)
		require.True(t, rdb.SIsMember(ctx, common.RedisKeys.SchedulerContainerIndex("test-stub"), key).Val(), "%s in its stub's index", containerID)
		require.True(t, rdb.SIsMember(ctx, common.RedisKeys.SchedulerContainerWorkspaceIndex("workspace"), key).Val(), "%s in its workspace's index", containerID)
		require.True(t, rdb.SIsMember(ctx, common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex("workspace"), containerID).Val(), "%s has a reservation", containerID)
	}
	require.Equal(t,
		hash(common.RedisKeys.WorkspaceConcurrencyLimitReservation("workspace", "sandbox-locked"), "container_id", "created_at"),
		hash(common.RedisKeys.WorkspaceConcurrencyLimitReservation("workspace", "sandbox-unlocked"), "container_id", "created_at"))
	usage := hash(common.RedisKeys.WorkspaceConcurrencyLimitUsage("workspace"))
	require.Equal(t, "3000", usage["cpu"])
	require.Equal(t, "4", usage["gpu_count"])
}

func TestAdmissionWaitsForWhoeverHoldsTheContainerLock(t *testing.T) {
	repo, rdb, _ := countedContainerRepo(t)
	ctx := context.Background()
	quota := &types.ConcurrencyLimit{GPULimit: 10, CPUMillicoreLimit: 100_000}
	require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(quota, testContainerRequest("sandbox-other", "workspace", 100)))
	request := testContainerRequest("sandbox-1", "workspace", 100)
	lock := holdContainerLock(t, rdb, "sandbox-1")

	// Held for longer than an admission waits: it fails as it always did, and
	// nothing was reserved or written for the container.
	err := repo.CreateContainerStateWithConcurrencyLimit(quota, request)
	require.ErrorIs(t, err, redislock.ErrNotObtained)
	require.Zero(t, rdb.Exists(ctx, common.RedisKeys.SchedulerContainerState("sandbox-1")).Val())
	require.Equal(t, "100", rdb.HGet(ctx, common.RedisKeys.WorkspaceConcurrencyLimitUsage("workspace"), "cpu").Val())

	admitted := make(chan error, 1)
	go func() { admitted <- repo.CreateContainerStateWithConcurrencyLimit(quota, request) }()
	select {
	case err := <-admitted:
		t.Fatalf("the admission did not wait for the lock: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	require.Zero(t, rdb.Exists(ctx, common.RedisKeys.SchedulerContainerState("sandbox-1")).Val())

	require.NoError(t, lock.Release(ctx))
	select {
	case err := <-admitted:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the admission did not go ahead once the lock was released")
	}
	requireContainerStatus(t, repo, "sandbox-1", types.ContainerStatusPending)
	require.Equal(t, "200", rdb.HGet(ctx, common.RedisKeys.WorkspaceConcurrencyLimitUsage("workspace"), "cpu").Val())
}

// The Redis client sends a command again when it does not get the reply. An
// admission that had in fact gone through must not then be told the container
// already exists: callers take that to mean someone else is starting it.
func TestAnAdmissionSentTwiceAdmitsTheContainerOnce(t *testing.T) {
	repo, rdb, _ := countedContainerRepo(t)
	quota := &types.ConcurrencyLimit{GPULimit: 10, CPUMillicoreLimit: 100_000}
	require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(quota, testContainerRequest("sandbox-other", "workspace", 100)))
	request := testContainerRequest("sandbox-1", "workspace", 100)

	for range 2 {
		_, err := repo.tryCreateContainerState(quota, request, "this-call")
		require.NoError(t, err)
	}
	require.Equal(t, "200", rdb.HGet(context.Background(), common.RedisKeys.WorkspaceConcurrencyLimitUsage("workspace"), "cpu").Val(), "reserved once")

	_, err := repo.tryCreateContainerState(quota, request, "another-call")
	var duplicate *types.ContainerAlreadyScheduledError
	require.ErrorAs(t, err, &duplicate)
	require.ErrorAs(t, repo.CreateContainerStateWithConcurrencyLimit(quota, request.Clone()), &duplicate)
}

func TestOneOfTwoGatewaysAdmitsADuplicateWhenOnlyOneTakesTheLock(t *testing.T) {
	repo, rdb, _ := countedContainerRepo(t)
	quota := &types.ConcurrencyLimit{GPULimit: 10, CPUMillicoreLimit: 100_000}

	const duplicates = 25
	for n := range duplicates {
		request := testContainerRequest(fmt.Sprintf("sandbox-duplicate-%02d", n), "workspace", 100)
		results := make(chan error, 2)
		go func() { results <- repo.createLockedContainerState(quota, request.Clone()) }()
		go func() { results <- repo.createContainerState(quota, request.Clone()) }()

		var admitted, refused int
		for range 2 {
			err := <-results
			var duplicate *types.ContainerAlreadyScheduledError
			switch {
			case err == nil:
				admitted++
			case errors.As(err, &duplicate):
				refused++
			default:
				t.Fatal(err)
			}
		}
		require.Equal(t, []int{1, 1}, []int{admitted, refused}, "admitted and refused creates of %s", request.ContainerId)
	}
	require.Equal(t, fmt.Sprint(duplicates*100), rdb.HGet(context.Background(), common.RedisKeys.WorkspaceConcurrencyLimitUsage("workspace"), "cpu").Val())
}

func TestAnAdmissionOverTheLimitLeavesNothingBehind(t *testing.T) {
	repo, rdb, _ := countedContainerRepo(t)
	ctx := context.Background()
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(quota, testContainerRequest("sandbox-1", "workspace", 100)))

	err := repo.CreateContainerStateWithConcurrencyLimit(quota, testContainerRequest("sandbox-2", "workspace", 100))
	var throttled *types.ThrottledByConcurrencyLimitError
	require.ErrorAs(t, err, &throttled)
	require.Equal(t, "cpu quota exceeded", throttled.Reason)

	key := common.RedisKeys.SchedulerContainerState("sandbox-2")
	require.Zero(t, rdb.Exists(ctx, key, common.RedisKeys.WorkspaceConcurrencyLimitReservation("workspace", "sandbox-2")).Val())
	require.False(t, rdb.SIsMember(ctx, common.RedisKeys.SchedulerContainerIndex("test-stub"), key).Val())
	require.False(t, rdb.SIsMember(ctx, common.RedisKeys.SchedulerContainerWorkspaceIndex("workspace"), key).Val())
	require.ErrorIs(t, rdb.ZScore(ctx, common.RedisKeys.SchedulerContainerStateIndex(), key).Err(), redis.Nil)
	require.Equal(t, "100", rdb.HGet(ctx, common.RedisKeys.WorkspaceConcurrencyLimitUsage("workspace"), "cpu").Val())
}

func TestContainerAddressesAreSetInOneRoundTrip(t *testing.T) {
	repo, _, counts := countedContainerRepo(t)
	addresses := map[int32]string{8001: "10.0.0.1:30001", 8002: "10.0.0.1:30002"}

	commands, roundTrips := redisCost(counts, func() {
		require.NoError(t, repo.SetContainerAddresses("sandbox-1", addresses, "10.0.0.1:30001"))
	})
	require.Equal(t, 1, roundTrips)
	require.Equal(t, map[string]int{"SET": 2}, commands)

	got, err := repo.GetContainerAddressMap("sandbox-1")
	require.NoError(t, err)
	require.Equal(t, addresses, got)
	address, err := repo.GetContainerAddress("sandbox-1")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1:30001", address)
}
