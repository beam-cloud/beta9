package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// What placing a container costs Redis is pinned here command by command, so
// a change to it shows up as a failing count and not as a busier Redis.

func countedContainerRepo(t *testing.T) (*ContainerRedisRepository, *common.RedisClient, *RedisCommandCounts) {
	t.Helper()
	rdb, counts, err := NewRedisClientForTestWithCounts()
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	return NewContainerRedisRepositoryForTest(rdb).(*ContainerRedisRepository), rdb, counts
}

// redisCost is what do asks of Redis: every command the server runs, those a
// script issues included, and the round trips.
func redisCost(counts *RedisCommandCounts, do func()) (map[string]int, int) {
	counts.Start()
	do()
	return counts.Stop()
}

func requireContainerStatus(t *testing.T, repo *ContainerRedisRepository, containerID string, want types.ContainerStatus) {
	t.Helper()
	state, err := repo.GetContainerState(containerID)
	require.NoError(t, err)
	require.Equal(t, want, state.Status, containerID)
}

func TestUpdatingAContainerStatusIsOneRedisRoundTrip(t *testing.T) {
	repo, _, counts := countedContainerRepo(t)
	for _, containerID := range []string{"sandbox-warm", "sandbox-1"} {
		require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(nil, testContainerRequest(containerID, "workspace", 100)))
	}
	// Redis is sent a script whole the first time it is used.
	require.NoError(t, repo.UpdateContainerStatus("sandbox-warm", types.ContainerStatusPending, types.ContainerStateTtlSWhilePending))

	// A worker renews the pending lease when it claims the container, then
	// reports it running. Each took the container's lock: four round trips.
	for _, status := range []types.ContainerStatus{types.ContainerStatusPending, types.ContainerStatusRunning} {
		commands, roundTrips := redisCost(counts, func() {
			require.NoError(t, repo.UpdateContainerStatus("sandbox-1", status, types.ContainerStateTtlS))
		})
		require.Equal(t, 1, roundTrips, status)
		require.Equal(t, map[string]int{"EVALSHA": 1, "EXISTS": 1, "HGETALL": 1, "HSET": 1, "EXPIRE": 1, "ZADD": 1}, commands, status)
	}
	requireContainerStatus(t, repo, "sandbox-1", types.ContainerStatusRunning)
	requireLease(t, repo.rdb, "sandbox-1", types.ContainerStateTtlS)
}

func TestAdmittingAContainerIsOneRedisRoundTrip(t *testing.T) {
	repo, _, counts := countedContainerRepo(t)
	quota := &types.ConcurrencyLimit{GPULimit: 10, CPUMillicoreLimit: 100_000}
	// The first admission to a workspace also sets up its usage counter.
	require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(quota, testContainerRequest("sandbox-warm", "workspace", 100)))

	// It was six round trips: take the lock, read the status, check the usage
	// counter, reserve, write the state, release the lock.
	commands, roundTrips := redisCost(counts, func() {
		require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(quota, testContainerRequest("sandbox-1", "workspace", 100)))
	})
	require.Equal(t, 1, roundTrips, "with a concurrency limit")
	require.Equal(t, map[string]int{
		"EVALSHA": 1,
		"HGET":    5, // status, and of the usage counter: initialized twice, gpu, cpu
		"EXISTS":  2, // the lock, the reservation
		"HINCRBY": 2,
		"HSET":    3, // usage counter, reservation, state
		"SADD":    3, // reservation index, stub index, workspace index
		"EXPIRE":  1,
		"ZADD":    1,
	}, commands, "with a concurrency limit")

	commands, roundTrips = redisCost(counts, func() {
		require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(nil, testContainerRequest("sandbox-2", "workspace", 100)))
	})
	require.Equal(t, 1, roundTrips, "without a concurrency limit")
	require.Equal(t, map[string]int{"EVALSHA": 1, "HGET": 1, "EXISTS": 1, "HSET": 1, "SADD": 2, "EXPIRE": 1, "ZADD": 1}, commands, "without a concurrency limit")

	for _, containerID := range []string{"sandbox-1", "sandbox-2"} {
		requireContainerStatus(t, repo, containerID, types.ContainerStatusPending)
		requireLease(t, repo.rdb, containerID, types.ContainerStateTtlSWhilePending)
	}
}

func TestGettingAWorkerAddressIsOneRedisRoundTrip(t *testing.T) {
	repo, _, counts := countedContainerRepo(t)
	require.NoError(t, repo.SetWorkerAddress("sandbox-1", "10.0.0.1:1993"))

	commands, roundTrips := redisCost(counts, func() {
		address, err := repo.GetWorkerAddress(context.Background(), "sandbox-1")
		require.NoError(t, err)
		require.Equal(t, "10.0.0.1:1993", address)
	})
	require.Equal(t, 1, roundTrips)
	require.Equal(t, map[string]int{"GET": 2}, commands) // the address, the request's status
	require.False(t, workerAddressLoopRunning(repo), "a container that has its worker needs nobody to wait for it")
}

func TestWaitingForAWorkerAddressReadsOnceAnInterval(t *testing.T) {
	repo, _, counts := countedContainerRepo(t)
	placed := make(chan error, 1)
	go func() {
		_, err := repo.GetWorkerAddress(context.Background(), "sandbox-1")
		placed <- err
	}()
	require.Eventually(t, func() bool { return workerAddressLoopRunning(repo) }, 5*time.Second, time.Millisecond)

	// The caller used to read the address 40 times a second and the
	// request's status 10 times. It is told of the address when it is set,
	// so both are read only to catch what it was not told.
	started := time.Now()
	commands, _ := redisCost(counts, func() { time.Sleep(4 * workerAddressPollInterval) })
	intervals := int(time.Since(started)/workerAddressPollInterval) + 1
	require.Positive(t, commands["GET"])
	require.LessOrEqual(t, commands["GET"], 2*intervals) // the address, the request's status

	require.NoError(t, repo.SetWorkerAddress("sandbox-1", "10.0.0.1:1993"))
	select {
	case err := <-placed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the caller was not given the worker address")
	}
}

func TestRecordingASandboxReadsItsActivityOnlyWhenAnHourStarts(t *testing.T) {
	repo, rdb, counts := countedContainerRepo(t)
	ctx := context.Background()
	key := common.RedisKeys.SchedulerAppSandboxActivity("workspace", "app")
	hour := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	record := func(at time.Time) (map[string]int, int) {
		return redisCost(counts, func() { require.NoError(t, repo.RecordSandboxCreated("workspace", "app", at)) })
	}

	// The first sandbox of an hour adds a bucket, so stale ones are looked for.
	commands, _ := record(hour.Add(time.Minute))
	require.Equal(t, map[string]int{"HINCRBY": 1, "EXPIRE": 1, "HKEYS": 1}, commands)

	// Every other one only counts. It used to list the buckets as well.
	commands, roundTrips := record(hour.Add(2 * time.Minute))
	require.Equal(t, map[string]int{"HINCRBY": 1, "EXPIRE": 1}, commands)
	require.Equal(t, 1, roundTrips)
	require.Equal(t, map[string]string{fmt.Sprint(hour.Unix()): "2"}, rdb.HGetAll(ctx, key).Val())
	require.Equal(t, sandboxActivityRetention, rdb.TTL(ctx, key).Val())

	// A new hour past the retention window trims the bucket that fell out of it.
	later := hour.Add(sandboxActivityRetention + time.Hour)
	commands, _ = record(later.Add(time.Minute))
	require.Equal(t, map[string]int{"HINCRBY": 1, "EXPIRE": 1, "HKEYS": 1, "HDEL": 1}, commands)
	require.Equal(t, map[string]string{fmt.Sprint(later.Unix()): "1"}, rdb.HGetAll(ctx, key).Val())

	// A late write to an hour already out of the window is trimmed at once.
	commands, _ = record(hour.Add(3 * time.Minute))
	require.Equal(t, 1, commands["HDEL"])
	require.Equal(t, map[string]string{fmt.Sprint(later.Unix()): "1"}, rdb.HGetAll(ctx, key).Val())
}

// A request that was queued without its container id first is still
// delivered: the script falls back to parsing it.
func TestARequestEncodedInAnotherOrderIsStillDelivered(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	ctx := context.Background()
	workers := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-1", Status: types.WorkerStatusAvailable, FreeCpu: 10_000, FreeMemory: 10_000}
	require.NoError(t, workers.AddWorker(worker))

	for _, containerID := range []string{"sandbox-reordered", `sandbox-"quoted"`} {
		encodedID, err := json.Marshal(containerID)
		require.NoError(t, err)
		raw := fmt.Sprintf(`{"cpu":100,"memory":128,"container_id":%s}`, encodedID)
		if containerID != "sandbox-reordered" {
			// An id the fast path can't read whole must not be read in part.
			raw = fmt.Sprintf(`{"container_id":%s,"cpu":100,"memory":128}`, encodedID)
		}
		require.NoError(t, rdb.HSet(ctx, common.RedisKeys.SchedulerContainerState(containerID),
			"container_id", containerID, "status", string(types.ContainerStatusPending), schedulerAssignmentIDField, "assignment-1").Err())
		require.NoError(t, rdb.RPush(ctx, common.RedisKeys.SchedulerWorkerRequests(worker.Id), raw).Err())

		delivered, err := workers.GetNextContainerRequests(worker.Id, 1)
		require.NoError(t, err)
		require.Len(t, delivered, 1, containerID)
		require.Equal(t, containerID, delivered[0].ContainerId)
		require.Equal(t, "assignment-1:1", delivered[0].DeliveryToken)
	}
}
