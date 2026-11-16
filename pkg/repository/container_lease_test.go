package repository

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// leaseTestRepo returns a container repository on an empty Redis, and a
// counter of the round trips made to it.
func leaseTestRepo(t *testing.T) (*ContainerRedisRepository, *common.RedisClient, *redisRoundTrips) {
	t.Helper()
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	trips := &redisRoundTrips{}
	rdb.AddHook(trips)
	return NewContainerRedisRepositoryForTest(rdb).(*ContainerRedisRepository), rdb, trips
}

// expiringContainer stores a container on worker "worker" in workspace
// "workspace" whose lease is about to run out: ten seconds are left on its
// state and on its entry in the state index. It returns the state key.
func expiringContainer(t *testing.T, repo *ContainerRedisRepository, containerID string, status types.ContainerStatus) string {
	t.Helper()
	ctx := context.Background()
	key := common.RedisKeys.SchedulerContainerState(containerID)
	require.NoError(t, repo.SetContainerState(containerID, &types.ContainerState{
		ContainerId: containerID, StubId: "stub", WorkspaceId: "workspace", WorkerId: "worker", Status: status,
	}))
	require.NoError(t, repo.rdb.Expire(ctx, key, 10*time.Second).Err())
	require.NoError(t, repo.rdb.ZAdd(ctx, common.RedisKeys.SchedulerContainerStateIndex(), redis.Z{Score: float64(time.Now().Unix() + 10), Member: key}).Err())
	return key
}

// requireLease checks how long a container's state is kept, and that the
// state index expects it to expire at the same moment.
func requireLease(t *testing.T, rdb *common.RedisClient, containerID string, seconds int64) {
	t.Helper()
	key := common.RedisKeys.SchedulerContainerState(containerID)
	assertContainerStateTTL(t, rdb, containerID, time.Duration(seconds)*time.Second)
	score, err := rdb.ZScore(context.Background(), common.RedisKeys.SchedulerContainerStateIndex(), key).Result()
	require.NoError(t, err)
	require.InDelta(t, time.Now().Unix()+seconds, score, 2, "state index score of %s", containerID)
}

func renewals(runtimeStarted bool, containerIDs ...string) []types.ContainerLeaseRenewal {
	out := make([]types.ContainerLeaseRenewal, len(containerIDs))
	for i, containerID := range containerIDs {
		out[i] = types.ContainerLeaseRenewal{ContainerId: containerID, RuntimeStarted: runtimeStarted}
	}
	return out
}

func TestRenewingManyContainerLeasesIsOneRedisRoundTrip(t *testing.T) {
	repo, rdb, trips := leaseTestRepo(t)
	ctx := context.Background()

	const containers = 500
	containerIDs := make([]string, containers)
	for n := range containers {
		containerIDs[n] = fmt.Sprintf("sandbox-stub-%03d", n)
		expiringContainer(t, repo, containerIDs[n], types.ContainerStatusRunning)
	}
	// Redis has the script from the first heartbeat on.
	repo.RenewContainerLeases(ctx, "worker", "", renewals(true, containerIDs[0]))

	trips.enabled.Store(true)
	leases := repo.RenewContainerLeases(ctx, "worker", "", renewals(true, containerIDs...))
	trips.enabled.Store(false)

	require.Equal(t, int64(1), trips.calls.Load(), "round trips to renew %d containers", containers)
	require.Len(t, leases, containers)
	for n, lease := range leases {
		require.Equal(t, types.ContainerLease{ContainerId: containerIDs[n], Status: types.ContainerStatusRunning}, lease)
		requireLease(t, rdb, containerIDs[n], types.ContainerStateTtlS)
	}
}

func TestRenewingAContainerLeaseKeepsItsStateAndIndexEntryAlive(t *testing.T) {
	repo, rdb, _ := leaseTestRepo(t)
	expiringContainer(t, repo, "sandbox-running", types.ContainerStatusRunning)
	expiringContainer(t, repo, "sandbox-pending", types.ContainerStatusPending)

	leases := repo.RenewContainerLeases(context.Background(), "worker", "", renewals(false, "sandbox-running", "sandbox-pending"))

	require.Equal(t, []types.ContainerLease{
		{ContainerId: "sandbox-running", Status: types.ContainerStatusRunning},
		{ContainerId: "sandbox-pending", Status: types.ContainerStatusPending},
	}, leases)
	requireLease(t, rdb, "sandbox-running", types.ContainerStateTtlS)
	// A container that has not started is given the time a slow start needs.
	requireLease(t, rdb, "sandbox-pending", types.ContainerStateTtlSWhilePending)
}

func TestRenewingAContainerWithoutStateReportsItMissing(t *testing.T) {
	repo, rdb, _ := leaseTestRepo(t)
	ctx := context.Background()
	expiringContainer(t, repo, "sandbox-alive", types.ContainerStatusRunning)

	leases := repo.RenewContainerLeases(ctx, "worker", "", renewals(true, "sandbox-gone", "sandbox-alive"))

	require.Len(t, leases, 2)
	var notFound *types.ErrContainerStateNotFound
	require.ErrorAs(t, leases[0].Err, &notFound)
	require.Equal(t, "sandbox-gone", notFound.ContainerId)
	// The heartbeat never brings a deleted container back.
	key := common.RedisKeys.SchedulerContainerState("sandbox-gone")
	require.Zero(t, rdb.Exists(ctx, key).Val())
	require.Error(t, rdb.ZScore(ctx, common.RedisKeys.SchedulerContainerStateIndex(), key).Err())
	// The containers sent with it are renewed all the same.
	require.Equal(t, types.ContainerLease{ContainerId: "sandbox-alive", Status: types.ContainerStatusRunning}, leases[1])
	requireLease(t, rdb, "sandbox-alive", types.ContainerStateTtlS)
}

func TestStoppingContainerIsReportedAndItsLeaseLeftAlone(t *testing.T) {
	repo, rdb, _ := leaseTestRepo(t)
	ctx := context.Background()
	key := expiringContainer(t, repo, "sandbox-evicted", types.ContainerStatusStopping)
	// What the scheduler writes when it evicts a container.
	require.NoError(t, rdb.HSet(ctx, key, "evicting", "true", "drain_seconds", 45).Err())

	for _, runtimeStarted := range []bool{false, true} {
		leases := repo.RenewContainerLeases(ctx, "worker", "", renewals(runtimeStarted, "sandbox-evicted"))

		require.Equal(t, []types.ContainerLease{
			{ContainerId: "sandbox-evicted", Status: types.ContainerStatusStopping, Evicting: true, DrainSeconds: 45},
		}, leases)
		// STOPPING is final: it is not renewed and does not become RUNNING again.
		requireLease(t, rdb, "sandbox-evicted", 10)
		require.Equal(t, string(types.ContainerStatusStopping), rdb.HGet(ctx, key, "status").Val())
		require.False(t, rdb.HExists(ctx, key, "started_at").Val())
	}
}

func TestPendingContainerBecomesRunningOnceItsRuntimeHasStarted(t *testing.T) {
	repo, rdb, _ := leaseTestRepo(t)
	ctx := context.Background()
	key := expiringContainer(t, repo, "sandbox-starting", types.ContainerStatusPending)

	// Not started yet: it stays PENDING and has no start time.
	leases := repo.RenewContainerLeases(ctx, "worker", "", renewals(false, "sandbox-starting"))
	require.Equal(t, []types.ContainerLease{{ContainerId: "sandbox-starting", Status: types.ContainerStatusPending}}, leases)
	require.False(t, rdb.HExists(ctx, key, "started_at").Val())

	started := time.Now().Unix()
	leases = repo.RenewContainerLeases(ctx, "worker", "", renewals(true, "sandbox-starting"))
	require.Equal(t, []types.ContainerLease{{ContainerId: "sandbox-starting", Status: types.ContainerStatusRunning, Started: true}}, leases)
	state, err := repo.GetContainerState("sandbox-starting")
	require.NoError(t, err)
	require.Equal(t, types.ContainerStatusRunning, state.Status)
	require.InDelta(t, started, state.StartedAt, 2)
	requireLease(t, rdb, "sandbox-starting", types.ContainerStateTtlS)

	// The start time is set once. Later heartbeats leave it as it is.
	require.NoError(t, rdb.HSet(ctx, key, "started_at", 1700000000).Err())
	leases = repo.RenewContainerLeases(ctx, "worker", "", renewals(true, "sandbox-starting"))
	require.Equal(t, []types.ContainerLease{{ContainerId: "sandbox-starting", Status: types.ContainerStatusRunning}}, leases)
	state, err = repo.GetContainerState("sandbox-starting")
	require.NoError(t, err)
	require.Equal(t, int64(1700000000), state.StartedAt)
}

func TestRunningContainerDoesNotGoBackToPending(t *testing.T) {
	repo, rdb, _ := leaseTestRepo(t)
	ctx := context.Background()
	key := expiringContainer(t, repo, "sandbox-running", types.ContainerStatusRunning)
	require.NoError(t, rdb.HSet(ctx, key, "started_at", 1700000000).Err())

	// A heartbeat prepared before the worker saw the runtime start.
	leases := repo.RenewContainerLeases(ctx, "worker", "", renewals(false, "sandbox-running"))

	require.Equal(t, []types.ContainerLease{{ContainerId: "sandbox-running", Status: types.ContainerStatusRunning}}, leases)
	state, err := repo.GetContainerState("sandbox-running")
	require.NoError(t, err)
	require.Equal(t, types.ContainerStatusRunning, state.Status)
	require.Equal(t, int64(1700000000), state.StartedAt)
	requireLease(t, rdb, "sandbox-running", types.ContainerStateTtlS)
}

func TestWorkerRenewsOnlyContainersAssignedToIt(t *testing.T) {
	repo, rdb, _ := leaseTestRepo(t)
	ctx := context.Background()
	expiringContainer(t, repo, "sandbox-own", types.ContainerStatusRunning)
	unassigned := expiringContainer(t, repo, "sandbox-unassigned", types.ContainerStatusPending)
	require.NoError(t, rdb.HSet(ctx, unassigned, "worker_id", "").Err())

	for _, test := range []struct {
		name                  string
		workerID, workspaceID string
		containerID           string
		renewed               bool
	}{
		{name: "its own container", workerID: "worker", containerID: "sandbox-own", renewed: true},
		{name: "its own container in its workspace", workerID: "worker", workspaceID: "workspace", containerID: "sandbox-own", renewed: true},
		{name: "another worker's container", workerID: "other-worker", containerID: "sandbox-own"},
		{name: "a container of another workspace", workerID: "worker", workspaceID: "other-workspace", containerID: "sandbox-own"},
		{name: "a container no worker has claimed", workerID: "worker", containerID: "sandbox-unassigned"},
		{name: "a container no worker has claimed, for a worker without an id", containerID: "sandbox-unassigned"},
		{name: "no container", workerID: "worker"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.containerID != "" {
				key := common.RedisKeys.SchedulerContainerState(test.containerID)
				require.NoError(t, rdb.Expire(ctx, key, 10*time.Second).Err())
				require.NoError(t, rdb.ZAdd(ctx, common.RedisKeys.SchedulerContainerStateIndex(), redis.Z{Score: float64(time.Now().Unix() + 10), Member: key}).Err())
			}

			leases := repo.RenewContainerLeases(ctx, test.workerID, test.workspaceID, renewals(true, test.containerID))

			require.Len(t, leases, 1)
			if test.renewed {
				require.NoError(t, leases[0].Err)
				requireLease(t, rdb, test.containerID, types.ContainerStateTtlS)
				return
			}
			var notAssigned *types.ErrContainerNotAssigned
			require.ErrorAs(t, leases[0].Err, &notAssigned)
			if test.containerID != "" {
				requireLease(t, rdb, test.containerID, 10)
			}
		})
	}
	// A refused heartbeat changes nothing, a claim included.
	require.Equal(t, string(types.ContainerStatusPending), rdb.HGet(ctx, unassigned, "status").Val())
}

func TestManagedEndpointLeaseIsNotCutShortByABatchedRenewal(t *testing.T) {
	repo, rdb, _ := leaseTestRepo(t)
	ctx := context.Background()
	key := expiringContainer(t, repo, "managed-model-replica", types.ContainerStatusRunning)
	require.NoError(t, rdb.Expire(ctx, key, time.Duration(types.ContainerStateTtlSManagedEndpoint)*time.Second).Err())

	leases := repo.RenewContainerLeases(ctx, "worker", "", renewals(true, "managed-model-replica"))

	require.Len(t, leases, 1)
	require.ErrorContains(t, leases[0].Err, "UpdateContainerStatus")
	assertContainerStateTTL(t, rdb, "managed-model-replica", time.Duration(types.ContainerStateTtlSManagedEndpoint)*time.Second)
}

func TestContainerWithAnUnknownStatusIsNotRenewed(t *testing.T) {
	repo, rdb, _ := leaseTestRepo(t)
	ctx := context.Background()
	key := expiringContainer(t, repo, "sandbox-odd", types.ContainerStatusRunning)
	require.NoError(t, rdb.HSet(ctx, key, "status", "PAUSED").Err())

	leases := repo.RenewContainerLeases(ctx, "worker", "", renewals(true, "sandbox-odd"))

	require.Len(t, leases, 1)
	require.EqualError(t, leases[0].Err, "invalid status: PAUSED")
	requireLease(t, rdb, "sandbox-odd", 10)
}

func TestLeasesAreRenewedAfterRedisForgetsTheScript(t *testing.T) {
	repo, rdb, trips := leaseTestRepo(t)
	ctx := context.Background()
	containerIDs := []string{"sandbox-a", "sandbox-b", "sandbox-c"}
	for _, containerID := range containerIDs {
		expiringContainer(t, repo, containerID, types.ContainerStatusRunning)
	}
	repo.RenewContainerLeases(ctx, "worker", "", renewals(true, containerIDs...))

	// A restarted or failed-over Redis has none of the scripts it was sent.
	require.NoError(t, rdb.ScriptFlush(ctx).Err())
	for _, containerID := range containerIDs {
		expiringContainer(t, repo, containerID, types.ContainerStatusRunning)
	}

	trips.enabled.Store(true)
	leases := repo.RenewContainerLeases(ctx, "worker", "", renewals(true, containerIDs...))
	afterRestart := trips.calls.Swap(0)
	repo.RenewContainerLeases(ctx, "worker", "", renewals(true, containerIDs...))
	trips.enabled.Store(false)

	for n, lease := range leases {
		require.Equal(t, types.ContainerLease{ContainerId: containerIDs[n], Status: types.ContainerStatusRunning}, lease)
		requireLease(t, rdb, containerIDs[n], types.ContainerStateTtlS)
	}
	// One more round trip sends the script again, once.
	require.Equal(t, int64(2), afterRestart)
	require.Equal(t, int64(1), trips.calls.Load())
}

func TestEveryContainerAnnouncesItselfOnceInSixRenewals(t *testing.T) {
	const containers = 600
	start := time.Now().Unix()

	perSlot := make([]int, containerLeaseAnnounceEvery)
	for n := range containers {
		containerID := fmt.Sprintf("sandbox-stub-%04d", n)
		announced := 0
		for slot := range containerLeaseAnnounceEvery {
			if containerLeaseAnnounces(containerID, start+int64(slot*containerLeaseAnnounceSlot)) {
				announced++
				perSlot[slot]++
			}
		}
		require.Equal(t, 1, announced, "%s over six renewals", containerID)
	}

	// They take turns: no renewal interval carries far more than its share.
	for slot, count := range perSlot {
		require.InDelta(t, containers/containerLeaseAnnounceEvery, count, 40, "containers announcing in interval %d", slot)
	}
}

func TestAnAnnouncingRenewalChangesNothingInTheState(t *testing.T) {
	repo, rdb, _ := leaseTestRepo(t)
	ctx := context.Background()
	key := expiringContainer(t, repo, "sandbox-stub-1", types.ContainerStatusRunning)
	before, err := rdb.HGetAll(ctx, key).Result()
	require.NoError(t, err)

	// Six renewals an interval apart include the one that announces.
	for slot := range containerLeaseAnnounceEvery {
		now := time.Now().Unix() + int64(slot*containerLeaseAnnounceSlot)
		flags := ""
		if containerLeaseAnnounces("sandbox-stub-1", now) {
			flags = "a"
		}
		reply, err := renewContainerLeasesScript.Run(ctx, rdb, []string{common.RedisKeys.SchedulerContainerStateIndex(), key},
			"worker", "", now, types.ContainerStateTtlS, types.ContainerStateTtlSWhilePending,
			string(types.ContainerStatusPending), string(types.ContainerStatusRunning), flags).Slice()
		require.NoError(t, err)
		require.Len(t, reply, 1)
		require.Equal(t, types.ContainerLease{ContainerId: "sandbox-stub-1", Status: types.ContainerStatusRunning}, containerLease("sandbox-stub-1", reply[0]))
	}

	after, err := rdb.HGetAll(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

// pipelineSizes records how many commands each pipeline carried.
type pipelineSizes struct {
	sizes []int
}

func (h *pipelineSizes) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (h *pipelineSizes) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h *pipelineSizes) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.sizes = append(h.sizes, len(cmds))
		return next(ctx, cmds)
	}
}

func TestFiftyContainersShareOneRunOfTheLeaseScript(t *testing.T) {
	repo, rdb, _ := leaseTestRepo(t)
	ctx := context.Background()

	const containers = 120
	containerIDs := make([]string, containers)
	for n := range containers {
		containerIDs[n] = fmt.Sprintf("sandbox-stub-%03d", n)
		expiringContainer(t, repo, containerIDs[n], types.ContainerStatusRunning)
	}
	// One is gone and one belongs to another worker: each container has its
	// own answer, wherever in a run of the script it falls.
	require.NoError(t, rdb.Del(ctx, common.RedisKeys.SchedulerContainerState(containerIDs[7])).Err())
	require.NoError(t, rdb.HSet(ctx, common.RedisKeys.SchedulerContainerState(containerIDs[101]), "worker_id", "another").Err())
	repo.RenewContainerLeases(ctx, "worker", "", renewals(false, containerIDs[0]))

	scripts := &pipelineSizes{}
	rdb.AddHook(scripts)
	leases := repo.RenewContainerLeases(ctx, "worker", "", renewals(false, containerIDs...))

	// 50, 50 and 20 containers: three runs of the script in one pipeline.
	require.Equal(t, []int{3}, scripts.sizes)
	require.Len(t, leases, containers)
	for n, lease := range leases {
		require.Equal(t, containerIDs[n], lease.ContainerId)
		switch n {
		case 7:
			var notFound *types.ErrContainerStateNotFound
			require.ErrorAs(t, lease.Err, &notFound)
		case 101:
			var notAssigned *types.ErrContainerNotAssigned
			require.ErrorAs(t, lease.Err, &notAssigned)
		default:
			require.NoError(t, lease.Err, containerIDs[n])
			require.Equal(t, types.ContainerStatusRunning, lease.Status)
			requireLease(t, rdb, containerIDs[n], types.ContainerStateTtlS)
		}
	}
}

func TestAStateThatCannotBeReadFailsOnlyItsOwnLease(t *testing.T) {
	repo, rdb, _ := leaseTestRepo(t)
	ctx := context.Background()

	containerIDs := []string{"sandbox-stub-1", "sandbox-stub-2", "sandbox-stub-3"}
	for _, containerID := range containerIDs {
		expiringContainer(t, repo, containerID, types.ContainerStatusRunning)
	}
	// The second container's state is not a hash, so the script cannot read it.
	broken := common.RedisKeys.SchedulerContainerState(containerIDs[1])
	require.NoError(t, rdb.Del(ctx, broken).Err())
	require.NoError(t, rdb.Set(ctx, broken, "not a hash", 0).Err())

	leases := repo.RenewContainerLeases(ctx, "worker", "", renewals(false, containerIDs...))

	require.Len(t, leases, 3)
	require.Error(t, leases[1].Err)
	var notFound *types.ErrContainerStateNotFound
	require.False(t, errors.As(leases[1].Err, &notFound), "a state that cannot be read is not a missing one: %v", leases[1].Err)
	// The containers before and after it in the same run are renewed.
	for _, n := range []int{0, 2} {
		require.NoError(t, leases[n].Err)
		requireLease(t, rdb, containerIDs[n], types.ContainerStateTtlS)
	}
}
