package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// orphanSweepTest is a Redis with a sweeper whose clock the test moves. The
// test Redis does not implement OBJECT IDLETIME, like a Redis with an LFU
// eviction policy, so unless a test supplies idle times the sweeper has only
// its own earlier sweeps to tell how long a key has been orphaned.
type orphanSweepTest struct {
	t          *testing.T
	ctx        context.Context
	rdb        *common.RedisClient
	containers ContainerRepository
	sweeper    *OrphanSweeper
	now        time.Time
}

func newOrphanSweepTest(t *testing.T) *orphanSweepTest {
	t.Helper()
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)

	st := &orphanSweepTest{
		t:          t,
		ctx:        context.Background(),
		rdb:        rdb,
		containers: NewContainerRedisRepositoryForTest(rdb),
		sweeper:    NewOrphanSweeper(rdb),
		now:        time.Now(),
	}
	st.sweeper.now = func() time.Time { return st.now }
	st.sweeper.pause = func(context.Context) {}
	return st
}

func (st *orphanSweepTest) sweep() OrphanSweepStats {
	st.t.Helper()
	stats, err := st.sweeper.Sweep(st.ctx)
	require.NoError(st.t, err)
	return stats
}

// sweepAfter moves the clock forward and sweeps.
func (st *orphanSweepTest) sweepAfter(elapsed time.Duration) OrphanSweepStats {
	st.t.Helper()
	st.now = st.now.Add(elapsed)
	return st.sweep()
}

// redisReportsIdleTimes makes Redis track idle time, as it does under an LRU
// eviction policy. Keys that are not listed were just used.
func (st *orphanSweepTest) redisReportsIdleTimes(idle map[string]time.Duration) {
	st.sweeper.idleTimes = func(_ context.Context, keys []string) ([]time.Duration, bool) {
		times := make([]time.Duration, len(keys))
		for i, key := range keys {
			times[i] = idle[key]
		}
		return times, true
	}
}

func (st *orphanSweepTest) run(containerID string) {
	st.t.Helper()
	require.NoError(st.t, st.containers.SetContainerState(containerID, &types.ContainerState{
		ContainerId: containerID,
		StubId:      "stub",
		WorkspaceId: "workspace-id",
		Status:      types.ContainerStatusRunning,
	}))
}

// writeContainerKeys writes every key a container owns that has no TTL, the
// way the gateway writes them, and returns their names by sweep rule.
func (st *orphanSweepTest) writeContainerKeys(containerID string) map[string]string {
	st.t.Helper()
	require.NoError(st.t, st.containers.SetWorkerAddress(containerID, "10.0.0.1:1993"))
	require.NoError(st.t, st.containers.SetContainerAddress(containerID, "10.0.0.1:8001"))
	require.NoError(st.t, st.containers.SetContainerAddressMap(containerID, map[int32]string{8001: "10.0.0.1:8001"}))
	require.NoError(st.t, st.containers.SetPodKeepWarmLock(st.ctx, "workspace", "stub", containerID, -1))
	// Only older gateways wrote this one: a count of open connections.
	connections := "pod:workspace:stub:container_connections:" + containerID
	require.NoError(st.t, st.rdb.Set(st.ctx, connections, 1, 0).Err())

	return map[string]string{
		orphanRuleWorkerAddress:        common.RedisKeys.SchedulerWorkerAddress(containerID),
		orphanRuleContainerAddress:     common.RedisKeys.SchedulerContainerAddress(containerID),
		orphanRuleContainerAddressMap:  common.RedisKeys.SchedulerContainerAddressMap(containerID),
		orphanRuleKeepWarmLock:         common.RedisKeys.PodKeepWarmLock("workspace", "stub", containerID),
		orphanRuleContainerConnections: connections,
	}
}

func (st *orphanSweepTest) exists(key string) bool {
	st.t.Helper()
	exists, err := st.rdb.Exists(st.ctx, key).Result()
	require.NoError(st.t, err)
	return exists == 1
}

func (st *orphanSweepTest) requireAll(keys map[string]string, exist bool, why string) {
	st.t.Helper()
	for rule, key := range keys {
		require.Equal(st.t, exist, st.exists(key), "%s: %s (%s)", why, key, rule)
	}
}

func requireNothingRemoved(t *testing.T, stats OrphanSweepStats) {
	t.Helper()
	for rule, removed := range stats.Removed {
		require.Zero(t, removed, rule)
	}
}

func TestSweepRemovesTheKeysOfAContainerSeenGoneInTwoSweeps(t *testing.T) {
	st := newOrphanSweepTest(t)

	gone := st.writeContainerKeys("sandbox-stub-gone")
	live := st.writeContainerKeys("sandbox-stub-live")
	st.run("sandbox-stub-live")
	expiring := st.writeContainerKeys("sandbox-stub-expiring")
	for _, key := range expiring {
		require.NoError(t, st.rdb.Expire(st.ctx, key, time.Hour).Err())
	}
	pausedVM := st.writeContainerKeys("vm-stub-paused")

	// The first sweep only takes note of what is orphaned.
	stats := st.sweep()
	requireNothingRemoved(t, stats)
	st.requireAll(gone, true, "removed by the first sweep that saw it")

	// A container that is being scheduled wrote its keys just before the
	// next sweep and has no state yet.
	starting := st.writeContainerKeys("sandbox-stub-starting")

	stats = st.sweepAfter(orphanSweepInterval)
	st.requireAll(gone, false, "orphan left behind")
	for rule := range gone {
		require.Equal(t, 1, stats.Removed[rule], rule)
	}
	st.requireAll(live, true, "a running container lost a key")
	st.requireAll(expiring, true, "a key with a TTL was removed")
	st.requireAll(starting, true, "a key written since the last sweep was removed")
	st.requireAll(pausedVM, true, "a paused VM lost a key")

	// The container that was starting is running by the next sweep.
	st.run("sandbox-stub-starting")
	stats = st.sweepAfter(orphanSweepInterval)
	requireNothingRemoved(t, stats)
	st.requireAll(starting, true, "a container that started lost a key")

	// A VM's keys go only after a week without state.
	st.sweepAfter(orphanVMKeyMinAge - 2*orphanSweepInterval - time.Minute)
	st.requireAll(pausedVM, true, "a VM paused for under a week lost a key")
	st.sweepAfter(time.Minute)
	st.requireAll(pausedVM, false, "a VM gone for a week left a key behind")
	st.requireAll(live, true, "a running container lost a key")
}

func TestSweepRemovesAVMsKeysOnceRedisSaysTheyWentUnusedForAWeek(t *testing.T) {
	st := newOrphanSweepTest(t)

	goneVM := st.writeContainerKeys("vm-stub-gone")
	pausedVM := st.writeContainerKeys("vm-stub-paused")
	liveVM := st.writeContainerKeys("vm-stub-live")
	st.run("vm-stub-live")
	expiring := st.writeContainerKeys("vm-stub-expiring")
	sandbox := st.writeContainerKeys("sandbox-stub-gone")

	month := 30 * 24 * time.Hour
	idleTimes := map[string]time.Duration{}
	for _, key := range goneVM {
		idleTimes[key] = orphanVMKeyMinAge
	}
	for _, key := range pausedVM {
		idleTimes[key] = orphanVMKeyMinAge - time.Second
	}
	for _, key := range liveVM {
		idleTimes[key] = month
	}
	for _, key := range expiring {
		idleTimes[key] = month
		require.NoError(t, st.rdb.Expire(st.ctx, key, time.Hour).Err())
	}
	for _, key := range sandbox {
		idleTimes[key] = month
	}
	idleQueue := common.RedisKeys.SchedulerWorkerRequests("worker-gone")
	recentQueue := common.RedisKeys.SchedulerWorkerRequests("worker-just-gone")
	for key, unused := range map[string]time.Duration{
		idleQueue:   orphanWorkerRequestsMinAge,
		recentQueue: orphanWorkerRequestsMinAge - time.Second,
	} {
		require.NoError(t, st.rdb.RPush(st.ctx, key, "request").Err())
		idleTimes[key] = unused
	}
	st.redisReportsIdleTimes(idleTimes)

	// However long Redis says a key went unused, one sweep removes nothing.
	stats := st.sweep()
	requireNothingRemoved(t, stats)

	// By the second sweep this gateway has watched the VM's keys for half an
	// hour, not a week. Redis vouches for the rest of the week, but not for
	// a key holding a small integer: all keys with that value share one
	// object, so its idle time is not the key's.
	sharesItsValue := func(rule string) bool {
		return rule == orphanRuleKeepWarmLock || rule == orphanRuleContainerConnections
	}
	stats = st.sweepAfter(orphanSweepInterval)
	for rule, key := range goneVM {
		require.Equal(t, sharesItsValue(rule), st.exists(key), "%s (%s)", key, rule)
	}
	require.False(t, st.exists(idleQueue), "the queue of a worker gone for a day was left behind")
	require.Equal(t, 1, stats.Removed[orphanRuleWorkerRequests])
	require.True(t, st.exists(recentQueue), "a queue used in the last day was removed")
	st.requireAll(sandbox, false, "orphan left behind")
	st.requireAll(pausedVM, true, "a VM paused for under a week lost a key")
	st.requireAll(liveVM, true, "a running VM lost a key")
	st.requireAll(expiring, true, "a key with a TTL was removed")
}

func TestSweepRemovesTheRequestQueueOfAWorkerGoneForADay(t *testing.T) {
	st := newOrphanSweepTest(t)

	queue := func(workerID string) string {
		key := common.RedisKeys.SchedulerWorkerRequests(workerID)
		require.NoError(t, st.rdb.RPush(st.ctx, key, "request").Err())
		return key
	}
	gone := queue("worker-gone")
	live := queue("worker-live")
	require.NoError(t, st.rdb.HSet(st.ctx, common.RedisKeys.SchedulerWorkerState("worker-live"), "id", "worker-live").Err())
	expiring := queue("worker-expiring")
	require.NoError(t, st.rdb.Expire(st.ctx, expiring, time.Hour).Err())
	// Requests a worker has taken but not acknowledged are not a queue.
	pending := common.RedisKeys.SchedulerWorkerPendingRequests("worker-gone")
	require.NoError(t, st.rdb.HSet(st.ctx, pending, "container", "request").Err())

	st.sweep()
	st.sweepAfter(orphanWorkerRequestsMinAge - time.Minute)
	require.True(t, st.exists(gone), "a queue orphaned for under a day was removed")

	stats := st.sweepAfter(time.Minute)
	require.False(t, st.exists(gone), "the queue of a worker gone for a day was left behind")
	require.Equal(t, 1, stats.Removed[orphanRuleWorkerRequests])
	require.True(t, st.exists(live), "a registered worker lost its queue")
	require.True(t, st.exists(expiring), "a queue with a TTL was removed")
	require.True(t, st.exists(pending), "pending requests were removed")
}

func TestSweepPrunesIndexMembersOfContainersThatAreGone(t *testing.T) {
	st := newOrphanSweepTest(t)

	stubIndex := common.RedisKeys.SchedulerContainerIndex("stub")
	workspaceIndex := common.RedisKeys.SchedulerContainerWorkspaceIndex("workspace-id")
	workerIndex := common.RedisKeys.SchedulerContainerWorkerIndex("worker")
	indexes := []string{stubIndex, workspaceIndex, workerIndex}

	// Three containers ran on one worker. One still runs, one expired, and
	// one just exited: its exit code is still there for the failure history.
	stateKey := common.RedisKeys.SchedulerContainerState
	for _, containerID := range []string{"sandbox-stub-live", "sandbox-stub-expired", "sandbox-stub-exited"} {
		st.run(containerID)
		require.NoError(t, st.rdb.SAdd(st.ctx, workerIndex, stateKey(containerID)).Err())
	}
	require.NoError(t, st.rdb.Del(st.ctx, stateKey("sandbox-stub-expired"), stateKey("sandbox-stub-exited")).Err())
	require.NoError(t, st.containers.SetContainerExitCode("sandbox-stub-exited", 1))
	// A member that is not a container's state key is not the sweep's to judge.
	require.NoError(t, st.rdb.SAdd(st.ctx, workerIndex, "something-else").Err())

	// The indexes of a stub and a worker whose containers have all expired.
	deadStubIndex := common.RedisKeys.SchedulerContainerIndex("deleted-stub")
	deadWorkerIndex := common.RedisKeys.SchedulerContainerWorkerIndex("dead-worker")
	for _, index := range []string{deadStubIndex, deadWorkerIndex} {
		require.NoError(t, st.rdb.SAdd(st.ctx, index, stateKey("sandbox-deleted-stub-1"), stateKey("sandbox-deleted-stub-2")).Err())
	}

	stats := st.sweep()

	for _, index := range indexes {
		want := []string{stateKey("sandbox-stub-live"), stateKey("sandbox-stub-exited")}
		if index == workerIndex {
			want = append(want, "something-else")
		}
		members, err := st.rdb.SMembers(st.ctx, index).Result()
		require.NoError(t, err)
		require.ElementsMatch(t, want, members, index)
	}
	require.False(t, st.exists(deadStubIndex), "the index of a stub with no containers was left behind")
	require.False(t, st.exists(deadWorkerIndex), "the index of a worker with no containers was left behind")
	require.Equal(t, 3, stats.Removed[orphanRuleStubIndex])
	require.Equal(t, 1, stats.Removed[orphanRuleWorkspaceIndex])
	require.Equal(t, 3, stats.Removed[orphanRuleWorkerIndex])
}

func TestSweepPrunesStateIndexEntriesThatExpiredOverAnHourAgo(t *testing.T) {
	st := newOrphanSweepTest(t)

	index := common.RedisKeys.SchedulerContainerStateIndex()
	stateKey := common.RedisKeys.SchedulerContainerState
	entry := func(containerID string, expiry time.Time) {
		require.NoError(t, st.rdb.ZAdd(st.ctx, index, redis.Z{Score: float64(expiry.Unix()), Member: stateKey(containerID)}).Err())
	}

	entry("sandbox-stub-expired", st.now.Add(-2*time.Hour))
	entry("sandbox-stub-just-expired", st.now.Add(-10*time.Minute))
	// A running container whose index entry was not kept up to date.
	st.run("sandbox-stub-live")
	entry("sandbox-stub-live", st.now.Add(-2*time.Hour))
	st.run("sandbox-stub-fresh")
	// An entry that is not a container's state key is not the sweep's to judge.
	require.NoError(t, st.rdb.ZAdd(st.ctx, index, redis.Z{Score: 1, Member: "something-else"}).Err())

	stats := st.sweep()

	entries, err := st.rdb.ZRange(st.ctx, index, 0, -1).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		stateKey("sandbox-stub-just-expired"),
		stateKey("sandbox-stub-live"),
		stateKey("sandbox-stub-fresh"),
		"something-else",
	}, entries)
	require.Equal(t, 1, stats.Removed[orphanRuleStateIndex])
}

func TestSweepReleasesTheQuotaOfAContainerThatIsGone(t *testing.T) {
	st := newOrphanSweepTest(t)
	repo := st.containers.(*ContainerRedisRepository)
	quota := &types.ConcurrencyLimit{CPUMillicoreLimit: 10_000}

	// Two containers hold quota. One still runs; the other's state expired,
	// which releases nothing.
	gone := testContainerRequest("sandbox-stub-gone", "workspace-id", 1_000)
	live := testContainerRequest("sandbox-stub-live", "workspace-id", 2_000)
	for _, request := range []*types.ContainerRequest{gone, live} {
		require.NoError(t, repo.CreateContainerStateWithConcurrencyLimit(quota, request))
	}
	require.NoError(t, st.rdb.Del(st.ctx, common.RedisKeys.SchedulerContainerState(gone.ContainerId)).Err())

	usedCPU := func() int64 {
		used, err := st.rdb.HGet(st.ctx, common.RedisKeys.WorkspaceConcurrencyLimitUsage("workspace-id"), "cpu").Int64()
		require.NoError(t, err)
		return used
	}
	reserved := func() []string {
		ids, err := st.rdb.SMembers(st.ctx, common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex("workspace-id")).Result()
		require.NoError(t, err)
		return ids
	}
	goneReservation := common.RedisKeys.WorkspaceConcurrencyLimitReservation("workspace-id", gone.ContainerId)
	liveReservation := common.RedisKeys.WorkspaceConcurrencyLimitReservation("workspace-id", live.ContainerId)

	st.sweep()
	require.True(t, st.exists(goneReservation), "removed by the first sweep that saw it")
	require.EqualValues(t, 3_000, usedCPU())

	stats := st.sweepAfter(orphanSweepInterval)
	require.Equal(t, 1, stats.Removed[orphanRuleReservation])
	require.False(t, st.exists(goneReservation), "the reservation of a container that is gone was left behind")
	require.True(t, st.exists(liveReservation), "a running container lost its reservation")
	require.EqualValues(t, 2_000, usedCPU(), "the workspace still counts a container that is gone")
	require.Equal(t, []string{live.ContainerId}, reserved())
}

func TestSweepLeavesEveryOtherKeyAlone(t *testing.T) {
	st := newOrphanSweepTest(t)

	// None of these has a TTL or belongs to a container that has state.
	others := []string{
		"cache:fs:a1b2c3",
		"bot:workspace:stub:session:markers",
		"task:workspace:stub:task-id",
		"pod:run_task:task-id",
		"endpoint:workspace:stub:keep_warm_lock:endpoint-stub-gone",
		"gateway:auth:workspace:token",
		"workspace:{workspace-id}:concurrency_limit:usage",
		"workspace:concurrency_limit:workspace-id",
		common.RedisKeys.SchedulerContainerExitCode("sandbox-stub-gone"),
		common.RedisKeys.SchedulerContainerRequestStatus("sandbox-stub-gone"),
		common.RedisKeys.SchedulerWorkerPendingRequests("worker-gone"),
		common.RedisKeys.SchedulerBackendRoute("route"),
		common.RedisKeys.SchedulerWorkerAddress("") + "sandbox:nested",
	}
	for _, key := range others {
		_, isCandidate := classifyOrphanKey(key)
		require.False(t, isCandidate, key)
		require.NoError(t, st.rdb.Set(st.ctx, key, "value", 0).Err())
	}
	sets := []string{
		common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex("workspace-id"),
		common.RedisKeys.SchedulerBackendRouteIndex("sandbox-stub-gone"),
		common.RedisKeys.SchedulerWorkerIndex(),
	}
	for _, key := range sets {
		_, isCandidate := classifyOrphanKey(key)
		require.False(t, isCandidate, key)
		require.NoError(t, st.rdb.SAdd(st.ctx, key, "sandbox-stub-gone").Err())
	}

	st.sweep()
	stats := st.sweepAfter(30 * 24 * time.Hour)

	requireNothingRemoved(t, stats)
	for _, key := range append(others, sets...) {
		require.True(t, st.exists(key), key)
	}
}

func TestSweepWorksThroughMoreKeysThanFitInOneBatch(t *testing.T) {
	st := newOrphanSweepTest(t)

	const count = 2*orphanSweepBatchSize + 500
	stubIndex := common.RedisKeys.SchedulerContainerIndex("stub")
	pipe := st.rdb.Pipeline()
	for n := range count {
		containerID := fmt.Sprintf("sandbox-stub-%05d", n)
		pipe.Set(st.ctx, common.RedisKeys.SchedulerWorkerAddress(containerID), "10.0.0.1:1993", 0)
		pipe.SAdd(st.ctx, stubIndex, common.RedisKeys.SchedulerContainerState(containerID))
	}
	_, err := pipe.Exec(st.ctx)
	require.NoError(t, err)
	// One container among them still runs.
	st.run("sandbox-stub-00042")

	first := st.sweep()
	require.Equal(t, count-1, first.Waiting)

	second := st.sweepAfter(orphanSweepInterval)
	require.Equal(t, count-1, second.Removed[orphanRuleWorkerAddress])
	require.Zero(t, second.Waiting)
	// The index is read a page at a time and pruned between pages. Redis
	// still returns every member that stays; the test Redis pages by position
	// and skips some, which the second sweep picks up.
	require.Equal(t, count-1, first.Removed[orphanRuleStubIndex]+second.Removed[orphanRuleStubIndex])

	left, err := st.rdb.Keys(st.ctx, common.RedisKeys.SchedulerWorkerAddress("*"))
	require.NoError(t, err)
	require.Equal(t, []string{common.RedisKeys.SchedulerWorkerAddress("sandbox-stub-00042")}, left)
	members, err := st.rdb.SMembers(st.ctx, stubIndex).Result()
	require.NoError(t, err)
	require.Equal(t, []string{common.RedisKeys.SchedulerContainerState("sandbox-stub-00042")}, members)
}

func TestSweepForgetsAnOrphanWhoseContainerCameBack(t *testing.T) {
	st := newOrphanSweepTest(t)

	keys := st.writeContainerKeys("sandbox-stub-slow-start")
	st.sweep()

	// It had state at the next sweep, so the first sighting no longer counts.
	st.run("sandbox-stub-slow-start")
	st.sweepAfter(orphanSweepInterval)
	require.NoError(t, st.rdb.Del(st.ctx, common.RedisKeys.SchedulerContainerState("sandbox-stub-slow-start")).Err())

	st.sweepAfter(time.Minute)
	st.requireAll(keys, true, "removed without two sweeps in a row seeing it orphaned")
}

func TestTwoReplicasDoNotSweepTogether(t *testing.T) {
	st := newOrphanSweepTest(t)
	other := NewOrphanSweeper(st.rdb)
	other.pause = func(context.Context) {}

	// The first replica is stopped in the middle of its sweep.
	sweeping := make(chan struct{})
	finish := make(chan struct{})
	st.sweeper.pause = func(context.Context) {
		select {
		case sweeping <- struct{}{}:
			<-finish
		default:
		}
	}
	type result struct {
		swept bool
		err   error
	}
	first := make(chan result, 1)
	go func() {
		swept, err := st.sweeper.SweepIfDue(st.ctx)
		first <- result{swept, err}
	}()
	select {
	case <-sweeping:
	case <-time.After(5 * time.Second):
		t.Fatal("the first replica never started sweeping")
	}

	// Even with the cooldown out of the way, the lock keeps the second out.
	require.NoError(t, st.rdb.Del(st.ctx, orphanSweepRecentKey).Err())
	swept, err := other.SweepIfDue(st.ctx)
	require.NoError(t, err)
	require.False(t, swept, "a second replica swept while the first was sweeping")

	close(finish)
	done := <-first
	require.NoError(t, done.err)
	require.True(t, done.swept)

	// The lock is free again, so the second replica can sweep.
	swept, err = other.SweepIfDue(st.ctx)
	require.NoError(t, err)
	require.True(t, swept, "the second replica could not sweep after the first finished")

	// And then no replica sweeps again until the cooldown is over.
	swept, err = st.sweeper.SweepIfDue(st.ctx)
	require.NoError(t, err)
	require.False(t, swept, "a sweep ran within the cooldown of the last one")
}
