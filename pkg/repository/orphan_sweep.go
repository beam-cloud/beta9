package repository

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// A container's address keys, keep-warm lock and quota reservation, and a
// worker's request queue, are stored without a TTL and deleted together with
// their owner's state. When that state expires instead of being deleted (the
// worker died, a gateway restarted mid-stop), nothing deletes them, and the
// index sets keep pointing at state that is gone. The sweep walks the keyspace
// now and then and removes what was left behind.

const (
	orphanSweepFirstRunDelay = 5 * time.Minute
	orphanSweepInterval      = 30 * time.Minute
	// After a sweep, no replica sweeps again for most of the interval.
	orphanSweepCooldown = orphanSweepInterval - time.Minute

	orphanSweepLockKey        = "gateway:orphan_sweep:lock"
	orphanSweepRecentKey      = "gateway:orphan_sweep:recent"
	orphanSweepLockTTLSeconds = 60

	// Keys per SCAN call and commands per pipeline.
	orphanSweepBatchSize = 1000
	// Index sets read per pipeline. Each can return a page of members.
	orphanSweepIndexBatchSize = 100
	orphanSweepPause          = 20 * time.Millisecond
	// Orphans remembered from one sweep to the next. Past this, an orphan is
	// noticed again by a later sweep, once earlier ones have been removed.
	orphanSweepMaxRemembered = 100_000

	// How long a key must have been orphaned before it is removed. Two sweeps
	// in a row must also have seen it orphaned.
	orphanContainerKeyMinAge = 10 * time.Minute
	// A paused VM has no container state and can be resumed days later.
	orphanVMKeyMinAge          = 7 * 24 * time.Hour
	orphanWorkerRequestsMinAge = 24 * time.Hour
	// How far in the past a state index entry's expiry must be.
	orphanStateIndexMinAge = time.Hour
)

// The rules of the sweep. Each is a counter in the sweep's summary.
const (
	orphanRuleWorkerAddress        = "worker_addr"
	orphanRuleContainerAddress     = "container_addr"
	orphanRuleContainerAddressMap  = "container_addr_map"
	orphanRuleKeepWarmLock         = "keep_warm_lock"
	orphanRuleContainerConnections = "container_connections"
	orphanRuleReservation          = "reservation"
	orphanRuleWorkerRequests       = "worker_requests"
	orphanRuleStubIndex            = "stub_index_members"
	orphanRuleWorkspaceIndex       = "workspace_index_members"
	orphanRuleWorkerIndex          = "worker_index_members"
	orphanRuleStateIndex           = "state_index_members"
)

var orphanSweepRules = []string{
	orphanRuleWorkerAddress,
	orphanRuleContainerAddress,
	orphanRuleContainerAddressMap,
	orphanRuleKeepWarmLock,
	orphanRuleContainerConnections,
	orphanRuleReservation,
	orphanRuleWorkerRequests,
	orphanRuleStubIndex,
	orphanRuleWorkspaceIndex,
	orphanRuleWorkerIndex,
	orphanRuleStateIndex,
}

var (
	orphanContainerStatePrefix      = common.RedisKeys.SchedulerContainerState("")
	orphanWorkerAddressPrefix       = common.RedisKeys.SchedulerWorkerAddress("")
	orphanContainerAddressPrefix    = common.RedisKeys.SchedulerContainerAddress("")
	orphanContainerAddressMapPrefix = common.RedisKeys.SchedulerContainerAddressMap("")
	orphanWorkerRequestsPrefix      = common.RedisKeys.SchedulerWorkerRequests("")
	orphanStubIndexPrefix           = common.RedisKeys.SchedulerContainerIndex("")
	orphanWorkspaceIndexPrefix      = common.RedisKeys.SchedulerContainerWorkspaceIndex("")
	orphanWorkerIndexPrefix         = common.RedisKeys.SchedulerContainerWorkerIndex("")
)

// Pieces of pod:<workspace name>:<stub id>:keep_warm_lock:<container id>, of
// the connection count older gateways wrote beside it, and of
// workspace:{<workspace id>}:concurrency_limit:reservation:<container id>.
const (
	orphanPodPrefix                  = "pod:"
	orphanKeepWarmLockInfix          = ":keep_warm_lock:"
	orphanContainerConnectionsInfix  = ":container_connections:"
	orphanReservationWorkspacePrefix = "workspace:{"
	orphanReservationInfix           = "}:concurrency_limit:reservation:"
)

// orphanCandidate is a key the sweep knows how to clean up.
type orphanCandidate struct {
	key  string
	rule string
	// The state key of the container or worker the key belongs to. While it
	// exists the key is in use. Empty for an index, which is pruned member
	// by member instead.
	ownerKey string
	minAge   time.Duration
	// The idle time Redis reports for this key says nothing about the key.
	// Redis keeps one object for each small integer and shares it between
	// all keys holding that value, unless an LRU or LFU eviction policy is
	// on, and the idle time is the shared object's: a keep-warm lock (value
	// 1) written a second ago can look idle for hours.
	idleTimeUnreliable bool
	// Set for a quota reservation, which is released rather than deleted.
	workspaceId string
	containerId string
}

func (c orphanCandidate) isIndex() bool {
	return c.ownerKey == ""
}

// classifyOrphanKey says what the sweep may do with a key. It reports false
// for every key that is none of the sweep's business.
func classifyOrphanKey(key string) (orphanCandidate, bool) {
	switch {
	case strings.HasPrefix(key, orphanWorkerAddressPrefix):
		return containerOwnedKey(key, orphanRuleWorkerAddress, key[len(orphanWorkerAddressPrefix):])
	case strings.HasPrefix(key, orphanContainerAddressPrefix):
		return containerOwnedKey(key, orphanRuleContainerAddress, key[len(orphanContainerAddressPrefix):])
	case strings.HasPrefix(key, orphanContainerAddressMapPrefix):
		return containerOwnedKey(key, orphanRuleContainerAddressMap, key[len(orphanContainerAddressMapPrefix):])

	case strings.HasPrefix(key, orphanStubIndexPrefix):
		return containerIndex(key, orphanRuleStubIndex, key[len(orphanStubIndexPrefix):])
	case strings.HasPrefix(key, orphanWorkspaceIndexPrefix):
		return containerIndex(key, orphanRuleWorkspaceIndex, key[len(orphanWorkspaceIndexPrefix):])
	case strings.HasPrefix(key, orphanWorkerIndexPrefix):
		return containerIndex(key, orphanRuleWorkerIndex, key[len(orphanWorkerIndexPrefix):])

	case strings.HasPrefix(key, orphanWorkerRequestsPrefix):
		// A worker's pending requests live at requests:pending:<worker id>.
		// The colon check keeps them, and anything else nested, out.
		workerId := key[len(orphanWorkerRequestsPrefix):]
		if !isOrphanSweepId(workerId) {
			return orphanCandidate{}, false
		}
		return orphanCandidate{
			key:      key,
			rule:     orphanRuleWorkerRequests,
			ownerKey: common.RedisKeys.SchedulerWorkerState(workerId),
			minAge:   orphanWorkerRequestsMinAge,
		}, true

	case strings.HasPrefix(key, orphanPodPrefix):
		rule, infix := orphanRuleKeepWarmLock, orphanKeepWarmLockInfix
		if !strings.Contains(key, infix) {
			rule, infix = orphanRuleContainerConnections, orphanContainerConnectionsInfix
		}
		at := strings.LastIndex(key, infix)
		if at < 0 {
			return orphanCandidate{}, false
		}
		// The lock holds 1 and the connection key held a small count.
		candidate, ok := containerOwnedKey(key, rule, key[at+len(infix):])
		candidate.idleTimeUnreliable = true
		return candidate, ok

	case strings.HasPrefix(key, orphanReservationWorkspacePrefix):
		at := strings.Index(key, orphanReservationInfix)
		if at < 0 {
			return orphanCandidate{}, false
		}
		workspaceId := key[len(orphanReservationWorkspacePrefix):at]
		containerId := key[at+len(orphanReservationInfix):]
		candidate, ok := containerOwnedKey(key, orphanRuleReservation, containerId)
		if !ok || !isOrphanSweepId(workspaceId) {
			return orphanCandidate{}, false
		}
		candidate.workspaceId = workspaceId
		candidate.containerId = containerId
		return candidate, true
	}

	return orphanCandidate{}, false
}

func containerOwnedKey(key, rule, containerId string) (orphanCandidate, bool) {
	if !isOrphanSweepId(containerId) {
		return orphanCandidate{}, false
	}

	minAge := orphanContainerKeyMinAge
	if strings.HasPrefix(containerId, types.StubTypeVM+"-") {
		minAge = orphanVMKeyMinAge
	}
	return orphanCandidate{
		key:      key,
		rule:     rule,
		ownerKey: common.RedisKeys.SchedulerContainerState(containerId),
		minAge:   minAge,
	}, true
}

func containerIndex(key, rule, id string) (orphanCandidate, bool) {
	if !isOrphanSweepId(id) {
		return orphanCandidate{}, false
	}
	return orphanCandidate{key: key, rule: rule}, true
}

// isOrphanSweepId reports whether id is the last part of a key. An id with a
// colon in it means the key has more parts than the sweep expects.
func isOrphanSweepId(id string) bool {
	return id != "" && !strings.Contains(id, ":")
}

// orphanAgedOut reports whether a key that two sweeps in a row found orphaned
// has been orphaned for as long as its kind requires. Either proof will do:
// this process first saw it orphaned minAge ago (watched), or Redis says
// nobody has read or written it for minAge (idle; zero when Redis does not
// say, or cannot be trusted for this key). The second is how a VM's keys get
// their week when gateways restart more often than that.
func orphanAgedOut(minAge, watched, idle time.Duration) bool {
	return watched >= minAge || idle >= minAge
}

// OrphanSweepStats is what one sweep did.
type OrphanSweepStats struct {
	// Keys the sweep walked over.
	Scanned int
	// Keys and index members removed, by rule.
	Removed map[string]int
	// Orphans that were not old enough yet and are remembered for the next
	// sweep.
	Waiting int
}

// OrphanSweeper removes Redis keys left behind by containers and workers that
// no longer exist.
type OrphanSweeper struct {
	rdb  *common.RedisClient
	lock *common.RedisLock

	// When a sweep of this process first saw a key orphaned, for the orphans
	// its last complete sweep left in place.
	firstSeen map[string]time.Time

	// Replaced in tests.
	now       func() time.Time
	idleTimes func(ctx context.Context, keys []string) ([]time.Duration, bool)
	pause     func(ctx context.Context)
}

func NewOrphanSweeper(rdb *common.RedisClient) *OrphanSweeper {
	s := &OrphanSweeper{
		rdb:       rdb,
		lock:      common.NewRedisLock(rdb),
		firstSeen: map[string]time.Time{},
		now:       time.Now,
		pause:     pauseOrphanSweep,
	}
	s.idleTimes = s.objectIdleTimes
	return s
}

// Run sweeps until ctx is cancelled: first a few minutes after the gateway
// starts, then once per interval.
func (s *OrphanSweeper) Run(ctx context.Context) {
	timer := time.NewTimer(orphanSweepFirstRunDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		// A sweep that ran reports its own error in its summary line.
		if swept, err := s.SweepIfDue(ctx); err != nil && !swept && ctx.Err() == nil {
			log.Error().Err(err).Msg("orphan sweep could not start")
		}
		timer.Reset(orphanSweepInterval)
	}
}

// SweepIfDue runs one sweep unless another gateway replica is sweeping right
// now or one swept within the cooldown. It reports whether this call swept.
func (s *OrphanSweeper) SweepIfDue(ctx context.Context) (bool, error) {
	swept := false
	started := time.Now()
	var stats OrphanSweepStats

	// The lease is held, and kept alive, for as long as the sweep runs.
	err := s.lock.WithLease(ctx, orphanSweepLockKey, common.RedisLockOptions{TtlS: orphanSweepLockTTLSeconds}, func(ctx context.Context) error {
		due, err := s.rdb.SetNX(ctx, orphanSweepRecentKey, 1, orphanSweepCooldown).Result()
		if err != nil || !due {
			return err
		}

		swept = true
		stats, err = s.Sweep(ctx)
		return err
	})
	if common.IsRedisLockNotObtained(err) {
		return false, nil
	}
	if !swept {
		return false, err
	}

	// One line per sweep; nothing is logged per key.
	level := zerolog.InfoLevel
	if err != nil {
		level = zerolog.WarnLevel
	}
	event := log.WithLevel(level).Err(err).
		Dur("duration", time.Since(started)).
		Int("scanned", stats.Scanned).
		Int("waiting", stats.Waiting)
	for _, rule := range orphanSweepRules {
		event = event.Int(rule, stats.Removed[rule])
	}
	event.Msg("orphan sweep finished")
	return true, err
}

// orphanSweepRun is the state of one sweep.
type orphanSweepRun struct {
	now   time.Time
	stats OrphanSweepStats
	// Orphans this sweep left in place, with the time a sweep first saw them.
	waiting map[string]time.Time
	// Set once Redis has refused to report idle time, so it is asked once.
	idleUnavailable bool
}

func (run *orphanSweepRun) remember(key string, firstSeen time.Time) {
	if len(run.waiting) < orphanSweepMaxRemembered {
		run.waiting[key] = firstSeen
	}
}

// Sweep walks the whole keyspace once, a batch at a time, and removes the
// orphans it finds. The caller holds the sweep lock.
func (s *OrphanSweeper) Sweep(ctx context.Context) (OrphanSweepStats, error) {
	run := &orphanSweepRun{
		now:     s.now(),
		stats:   OrphanSweepStats{Removed: map[string]int{}},
		waiting: map[string]time.Time{},
	}

	nodes, err := s.scanNodes(ctx)
	if err != nil {
		return run.stats, err
	}
	for _, node := range nodes {
		cursor := uint64(0)
		for {
			keys, next, err := node.Scan(ctx, cursor, "", orphanSweepBatchSize).Result()
			if err != nil {
				return run.stats, err
			}
			run.stats.Scanned += len(keys)

			// COUNT is a hint; a server may return more keys than asked for.
			for start := 0; start < len(keys); start += orphanSweepBatchSize {
				if err := s.sweepKeys(ctx, run, keys[start:min(start+orphanSweepBatchSize, len(keys))]); err != nil {
					return run.stats, err
				}
			}

			s.pause(ctx)
			if err := ctx.Err(); err != nil {
				return run.stats, err
			}

			cursor = next
			if cursor == 0 {
				break
			}
		}
	}

	if err := s.pruneStateIndex(ctx, run); err != nil {
		return run.stats, err
	}

	// Only a sweep that saw every key replaces what the last one remembered.
	s.firstSeen = run.waiting
	run.stats.Waiting = len(run.waiting)
	return run.stats, nil
}

// scanNodes returns the servers whose keys have to be walked: the server
// itself, or each master of a cluster.
func (s *OrphanSweeper) scanNodes(ctx context.Context) ([]redis.Cmdable, error) {
	cluster, ok := s.rdb.UniversalClient.(*redis.ClusterClient)
	if !ok {
		return []redis.Cmdable{s.rdb.UniversalClient}, nil
	}

	var mu sync.Mutex
	var masters []redis.Cmdable
	err := cluster.ForEachMaster(ctx, func(_ context.Context, master *redis.Client) error {
		mu.Lock()
		defer mu.Unlock()
		masters = append(masters, master)
		return nil
	})
	return masters, err
}

func pauseOrphanSweep(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(orphanSweepPause):
	}
}

// execOrphanPipeline sends a pipeline. The caller checks each command's
// result, so an error reply to one command does not fail the batch; only
// failing to reach Redis does.
func execOrphanPipeline(ctx context.Context, pipe redis.Pipeliner) error {
	_, err := pipe.Exec(ctx)
	var reply redis.Error
	if err == nil || errors.As(err, &reply) {
		return nil
	}
	return err
}

func (s *OrphanSweeper) sweepKeys(ctx context.Context, run *orphanSweepRun, keys []string) error {
	var owned, indexes []orphanCandidate
	for _, key := range keys {
		candidate, ok := classifyOrphanKey(key)
		if !ok {
			continue
		}
		if candidate.isIndex() {
			indexes = append(indexes, candidate)
		} else {
			owned = append(owned, candidate)
		}
	}

	if err := s.removeOrphanedKeys(ctx, run, owned); err != nil {
		return err
	}
	return s.pruneIndexes(ctx, run, indexes)
}

// removeOrphanedKeys removes the candidates that pass every check: the owner
// has no state, the key has no TTL, the last sweep found the same, and the
// key has been orphaned or unused for long enough. A check that fails or
// cannot be made leaves the key alone.
func (s *OrphanSweeper) removeOrphanedKeys(ctx context.Context, run *orphanSweepRun, candidates []orphanCandidate) error {
	if len(candidates) == 0 {
		return nil
	}

	pipe := s.rdb.Pipeline()
	owners := make([]*redis.IntCmd, len(candidates))
	for i, candidate := range candidates {
		owners[i] = pipe.Exists(ctx, candidate.ownerKey)
	}
	if err := execOrphanPipeline(ctx, pipe); err != nil {
		return err
	}
	var orphans []orphanCandidate
	for i, candidate := range candidates {
		if exists, err := owners[i].Result(); err == nil && exists == 0 {
			orphans = append(orphans, candidate)
		}
	}
	if len(orphans) == 0 {
		return nil
	}

	// A key with a TTL removes itself. Redis answers -1 for a key without one.
	pipe = s.rdb.Pipeline()
	ttls := make([]*redis.DurationCmd, len(orphans))
	for i, orphan := range orphans {
		ttls[i] = pipe.TTL(ctx, orphan.key)
	}
	if err := execOrphanPipeline(ctx, pipe); err != nil {
		return err
	}
	var leaked []orphanCandidate
	for i, orphan := range orphans {
		if ttl, err := ttls[i].Result(); err == nil && ttl == time.Duration(-1) {
			leaked = append(leaked, orphan)
		}
	}
	if len(leaked) == 0 {
		return nil
	}

	var remove, undecided []orphanCandidate
	for _, orphan := range leaked {
		firstSeen, seenBefore := s.firstSeen[orphan.key]
		switch {
		case !seenBefore:
			// One sighting proves nothing: the owner's state may be missing
			// for a moment, as while a container is being scheduled. The
			// next sweep decides.
			run.remember(orphan.key, run.now)
		case orphanAgedOut(orphan.minAge, run.now.Sub(firstSeen), 0):
			remove = append(remove, orphan)
		default:
			undecided = append(undecided, orphan)
		}
	}

	// These were orphaned in the last sweep too, but this process has not
	// watched them for as long as their kind requires. Redis may know more.
	idle := s.idleTimesOf(ctx, run, undecided)
	for i, orphan := range undecided {
		firstSeen := s.firstSeen[orphan.key]
		unused := time.Duration(0)
		if idle != nil && !orphan.idleTimeUnreliable {
			unused = idle[i]
		}
		if orphanAgedOut(orphan.minAge, run.now.Sub(firstSeen), unused) {
			remove = append(remove, orphan)
		} else {
			run.remember(orphan.key, firstSeen)
		}
	}
	if len(remove) == 0 {
		return nil
	}

	// One UNLINK per key, so a cluster can send each to the node that has it.
	pipe = s.rdb.Pipeline()
	unlinks := make([]*redis.IntCmd, len(remove))
	for i, orphan := range remove {
		if orphan.rule == orphanRuleReservation {
			continue
		}
		unlinks[i] = pipe.Unlink(ctx, orphan.key)
	}
	if err := execOrphanPipeline(ctx, pipe); err != nil {
		return err
	}
	for i, orphan := range remove {
		if orphan.rule == orphanRuleReservation {
			if s.releaseOrphanedReservation(ctx, orphan) {
				run.stats.Removed[orphan.rule]++
			}
			continue
		}
		if removed, err := unlinks[i].Result(); err == nil {
			run.stats.Removed[orphan.rule] += int(removed)
		}
	}
	return nil
}

// idleTimesOf returns how long each candidate has gone unread and unwritten,
// or nil when Redis does not say.
func (s *OrphanSweeper) idleTimesOf(ctx context.Context, run *orphanSweepRun, candidates []orphanCandidate) []time.Duration {
	if len(candidates) == 0 || run.idleUnavailable {
		return nil
	}

	keys := make([]string, len(candidates))
	for i, candidate := range candidates {
		keys[i] = candidate.key
	}
	idle, tracked := s.idleTimes(ctx, keys)
	if !tracked {
		run.idleUnavailable = true
		return nil
	}
	return idle
}

// objectIdleTimes asks Redis how long ago each key was last read or written.
// It reports false when Redis does not say, as under an LFU eviction policy.
func (s *OrphanSweeper) objectIdleTimes(ctx context.Context, keys []string) ([]time.Duration, bool) {
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.DurationCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.ObjectIdleTime(ctx, key)
	}
	_, _ = pipe.Exec(ctx)

	idle := make([]time.Duration, len(keys))
	for i, cmd := range cmds {
		value, err := cmd.Result()
		if err == redis.Nil {
			// The key went away since it was scanned.
			continue
		}
		if err != nil {
			return nil, false
		}
		idle[i] = value
	}
	return idle, true
}

// releaseOrphanedReservation gives back the quota a vanished container still
// holds, the same way deleting its state would have: the workspace's usage
// counter goes down and the reservation and its index entry are removed. A
// counter that is being rebuilt drops stale reservations itself, so then the
// reservation is left for the rebuild.
func (s *OrphanSweeper) releaseOrphanedReservation(ctx context.Context, reservation orphanCandidate) bool {
	result, err := releaseConcurrencyReservationScript.Run(ctx, s.rdb, []string{
		common.RedisKeys.WorkspaceConcurrencyLimitUsage(reservation.workspaceId),
		reservation.key,
		common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex(reservation.workspaceId),
	}, time.Now().Unix(), reservation.containerId).Result()
	if err != nil {
		return false
	}
	released, _ := result.(int64)
	return released == 1
}

// orphanIndexMember is one entry of an index set: the state key of a
// container.
type orphanIndexMember struct {
	indexKey string
	rule     string
	stateKey string
}

// pruneIndexes removes, from the stub, workspace and worker indexes, the
// members whose container is gone. Reading an index prunes it the same way,
// but an index nobody lists any more (a deleted stub, a dead worker) is only
// pruned here. Redis deletes a set when its last member goes.
func (s *OrphanSweeper) pruneIndexes(ctx context.Context, run *orphanSweepRun, indexes []orphanCandidate) error {
	for start := 0; start < len(indexes); start += orphanSweepIndexBatchSize {
		batch := indexes[start:min(start+orphanSweepIndexBatchSize, len(indexes))]

		pipe := s.rdb.Pipeline()
		pages := make([]*redis.ScanCmd, len(batch))
		for i, index := range batch {
			pages[i] = pipe.SScan(ctx, index.key, 0, "", orphanSweepBatchSize)
		}
		if err := execOrphanPipeline(ctx, pipe); err != nil {
			return err
		}

		var members []orphanIndexMember
		for i, index := range batch {
			// An index that cannot be read is skipped.
			page, cursor, err := pages[i].Result()
			for err == nil {
				for _, member := range page {
					members = append(members, orphanIndexMember{indexKey: index.key, rule: index.rule, stateKey: member})
				}
				if cursor == 0 {
					break
				}
				// A large index does not fit in one page. Deal with the
				// members read so far before reading its next page.
				if err := s.pruneIndexMembers(ctx, run, members); err != nil {
					return err
				}
				members = members[:0]
				s.pause(ctx)
				page, cursor, err = s.rdb.SScan(ctx, index.key, cursor, "", orphanSweepBatchSize).Result()
			}
		}
		if err := s.pruneIndexMembers(ctx, run, members); err != nil {
			return err
		}
	}
	return nil
}

// pruneIndexMembers removes the members whose state key and exit code key
// are both gone, which is the rule listContainerStateByIndex applies. An exit
// code that is still there belongs to a container that just finished, whose
// index entry is read as failure history.
func (s *OrphanSweeper) pruneIndexMembers(ctx context.Context, run *orphanSweepRun, members []orphanIndexMember) error {
	for start := 0; start < len(members); start += orphanSweepBatchSize {
		batch := members[start:min(start+orphanSweepBatchSize, len(members))]

		pipe := s.rdb.Pipeline()
		states := make([]*redis.IntCmd, len(batch))
		for i, member := range batch {
			// Anything that is not a container state key is not ours to judge.
			if strings.HasPrefix(member.stateKey, orphanContainerStatePrefix) {
				states[i] = pipe.Exists(ctx, member.stateKey)
			}
		}
		if err := execOrphanPipeline(ctx, pipe); err != nil {
			return err
		}
		var gone []orphanIndexMember
		for i, member := range batch {
			if states[i] == nil {
				continue
			}
			if exists, err := states[i].Result(); err == nil && exists == 0 {
				gone = append(gone, member)
			}
		}
		if len(gone) == 0 {
			continue
		}

		pipe = s.rdb.Pipeline()
		exitCodes := make([]*redis.IntCmd, len(gone))
		for i, member := range gone {
			containerId := strings.TrimPrefix(member.stateKey, orphanContainerStatePrefix)
			exitCodes[i] = pipe.Exists(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId))
		}
		if err := execOrphanPipeline(ctx, pipe); err != nil {
			return err
		}

		pipe = s.rdb.Pipeline()
		removals := make([]*redis.IntCmd, len(gone))
		for i, member := range gone {
			if exists, err := exitCodes[i].Result(); err == nil && exists == 0 {
				removals[i] = pipe.SRem(ctx, member.indexKey, member.stateKey)
			}
		}
		if err := execOrphanPipeline(ctx, pipe); err != nil {
			return err
		}
		for i, member := range gone {
			if removals[i] == nil {
				continue
			}
			if removed, err := removals[i].Result(); err == nil {
				run.stats.Removed[member.rule] += int(removed)
			}
		}
	}
	return nil
}

// pruneStateIndex removes entries of the container state index whose state
// expired over an hour ago and no longer exists. A deleted state removes its
// own entry; an expired one is otherwise only removed when a gateway starts.
func (s *OrphanSweeper) pruneStateIndex(ctx context.Context, run *orphanSweepRun) error {
	indexKey := common.RedisKeys.SchedulerContainerStateIndex()
	expiredBefore := strconv.FormatInt(run.now.Add(-orphanStateIndexMinAge).Unix(), 10)

	// Entries that are kept stay in the range, so later pages start after them.
	kept := int64(0)
	for {
		stateKeys, err := s.rdb.ZRangeByScore(ctx, indexKey, &redis.ZRangeBy{
			Min:    "-inf",
			Max:    expiredBefore,
			Offset: kept,
			Count:  orphanSweepBatchSize,
		}).Result()
		if err != nil {
			return err
		}
		if len(stateKeys) == 0 {
			return nil
		}

		pipe := s.rdb.Pipeline()
		states := make([]*redis.IntCmd, len(stateKeys))
		for i, stateKey := range stateKeys {
			if strings.HasPrefix(stateKey, orphanContainerStatePrefix) {
				states[i] = pipe.Exists(ctx, stateKey)
			}
		}
		if err := execOrphanPipeline(ctx, pipe); err != nil {
			return err
		}
		var gone []interface{}
		for i, stateKey := range stateKeys {
			if states[i] == nil {
				continue
			}
			if exists, err := states[i].Result(); err == nil && exists == 0 {
				gone = append(gone, stateKey)
			}
		}

		if len(gone) > 0 {
			removed, err := s.rdb.ZRem(ctx, indexKey, gone...).Result()
			if err != nil {
				return err
			}
			run.stats.Removed[orphanRuleStateIndex] += int(removed)
		}
		kept += int64(len(stateKeys) - len(gone))

		s.pause(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}
