package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/stretchr/testify/require"
)

// Three kinds of work are done for a pool by one scheduler replica at a
// time: sizing it, reporting its state and cleaning up after its workers.
// Each has a lock, held for as long as the work runs.

// poolLocksFixture is a Redis with two scheduler replicas' view of its pool
// locks, and a count of the round trips made to it while counting is on.
type poolLocksFixture struct {
	server        *miniredis.Miniredis
	first, second *WorkerPoolRedisRepository
	roundTrips    *redisRoundTrips
}

func newPoolLocksForTest(t *testing.T) *poolLocksFixture {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	roundTrips := &redisRoundTrips{}
	rdb.AddHook(roundTrips)
	replica := func() *WorkerPoolRedisRepository {
		return NewWorkerPoolRedisRepositoryForTest(rdb).(*WorkerPoolRedisRepository)
	}
	return &poolLocksFixture{server: server, first: replica(), second: replica(), roundTrips: roundTrips}
}

func TestEachKindOfPoolWorkHasItsOwnLock(t *testing.T) {
	type lock func(*WorkerPoolRedisRepository, string, func()) error
	sizer := lock((*WorkerPoolRedisRepository).WithWorkerPoolSizerLock)
	state := lock((*WorkerPoolRedisRepository).WithWorkerPoolStateLock)
	cleaner := lock((*WorkerPoolRedisRepository).WithWorkerPoolCleanerLock)

	tests := []struct {
		name string
		lock lock
		key  string
		ttl  time.Duration
		// attempts is how often a replica asks for a lock that is held
		// before it leaves the work to the holder.
		attempts int64
		others   []lock
	}{
		{"sizing", sizer, common.RedisKeys.WorkerPoolSizerLock("gpu"), 30 * time.Second, 1, []lock{state, cleaner}},
		{"reporting state", state, common.RedisKeys.WorkerPoolStateLock("gpu"), 10 * time.Second, 1, []lock{sizer, cleaner}},
		{"cleaning", cleaner, common.RedisKeys.WorkerPoolCleanerLock("gpu"), 10 * time.Second, 4, []lock{sizer, state}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newPoolLocksForTest(t)
			server := f.server
			ran := func(lock lock, pool string) (bool, error) {
				ran := false
				err := lock(f.second, pool, func() { ran = true })
				return ran, err
			}

			err := test.lock(f.first, "gpu", func() {
				require.True(t, server.Exists(test.key))
				require.Equal(t, test.ttl, server.TTL(test.key))

				// Another replica leaves the work to this one.
				f.roundTrips.enabled.Store(true)
				did, err := ran(test.lock, "gpu")
				f.roundTrips.enabled.Store(false)
				require.ErrorIs(t, err, redislock.ErrNotObtained)
				require.False(t, did)
				require.Equal(t, test.attempts, f.roundTrips.calls.Load())

				// The pool's other work is not held up, nor is another pool.
				for _, other := range test.others {
					did, err := ran(other, "gpu")
					require.NoError(t, err)
					require.True(t, did)
				}
				did, err = ran(test.lock, "cpu")
				require.NoError(t, err)
				require.True(t, did)
			})
			require.NoError(t, err)

			// The lock is given up when the work is done.
			require.False(t, server.Exists(test.key))
			did, err := ran(test.lock, "gpu")
			require.NoError(t, err)
			require.True(t, did)
		})
	}
}

// A pass can take longer than its lock's TTL: cleaning lists every Job of
// the pool and each Job's pods. The lock is renewed while the pass runs, so
// that a second replica does not start the same work beside it.
func TestAPoolLockOutlastsItsTTLWhileItsWorkRuns(t *testing.T) {
	f := newPoolLocksForTest(t)
	server, first, second := f.server, f.first, f.second
	const key = "pool-lock"
	// A short TTL, so that the test need not wait long for a renewal.
	options := common.RedisLockOptions{TtlS: 1}

	working, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- first.withPoolLock(key, options, func() {
			close(working)
			<-finish
		})
	}()
	<-working

	// The work goes on for three times the lock's TTL.
	for range 3 {
		server.FastForward(900 * time.Millisecond)
		require.Eventually(t, func() bool { return server.TTL(key) > 900*time.Millisecond },
			2*time.Second, 10*time.Millisecond, "the lock was not renewed")
	}
	ran := false
	err := second.withPoolLock(key, options, func() { ran = true })
	require.ErrorIs(t, err, redislock.ErrNotObtained)
	require.False(t, ran)

	close(finish)
	require.NoError(t, <-done)
	require.NoError(t, second.withPoolLock(key, options, func() { ran = true }))
	require.True(t, ran)
}

// The lock that is held is the one the previous release takes and gives up,
// so that replicas of both releases keep each other out while a release is
// rolled out.
func TestAPoolLockKeepsOutAReplicaOfThePreviousRelease(t *testing.T) {
	f := newPoolLocksForTest(t)
	server, replica := f.server, f.first
	previous := common.NewRedisLock(replica.rdb)
	ctx := context.Background()

	for _, lock := range []struct {
		key     string
		options common.RedisLockOptions
		with    func(string, func()) error
	}{
		{common.RedisKeys.WorkerPoolStateLock("gpu"), common.RedisLockOptions{TtlS: 10, Retries: 0}, replica.WithWorkerPoolStateLock},
		{common.RedisKeys.WorkerPoolCleanerLock("gpu"), common.RedisLockOptions{TtlS: 10, Retries: 3}, replica.WithWorkerPoolCleanerLock},
	} {
		// While this release works, the previous one cannot take the lock.
		require.NoError(t, lock.with("gpu", func() {
			require.ErrorIs(t, previous.Acquire(ctx, lock.key, lock.options), redislock.ErrNotObtained)
		}))

		// While the previous one holds it, this release does not work.
		require.NoError(t, previous.Acquire(ctx, lock.key, lock.options))
		ran := false
		require.ErrorIs(t, lock.with("gpu", func() { ran = true }), redislock.ErrNotObtained)
		require.False(t, ran)
		require.NoError(t, previous.Release(lock.key))
		require.False(t, server.Exists(lock.key))
	}
}

func TestOnlyOneReplicaSizesAPoolAtATime(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	first, second := NewWorkerPoolRedisRepositoryForTest(rdb), NewWorkerPoolRedisRepositoryForTest(rdb)

	// The first replica is in the middle of sizing: buying a machine, say.
	sizing, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- first.WithWorkerPoolSizerLock("spot-l40s", func() {
			close(sizing)
			<-finish
		})
	}()
	<-sizing

	ran := false
	err = second.WithWorkerPoolSizerLock("spot-l40s", func() { ran = true })
	require.True(t, errors.Is(err, redislock.ErrNotObtained), "got %v", err)
	require.False(t, ran)

	// Another pool is not held up.
	require.NoError(t, second.WithWorkerPoolSizerLock("spot-a10g", func() { ran = true }))
	require.True(t, ran)

	// Once the first is done, the next tick of either may size the pool.
	close(finish)
	require.NoError(t, <-done)
	ran = false
	require.NoError(t, second.WithWorkerPoolSizerLock("spot-l40s", func() { ran = true }))
	require.True(t, ran)
}
