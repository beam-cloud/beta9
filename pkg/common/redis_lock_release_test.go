package common

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// A RedisLock remembers each lock it holds until the lock is released. A
// lock that is left to expire, or that Redis cannot be asked to release, is
// gone all the same, and must not be remembered for as long as the process
// runs: a caller that takes one lock per container would never stop growing.

func newRedisLockForTest(t *testing.T) (*RedisLock, *RedisClient, *miniredis.Miniredis) {
	server := miniredis.RunT(t)
	rdb, err := NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	return NewRedisLock(rdb), rdb, server
}

func TestReleasingALockThatExpiredForgetsIt(t *testing.T) {
	lock, _, server := newRedisLockForTest(t)
	ctx := context.Background()

	for _, key := range []string{"first", "second", "third"} {
		require.NoError(t, lock.Acquire(ctx, key, RedisLockOptions{TtlS: 1}))
		server.Del(key) // the lock was held for longer than its TTL
		require.ErrorIs(t, lock.Release(key), redislock.ErrLockNotHeld)

		_, err := lock.Token(key)
		require.ErrorIs(t, err, redislock.ErrLockNotHeld)
	}
	require.Empty(t, lock.locks)
}

func TestReleasingALockForgetsItWhenRedisDoesNotAnswer(t *testing.T) {
	lock, _, server := newRedisLockForTest(t)
	const key = "lock"
	require.NoError(t, lock.Acquire(context.Background(), key, RedisLockOptions{TtlS: 1}))

	server.SetError("LOADING Redis is loading the dataset in memory")
	err := lock.Release(key)
	require.Error(t, err)
	require.NotErrorIs(t, err, redislock.ErrLockNotHeld, "the caller is told that Redis failed")
	require.Empty(t, lock.locks)
}

// beforeNextCommand runs a function once, before the next command reaches
// Redis.
type beforeNextCommand struct {
	run func()
}

func (h *beforeNextCommand) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *beforeNextCommand) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *beforeNextCommand) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if run := h.run; run != nil {
			h.run = nil
			run()
		}
		return next(ctx, cmd)
	}
}

// What is forgotten is the lock that was released. The same key may have
// been locked again since: by the time the first holder's release reaches
// Redis its lock has expired, and someone else in this process holds the key.
func TestReleasingALockThatExpiredKeepsTheOneThatWasTakenSince(t *testing.T) {
	lock, rdb, server := newRedisLockForTest(t)
	ctx := context.Background()
	const key = "lock"
	opts := RedisLockOptions{TtlS: 1}
	require.NoError(t, lock.Acquire(ctx, key, opts))
	first, err := lock.Token(key)
	require.NoError(t, err)

	hook := &beforeNextCommand{}
	rdb.AddHook(hook)
	hook.run = func() {
		server.Del(key)
		require.NoError(t, lock.Acquire(ctx, key, opts))
	}
	require.ErrorIs(t, lock.Release(key), redislock.ErrLockNotHeld)

	second, err := lock.Token(key)
	require.NoError(t, err, "the lock that was taken since is still held")
	require.NotEqual(t, first, second)
	require.NoError(t, lock.Release(key))
	require.Empty(t, lock.locks)
}
