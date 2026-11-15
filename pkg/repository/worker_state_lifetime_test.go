package repository

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// A worker's state expires once the worker has sent no keepalive for
// types.WorkerStateTtlS. That is how a worker that died is noticed, and what
// returns the requests queued on it to the backlog (see forgetLostWorker). A
// pool controller saves the state too, each time it reconciles its pool, and
// that must not keep the state of a worker that died alive.

const workerKeepAliveLifetime = time.Duration(types.WorkerStateTtlS) * time.Second

func TestSavingAWorkerRenewsItsStateOnlyWhileItIsStarting(t *testing.T) {
	ctx := context.TODO()
	const left = 30 * time.Second

	// lifetimeAfterSaving returns how long the state of a worker has left
	// once its controller has saved it again. The worker is added with the
	// given status; became, if set, is what happens to it before then.
	lifetimeAfterSaving := func(t *testing.T, status types.WorkerStatus, became func(repo *WorkerRedisRepository, id string) error) (*WorkerRedisRepository, time.Duration) {
		rdb, err := NewRedisClientForTest()
		require.NoError(t, err)
		repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)
		worker := &types.Worker{Id: "worker-1", MachineId: "machine-1", Status: status, PoolName: "joined-a10g"}
		require.NoError(t, repo.AddWorker(worker))
		key := common.RedisKeys.SchedulerWorkerState(worker.Id)
		require.Equal(t, repo.config.CleanupPendingWorkerAgeLimit, rdb.TTL(ctx, key).Val(), "a new worker has the pending lifetime to start in")
		if became != nil {
			require.NoError(t, became(repo, worker.Id))
		}

		// Most of its lifetime has passed when its pool is reconciled.
		require.NoError(t, rdb.Expire(ctx, key, left).Err())
		current, err := repo.GetWorkerById(worker.Id)
		require.NoError(t, err)
		require.NoError(t, repo.AddWorker(current))
		return repo, rdb.TTL(ctx, key).Val()
	}

	t.Run("a worker that is starting gets its full lifetime again", func(t *testing.T) {
		repo, lifetime := lifetimeAfterSaving(t, types.WorkerStatusPending, nil)
		require.Equal(t, repo.config.CleanupPendingWorkerAgeLimit, lifetime)
	})

	t.Run("so does one that is being replaced, which is silent until its replacement starts", func(t *testing.T) {
		repo, lifetime := lifetimeAfterSaving(t, types.WorkerStatusAvailable, func(repo *WorkerRedisRepository, id string) error {
			_, err := repo.PrepareWorkerRollout(id, "generation-2")
			return err
		})
		require.Equal(t, repo.config.CleanupPendingWorkerAgeLimit, lifetime)
	})

	t.Run("a worker that is up lives by its own keepalives", func(t *testing.T) {
		_, lifetime := lifetimeAfterSaving(t, types.WorkerStatusAvailable, nil)
		require.Equal(t, left, lifetime)
	})

	t.Run("and so does one that is cordoned", func(t *testing.T) {
		_, lifetime := lifetimeAfterSaving(t, types.WorkerStatusAvailable, func(repo *WorkerRedisRepository, id string) error {
			return repo.SetWorkerCordon(id, true)
		})
		require.Equal(t, left, lifetime)
	})
}

// expireBeforeSave makes a worker's state expire after it has been read and
// before the transaction that saves it.
type expireBeforeSave struct {
	rdb *common.RedisClient
	key string
}

func (h *expireBeforeSave) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *expireBeforeSave) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (h *expireBeforeSave) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if cmd.Name() == "hset" && len(cmd.Args()) > 1 && cmd.Args()[1] == h.key {
				if err := h.rdb.Del(ctx, h.key).Err(); err != nil {
					return err
				}
				break
			}
		}
		return next(ctx, cmds)
	}
}

// A state that is saved without a lifetime has to have one already. If it
// expired a moment before, the save would make a state that never expires:
// a worker that died would stay on record, taking requests, for good.
func TestSavingAWorkerWhoseStateJustExpiredDoesNotLeaveItOnRecordForGood(t *testing.T) {
	ctx := context.TODO()
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)

	worker := &types.Worker{Id: "worker-1", MachineId: "machine-1", Status: types.WorkerStatusAvailable, PoolName: "joined-a10g"}
	require.NoError(t, repo.AddWorker(worker))
	current, err := repo.GetWorkerById(worker.Id)
	require.NoError(t, err)

	key := common.RedisKeys.SchedulerWorkerState(worker.Id)
	rdb.AddHook(&expireBeforeSave{rdb: rdb, key: key})
	require.NoError(t, repo.AddWorker(current))

	require.Equal(t, workerKeepAliveLifetime, rdb.TTL(ctx, key).Val(), "it has one keepalive's time to show that it is alive")
}

// A worker that is starting sends no keepalive yet. The pool that waits for
// it gives its state its full lifetime again; one that is up lives by its
// own.
func TestKeepPendingWorker(t *testing.T) {
	ctx := context.TODO()
	newWorker := func(t *testing.T, status types.WorkerStatus) (*WorkerRedisRepository, string) {
		rdb, err := NewRedisClientForTest()
		require.NoError(t, err)
		repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)
		require.NoError(t, repo.AddWorker(&types.Worker{Id: "worker-1", MachineId: "machine-1", Status: status, PoolName: "spot-l40s"}))
		// Most of its lifetime has passed.
		key := common.RedisKeys.SchedulerWorkerState("worker-1")
		require.NoError(t, repo.rdb.Expire(ctx, key, 30*time.Second).Err())
		return repo, key
	}

	t.Run("a pending worker gets its full lifetime again", func(t *testing.T) {
		repo, key := newWorker(t, types.WorkerStatusPending)
		kept, err := repo.KeepPendingWorker("worker-1")
		require.NoError(t, err)
		require.True(t, kept)
		require.Equal(t, repo.config.CleanupPendingWorkerAgeLimit, repo.rdb.TTL(ctx, key).Val())
	})

	t.Run("a worker that is up lives by its own heartbeat", func(t *testing.T) {
		repo, key := newWorker(t, types.WorkerStatusAvailable)
		kept, err := repo.KeepPendingWorker("worker-1")
		require.NoError(t, err)
		require.True(t, kept)
		require.Equal(t, 30*time.Second, repo.rdb.TTL(ctx, key).Val())
	})

	t.Run("a worker that is gone is not made again", func(t *testing.T) {
		repo, _ := newWorker(t, types.WorkerStatusPending)
		kept, err := repo.KeepPendingWorker("nobody")
		require.NoError(t, err)
		require.False(t, kept)
		_, err = repo.GetWorkerById("nobody")
		var notFound *types.ErrWorkerNotFound
		require.ErrorAs(t, err, &notFound)
	})
}
