package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type TaskRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

const (
	taskStateCleanupGrace = 10 * time.Minute
	taskMonitorLease      = 15 * time.Second
)

func NewTaskRedisRepository(r *common.RedisClient) TaskRepository {
	lock := common.NewRedisLock(r)
	return &TaskRedisRepository{rdb: r, lock: lock}
}

func (r *TaskRedisRepository) ClaimTask(ctx context.Context, workspaceName, stubId, taskId, containerId string) error {
	claimKey := common.RedisKeys.TaskClaim(workspaceName, stubId, taskId)
	claimIndexKey := common.RedisKeys.TaskClaimIndex(workspaceName, stubId)

	ttl := time.Duration(types.MaxTaskTTL) * time.Second
	if entryTTL := r.rdb.TTL(ctx, common.RedisKeys.TaskEntry(workspaceName, stubId, taskId)).Val(); entryTTL > 0 {
		ttl = entryTTL
	}
	pipe := r.rdb.TxPipeline()
	pipe.Set(ctx, claimKey, containerId, ttl)
	pipe.SAdd(ctx, claimIndexKey, taskId)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to claim task <%v>: %w", claimKey, err)
	}
	return nil
}

func (r *TaskRedisRepository) RemoveTaskClaim(ctx context.Context, workspaceName, stubId, taskId string) error {
	claimKey := common.RedisKeys.TaskClaim(workspaceName, stubId, taskId)
	claimIndexKey := common.RedisKeys.TaskClaimIndex(workspaceName, stubId)

	pipe := r.rdb.TxPipeline()
	pipe.Del(ctx, claimKey)
	pipe.SRem(ctx, claimIndexKey, taskId)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to remove task claim <%v>: %w", claimKey, err)
	}
	return nil
}

func (r *TaskRedisRepository) TasksClaimed(ctx context.Context, workspaceName, stubId string) (int, error) {
	tasks, err := r.rdb.SMembers(ctx, common.RedisKeys.TaskClaimIndex(workspaceName, stubId)).Result()
	if err != nil {
		return -1, err
	}

	return len(tasks), nil
}

func (r *TaskRedisRepository) IsClaimed(ctx context.Context, workspaceName, stubId, taskId string) (bool, error) {
	claimKey := common.RedisKeys.TaskClaim(workspaceName, stubId, taskId)
	exists, err := r.rdb.Exists(ctx, claimKey).Result()
	if err != nil {
		return false, fmt.Errorf("failed to retrieve claim task <%v>: %w", claimKey, err)
	}

	return exists > 0, nil
}

func (r *TaskRedisRepository) SetTaskState(ctx context.Context, workspaceName, stubId, taskId string, msg []byte) error {
	indexKey := common.RedisKeys.TaskIndex()
	stubIndexKey := common.RedisKeys.TaskIndexByStub(workspaceName, stubId)
	entryKey := common.RedisKeys.TaskEntry(workspaceName, stubId, taskId)

	pipe := r.rdb.TxPipeline()
	pipe.SAdd(ctx, indexKey, entryKey)
	pipe.Set(ctx, entryKey, msg, taskStateTTL(msg))
	pipe.SAdd(ctx, stubIndexKey, taskId)
	if _, err := pipe.Exec(ctx); err != nil {
		r.DeleteTaskState(ctx, workspaceName, stubId, taskId)
		return err
	}
	return nil
}

func taskStateTTL(msg []byte) time.Duration {
	var state struct {
		Policy struct {
			Expires time.Time `json:"expires"`
		} `json:"policy"`
	}
	if json.Unmarshal(msg, &state) == nil && !state.Policy.Expires.IsZero() {
		if ttl := time.Until(state.Policy.Expires) + taskStateCleanupGrace; ttl > time.Minute {
			return ttl
		}
		return time.Minute
	}
	return time.Duration(types.MaxTaskTTL)*time.Second + taskStateCleanupGrace
}

func (r *TaskRedisRepository) WithTaskMonitorLease(ctx context.Context, fn func(context.Context) error) error {
	return r.lock.WithLease(ctx, common.RedisKeys.TaskMonitorLock(), common.RedisLockOptions{
		TtlS: int(taskMonitorLease.Seconds()),
	}, fn)
}

func (r *TaskRedisRepository) GetTaskState(ctx context.Context, workspaceName, stubId, taskId string) (*types.TaskMessage, error) {
	msg, err := r.rdb.Get(ctx, common.RedisKeys.TaskEntry(workspaceName, stubId, taskId)).Bytes()
	if err != nil {
		return nil, err
	}

	taskMessage := &types.TaskMessage{}
	taskMessage.Decode(msg)
	return taskMessage, nil
}

func (r *TaskRedisRepository) TasksInFlight(ctx context.Context, workspaceName, stubId string) (int, error) {
	tasks, err := r.rdb.SMembers(ctx, common.RedisKeys.TaskIndexByStub(workspaceName, stubId)).Result()
	if err != nil {
		return -1, err
	}

	return len(tasks), nil
}

func (r *TaskRedisRepository) DeleteTaskState(ctx context.Context, workspaceName, stubId, taskId string) error {
	indexKey := common.RedisKeys.TaskIndex()
	entryKey := common.RedisKeys.TaskEntry(workspaceName, stubId, taskId)
	claimKey := common.RedisKeys.TaskClaim(workspaceName, stubId, taskId)
	claimIndexKey := common.RedisKeys.TaskClaimIndex(workspaceName, stubId)
	stubIndexKey := common.RedisKeys.TaskIndexByStub(workspaceName, stubId)

	_, err := r.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.SRem(ctx, indexKey, entryKey)
		pipe.SRem(ctx, claimIndexKey, taskId)
		pipe.SRem(ctx, stubIndexKey, taskId)
		pipe.Del(ctx, entryKey)
		pipe.Del(ctx, claimKey)
		return nil
	})

	return err
}

func (r *TaskRedisRepository) GetTasksInFlight(ctx context.Context) ([]*types.TaskMessage, error) {
	taskMessages := []*types.TaskMessage{}
	tasks, err := r.rdb.SMembers(ctx, common.RedisKeys.TaskIndex()).Result()
	if err != nil {
		return nil, err
	}

	for _, taskKey := range tasks {
		msg, err := r.rdb.Get(ctx, taskKey).Bytes()
		if err != nil {
			if err == redis.Nil {
				// Task key exists in index but actual task data doesn't exist
				// This is an inconsistent state, so let's clean it up properly
				// Parse the task key to extract workspace, stub, and task IDs
				parts := strings.Split(taskKey, ":")
				if len(parts) >= 4 {
					workspaceName := parts[1]
					stubId := parts[2]
					taskId := parts[3]

					err = r.DeleteTaskState(ctx, workspaceName, stubId, taskId)
					if err != nil {
						log.Error().Str("task_id", taskId).Err(err).Msg("failed to delete task state")
					}
				}
			}

			continue
		}

		taskMessage := &types.TaskMessage{}
		taskMessage.Decode(msg)
		taskMessages = append(taskMessages, taskMessage)
	}

	return taskMessages, nil
}

func (r *TaskRedisRepository) SetTaskRetryLock(ctx context.Context, workspaceName, stubId, taskId string) error {
	err := r.lock.Acquire(ctx, common.RedisKeys.TaskRetryLock(workspaceName, stubId, taskId), common.RedisLockOptions{TtlS: 300, Retries: 0})
	if err != nil {
		return err
	}

	return nil
}

func (r *TaskRedisRepository) RemoveTaskRetryLock(ctx context.Context, workspaceName, stubId, taskId string) error {
	return r.lock.Release(common.RedisKeys.TaskRetryLock(workspaceName, stubId, taskId))
}
