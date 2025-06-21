package repository

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

type TaskRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func NewTaskRedisRepository(r *common.RedisClient) TaskRepository {
	lock := common.NewRedisLock(r)
	return &TaskRedisRepository{rdb: r, lock: lock}
}

func (r *TaskRedisRepository) ClaimTask(ctx context.Context, workspaceName, stubId, taskId, containerId string) error {
	claimKey := common.RedisKeys.TaskClaim(workspaceName, stubId, taskId)
	claimIndexKey := common.RedisKeys.TaskClaimIndex(workspaceName, stubId)

	err := r.rdb.Set(ctx, claimKey, containerId, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to claim task <%v>: %w", claimKey, err)
	}

	err = r.rdb.SAdd(ctx, claimIndexKey, taskId).Err()
	if err != nil {
		return fmt.Errorf("failed to add task to claim index <%v>: %w", claimIndexKey, err)
	}

	return nil
}

func (r *TaskRedisRepository) RemoveTaskClaim(ctx context.Context, workspaceName, stubId, taskId string) error {
	claimKey := common.RedisKeys.TaskClaim(workspaceName, stubId, taskId)
	claimIndexKey := common.RedisKeys.TaskClaimIndex(workspaceName, stubId)

	err := r.rdb.Del(ctx, claimKey).Err()
	if err != nil {
		return fmt.Errorf("failed to remove task claim <%v>: %w", claimKey, err)
	}

	err = r.rdb.SRem(ctx, claimIndexKey, taskId).Err()
	if err != nil {
		return fmt.Errorf("failed to remove task from claim index <%v>: %w", claimIndexKey, err)
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

	err := r.rdb.SAdd(ctx, indexKey, entryKey).Err()
	if err != nil {
		return fmt.Errorf("failed to add task key to index <%v>: %w", indexKey, err)
	}

	err = r.rdb.SAdd(ctx, stubIndexKey, taskId).Err()
	if err != nil {
		return fmt.Errorf("failed to add task key to stub index <%v>: %w", indexKey, err)
	}

	err = r.rdb.Set(ctx, entryKey, msg, 0).Err()
	if err != nil {
		r.DeleteTaskState(ctx, workspaceName, stubId, taskId)
		return err
	}

	return nil
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
				// This is an inconsistent state, so let's clean it up
				r.rdb.SRem(ctx, common.RedisKeys.TaskIndex(), taskKey)
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
