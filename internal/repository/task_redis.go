package repository

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
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
	err := r.rdb.Set(ctx, claimKey, containerId, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to claim task <%v>: %w", claimKey, err)
	}

	return nil
}

func (r *TaskRedisRepository) TasksClaimed(ctx context.Context, workspaceName, stubId string) (int, error) {
	keys, err := r.rdb.Scan(ctx, common.RedisKeys.TaskClaim(workspaceName, stubId, "*"))
	if err != nil {
		return -1, err
	}

	return len(keys), nil
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
	entryKey := common.RedisKeys.TaskEntry(workspaceName, stubId, taskId)

	err := r.rdb.SAdd(ctx, indexKey, entryKey).Err()
	if err != nil {
		return fmt.Errorf("failed to add task key to index <%v>: %w", indexKey, err)
	}

	err = r.rdb.Set(ctx, entryKey, msg, 0).Err()
	if err != nil {
		r.DeleteTaskState(ctx, workspaceName, stubId, taskId)
		return err
	}

	return nil
}

func (r *TaskRedisRepository) DeleteTaskState(ctx context.Context, workspaceName, stubId, taskId string) error {
	indexKey := common.RedisKeys.TaskIndex()
	entryKey := common.RedisKeys.TaskEntry(workspaceName, stubId, taskId)
	claimKey := common.RedisKeys.TaskClaim(workspaceName, stubId, taskId)

	err := r.rdb.SRem(ctx, indexKey, entryKey).Err()
	if err != nil {
		return err
	}

	r.rdb.Del(ctx, entryKey)
	r.rdb.Del(ctx, claimKey)

	return nil
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
			continue
		}

		taskMessage := &types.TaskMessage{}
		taskMessage.Decode(msg)
		taskMessages = append(taskMessages, taskMessage)
	}

	return taskMessages, nil
}
