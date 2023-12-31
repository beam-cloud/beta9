package repository

import (
	"context"
	"time"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/types"
	"github.com/redis/go-redis/v9"
)

type TaskRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

const defaultTaskRunningExpiration int = 60

func NewTaskRedisRepository(r *common.RedisClient) TaskRepository {
	lock := common.NewRedisLock(r)
	return &TaskRedisRepository{rdb: r, lock: lock}
}

func (t *TaskRedisRepository) GetNextTask(queueName, containerId, identityExternalId string) ([]byte, error) {
	// Check if there are any tasks in the queue
	queueLength, err := t.rdb.LLen(context.TODO(), common.RedisKeys.QueueList(identityExternalId, queueName)).Result()
	if err != nil {
		return nil, err
	}

	if queueLength == 0 {
		return nil, nil
	}

	// Set a lock to prevent container from spinning down while processing this item in the queue
	err = t.rdb.Set(context.TODO(), common.RedisKeys.QueueProcessingLock(identityExternalId, queueName, containerId), 1, 0).Err()
	if err != nil {
		return nil, err
	}
	defer func() {
		t.rdb.Del(context.TODO(), common.RedisKeys.QueueProcessingLock(identityExternalId, queueName, containerId))
	}()

	// Now actually pop the task from the queue
	task, err := t.rdb.LPop(context.TODO(), common.RedisKeys.QueueList(identityExternalId, queueName)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}

		return nil, err
	}

	var tm types.TaskMessage
	err = tm.Decode(task)
	if err != nil {
		return nil, err
	}

	// Set a lock to prevent spin-down during decoding (decoding can take a long time for larger payloads)
	err = t.rdb.SetEx(context.TODO(), common.RedisKeys.QueueTaskRunningLock(identityExternalId, queueName, containerId, tm.ID), 1, time.Duration(defaultTaskRunningExpiration)*time.Second).Err()
	if err != nil {
		return nil, err
	}

	// Set a heartbeat to prevent retries before task actually starts processing
	err = t.rdb.SetEx(context.TODO(), common.RedisKeys.QueueTaskHeartbeat(identityExternalId, queueName, tm.ID), 1, 60*time.Second).Err()
	if err != nil {
		return nil, err
	}

	// Set the task claim
	err = t.rdb.Set(context.TODO(), common.RedisKeys.QueueTaskClaim(identityExternalId, queueName, tm.ID), task, 0).Err()
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (t *TaskRedisRepository) GetTasksInFlight(queueName, identityExternalId string) (int, error) {
	val, err := t.rdb.Get(context.TODO(), common.RedisKeys.QueueTasksInFlight(identityExternalId, queueName)).Int()
	if err != nil {
		return 0, err
	}

	return val, nil
}

func (t *TaskRedisRepository) IncrementTasksInFlight(queueName, identityExternalId string) error {
	err := t.rdb.Incr(context.TODO(), common.RedisKeys.QueueTasksInFlight(identityExternalId, queueName)).Err()
	if err != nil {
		return err
	}

	err = t.rdb.Expire(context.TODO(), common.RedisKeys.QueueTasksInFlight(identityExternalId, queueName), types.RequestTimeoutDurationS).Err()
	if err != nil {
		return err
	}

	return nil
}

func (t *TaskRedisRepository) DecrementTasksInFlight(queueName, identityExternalId string) error {
	err := t.rdb.Decr(context.TODO(), common.RedisKeys.QueueTasksInFlight(identityExternalId, queueName)).Err()
	if err != nil {
		return err
	}

	err = t.rdb.Expire(context.TODO(), common.RedisKeys.QueueTasksInFlight(identityExternalId, queueName), types.RequestTimeoutDurationS).Err()
	if err != nil {
		return err
	}

	return nil
}

func (t *TaskRedisRepository) StartTask(taskId, queueName, containerId, identityExternalId string) error {
	err := t.rdb.SetEx(context.TODO(), common.RedisKeys.QueueTaskRunningLock(identityExternalId, queueName, containerId, taskId), 1, time.Duration(defaultTaskRunningExpiration)*time.Second).Err()
	if err != nil {
		return err
	}

	return nil
}

func (t *TaskRedisRepository) EndTask(taskId, queueName, containerId, containerHostname, identityExternalId string, taskDuration float64, scaleDownDelay float64) error {
	err := t.rdb.SetEx(context.TODO(), common.RedisKeys.QueueKeepWarmLock(identityExternalId, queueName, containerId), 1, time.Duration(scaleDownDelay)*time.Second).Err()
	if err != nil {
		return err
	}

	err = t.rdb.Del(context.TODO(), common.RedisKeys.QueueTaskClaim(identityExternalId, queueName, taskId)).Err()
	if err != nil {
		return err
	}

	err = t.rdb.Del(context.TODO(), common.RedisKeys.QueueTaskRunningLock(identityExternalId, queueName, containerId, taskId)).Err()
	if err != nil {
		return err
	}

	err = t.rdb.RPush(context.TODO(), common.RedisKeys.QueueTaskDuration(identityExternalId, queueName), taskDuration).Err()
	if err != nil {
		return err
	}

	err = t.rdb.Publish(context.TODO(), common.RedisKeys.QueueTaskCompleteEvent(identityExternalId, queueName, taskId), containerHostname).Err()
	if err != nil {
		return err
	}

	return nil
}
