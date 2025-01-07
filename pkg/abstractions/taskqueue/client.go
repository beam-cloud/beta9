package taskqueue

import (
	"context"
	"strconv"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

const defaultTaskRunningExpiration int = 60

type taskQueueClient struct {
	rdb      *common.RedisClient
	taskRepo repository.TaskRepository
}

func newRedisTaskQueueClient(rdb *common.RedisClient, taskRepo repository.TaskRepository) *taskQueueClient {
	return &taskQueueClient{
		rdb:      rdb,
		taskRepo: taskRepo,
	}
}

// Add a new task to the queue
func (qc *taskQueueClient) Push(ctx context.Context, taskMessage *types.TaskMessage) error {
	encodedMessage, err := taskMessage.Encode()
	if err != nil {
		return err
	}

	err = qc.rdb.RPush(ctx, Keys.taskQueueList(taskMessage.WorkspaceName, taskMessage.StubId), encodedMessage).Err()
	if err != nil {
		return err
	}

	return nil
}

func (qc *taskQueueClient) Pop(ctx context.Context, workspaceName, stubId, containerId string) ([]byte, error) {
	queueLength, err := qc.rdb.LLen(ctx, Keys.taskQueueList(workspaceName, stubId)).Result()
	if err != nil {
		return nil, err
	}

	if queueLength == 0 {
		return nil, nil
	}

	// Set a lock to prevent container from spinning down while processing this item in the queue
	err = qc.rdb.Set(ctx, Keys.taskQueueProcessingLock(workspaceName, stubId, containerId), 1, 0).Err()
	if err != nil {
		return nil, err
	}
	defer func() {
		qc.rdb.Del(ctx, Keys.taskQueueProcessingLock(workspaceName, stubId, containerId))
	}()

	// Now actually pop the task from the queue
	task, err := qc.rdb.LPop(ctx, Keys.taskQueueList(workspaceName, stubId)).Bytes()
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

	err = qc.rdb.SAdd(ctx, Keys.taskQueueTaskRunningLockIndex(workspaceName, stubId, containerId), tm.TaskId).Err()
	if err != nil {
		return nil, err
	}

	// Set a lock to prevent spin-down during decoding (decoding can take a long time for larger payloads)
	err = qc.rdb.SetEx(ctx, Keys.taskQueueTaskRunningLock(workspaceName, stubId, containerId, tm.TaskId), 1, time.Duration(defaultTaskRunningExpiration)*time.Second).Err()
	if err != nil {
		return nil, err
	}

	// Set a heartbeat to prevent retries before task actually starts processing
	err = qc.rdb.SetEx(ctx, Keys.taskQueueTaskHeartbeat(workspaceName, stubId, tm.TaskId), 1, 60*time.Second).Err()
	if err != nil {
		return nil, err
	}

	return task, nil
}

// Get queue length
func (qc *taskQueueClient) QueueLength(ctx context.Context, workspaceName, stubId string) (int64, error) {
	res, err := qc.rdb.LLen(ctx, Keys.taskQueueList(workspaceName, stubId)).Result()
	if err != nil {
		return -1, err
	}

	return res, nil
}

// Check how many tasks are running (which is the same as the number of tasks "claimed")
func (qc *taskQueueClient) TasksRunning(ctx context.Context, workspaceName, stubId string) (int, error) {
	nTasks, err := qc.taskRepo.TasksClaimed(ctx, workspaceName, stubId)
	if err != nil {
		return -1, err
	}

	return nTasks, nil
}

// Get most recent task duration
func (qc *taskQueueClient) GetTaskDuration(ctx context.Context, workspaceName, stubId string) (float64, error) {
	res, err := qc.rdb.LPop(ctx, Keys.taskQueueTaskDuration(workspaceName, stubId)).Result()
	if err != nil {
		return -1, err
	}

	duration, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return -1, err
	}

	return duration, nil
}

func (qc *taskQueueClient) SetAverageTaskDuration(ctx context.Context, workspaceName, stubId string, duration float64) error {
	err := qc.rdb.Set(ctx, Keys.taskQueueAverageTaskDuration(workspaceName, stubId), duration, 0).Err()
	if err != nil {
		return err
	}
	return nil
}

func (qc *taskQueueClient) GetAverageTaskDuration(ctx context.Context, workspaceName, stubId string) (float64, error) {
	res, err := qc.rdb.Get(ctx, Keys.taskQueueAverageTaskDuration(workspaceName, stubId)).Result()
	if err != nil {
		return -1, err
	}

	duration, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return -1, err
	}

	return duration, nil
}
