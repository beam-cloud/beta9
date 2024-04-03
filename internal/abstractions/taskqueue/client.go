package taskqueue

import (
	"context"
	"strconv"
	"time"

	common "github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/redis/go-redis/v9"
)

const defaultTaskRunningExpiration int = 60

type taskQueueClient struct {
	rdb *common.RedisClient
}

func newRedisTaskQueueClient(rdb *common.RedisClient) *taskQueueClient {
	return &taskQueueClient{rdb: rdb}
}

// Add a new task to the queue
func (qc *taskQueueClient) Push(taskMessage *types.TaskMessage) error {
	encodedMessage, err := taskMessage.Encode()
	if err != nil {
		return err
	}

	err = qc.rdb.RPush(context.TODO(), Keys.taskQueueList(taskMessage.WorkspaceName, taskMessage.StubId), encodedMessage).Err()
	if err != nil {
		return err
	}

	return nil
}

func (qc *taskQueueClient) Pop(workspaceName, stubId, containerId string) ([]byte, error) {
	queueLength, err := qc.rdb.LLen(context.TODO(), Keys.taskQueueList(workspaceName, stubId)).Result()
	if err != nil {
		return nil, err
	}

	if queueLength == 0 {
		return nil, nil
	}

	// Set a lock to prevent container from spinning down while processing this item in the queue
	err = qc.rdb.Set(context.TODO(), Keys.taskQueueProcessingLock(workspaceName, stubId, containerId), 1, 0).Err()
	if err != nil {
		return nil, err
	}
	defer func() {
		qc.rdb.Del(context.TODO(), Keys.taskQueueProcessingLock(workspaceName, stubId, containerId))
	}()

	// Now actually pop the task from the queue
	task, err := qc.rdb.LPop(context.TODO(), Keys.taskQueueList(workspaceName, stubId)).Bytes()
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
	err = qc.rdb.SetEx(context.TODO(), Keys.taskQueueTaskRunningLock(workspaceName, stubId, containerId, tm.TaskId), 1, time.Duration(defaultTaskRunningExpiration)*time.Second).Err()
	if err != nil {
		return nil, err
	}

	// Set a heartbeat to prevent retries before task actually starts processing
	err = qc.rdb.SetEx(context.TODO(), Keys.taskQueueTaskHeartbeat(workspaceName, stubId, tm.TaskId), 1, 60*time.Second).Err()
	if err != nil {
		return nil, err
	}

	return task, nil
}

// Get queue length
func (qc *taskQueueClient) QueueLength(workspaceName, stubId string) (int64, error) {
	res, err := qc.rdb.LLen(context.TODO(), Keys.taskQueueList(workspaceName, stubId)).Result()
	if err != nil {
		return -1, err
	}

	return res, nil
}

// Check how many tasks are running
func (qc *taskQueueClient) TasksRunning(workspaceName, stubId string) (int, error) {
	keys, err := qc.rdb.Scan(context.TODO(), common.RedisKeys.TaskClaim(workspaceName, stubId, "*"))
	if err != nil {
		return -1, err
	}
	return len(keys), nil
}

// Get most recent task duration
func (qc *taskQueueClient) GetTaskDuration(workspaceName, stubId string) (float64, error) {
	res, err := qc.rdb.LPop(context.TODO(), Keys.taskQueueTaskDuration(workspaceName, stubId)).Result()
	if err != nil {
		return -1, err
	}

	duration, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return -1, err
	}

	return duration, nil
}

func (qc *taskQueueClient) SetAverageTaskDuration(workspaceName, stubId string, duration float64) error {
	err := qc.rdb.Set(context.TODO(), Keys.taskQueueAverageTaskDuration(workspaceName, stubId), duration, 0).Err()
	if err != nil {
		return err
	}
	return nil
}

func (qc *taskQueueClient) GetAverageTaskDuration(workspaceName, stubId string) (float64, error) {
	res, err := qc.rdb.Get(context.TODO(), Keys.taskQueueAverageTaskDuration(workspaceName, stubId)).Result()
	if err != nil {
		return -1, err
	}

	duration, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return -1, err
	}

	return duration, nil
}
