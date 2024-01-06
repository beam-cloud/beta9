package taskqueue

import (
	"context"
	"strconv"
	"sync"

	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/types"
	"github.com/gofrs/uuid"
)

type taskQueueClient struct {
	rdb *common.RedisClient
}

var taskMessagePool = sync.Pool{
	New: func() interface{} {
		return &types.TaskMessage{
			ID:     uuid.Must(uuid.NewV4()).String(),
			Args:   nil,
			Kwargs: nil,
		}
	},
}

func newRedisTaskQueueClient(rdb *common.RedisClient) *taskQueueClient {
	return &taskQueueClient{rdb: rdb}
}

// Add a new task to the queue
func (qc *taskQueueClient) Push(workspaceName, stubId, taskId string, args []interface{}, kwargs map[string]interface{}) error {
	taskMessage := qc.getTaskMessage(stubId)
	taskMessage.ID = taskId
	taskMessage.Args = args
	taskMessage.Kwargs = kwargs

	defer qc.releaseTaskMessage(taskMessage)
	encodedMessage, err := taskMessage.Encode()
	if err != nil {
		return err
	}

	err = qc.rdb.RPush(context.TODO(), Keys.taskQueueList(workspaceName, stubId), encodedMessage).Err()
	if err != nil {
		return err
	}

	return nil
}

func (qc *taskQueueClient) getTaskMessage(task string) *types.TaskMessage {
	msg := taskMessagePool.Get().(*types.TaskMessage)
	msg.Args = make([]interface{}, 0)
	msg.Kwargs = make(map[string]interface{})
	return msg
}

func (qc *taskQueueClient) releaseTaskMessage(v *types.TaskMessage) {
	v.Reset()
	taskMessagePool.Put(v)
}

// Get queue length
func (qc *taskQueueClient) QueueLength(workspaceName, stubId string) (int64, error) {
	res, err := qc.rdb.LLen(context.TODO(), Keys.taskQueueList(workspaceName, stubId)).Result()
	if err != nil {
		return -1, err
	}

	return res, nil
}

// Check if any tasks are running
func (qc *taskQueueClient) TaskRunning(workspaceName, stubId string) (bool, error) {
	keys, err := qc.rdb.Scan(context.TODO(), Keys.taskQueueTaskClaim(workspaceName, stubId, "*"))
	if err != nil {
		return false, err
	}

	return len(keys) > 0, nil
}

// Check how many tasks are running
func (qc *taskQueueClient) TasksRunning(workspaceName, stubId string) (int, error) {
	keys, err := qc.rdb.Scan(context.TODO(), Keys.taskQueueTaskClaim(workspaceName, stubId, "*"))
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

// Monitor tasks -- if a container is killed unexpectedly, it will automatically be re-added to the queue
// func (qc *taskQueueClient) MonitorTasks(ctx context.Context, beamRepo repository.BeamRepository) {
// 	monitorRate := time.Duration(5) * time.Second
// 	ticker := time.NewTicker(monitorRate)
// 	defer ticker.Stop()

// 	for range ticker.C {
// 		claimedTasks, err := qc.rdb.Scan(context.TODO(), common.RedisKeys.QueueTaskClaim("*", "*", "*"))
// 		if err != nil {
// 			return
// 		}

// 		for _, claimKey := range claimedTasks {
// 			v := strings.Split(claimKey, ":")
// 			identityId := v[1]
// 			queueName := v[2]
// 			taskId := v[5]

// 			res, err := qc.rdb.Exists(context.TODO(), common.RedisKeys.QueueTaskHeartbeat(identityId, queueName, taskId)).Result()
// 			if err != nil {
// 				continue
// 			}

// 			recentHeartbeat := res > 0
// 			if !recentHeartbeat {
// 				log.Printf("Missing heartbeat, reinserting task<%s:%s> into queue: %s\n", identityId, taskId, queueName)

// 				retries, err := qc.rdb.Get(context.TODO(), common.RedisKeys.QueueTaskRetries(identityId, queueName, taskId)).Int()
// 				if err != nil {
// 					retries = 0
// 				}

// 				task, err := beamRepo.GetAppTask(taskId)
// 				if err != nil {
// 					continue
// 				}

// 				taskPolicy := types.TaskPolicy{}
// 				err = json.Unmarshal(
// 					task.TaskPolicy,
// 					&taskPolicy,
// 				)
// 				if err != nil {
// 					taskPolicy = types.DefaultTaskPolicy
// 				}

// 				if retries >= int(taskPolicy.MaxRetries) {
// 					log.Printf("Hit retry limit, not reinserting task <%s> into queue: %s\n", taskId, queueName)

// 					_, err = beamRepo.UpdateActiveTask(taskId, types.BeamAppTaskStatusFailed, identityId)
// 					if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
// 						continue
// 					}

// 					err = qc.rdb.Del(context.TODO(), common.RedisKeys.QueueTaskClaim(identityId, queueName, taskId)).Err()
// 					if err != nil {
// 						log.Printf("Unable to delete task claim: %s\n", taskId)
// 					}

// 					continue
// 				}

// 				retries += 1
// 				err = qc.rdb.Set(context.TODO(), common.RedisKeys.QueueTaskRetries(identityId, queueName, taskId), retries, 0).Err()
// 				if err != nil {
// 					continue
// 				}

// 				_, err = beamRepo.UpdateActiveTask(taskId, types.BeamAppTaskStatusRetry, identityId)
// 				if err != nil {
// 					continue
// 				}

// 				encodedMessage, err := qc.rdb.Get(context.TODO(), common.RedisKeys.QueueTaskClaim(identityId, queueName, taskId)).Result()
// 				if err != nil {
// 					continue
// 				}

// 				err = qc.rdb.Del(context.TODO(), common.RedisKeys.QueueTaskClaim(identityId, queueName, taskId)).Err()
// 				if err != nil {
// 					log.Printf("Unable to delete task claim: %s\n", taskId)
// 					continue
// 				}

// 				err = qc.rdb.RPush(context.TODO(), common.RedisKeys.QueueList(identityId, queueName), encodedMessage).Err()
// 				if err != nil {
// 					log.Printf("Unable to insert task <%s> into queue <%s>: %v\n", taskId, queueName, err)
// 					continue
// 				}

// 			}
// 		}
// 	}
// }

/*


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
*/
