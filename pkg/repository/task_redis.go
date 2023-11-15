package repository

import (
	"context"
	"log"
	"time"

	"github.com/beam-cloud/beam/pkg/common"
	"github.com/beam-cloud/beam/pkg/types"
	pb "github.com/beam-cloud/beam/proto"
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

func (t *TaskRedisRepository) GetTaskStream(queueName, containerId, identityExternalId string, stream pb.Scheduler_GetTaskStreamServer) error {
	script := `
	if redis.call('LLEN', KEYS[1]) > 0 then
	  return redis.call('LPOP', KEYS[1])
	else
	  return nil
	end`

	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := t.rdb.Eval(context.TODO(), script, []string{common.RedisKeys.QueueList(identityExternalId, queueName)}).Result()
			if err != nil {
				if err == redis.Nil {
					time.Sleep(time.Millisecond * 100)
					continue
				}
				return err
			}

			task := []byte(res.(string))

			var tm types.TaskMessage
			err = tm.Decode(task)
			if err != nil {
				return err
			}

			err = t.rdb.Set(context.TODO(), common.RedisKeys.QueueTaskClaim(identityExternalId, queueName, tm.ID), task, 0).Err()
			if err != nil {
				return err
			}

			err = t.rdb.SetEx(context.TODO(), common.RedisKeys.QueueTaskHeartbeat(identityExternalId, queueName, tm.ID), 1, 60*time.Second).Err()
			if err != nil {
				return err
			}

			err = stream.Send(&pb.TaskStreamResponse{Task: task})
			if err != nil {
				return err
			}

			time.Sleep(time.Millisecond * 100)
		}
	}

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

func (t *TaskRedisRepository) MonitorTask(task *types.BeamAppTask, queueName, containerId, identityExternalId string, timeout int64, stream pb.Scheduler_MonitorTaskServer, timeoutCallback func() error) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	// Listen for task cancellation events
	channelKey := common.RedisKeys.QueueTaskCancel(identityExternalId, queueName, task.TaskId)
	cancelFlag := make(chan bool, 1)
	timeoutFlag := make(chan bool, 1)

	var timeoutChan <-chan time.Time
	taskStartTimeSeconds := task.StartedAt.Unix()
	currentTimeSeconds := time.Now().Unix()
	timeoutTimeSeconds := taskStartTimeSeconds + timeout
	leftoverTimeoutSeconds := timeoutTimeSeconds - currentTimeSeconds

	if timeout <= 0 {
		timeoutChan = make(<-chan time.Time) // Indicates that the task does not have a timeout
	} else if leftoverTimeoutSeconds <= 0 {
		err := timeoutCallback()
		if err != nil {
			log.Printf("error timing out task: %v", err)
			return err
		}
		timeoutFlag <- true
		timeoutChan = make(<-chan time.Time)
	} else {
		timeoutChan = time.After(time.Duration(leftoverTimeoutSeconds) * time.Second)
	}

	go func() {
	retry:
		for {
			messages, errs := t.rdb.Subscribe(ctx, channelKey)

			for {
				select {
				case <-timeoutChan:
					err := timeoutCallback()
					if err != nil {
						log.Printf("error timing out task: %v", err)
					}
					timeoutFlag <- true
					return

				case msg := <-messages:
					if msg.Payload == task.TaskId {
						cancelFlag <- true
						return
					}

				case <-ctx.Done():
					return

				case err := <-errs:
					log.Printf("error with monitor task subscription: %v", err)
					break retry
				}
			}
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil

		case <-cancelFlag:
			err := t.rdb.Del(context.TODO(), common.RedisKeys.QueueTaskClaim(identityExternalId, queueName, task.TaskId)).Err()
			if err != nil {
				return err
			}
			stream.Send(&pb.MonitorTaskResponse{Ok: true, Canceled: true, Complete: false, TimedOut: false})
			return nil
		case <-timeoutFlag:
			err := t.rdb.Del(context.TODO(), common.RedisKeys.QueueTaskClaim(identityExternalId, queueName, task.TaskId)).Err()
			if err != nil {
				log.Printf("error deleting task claim: %v", err)
				continue
			}
			stream.Send(&pb.MonitorTaskResponse{Ok: true, Canceled: false, Complete: false, TimedOut: true})
			return nil

		default:
			if task.Status == types.BeamAppTaskStatusCancelled {
				stream.Send(&pb.MonitorTaskResponse{Ok: true, Canceled: true, Complete: false, TimedOut: false})
				return nil
			}

			err := t.rdb.SetEx(context.TODO(), common.RedisKeys.QueueTaskHeartbeat(identityExternalId, queueName, task.TaskId), 1, time.Duration(60)*time.Second).Err()
			if err != nil {
				return err
			}

			err = t.rdb.SetEx(context.TODO(), common.RedisKeys.QueueTaskRunningLock(identityExternalId, queueName, containerId, task.TaskId), 1, time.Duration(defaultTaskRunningExpiration)*time.Second).Err()
			if err != nil {
				return err
			}

			// Check if the task is currently claimed
			exists, err := t.rdb.Exists(context.TODO(), common.RedisKeys.QueueTaskClaim(identityExternalId, queueName, task.TaskId)).Result()
			if err != nil {
				return err
			}

			// If the task claim key doesn't exist, send a message and return
			if exists == 0 {
				stream.Send(&pb.MonitorTaskResponse{Ok: true, Canceled: false, Complete: true, TimedOut: false})
				return nil
			}

			stream.Send(&pb.MonitorTaskResponse{Ok: true, Canceled: false, Complete: false, TimedOut: false})
			time.Sleep(time.Second * 1)
		}
	}
}
