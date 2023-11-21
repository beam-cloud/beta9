package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	common "github.com/beam-cloud/beam/pkg/common"
	"github.com/beam-cloud/beam/pkg/repository"
	"github.com/beam-cloud/beam/pkg/types"
	"github.com/gin-gonic/gin"
	"github.com/gofrs/uuid"
	"gorm.io/gorm"
)

const (
	MaxTaskRetries int = 3
)

type QueueClient struct {
	rdb *common.RedisClient
}

var taskMessagePool = sync.Pool{
	New: func() interface{} {
		eta := time.Now().Format(time.RFC3339)
		return &types.TaskMessage{
			ID:      uuid.Must(uuid.NewV4()).String(),
			Retries: 0,
			Kwargs:  nil,
			ETA:     &eta,
		}
	},
}

func NewQueueClient(rdb *common.RedisClient) *QueueClient {
	log.Println("Initializing queue client")
	return &QueueClient{rdb: rdb}
}

// Add a new task to the queue
func (qc *QueueClient) Push(identityId string, queueName string, taskId string, ctx *gin.Context) (string, error) {
	var payload any
	if ctx != nil {
		err := ctx.BindJSON(&payload)
		if err != nil {
			return "", err
		}
	}

	taskName := queueName
	_, err := qc.delay(taskId, taskName, identityId, queueName, payload)
	if err != nil {
		return "", err
	}

	return taskId, nil
}

func (qc *QueueClient) getTaskMessage(task string) *types.TaskMessage {
	msg := taskMessagePool.Get().(*types.TaskMessage)
	msg.Task = task
	msg.Args = make([]interface{}, 0)
	msg.Kwargs = make(map[string]interface{})
	msg.ETA = nil
	return msg
}

func (qc *QueueClient) releaseTaskMessage(v *types.TaskMessage) {
	v.Reset()
	taskMessagePool.Put(v)
}

func (qc *QueueClient) delay(taskId string, task string, identityId string, queueName string, args ...interface{}) (*AsyncResult, error) {
	taskMessage := qc.getTaskMessage(task)
	taskMessage.ID = taskId
	taskMessage.Args = args

	defer qc.releaseTaskMessage(taskMessage)
	encodedMessage, err := taskMessage.Encode()
	if err != nil {
		return nil, err
	}

	err = qc.rdb.RPush(context.TODO(), common.RedisKeys.QueueList(identityId, queueName), encodedMessage).Err()
	if err != nil {
		return nil, err
	}

	return &AsyncResult{
		TaskID: taskMessage.ID,
	}, nil
}

// Get queue length
func (qc *QueueClient) QueueLength(identityId, queueName string) (int64, error) {
	res, err := qc.rdb.LLen(context.TODO(), common.RedisKeys.QueueList(identityId, queueName)).Result()
	if err != nil {
		return -1, err
	}

	return res, nil
}

// Check if any tasks are running
func (qc *QueueClient) TaskRunning(identityId, queueName string) (bool, error) {
	keys, err := qc.rdb.Scan(context.TODO(), common.RedisKeys.QueueTaskClaim(identityId, queueName, "*"))
	if err != nil {
		return false, err
	}

	return len(keys) > 0, nil
}

// Check how many tasks are running
func (qc *QueueClient) TasksRunning(identityId, queueName string) (int, error) {
	keys, err := qc.rdb.Scan(context.TODO(), common.RedisKeys.QueueTaskClaim(identityId, queueName, "*"))
	if err != nil {
		return -1, err
	}

	return len(keys), nil
}

// Get most recent task duration
func (qc *QueueClient) GetTaskDuration(identityId, queueName string) (float64, error) {
	res, err := qc.rdb.LPop(context.TODO(), common.RedisKeys.QueueTaskDuration(identityId, queueName)).Result()
	if err != nil {
		return -1, err
	}

	duration, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return -1, err
	}

	return duration, nil
}

func (qc *QueueClient) SetAverageTaskDuration(identityId, queueName string, duration float64) error {
	err := qc.rdb.Set(context.TODO(), common.RedisKeys.QueueAverageTaskDuration(identityId, queueName), duration, 0).Err()
	if err != nil {
		return err
	}
	return nil
}

func (qc *QueueClient) GetAverageTaskDuration(identityId, queueName string) (float64, error) {
	res, err := qc.rdb.Get(context.TODO(), common.RedisKeys.QueueAverageTaskDuration(identityId, queueName)).Result()
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
func (qc *QueueClient) MonitorTasks(ctx context.Context, beamRepo repository.BeamRepository) {
	monitorRate := time.Duration(5) * time.Second
	ticker := time.NewTicker(monitorRate)
	defer ticker.Stop()

	for range ticker.C {
		claimedTasks, err := qc.rdb.Scan(context.TODO(), common.RedisKeys.QueueTaskClaim("*", "*", "*"))
		if err != nil {
			return
		}

		for _, claimKey := range claimedTasks {
			v := strings.Split(claimKey, ":")
			identityId := v[1]
			queueName := v[2]
			taskId := v[5]

			res, err := qc.rdb.Exists(context.TODO(), common.RedisKeys.QueueTaskHeartbeat(identityId, queueName, taskId)).Result()
			if err != nil {
				continue
			}

			recentHeartbeat := res > 0
			if !recentHeartbeat {
				log.Printf("Missing heartbeat, reinserting task<%s:%s> into queue: %s\n", identityId, taskId, queueName)

				retries, err := qc.rdb.Get(context.TODO(), common.RedisKeys.QueueTaskRetries(identityId, queueName, taskId)).Int()
				if err != nil {
					retries = 0
				}

				task, err := beamRepo.GetAppTask(taskId)
				if err != nil {
					continue
				}

				taskPolicy := types.TaskPolicy{}
				err = json.Unmarshal(
					task.TaskPolicy,
					&taskPolicy,
				)
				if err != nil {
					taskPolicy = common.DefaultTaskPolicy
				}

				if retries >= int(taskPolicy.MaxRetries) {
					log.Printf("Hit retry limit, not reinserting task <%s> into queue: %s\n", taskId, queueName)

					_, err = beamRepo.UpdateActiveTask(taskId, types.BeamAppTaskStatusFailed, identityId)
					if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
						continue
					}

					err = qc.rdb.Del(context.TODO(), common.RedisKeys.QueueTaskClaim(identityId, queueName, taskId)).Err()
					if err != nil {
						log.Printf("Unable to delete task claim: %s\n", taskId)
					}

					continue
				}

				retries += 1
				err = qc.rdb.Set(context.TODO(), common.RedisKeys.QueueTaskRetries(identityId, queueName, taskId), retries, 0).Err()
				if err != nil {
					continue
				}

				_, err = beamRepo.UpdateActiveTask(taskId, types.BeamAppTaskStatusRetry, identityId)
				if err != nil {
					continue
				}

				encodedMessage, err := qc.rdb.Get(context.TODO(), common.RedisKeys.QueueTaskClaim(identityId, queueName, taskId)).Result()
				if err != nil {
					continue
				}

				err = qc.rdb.Del(context.TODO(), common.RedisKeys.QueueTaskClaim(identityId, queueName, taskId)).Err()
				if err != nil {
					log.Printf("Unable to delete task claim: %s\n", taskId)
					continue
				}

				err = qc.rdb.RPush(context.TODO(), common.RedisKeys.QueueList(identityId, queueName), encodedMessage).Err()
				if err != nil {
					log.Printf("Unable to insert task <%s> into queue <%s>: %v\n", taskId, queueName, err)
					continue
				}

			}
		}
	}
}

// AsyncResult represents pending result
type AsyncResult struct {
	TaskID string
}
