package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
)

type TaskQueueService interface {
	TaskQueuePut(context.Context, *pb.TaskQueuePutRequest) (*pb.TaskQueuePutResponse, error)
	// TaskQueueLength(stubId string) (int64, error)
	// TaskRunning(identityId, queueName string) (bool, error)
	// TasksRunning(identityId, queueName string) (int, error)
	// GetTaskDuration(identityId, queueName string) (float64, error)
	// SetAverageTaskDuration(identityId, queueName string, duration float64) error
	// GetAverageTaskDuration(identityId, queueName string) (float64, error)
	// MonitorTasks(ctx context.Context, beamRepo repository.BeamRepository)
}

const (
	taskQueueContainerPrefix string = "taskqueue-"
)

type TaskQueueRedis struct {
	ctx           context.Context
	rdb           *common.RedisClient
	containerRepo repository.ContainerRepository
	scheduler     *scheduler.Scheduler
	pb.UnimplementedTaskQueueServiceServer
	queueInstances  *common.SafeMap[*taskQueueInstance]
	keyEventManager *common.KeyEventManager
	keyEventChan    chan common.KeyEvent
	queueClient     *taskQueueClient
}

func NewTaskQueueRedis(ctx context.Context,
	rdb *common.RedisClient,
	scheduler *scheduler.Scheduler,
	containerRepo repository.ContainerRepository,
) (*TaskQueueRedis, error) {
	keyEventChan := make(chan common.KeyEvent)
	keyEventManager, err := common.NewKeyEventManager(rdb)
	if err != nil {
		return nil, err
	}

	go keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerState(taskQueueContainerPrefix), keyEventChan)

	tq := &TaskQueueRedis{
		ctx:             ctx,
		rdb:             rdb,
		scheduler:       scheduler,
		keyEventChan:    keyEventChan,
		keyEventManager: keyEventManager,
		containerRepo:   containerRepo,
		queueClient:     newRedisTaskQueueClient(rdb),
	}

	go tq.handleContainerEvents()
	return tq, nil
}

func (tq *TaskQueueRedis) TaskQueuePut(context.Context, *pb.TaskQueuePutRequest) (*pb.TaskQueuePutResponse, error) {
	_, exists := tq.queueInstances.Get("")
	if !exists {
		err := tq.createQueueInstance("test")
		if err != nil {

		}
	}

	return nil, nil
}

func (tq *TaskQueueRedis) createQueueInstance(stubId string) error {
	_, exists := tq.queueInstances.Get(stubId)
	if exists {
		return errors.New("queue already in memory")
	}

	lock := common.NewRedisLock(tq.rdb)
	queue := &taskQueueInstance{
		lock:               lock,
		name:               "test",
		scheduler:          tq.scheduler,
		containerRepo:      tq.containerRepo,
		containerEventChan: make(chan types.ContainerEvent, 1),
		containers:         make(map[string]bool),
		scaleEventChan:     make(chan int, 1),
		rdb:                tq.rdb,
	}

	autoscaler := newAutoscaler(queue)
	queue.autoscaler = autoscaler

	tq.queueInstances.Set(stubId, queue)
	go queue.monitor()

	return nil
}

func (tq *TaskQueueRedis) handleContainerEvents() {
	for {
		select {
		case event := <-tq.keyEventChan:
			containerId := fmt.Sprintf("%s%s", taskQueueContainerPrefix, event.Key)

			log.Println("(rx event) container ID: ", containerId)

			operation := event.Operation
			containerIdParts := strings.Split(containerId, "-")
			stubId := strings.Join(containerIdParts[1:3], "-")

			queue, exists := tq.queueInstances.Get(stubId)
			if !exists {
				err := tq.createQueueInstance(stubId)
				if err != nil {
					log.Printf("err creating instance: %+v\n", err)
					continue
				}
			}

			switch operation {
			case common.KeyOperationSet, common.KeyOperationHSet:
				queue.containerEventChan <- types.ContainerEvent{
					ContainerId: containerId,
					Change:      +1,
				}
			case common.KeyOperationDel, common.KeyOperationExpired:
				queue.containerEventChan <- types.ContainerEvent{
					ContainerId: containerId,
					Change:      -1,
				}
			}
		case <-tq.ctx.Done():
			return
		}
	}
}

// Redis keys
var (
	taskQueuePrefix              string = "taskqueue"
	taskQueueInstanceLock        string = "taskqueue:%s:%s"
	taskQueueList                string = "taskqueue:%s:%s"
	taskQueueTaskDuration        string = "taskqueue:%s:%s:task_duration"
	taskQueueAverageTaskDuration string = "taskqueue:%s:%s:avg_task_duration"
	taskQueueTaskClaim           string = "taskqueue:%s:%s:task:claim:%s"
	taskQueueTaskRetries         string = "taskqueue:%s:%s:task:retries:%s"
	taskQueueTaskHeartbeat       string = "taskqueue:%s:%s:task:heartbeat:%s"
	taskQueueProcessingLock      string = "taskqueue:%s:%s:processing_lock:%s"
	taskQueueKeepWarmLock        string = "taskqueue:%s:%s:keep_warm_lock:%s"
	taskQueueTaskRunningLock     string = "taskqueue:%s:%s:task_running:%s:%s"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) taskQueuePrefix() string {
	return taskQueuePrefix
}

func (k *keys) taskQueueInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueInstanceLock, workspaceName, stubId)
}
