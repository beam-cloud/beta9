package taskqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/beam-cloud/beam/internal/auth"
	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
	"gorm.io/gorm"
)

type TaskQueueService interface {
	TaskQueuePut(context.Context, *pb.TaskQueuePutRequest) (*pb.TaskQueuePutResponse, error)
	TaskQueuePop(context.Context, *pb.TaskQueuePopRequest) (*pb.TaskQueuePopResponse, error)
	TaskQueueLength(context.Context, *pb.TaskQueueLengthRequest) (*pb.TaskQueueLengthResponse, error)
}

const (
	taskQueueContainerPrefix string = "taskqueue-"
)

type TaskQueueRedis struct {
	ctx           context.Context
	rdb           *common.RedisClient
	containerRepo repository.ContainerRepository
	backendRepo   repository.BackendRepository
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
	backendRepo repository.BackendRepository,
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
		backendRepo:     backendRepo,
		queueClient:     newRedisTaskQueueClient(rdb),
		queueInstances:  common.NewSafeMap[*taskQueueInstance](),
	}

	go tq.handleContainerEvents()
	go tq.monitorTasks(ctx)

	return tq, nil
}

type TaskPayload struct {
	Args   []interface{}          `json:"args"`
	Kwargs map[string]interface{} `json:"kwargs"`
}

func (tq *TaskQueueRedis) TaskQueuePut(ctx context.Context, in *pb.TaskQueuePutRequest) (*pb.TaskQueuePutResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	queue, exists := tq.queueInstances.Get(in.StubId)
	if !exists {
		err := tq.createQueueInstance(in.StubId, authInfo.Workspace)
		if err != nil {
			return &pb.TaskQueuePutResponse{
				Ok: false,
			}, nil
		}

		queue, _ = tq.queueInstances.Get(in.StubId)
	}

	task, err := tq.backendRepo.CreateTask(ctx, "", authInfo.Workspace.Id, queue.stub.Id)
	if err != nil {
		return nil, err
	}

	var payload TaskPayload
	err = json.Unmarshal(in.Payload, &payload)
	if err != nil {
		return &pb.TaskQueuePutResponse{
			Ok: false,
		}, nil
	}

	err = queue.client.Push(queue.workspace.Name, queue.stub.ExternalId, task.ExternalId, payload.Args, payload.Kwargs)
	if err != nil {
		tq.backendRepo.DeleteTask(ctx, task.ExternalId)
	}

	return &pb.TaskQueuePutResponse{
		Ok:     err == nil,
		TaskId: task.ExternalId,
	}, nil
}

func (tq *TaskQueueRedis) TaskQueuePop(ctx context.Context, in *pb.TaskQueuePopRequest) (*pb.TaskQueuePopResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	queue, exists := tq.queueInstances.Get(in.StubId)
	if !exists {
		err := tq.createQueueInstance(in.StubId, authInfo.Workspace)
		if err != nil {
			return &pb.TaskQueuePopResponse{
				Ok: false,
			}, nil
		}

		queue, _ = tq.queueInstances.Get(in.StubId)
	}

	msg, err := queue.client.Pop(authInfo.Workspace.Name, in.StubId, in.ContainerId)
	if err != nil || msg == nil {
		return &pb.TaskQueuePopResponse{Ok: false}, nil
	}

	var tm types.TaskMessage
	err = tm.Decode(msg)
	if err != nil {
		return nil, err
	}

	task, err := tq.backendRepo.GetTask(ctx, tm.ID)
	if err != nil {
		return &pb.TaskQueuePopResponse{
			Ok: false,
		}, nil
	}
	task.StartedAt = sql.NullTime{Time: time.Now(), Valid: true}
	task.Status = types.TaskStatusRunning

	err = tq.rdb.SetEx(context.TODO(), Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, in.StubId, in.ContainerId, tm.ID), 1, time.Duration(defaultTaskRunningExpiration)*time.Second).Err()
	if err != nil {
		return &pb.TaskQueuePopResponse{
			Ok: false,
		}, nil
	}

	_, err = tq.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	return &pb.TaskQueuePopResponse{
		Ok: true, TaskMsg: msg,
	}, nil
}

func (tq *TaskQueueRedis) TaskQueueComplete(ctx context.Context, in *pb.TaskQueueCompleteRequest) (*pb.TaskQueueCompleteResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	task, err := tq.backendRepo.GetTask(ctx, in.TaskId)
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.SetEx(context.TODO(), Keys.taskQueueKeepWarmLock(authInfo.Workspace.Name, in.StubId, in.ContainerId), 1, time.Duration(in.ScaleDownDelay)*time.Second).Err()
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.Del(context.TODO(), Keys.taskQueueTaskClaim(authInfo.Workspace.Name, in.StubId, task.ExternalId)).Err()
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.Del(context.TODO(), Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, in.StubId, in.ContainerId, in.TaskId)).Err()
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.RPush(context.TODO(), Keys.taskQueueTaskDuration(authInfo.Workspace.Name, in.StubId), in.TaskDuration).Err()
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	task.EndedAt = sql.NullTime{Time: time.Now(), Valid: true}
	task.Status = types.TaskStatus(in.TaskStatus)

	_, err = tq.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	return &pb.TaskQueueCompleteResponse{
		Ok: err == nil,
	}, nil
}

func (tq *TaskQueueRedis) TaskQueueLength(ctx context.Context, in *pb.TaskQueueLengthRequest) (*pb.TaskQueueLengthResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	length, err := tq.queueClient.QueueLength(authInfo.Workspace.Name, in.StubId)
	if err != nil {
		return &pb.TaskQueueLengthResponse{Ok: false}, nil
	}

	return &pb.TaskQueueLengthResponse{
		Ok:     true,
		Length: length,
	}, nil
}

func (tq *TaskQueueRedis) createQueueInstance(stubId string, workspace *types.Workspace) error {
	_, exists := tq.queueInstances.Get(stubId)
	if exists {
		return errors.New("queue already in memory")
	}

	stub, err := tq.backendRepo.GetStubByExternalId(tq.ctx, stubId, workspace.Id)
	if err != nil {
		return err
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return err
	}

	token, err := tq.backendRepo.RetrieveActiveToken(tq.ctx, stub.Workspace.Id)
	if err != nil {
		return err
	}

	ctx, cancelFunc := context.WithCancel(tq.ctx)
	lock := common.NewRedisLock(tq.rdb)
	queue := &taskQueueInstance{
		ctx:                ctx,
		cancelFunc:         cancelFunc,
		lock:               lock,
		name:               fmt.Sprintf("%s-%s", stub.Name, stub.ExternalId),
		workspace:          workspace,
		stub:               &stub.Stub,
		object:             &stub.Object,
		token:              token,
		stubConfig:         stubConfig,
		scheduler:          tq.scheduler,
		containerRepo:      tq.containerRepo,
		containerEventChan: make(chan types.ContainerEvent, 1),
		containers:         make(map[string]bool),
		scaleEventChan:     make(chan int, 1),
		rdb:                tq.rdb,
		client:             tq.queueClient,
	}

	autoscaler := newAutoscaler(queue)
	queue.autoscaler = autoscaler

	tq.queueInstances.Set(stubId, queue)
	go queue.monitor()

	// Clean up the queue instance once it's done
	go func(q *taskQueueInstance) {
		<-q.ctx.Done()
		tq.queueInstances.Delete(stubId)
	}(queue)

	return nil
}

// Monitor tasks -- if a container is killed unexpectedly, it will automatically be re-added to the queue
func (tq *TaskQueueRedis) monitorTasks(ctx context.Context) {
	monitorRate := time.Duration(5) * time.Second
	ticker := time.NewTicker(monitorRate)
	defer ticker.Stop()

	for range ticker.C {
		claimedTasks, err := tq.rdb.Scan(context.TODO(), Keys.taskQueueTaskClaim("*", "*", "*"))
		if err != nil {
			return
		}

		for _, claimKey := range claimedTasks {
			v := strings.Split(claimKey, ":")
			workspaceName := v[1]
			stubId := v[2]
			taskId := v[5]

			res, err := tq.rdb.Exists(context.TODO(), Keys.taskQueueTaskHeartbeat(workspaceName, stubId, taskId)).Result()
			if err != nil {
				continue
			}

			recentHeartbeat := res > 0
			if !recentHeartbeat {
				log.Printf("<taskqueue> missing heartbeat, reinserting task<%s:%s> into queue: %s\n", workspaceName, taskId, stubId)

				retries, err := tq.rdb.Get(context.TODO(), Keys.taskQueueTaskRetries(workspaceName, stubId, taskId)).Int()
				if err != nil {
					retries = 0
				}

				task, err := tq.backendRepo.GetTask(ctx, taskId)
				if err != nil {
					continue
				}

				taskPolicy := types.DefaultTaskPolicy // types.TaskPolicy{}

				// err = json.Unmarshal(
				// 	task.TaskPolicy,
				// 	&taskPolicy,
				// )
				// if err != nil {
				// 	taskPolicy = types.DefaultTaskPolicy
				// }

				if retries >= int(taskPolicy.MaxRetries) {
					log.Printf("<taskqueue> hit retry limit, not reinserting task <%s> into queue: %s\n", taskId, stubId)

					task.Status = types.TaskStatusError
					_, err = tq.backendRepo.UpdateTask(ctx, taskId, *task)
					if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
						continue
					}

					err = tq.rdb.Del(context.TODO(), Keys.taskQueueTaskClaim(workspaceName, stubId, taskId)).Err()
					if err != nil {
						log.Printf("<taskqueue> unable to delete task claim: %s\n", taskId)
					}

					continue
				}

				retries += 1
				err = tq.rdb.Set(context.TODO(), Keys.taskQueueTaskRetries(workspaceName, stubId, taskId), retries, 0).Err()
				if err != nil {
					continue
				}

				task.Status = types.TaskStatusRetry
				_, err = tq.backendRepo.UpdateTask(ctx, taskId, *task)
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}

				encodedMessage, err := tq.rdb.Get(context.TODO(), Keys.taskQueueTaskClaim(workspaceName, stubId, taskId)).Result()
				if err != nil {
					continue
				}

				err = tq.rdb.Del(context.TODO(), Keys.taskQueueTaskClaim(workspaceName, stubId, taskId)).Err()
				if err != nil {
					log.Printf("<taskqueue> unable to delete task claim: %s\n", taskId)
					continue
				}

				err = tq.rdb.RPush(context.TODO(), Keys.taskQueueList(workspaceName, stubId), encodedMessage).Err()
				if err != nil {
					log.Printf("<taskqueue> unable to insert task <%s> into queue <%s>: %v\n", taskId, stubId, err)
					continue
				}

			}
		}
	}
}

func (tq *TaskQueueRedis) handleContainerEvents() {
	for {
		select {
		case event := <-tq.keyEventChan:
			containerId := fmt.Sprintf("%s%s", taskQueueContainerPrefix, event.Key)

			log.Println("(rx event) container ID: ", containerId)

			// operation := event.Operation
			// containerIdParts := strings.Split(containerId, "-")
			// stubId := strings.Join(containerIdParts[1:3], "-")

			// queue, exists := tq.queueInstances.Get(stubId)
			// if !exists {
			// 	err := tq.createQueueInstance("", nil)
			// 	if err != nil {
			// 		log.Printf("err creating instance: %+v\n", err)
			// 		continue
			// 	}
			// }

			// switch operation {
			// case common.KeyOperationSet, common.KeyOperationHSet:
			// 	queue.containerEventChan <- types.ContainerEvent{
			// 		ContainerId: containerId,
			// 		Change:      +1,
			// 	}
			// case common.KeyOperationDel, common.KeyOperationExpired:
			// 	queue.containerEventChan <- types.ContainerEvent{
			// 		ContainerId: containerId,
			// 		Change:      -1,
			// 	}
			// }
		case <-tq.ctx.Done():
			return
		}
	}
}

// Redis keys
var (
	taskQueuePrefix              string = "taskqueue"
	taskQueueInstanceLock        string = "taskqueue:%s:%s:instance_lock"
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

func (k *keys) taskQueueList(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueList, workspaceName, stubId)
}

func (k *keys) taskQueueTaskClaim(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskClaim, workspaceName, stubId, taskId)
}

func (k *keys) taskQueueTaskHeartbeat(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskHeartbeat, workspaceName, stubId, taskId)
}

func (k *keys) taskQueueTaskRetries(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskRetries, workspaceName, stubId, taskId)
}

func (k *keys) taskQueueTaskDuration(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueTaskDuration, workspaceName, stubId)
}

func (k *keys) taskQueueAverageTaskDuration(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueAverageTaskDuration, workspaceName, stubId)
}

func (k *keys) taskQueueTaskRunningLock(workspaceName, stubId, containerId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskRunningLock, workspaceName, stubId, containerId, taskId)
}

func (k *keys) taskQueueProcessingLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(taskQueueProcessingLock, workspaceName, stubId, containerId)
}

func (k *keys) taskQueueKeepWarmLock(workspaceName, stubId, containerId string) string {
	return fmt.Sprintf(taskQueueKeepWarmLock, workspaceName, stubId, containerId)
}
