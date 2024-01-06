package taskqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/beam-cloud/beam/internal/auth"
	common "github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
)

type TaskQueueService interface {
	TaskQueuePut(context.Context, *pb.TaskQueuePutRequest) (*pb.TaskQueuePutResponse, error)
	TaskQueuePop(context.Context, *pb.TaskQueuePopRequest) (*pb.TaskQueuePopResponse, error)
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
	return tq, nil
}

type Payload struct {
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

	var payload Payload
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
	if err != nil {
		return &pb.TaskQueuePopResponse{Ok: false}, nil
	}

	return &pb.TaskQueuePopResponse{
		Ok: true, TaskMsg: msg,
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

	return nil
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
