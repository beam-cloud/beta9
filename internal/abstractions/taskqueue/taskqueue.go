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

	"github.com/beam-cloud/beta9/internal/auth"
	common "github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/task"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

type TaskQueueService interface {
	pb.TaskQueueServiceServer
	TaskQueuePut(context.Context, *pb.TaskQueuePutRequest) (*pb.TaskQueuePutResponse, error)
	TaskQueuePop(context.Context, *pb.TaskQueuePopRequest) (*pb.TaskQueuePopResponse, error)
	TaskQueueLength(context.Context, *pb.TaskQueueLengthRequest) (*pb.TaskQueueLengthResponse, error)
	TaskQueueComplete(ctx context.Context, in *pb.TaskQueueCompleteRequest) (*pb.TaskQueueCompleteResponse, error)
	TaskQueueMonitor(req *pb.TaskQueueMonitorRequest, stream pb.TaskQueueService_TaskQueueMonitorServer) error
}

type TaskQueueServiceOpts struct {
	Config         types.AppConfig
	RedisClient    *common.RedisClient
	BackendRepo    repository.BackendRepository
	TaskRepo       repository.TaskRepository
	ContainerRepo  repository.ContainerRepository
	TaskDispatcher *task.Dispatcher
	Scheduler      *scheduler.Scheduler
	Tailscale      *network.Tailscale
	RouteGroup     *echo.Group
}

const (
	taskQueueContainerPrefix string = "taskqueue"
	taskQueueRoutePrefix     string = "/taskqueue"
)

type RedisTaskQueue struct {
	ctx             context.Context
	rdb             *common.RedisClient
	stubConfigCache *common.SafeMap[*types.StubConfigV1]
	taskDispatcher  *task.Dispatcher
	taskRepo        repository.TaskRepository
	containerRepo   repository.ContainerRepository
	backendRepo     repository.BackendRepository
	scheduler       *scheduler.Scheduler
	pb.UnimplementedTaskQueueServiceServer
	queueInstances  *common.SafeMap[*taskQueueInstance]
	keyEventManager *common.KeyEventManager
	keyEventChan    chan common.KeyEvent
	queueClient     *taskQueueClient
}

func NewRedisTaskQueueService(
	ctx context.Context,
	opts TaskQueueServiceOpts,
) (TaskQueueService, error) {
	keyEventChan := make(chan common.KeyEvent)
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	go keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerState(taskQueueContainerPrefix), keyEventChan)

	tq := &RedisTaskQueue{
		ctx:             ctx,
		rdb:             opts.RedisClient,
		scheduler:       opts.Scheduler,
		stubConfigCache: common.NewSafeMap[*types.StubConfigV1](),
		keyEventChan:    keyEventChan,
		keyEventManager: keyEventManager,
		taskDispatcher:  opts.TaskDispatcher,
		taskRepo:        opts.TaskRepo,
		containerRepo:   opts.ContainerRepo,
		backendRepo:     opts.BackendRepo,
		queueClient:     newRedisTaskQueueClient(opts.RedisClient),
		queueInstances:  common.NewSafeMap[*taskQueueInstance](),
	}

	tq.taskDispatcher.Register(string(types.ExecutorTaskQueue), tq.taskQueueTaskFactory)

	go tq.handleContainerEvents()

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(opts.BackendRepo)
	registerTaskQueueRoutes(opts.RouteGroup.Group(taskQueueRoutePrefix, authMiddleware), tq)

	return tq, nil
}

type TaskQueueTask struct {
	msg *types.TaskMessage
	tq  *RedisTaskQueue
}

func (t *TaskQueueTask) Execute(ctx context.Context) error {
	queue, exists := t.tq.queueInstances.Get(t.msg.StubId)
	if !exists {
		err := t.tq.createQueueInstance(t.msg.StubId)
		if err != nil {
			return err
		}

		queue, _ = t.tq.queueInstances.Get(t.msg.StubId)
	}

	_, err := t.tq.backendRepo.CreateTask(ctx, &types.TaskParams{
		TaskId:      t.msg.TaskId,
		StubId:      queue.stub.Id,
		WorkspaceId: queue.stub.WorkspaceId,
	})
	if err != nil {
		return err
	}

	err = t.tq.queueClient.Push(t.msg)
	if err != nil {
		t.tq.backendRepo.DeleteTask(context.TODO(), t.msg.TaskId)
		return err
	}

	return nil
}

func (t *TaskQueueTask) Retry(ctx context.Context) error {
	_, exists := t.tq.queueInstances.Get(t.msg.StubId)
	if !exists {
		err := t.tq.createQueueInstance(t.msg.StubId)
		if err != nil {
			return err
		}
	}

	task, err := t.tq.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return err
	}

	task.Status = types.TaskStatusRetry
	_, err = t.tq.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	return t.tq.queueClient.Push(t.msg)
}

func (t *TaskQueueTask) HeartBeat(ctx context.Context) (bool, error) {
	res, err := t.tq.rdb.Exists(ctx, Keys.taskQueueTaskHeartbeat(t.msg.WorkspaceName, t.msg.StubId, t.msg.TaskId)).Result()
	if err != nil {
		return false, err
	}

	return res > 0, nil
}

func (t *TaskQueueTask) Cancel(ctx context.Context) error {
	task, err := t.tq.backendRepo.GetTask(ctx, t.msg.TaskId)
	if err != nil {
		return err
	}

	task.Status = types.TaskStatusError
	_, err = t.tq.backendRepo.UpdateTask(ctx, t.msg.TaskId, *task)
	if err != nil {
		return err
	}

	return nil
}

func (t *TaskQueueTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{
		TaskId:        t.msg.TaskId,
		StubId:        t.msg.StubId,
		WorkspaceName: t.msg.WorkspaceName,
	}
}

func (tq *RedisTaskQueue) taskQueueTaskFactory(ctx context.Context, msg *types.TaskMessage) (types.TaskInterface, error) {
	return &TaskQueueTask{
		tq:  tq,
		msg: msg,
	}, nil
}

func (tq *RedisTaskQueue) getStubConfig(stubId string) (*types.StubConfigV1, error) {
	config, exists := tq.stubConfigCache.Get(stubId)

	if !exists {
		stub, err := tq.backendRepo.GetStubByExternalId(tq.ctx, stubId)
		if err != nil {
			return nil, nil
		}

		var stubConfig types.StubConfigV1 = types.StubConfigV1{}
		err = json.Unmarshal([]byte(stub.Config), &stubConfig)
		if err != nil {
			return nil, err
		}

		config = &stubConfig
		tq.stubConfigCache.Set(stubId, config)
	}

	return config, nil
}

func (tq *RedisTaskQueue) put(ctx context.Context, workspaceName, stubId string, payload *types.TaskPayload) (string, error) {
	stubConfig, err := tq.getStubConfig(stubId)
	if err != nil {
		return "", err
	}

	task, err := tq.taskDispatcher.Send(ctx, string(types.ExecutorTaskQueue), workspaceName, stubId, payload, stubConfig.TaskPolicy)
	if err != nil {
		return "", err
	}

	meta := task.Metadata()
	return meta.TaskId, nil
}

func (tq *RedisTaskQueue) TaskQueuePut(ctx context.Context, in *pb.TaskQueuePutRequest) (*pb.TaskQueuePutResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	var payload types.TaskPayload
	err := json.Unmarshal(in.Payload, &payload)
	if err != nil {
		return &pb.TaskQueuePutResponse{
			Ok: false,
		}, nil
	}

	workspaceName := authInfo.Workspace.Name

	taskId, err := tq.put(ctx, workspaceName, in.StubId, &payload)
	return &pb.TaskQueuePutResponse{
		Ok:     err == nil,
		TaskId: taskId,
	}, nil
}

func (tq *RedisTaskQueue) TaskQueuePop(ctx context.Context, in *pb.TaskQueuePopRequest) (*pb.TaskQueuePopResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	queue, exists := tq.queueInstances.Get(in.StubId)
	if !exists {
		err := tq.createQueueInstance(in.StubId)
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

	err = tq.taskDispatcher.Claim(ctx, authInfo.Workspace.Name, tm.StubId, tm.TaskId, in.ContainerId)
	if err != nil {
		return nil, err
	}

	task, err := tq.backendRepo.GetTask(ctx, tm.TaskId)
	if err != nil {
		return &pb.TaskQueuePopResponse{
			Ok: false,
		}, nil
	}

	task.ContainerId = in.ContainerId
	task.StartedAt = sql.NullTime{Time: time.Now(), Valid: true}
	task.Status = types.TaskStatusRunning

	err = tq.rdb.SetEx(context.TODO(), Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, in.StubId, in.ContainerId, tm.TaskId), 1, time.Duration(defaultTaskRunningExpiration)*time.Second).Err()
	if err != nil {
		return &pb.TaskQueuePopResponse{
			Ok: false,
		}, nil
	}

	_, err = tq.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	if err != nil {
		return &pb.TaskQueuePopResponse{
			Ok: false,
		}, nil
	}

	return &pb.TaskQueuePopResponse{
		Ok: true, TaskMsg: msg,
	}, nil
}

func (tq *RedisTaskQueue) TaskQueueComplete(ctx context.Context, in *pb.TaskQueueCompleteRequest) (*pb.TaskQueueCompleteResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	task, err := tq.backendRepo.GetTask(ctx, in.TaskId)
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.SetEx(ctx, Keys.taskQueueKeepWarmLock(authInfo.Workspace.Name, in.StubId, in.ContainerId), 1, time.Duration(in.KeepWarmSeconds)*time.Second).Err()
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.Del(ctx, Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, in.StubId, in.ContainerId, in.TaskId)).Err()
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.RPush(ctx, Keys.taskQueueTaskDuration(authInfo.Workspace.Name, in.StubId), in.TaskDuration).Err()
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	task.EndedAt = sql.NullTime{Time: time.Now(), Valid: true}
	task.Status = types.TaskStatus(in.TaskStatus)

	err = tq.taskDispatcher.Complete(ctx, authInfo.Workspace.Name, in.StubId, in.TaskId)
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}, nil
	}

	_, err = tq.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	return &pb.TaskQueueCompleteResponse{
		Ok: err == nil,
	}, nil
}

func (tq *RedisTaskQueue) TaskQueueMonitor(req *pb.TaskQueueMonitorRequest, stream pb.TaskQueueService_TaskQueueMonitorServer) error {
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	task, err := tq.backendRepo.GetTask(ctx, req.TaskId)
	if err != nil {
		return err
	}

	stubConfig, err := tq.getStubConfig(req.StubId)
	if err != nil {
		return err
	}

	timeout := int64(stubConfig.TaskPolicy.Timeout)
	timeoutCallback := func() error {
		task.Status = types.TaskStatusTimeout
		_, err = tq.backendRepo.UpdateTask(
			stream.Context(),
			task.ExternalId,
			*task,
		)
		if err != nil {
			return err
		}

		return nil
	}

	// Listen for task cancellation events
	channelKey := Keys.taskQueueTaskCancel(authInfo.Workspace.Name, req.StubId, task.ExternalId)
	cancelFlag := make(chan bool, 1)
	timeoutFlag := make(chan bool, 1)

	var timeoutChan <-chan time.Time
	taskStartTimeSeconds := task.StartedAt.Time.Unix()
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
			messages, errs := tq.rdb.Subscribe(ctx, channelKey)

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
					if msg.Payload == task.ExternalId {
						cancelFlag <- true
						return
					}

				case <-ctx.Done():
					return

				case err := <-errs:
					if err != nil {
						log.Printf("error with monitor task subscription: %v", err)
						break retry
					}
				}
			}
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil

		case <-cancelFlag:
			err := tq.taskDispatcher.Complete(ctx, authInfo.Workspace.Name, req.StubId, task.ExternalId)
			if err != nil {
				return err
			}

			stream.Send(&pb.TaskQueueMonitorResponse{Ok: true, Cancelled: true, Complete: false, TimedOut: false})
			return nil

		case <-timeoutFlag:
			err := tq.taskDispatcher.Complete(ctx, authInfo.Workspace.Name, req.StubId, task.ExternalId)
			if err != nil {
				return err
			}

			stream.Send(&pb.TaskQueueMonitorResponse{Ok: true, Cancelled: false, Complete: false, TimedOut: true})
			return nil

		default:
			if task.Status == types.TaskStatusCancelled {
				stream.Send(&pb.TaskQueueMonitorResponse{Ok: true, Cancelled: true, Complete: false, TimedOut: false})
				return nil
			}

			err := tq.rdb.SetEx(ctx, Keys.taskQueueTaskHeartbeat(authInfo.Workspace.Name, req.StubId, task.ExternalId), 1, time.Duration(60)*time.Second).Err()
			if err != nil {
				return err
			}

			err = tq.rdb.SetEx(ctx, Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, req.StubId, req.ContainerId, task.ExternalId), 1, time.Duration(defaultTaskRunningExpiration)*time.Second).Err()
			if err != nil {
				return err
			}

			claimed, err := tq.taskRepo.IsClaimed(ctx, authInfo.Workspace.Name, req.StubId, task.ExternalId)
			if err != nil {
				return err
			}

			if !claimed {
				stream.Send(&pb.TaskQueueMonitorResponse{Ok: true, Cancelled: false, Complete: true, TimedOut: false})
			}

			stream.Send(&pb.TaskQueueMonitorResponse{Ok: true, Cancelled: false, Complete: false, TimedOut: false})
			time.Sleep(time.Second * 1)
		}
	}
}

func (tq *RedisTaskQueue) TaskQueueLength(ctx context.Context, in *pb.TaskQueueLengthRequest) (*pb.TaskQueueLengthResponse, error) {
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

func (tq *RedisTaskQueue) createQueueInstance(stubId string) error {
	_, exists := tq.queueInstances.Get(stubId)
	if exists {
		return errors.New("queue already in memory")
	}

	stub, err := tq.backendRepo.GetStubByExternalId(tq.ctx, stubId)
	if err != nil {
		return errors.New("invalid stub id")
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
		workspace:          &stub.Workspace,
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

func (tq *RedisTaskQueue) handleContainerEvents() {
	for {
		select {
		case event := <-tq.keyEventChan:
			containerId := fmt.Sprintf("%s%s", taskQueueContainerPrefix, event.Key)

			operation := event.Operation
			containerIdParts := strings.Split(containerId, "-")
			stubId := strings.Join(containerIdParts[1:6], "-")

			queue, exists := tq.queueInstances.Get(stubId)
			if !exists {
				err := tq.createQueueInstance(stubId)
				if err != nil {
					continue
				}

				queue, _ = tq.queueInstances.Get(stubId)
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
	taskQueueList                string = "taskqueue:%s:%s"
	taskQueueInstanceLock        string = "taskqueue:%s:%s:instance_lock"
	taskQueueTaskDuration        string = "taskqueue:%s:%s:task_duration"
	taskQueueAverageTaskDuration string = "taskqueue:%s:%s:avg_task_duration"
	taskQueueTaskCancel          string = "taskqueue:%s:%s:task:cancel:%s"
	taskQueueTaskHeartbeat       string = "taskqueue:%s:%s:task:heartbeat:%s"
	taskQueueProcessingLock      string = "taskqueue:%s:%s:processing_lock:%s"
	taskQueueKeepWarmLock        string = "taskqueue:%s:%s:keep_warm_lock:%s"
	taskQueueTaskRunningLock     string = "taskqueue:%s:%s:task_running:%s:%s"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) taskQueueInstanceLock(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueInstanceLock, workspaceName, stubId)
}

func (k *keys) taskQueueList(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueList, workspaceName, stubId)
}

func (k *keys) taskQueueTaskCancel(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskCancel, workspaceName, stubId, taskId)
}

func (k *keys) taskQueueTaskHeartbeat(workspaceName, stubId, taskId string) string {
	return fmt.Sprintf(taskQueueTaskHeartbeat, workspaceName, stubId, taskId)
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
