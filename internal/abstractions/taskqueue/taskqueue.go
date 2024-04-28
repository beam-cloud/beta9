package taskqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	abstractions "github.com/beam-cloud/beta9/internal/abstractions/common"
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
	StartTaskQueueServe(in *pb.StartTaskQueueServeRequest, stream pb.TaskQueueService_StartTaskQueueServeServer) error
	StopTaskQueueServe(ctx context.Context, in *pb.StopTaskQueueServeRequest) (*pb.StopTaskQueueServeResponse, error)
}

type TaskQueueServiceOpts struct {
	Config         types.AppConfig
	RedisClient    *common.RedisClient
	BackendRepo    repository.BackendRepository
	TaskRepo       repository.TaskRepository
	ContainerRepo  repository.ContainerRepository
	Scheduler      *scheduler.Scheduler
	Tailscale      *network.Tailscale
	RouteGroup     *echo.Group
	TaskDispatcher *task.Dispatcher
}

const (
	taskQueueContainerPrefix                 string        = "taskqueue"
	taskQueueRoutePrefix                     string        = "/taskqueue"
	taskQueueDefaultTaskExpiration           int           = 3600 * 12 // 12 hours
	taskQueueServeContainerTimeout           time.Duration = 600 * time.Second
	taskQueueServeContainerKeepaliveInterval time.Duration = 30 * time.Second
)

type RedisTaskQueue struct {
	ctx             context.Context
	config          types.AppConfig
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
	queueClient     *taskQueueClient
	tailscale       *network.Tailscale
}

func NewRedisTaskQueueService(
	ctx context.Context,
	opts TaskQueueServiceOpts,
) (TaskQueueService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()

	tq := &RedisTaskQueue{
		ctx:             ctx,
		config:          config,
		rdb:             opts.RedisClient,
		scheduler:       opts.Scheduler,
		stubConfigCache: common.NewSafeMap[*types.StubConfigV1](),
		keyEventManager: keyEventManager,
		taskDispatcher:  opts.TaskDispatcher,
		taskRepo:        opts.TaskRepo,
		containerRepo:   opts.ContainerRepo,
		backendRepo:     opts.BackendRepo,
		queueClient:     newRedisTaskQueueClient(opts.RedisClient, opts.TaskRepo),
		queueInstances:  common.NewSafeMap[*taskQueueInstance](),
		tailscale:       opts.Tailscale,
	}

	// Listen for container events with a certain prefix
	// For example if a container is created, destroyed, or updated
	eventManager, err := abstractions.NewContainerEventManager(taskQueueContainerPrefix, keyEventManager, tq.InstanceFactory)
	if err != nil {
		return nil, err
	}
	eventManager.Listen(ctx)

	// Register task dispatcher
	tq.taskDispatcher.Register(string(types.ExecutorTaskQueue), tq.taskQueueTaskFactory)

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(opts.BackendRepo)
	registerTaskQueueRoutes(opts.RouteGroup.Group(taskQueueRoutePrefix, authMiddleware), tq)

	return tq, nil
}

func (tq *RedisTaskQueue) taskQueueTaskFactory(ctx context.Context, msg types.TaskMessage) (types.TaskInterface, error) {
	return &TaskQueueTask{
		tq:  tq,
		msg: &msg,
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

func (tq *RedisTaskQueue) put(ctx context.Context, authInfo *auth.AuthInfo, stubId string, payload *types.TaskPayload) (string, error) {
	stubConfig, err := tq.getStubConfig(stubId)
	if err != nil {
		return "", err
	}

	policy := stubConfig.TaskPolicy
	policy.Expires = time.Now().Add(time.Duration(taskQueueDefaultTaskExpiration) * time.Second)

	task, err := tq.taskDispatcher.SendAndExecute(ctx, string(types.ExecutorTaskQueue), authInfo, stubId, payload, policy)
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

	taskId, err := tq.put(ctx, authInfo, in.StubId, &payload)
	return &pb.TaskQueuePutResponse{
		Ok:     err == nil,
		TaskId: taskId,
	}, nil
}

func (tq *RedisTaskQueue) TaskQueuePop(ctx context.Context, in *pb.TaskQueuePopRequest) (*pb.TaskQueuePopResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	instance, err := tq.getOrCreateQueueInstance(in.StubId)
	if err != nil {
		return &pb.TaskQueuePopResponse{
			Ok: false,
		}, nil
	}

	msg, err := instance.client.Pop(ctx, authInfo.Workspace.Name, in.StubId, in.ContainerId)
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
					if msg != nil && task != nil && msg.Payload == task.ExternalId {
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

			err = tq.rdb.SetEx(ctx, Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, req.StubId, req.ContainerId, task.ExternalId), 1, time.Duration(1)*time.Second).Err()
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

	length, err := tq.queueClient.QueueLength(ctx, authInfo.Workspace.Name, in.StubId)
	if err != nil {
		return &pb.TaskQueueLengthResponse{Ok: false}, nil
	}

	return &pb.TaskQueueLengthResponse{
		Ok:     true,
		Length: length,
	}, nil
}

func (tq *RedisTaskQueue) InstanceFactory(stubId string, options ...func(abstractions.IAutoscaledInstance)) (abstractions.IAutoscaledInstance, error) {
	return tq.getOrCreateQueueInstance(stubId)
}

func (tq *RedisTaskQueue) getOrCreateQueueInstance(stubId string, options ...func(*taskQueueInstance)) (*taskQueueInstance, error) {
	instance, exists := tq.queueInstances.Get(stubId)
	if exists {
		return instance, nil
	}

	stub, err := tq.backendRepo.GetStubByExternalId(tq.ctx, stubId)
	if err != nil {
		return nil, errors.New("invalid stub id")
	}

	var stubConfig *types.StubConfigV1 = &types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), stubConfig)
	if err != nil {
		return nil, err
	}

	token, err := tq.backendRepo.RetrieveActiveToken(tq.ctx, stub.Workspace.Id)
	if err != nil {
		return nil, err
	}

	// Create queue instance to hold taskqueue specific methods/fields
	instance = &taskQueueInstance{
		client: tq.queueClient,
	}

	// Create base autoscaled instance
	autoscaledInstance, err := abstractions.NewAutoscaledInstance(tq.ctx, &abstractions.AutoscaledInstanceConfig{
		Name:                fmt.Sprintf("%s-%s", stub.Name, stub.ExternalId),
		Rdb:                 tq.rdb,
		Stub:                &stub.Stub,
		StubConfig:          stubConfig,
		Object:              &stub.Object,
		Workspace:           &stub.Workspace,
		Token:               token,
		Scheduler:           tq.scheduler,
		ContainerRepo:       tq.containerRepo,
		BackendRepo:         tq.backendRepo,
		TaskRepo:            tq.taskRepo,
		InstanceLockKey:     Keys.taskQueueInstanceLock(stub.Workspace.Name, stubId),
		StartContainersFunc: instance.startContainers,
		StopContainersFunc:  instance.stopContainers,
	})
	if err != nil {
		return nil, err
	}

	// Embed autoscaled instance struct
	instance.AutoscaledInstance = autoscaledInstance

	// Set all options on the instance
	for _, o := range options {
		o(instance)
	}

	if instance.Autoscaler == nil {
		switch instance.Stub.Type {
		case types.StubTypeTaskQueueDeployment, types.StubTypeTaskQueue:
			instance.Autoscaler = abstractions.NewAutoscaler(instance, taskQueueAutoscalerSampleFunc, taskQueueScaleFunc)
		case types.StubTypeTaskQueueServe:
			instance.Autoscaler = abstractions.NewAutoscaler(instance, taskQueueAutoscalerSampleFunc, taskQueueServeScaleFunc)
		}
	}

	if len(instance.EntryPoint) == 0 {
		instance.EntryPoint = []string{instance.StubConfig.PythonVersion, "-m", "beta9.runner.taskqueue"}
	}

	tq.queueInstances.Set(stubId, instance)

	// Monitor and then clean up the instance once it's done
	go instance.Monitor()
	go func(q *taskQueueInstance) {
		<-q.Ctx.Done()
		tq.queueInstances.Delete(stubId)
	}(instance)

	return instance, nil
}

// Redis keys
var (
	taskQueueList                string = "taskqueue:%s:%s"
	taskQueueServeLock           string = "taskqueue:%s:%s:serve_lock"
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

func (k *keys) taskQueueServeLock(workspaceName, stubId string) string {
	return fmt.Sprintf(taskQueueServeLock, workspaceName, stubId)
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
