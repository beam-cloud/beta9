package taskqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	taskdispatcher "github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TaskQueueService interface {
	pb.TaskQueueServiceServer
	TaskQueuePut(ctx context.Context, req *pb.TaskQueuePutRequest) (*pb.TaskQueuePutResponse, error)
	TaskQueuePop(ctx context.Context, req *pb.TaskQueuePopRequest) (*pb.TaskQueuePopResponse, error)
	TaskQueueLength(ctx context.Context, req *pb.TaskQueueLengthRequest) (*pb.TaskQueueLengthResponse, error)
	TaskQueueComplete(ctx context.Context, req *pb.TaskQueueCompleteRequest) (*pb.TaskQueueCompleteResponse, error)
	TaskQueueMonitor(req *pb.TaskQueueMonitorRequest, stream pb.TaskQueueService_TaskQueueMonitorServer) error
	StartTaskQueueServe(ctx context.Context, req *pb.StartTaskQueueServeRequest) (*pb.StartTaskQueueServeResponse, error)
}

type TaskQueueServiceOpts struct {
	Config           types.AppConfig
	RedisClient      *common.RedisClient
	BackendRepo      repository.BackendRepository
	WorkspaceRepo    repository.WorkspaceRepository
	TaskRepo         repository.TaskRepository
	ContainerRepo    repository.ContainerRepository
	Scheduler        *scheduler.Scheduler
	Tailscale        *network.Tailscale
	RouteGroup       *echo.Group
	TaskDispatcher   *taskdispatcher.Dispatcher
	EventRepo        repository.EventRepository
	UsageMetricsRepo repository.UsageMetricsRepository
}

const (
	DefaultTaskQueueTaskTTL uint32 = 3600 * 2 // 2 hours

	taskQueueContainerPrefix string = "taskqueue"
	taskQueueRoutePrefix     string = "/taskqueue"
)

type RedisTaskQueue struct {
	ctx             context.Context
	mu              sync.Mutex
	config          types.AppConfig
	rdb             *common.RedisClient
	stubConfigCache *common.SafeMap[*types.StubConfigV1]
	taskDispatcher  *taskdispatcher.Dispatcher
	taskRepo        repository.TaskRepository
	containerRepo   repository.ContainerRepository
	workspaceRepo   repository.WorkspaceRepository
	backendRepo     repository.BackendRepository
	scheduler       *scheduler.Scheduler
	pb.UnimplementedTaskQueueServiceServer
	queueInstances     *common.SafeMap[*taskQueueInstance]
	keyEventManager    *common.KeyEventManager
	queueClient        *taskQueueClient
	tailscale          *network.Tailscale
	eventRepo          repository.EventRepository
	usageMetricsRepo   repository.UsageMetricsRepository
	controller         *abstractions.InstanceController
	storageClientCache sync.Map
}

func NewRedisTaskQueueService(
	ctx context.Context,
	opts TaskQueueServiceOpts,
) (TaskQueueService, error) {
	keyEventManager := common.NewKeyEventManager(opts.RedisClient)

	tq := &RedisTaskQueue{
		ctx:                ctx,
		mu:                 sync.Mutex{},
		config:             opts.Config,
		rdb:                opts.RedisClient,
		scheduler:          opts.Scheduler,
		stubConfigCache:    common.NewSafeMap[*types.StubConfigV1](),
		keyEventManager:    keyEventManager,
		workspaceRepo:      opts.WorkspaceRepo,
		taskDispatcher:     opts.TaskDispatcher,
		taskRepo:           opts.TaskRepo,
		containerRepo:      opts.ContainerRepo,
		backendRepo:        opts.BackendRepo,
		queueClient:        newRedisTaskQueueClient(opts.RedisClient, opts.TaskRepo),
		queueInstances:     common.NewSafeMap[*taskQueueInstance](),
		tailscale:          opts.Tailscale,
		eventRepo:          opts.EventRepo,
		usageMetricsRepo:   opts.UsageMetricsRepo,
		storageClientCache: sync.Map{},
	}

	// Listen for container events with a certain prefix
	// For example if a container is created, destroyed, or updated
	eventManager, err := abstractions.NewContainerEventManager(ctx, []string{taskQueueContainerPrefix}, keyEventManager, tq.InstanceFactory)
	if err != nil {
		return nil, err
	}
	eventManager.Listen()

	// Initialize deployment manager
	tq.controller = abstractions.NewInstanceController(
		ctx,
		tq.InstanceFactory,
		tq.GetInstance,
		[]string{types.StubTypeTaskQueueDeployment},
		opts.BackendRepo,
		opts.ContainerRepo,
		opts.RedisClient,
	)
	err = tq.controller.Init()
	if err != nil {
		return nil, err
	}

	// Register task dispatcher
	tq.taskDispatcher.Register(string(types.ExecutorTaskQueue), tq.taskQueueTaskFactory)

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(opts.BackendRepo, opts.WorkspaceRepo)
	registerTaskQueueRoutes(opts.RouteGroup.Group(taskQueueRoutePrefix, authMiddleware), tq)

	return tq, nil
}

func (tq *RedisTaskQueue) isPublic(stubId string) (*types.Workspace, error) {
	instance, err := tq.getOrCreateQueueInstance(stubId)
	if err != nil {
		return nil, err
	}

	if instance.StubConfig.Authorized {
		return nil, errors.New("unauthorized")
	}

	return instance.Workspace, nil
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
			return nil, err
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
	instance, err := tq.getOrCreateQueueInstance(stubId)
	if err != nil {
		return "", err
	}

	tasksInFlight, err := tq.taskRepo.TasksInFlight(ctx, instance.Workspace.Name, stubId)
	if err != nil {
		return "", err
	}

	if tasksInFlight >= int(instance.StubConfig.MaxPendingTasks) {
		return "", &types.ErrExceededTaskLimit{MaxPendingTasks: instance.StubConfig.MaxPendingTasks}
	}

	policy := instance.StubConfig.TaskPolicy
	if policy.TTL == 0 {
		// Required for backwards compatibility
		policy.TTL = DefaultTaskQueueTaskTTL
	}
	policy.Expires = time.Now().Add(time.Duration(policy.TTL) * time.Second)

	task, err := tq.taskDispatcher.SendAndExecute(ctx, string(types.ExecutorTaskQueue), &auth.AuthInfo{
		Workspace: instance.Workspace,
		Token:     instance.Token,
	}, stubId, payload, policy, authInfo)
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

	// Retrieve the next "valid" task (not in a completed state) from the queue
	task, msg := func() (*types.TaskWithRelated, []byte) {
		for {
			msg, err := instance.client.Pop(ctx, authInfo.Workspace.Name, in.StubId, in.ContainerId)
			if err != nil {
				return nil, nil
			}

			// There are no items left in the queue
			if msg == nil {
				return nil, nil
			}

			var tm types.TaskMessage
			err = tm.Decode(msg)
			if err != nil {
				continue
			}

			t, err := tq.backendRepo.GetTaskWithRelated(ctx, tm.TaskId)
			if err != nil {
				continue
			}

			if t.Status.IsCompleted() {
				instance.client.rdb.Del(ctx, Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, in.StubId, in.ContainerId, t.ExternalId))
				instance.client.rdb.SRem(ctx, Keys.taskQueueTaskRunningLockIndex(authInfo.Workspace.Name, in.StubId, in.ContainerId), t.ExternalId)
				continue
			}

			return t, msg
		}
	}()

	// We couldn't get a valid task from the queue
	if task == nil {
		return &pb.TaskQueuePopResponse{Ok: false}, nil
	}

	err = tq.taskDispatcher.Claim(ctx, authInfo.Workspace.Name, task.Stub.ExternalId, task.ExternalId, in.ContainerId)
	if err != nil {
		return &pb.TaskQueuePopResponse{Ok: false}, nil
	}

	task.ContainerId = in.ContainerId
	task.StartedAt = types.NullTime{}.Now()
	task.Status = types.TaskStatusRunning

	err = tq.rdb.SAdd(ctx, Keys.taskQueueTaskRunningLockIndex(authInfo.Workspace.Name, in.StubId, in.ContainerId), task.ExternalId).Err()
	if err != nil {
		return &pb.TaskQueuePopResponse{
			Ok: false,
		}, nil
	}

	err = tq.rdb.SetEx(context.TODO(), Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, in.StubId, in.ContainerId, task.ExternalId), 1, types.TaskQueueAdmissionLeaseTTL).Err()
	if err != nil {
		return &pb.TaskQueuePopResponse{
			Ok: false,
		}, nil
	}

	_, err = tq.backendRepo.UpdateTask(ctx, task.ExternalId, task.Task)
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
	if in == nil || authInfo == nil || authInfo.Workspace == nil || in.ContainerId == "" {
		return nil, status.Error(codes.InvalidArgument, "task completion requires an authenticated owner")
	}
	policy := tq.config.GatewayService.TaskCompletion
	ctx, cancel, err := taskdispatcher.CompletionContext(ctx, policy)
	if err != nil {
		return nil, err
	}
	defer cancel()
	meta := types.TaskMetadata{WorkspaceName: authInfo.Workspace.Name, StubId: in.StubId, TaskId: in.TaskId, ContainerId: in.ContainerId}
	related, err := tq.backendRepo.GetTaskWithRelated(ctx, in.TaskId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			_ = taskdispatcher.ForgetDeletedTask(ctx, tq.rdb, meta)
		}
		return nil, taskdispatcher.CompletionError(err)
	}
	if related == nil {
		return nil, status.Error(codes.NotFound, "task no longer exists")
	}
	if related.Workspace.ExternalId != authInfo.Workspace.ExternalId || related.Stub.ExternalId != in.StubId {
		return nil, status.Error(codes.PermissionDenied, "task does not belong to this workspace and stub")
	}
	task := &related.Task
	if task.ContainerId != in.ContainerId {
		return nil, status.Error(codes.FailedPrecondition, "container does not own task")
	}
	if task.Status.IsCompleted() {
		if err := tq.taskDispatcher.Complete(ctx, authInfo.Workspace.Name, in.StubId, in.TaskId); err != nil {
			return nil, taskdispatcher.CompletionError(err)
		}
		if task.Status != types.TaskStatus(in.TaskStatus) {
			return nil, status.Error(codes.FailedPrecondition, "task already has a terminal outcome")
		}
		return &pb.TaskQueueCompleteResponse{Ok: true}, nil
	}
	if task.Status != types.TaskStatusRunning {
		return nil, status.Error(codes.FailedPrecondition, "task is not running")
	}
	requested := types.TaskStatus(in.TaskStatus)
	if !requested.IsCompleted() && requested != types.TaskStatusRetry {
		return nil, status.Error(codes.InvalidArgument, "invalid completion status")
	}
	ctx, cancelWindow, err := taskdispatcher.BeginCompletion(ctx, tq.rdb, meta, policy.Timeout)
	if err != nil {
		return nil, err
	}
	defer cancelWindow()
	ctx, stopHeartbeat, err := taskdispatcher.KeepFinalizationAlive(ctx, in.TaskId, func(ctx context.Context) error {
		return tq.refreshTaskLeases(ctx, meta.WorkspaceName, meta.StubId, meta.ContainerId, meta.TaskId)
	})
	if err != nil {
		return nil, err
	}

	// Keep the task's admission lease until result persistence and the final
	// task update finish. A worker rollout may stop this container as soon as
	// its last lease disappears.
	finalized := false
	defer func() {
		// Stop and join the renewer before deleting leases, so it cannot
		// recreate a lease after successful finalization.
		stopHeartbeat()
		if !finalized {
			return
		}
		pipe := tq.rdb.TxPipeline()
		pipe.SRem(context.WithoutCancel(ctx), Keys.taskQueueTaskRunningLockIndex(authInfo.Workspace.Name, in.StubId, in.ContainerId), in.TaskId)
		pipe.Del(context.WithoutCancel(ctx), Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, in.StubId, in.ContainerId, in.TaskId))
		_, _ = pipe.Exec(context.WithoutCancel(ctx))
	}()

	if in.KeepWarmSeconds > 0 {
		err = tq.rdb.SetEx(ctx, Keys.taskQueueKeepWarmLock(authInfo.Workspace.Name, in.StubId, in.ContainerId), 1, time.Duration(in.KeepWarmSeconds)*time.Second).Err()
		if err != nil {
			return nil, taskdispatcher.CompletionError(err)
		}
	}

	err = tq.rdb.RPush(ctx, Keys.taskQueueTaskDuration(authInfo.Workspace.Name, in.StubId), in.TaskDuration).Err()
	if err != nil {
		return nil, taskdispatcher.CompletionError(err)
	}

	task.EndedAt = types.NullTime{}.Now()
	task.Status = types.TaskStatus(in.TaskStatus)

	if task.Status == types.TaskStatusRetry {
		response := tq.retryTask(ctx, authInfo, in)
		finalized = response.Ok
		return response, nil
	}

	workspace := authInfo.Workspace

	if in.Result != nil && workspace.StorageAvailable() {
		uploadCtx, cancelUpload := context.WithTimeout(ctx, policy.ResultUploadTimeout)
		err = tq.taskDispatcher.StoreTaskResult(uploadCtx, workspace, task.ExternalId, in.Result)
		cancelUpload()
		if err != nil {
			log.Error().Err(err).Msgf("error storing task result for task <%s>", task.ExternalId)
			return nil, taskdispatcher.CompletionError(err)
		}
	}

	_, err = tq.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	if err != nil {
		log.Error().Err(err).Str("task_id", in.TaskId).Str("container_id", in.ContainerId).Msg("failed to persist task queue completion; dispatcher state retained")
		return nil, taskdispatcher.CompletionError(err)
	}

	// Removing the claim tells the runner's monitor that the task is complete.
	// Keep it alive through result storage and the database update so its
	// container lease cannot expire while finalization is still in progress.
	err = tq.taskDispatcher.Complete(ctx, authInfo.Workspace.Name, in.StubId, in.TaskId)
	finalized = err == nil
	if err != nil {
		return nil, taskdispatcher.CompletionError(err)
	}
	return &pb.TaskQueueCompleteResponse{Ok: true}, nil
}

// Monitors and accepted completions renew the same task and container leases.
func (tq *RedisTaskQueue) refreshTaskLeases(ctx context.Context, workspace, stubID, containerID, taskID string) error {
	return taskdispatcher.RenewTaskLiveness(ctx, tq.rdb, types.TaskMetadata{WorkspaceName: workspace, StubId: stubID, ContainerId: containerID, TaskId: taskID},
		Keys.taskQueueTaskHeartbeat(workspace, stubID, taskID),
		Keys.taskQueueTaskRunningLock(workspace, stubID, containerID, taskID),
		Keys.taskQueueTaskRunningLockIndex(workspace, stubID, containerID))
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
	channelKey := common.RedisKeys.TaskCancel(authInfo.Workspace.Name, req.StubId, req.TaskId)
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
			log.Error().Err(err).Msg("error timing out task")
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
				case <-tq.ctx.Done():
					return

				case <-timeoutChan:
					err := timeoutCallback()
					if err != nil {
						log.Error().Err(err).Msg("task timeout err")
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
						log.Error().Err(err).Msg("monitor task subscription err")
						break retry
					}
				}
			}
		}
	}()

	for {
		select {
		case <-tq.ctx.Done():
			return nil

		case <-stream.Context().Done():
			task, err := tq.backendRepo.GetTask(ctx, req.TaskId)
			if err != nil {
				return err
			}

			if task.Status.IsCompleted() {
				tq.rdb.Del(context.Background(), Keys.taskQueueTaskRunningLock(authInfo.Workspace.Name, req.StubId, req.ContainerId, task.ExternalId))
			}

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

			err := tq.refreshTaskLeases(ctx, authInfo.Workspace.Name, req.StubId, req.ContainerId, task.ExternalId)
			if err != nil {
				claimed, checkErr := tq.taskRepo.IsClaimed(ctx, authInfo.Workspace.Name, req.StubId, task.ExternalId)
				if checkErr == nil && !claimed {
					return stream.Send(&pb.TaskQueueMonitorResponse{Ok: true, Complete: true})
				}
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
			time.Sleep(types.TaskHeartbeatInterval)
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

func (tq *RedisTaskQueue) InstanceFactory(ctx context.Context, stubId string, options ...func(abstractions.IAutoscaledInstance)) (abstractions.IAutoscaledInstance, error) {
	return tq.getOrCreateQueueInstance(stubId)
}

func (tq *RedisTaskQueue) GetInstance(stubId string) (abstractions.IAutoscaledInstance, bool) {
	instance, exists := tq.queueInstances.Get(stubId)
	if !exists {
		return nil, false
	}
	return instance, true
}

func (tq *RedisTaskQueue) getOrCreateQueueInstance(stubId string, options ...func(*taskQueueInstance)) (*taskQueueInstance, error) {
	instance, exists := tq.queueInstances.Get(stubId)
	if exists {
		return instance, nil
	}

	// The reason we lock here, and then check again -- is because if the instance does not exist, we may have two separate
	// goroutines trying to create the instance. So, we check first, then get the mutex. If another
	// routine got the lock, it should have created the instance, so we check once again. That way
	// we don't create two instances of the same stub, but we also ensure that we return quickly if the instance
	// _does_ already exist.
	tq.mu.Lock()
	defer tq.mu.Unlock()

	instance, exists = tq.queueInstances.Get(stubId)
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
		AppConfig:           tq.config,
		Rdb:                 tq.rdb,
		Stub:                stub,
		StubConfig:          stubConfig,
		Object:              &stub.Object,
		Workspace:           &stub.Workspace,
		Token:               token,
		Scheduler:           tq.scheduler,
		ContainerRepo:       tq.containerRepo,
		BackendRepo:         tq.backendRepo,
		EventRepo:           tq.eventRepo,
		UsageMetricsRepo:    tq.usageMetricsRepo,
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
		if stub.Type.IsDeployment() || stub.Type == types.StubType(types.StubTypeTaskQueue) {
			instance.Autoscaler = abstractions.NewAutoscaler(instance, taskQueueAutoscalerSampleFunc, taskQueueScaleFunc)
		} else if stub.Type.IsServe() {
			instance.Autoscaler = abstractions.NewAutoscaler(instance, taskQueueAutoscalerSampleFunc, taskQueueServeScaleFunc)
		}
	}

	if len(instance.EntryPoint) == 0 {
		instance.EntryPoint = []string{instance.StubConfig.PythonVersion, "-m", "beta9.runner.taskqueue"}
	}

	tq.queueInstances.Set(stubId, instance)

	// Monitor and then clean up the instance once it's done
	go instance.Monitor()
	go func(i *taskQueueInstance) {
		<-i.Ctx.Done()
		tq.queueInstances.Delete(stubId)
	}(instance)

	return instance, nil
}

func (tq *RedisTaskQueue) retryTask(ctx context.Context, authInfo *auth.AuthInfo, in *pb.TaskQueueCompleteRequest) *pb.TaskQueueCompleteResponse {
	task, err := tq.taskDispatcher.Retrieve(ctx, authInfo.Workspace.Name, in.StubId, in.TaskId)
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}
	}

	msg := ""
	if task.Message().Retries >= task.Message().Policy.MaxRetries {
		msg = fmt.Sprintf("Exceeded retry limit of %d for task <%s>", task.Message().Policy.MaxRetries, task.Message().TaskId)
	}

	err = tq.taskDispatcher.RetryTask(ctx, task)
	if err != nil {
		return &pb.TaskQueueCompleteResponse{
			Ok: false,
		}
	}

	return &pb.TaskQueueCompleteResponse{
		Ok:      true,
		Message: msg,
	}
}
