package function

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/task"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

type FunctionService interface {
	pb.FunctionServiceServer
	FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error
	FunctionGetArgs(ctx context.Context, in *pb.FunctionGetArgsRequest) (*pb.FunctionGetArgsResponse, error)
	FunctionSetResult(ctx context.Context, in *pb.FunctionSetResultRequest) (*pb.FunctionSetResultResponse, error)
}

const (
	DefaultFunctionTaskTTL uint32 = 3600 * 12 // 12 hours

	functionRoutePrefix              string        = "/function"
	scheduleRoutePrefix              string        = "/schedule"
	defaultFunctionContainerCpu      int64         = 100
	defaultFunctionContainerMemory   int64         = 128
	defaultFunctionHeartbeatTimeoutS int64         = 60
	functionArgsExpirationTimeout    time.Duration = 600 * time.Second
	functionResultExpirationTimeout  time.Duration = 600 * time.Second
)

type ContainerFunctionService struct {
	pb.UnimplementedFunctionServiceServer
	ctx              context.Context
	taskDispatcher   *task.Dispatcher
	backendRepo      repository.BackendRepository
	workspaceRepo    repository.WorkspaceRepository
	taskRepo         repository.TaskRepository
	containerRepo    repository.ContainerRepository
	eventRepo        repository.EventRepository
	usageMetricsRepo repository.UsageMetricsRepository
	scheduler        *scheduler.Scheduler
	tailscale        *network.Tailscale
	config           types.AppConfig
	keyEventManager  *common.KeyEventManager
	rdb              *common.RedisClient
	routeGroup       *echo.Group
}

type FunctionServiceOpts struct {
	Config           types.AppConfig
	RedisClient      *common.RedisClient
	BackendRepo      repository.BackendRepository
	WorkspaceRepo    repository.WorkspaceRepository
	TaskRepo         repository.TaskRepository
	ContainerRepo    repository.ContainerRepository
	Scheduler        *scheduler.Scheduler
	Tailscale        *network.Tailscale
	RouteGroup       *echo.Group
	TaskDispatcher   *task.Dispatcher
	EventRepo        repository.EventRepository
	UsageMetricsRepo repository.UsageMetricsRepository
}

func NewContainerFunctionService(ctx context.Context,
	opts FunctionServiceOpts,
) (FunctionService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	fs := &ContainerFunctionService{
		ctx:              ctx,
		config:           opts.Config,
		backendRepo:      opts.BackendRepo,
		workspaceRepo:    opts.WorkspaceRepo,
		taskRepo:         opts.TaskRepo,
		containerRepo:    opts.ContainerRepo,
		scheduler:        opts.Scheduler,
		tailscale:        opts.Tailscale,
		rdb:              opts.RedisClient,
		keyEventManager:  keyEventManager,
		taskDispatcher:   opts.TaskDispatcher,
		routeGroup:       opts.RouteGroup,
		eventRepo:        opts.EventRepo,
		usageMetricsRepo: opts.UsageMetricsRepo,
	}
	// Register task dispatcher
	fs.taskDispatcher.Register(string(types.ExecutorFunction), fs.functionTaskFactory)

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(fs.backendRepo, fs.workspaceRepo)
	registerFunctionRoutes(fs.routeGroup.Group(functionRoutePrefix, authMiddleware), fs)
	registerFunctionRoutes(fs.routeGroup.Group(scheduleRoutePrefix, authMiddleware), fs)

	go fs.listenForScheduledJobs()
	go abstractions.ListenForTaskSchedulingFailures(ctx, fs.rdb, fs.backendRepo, fs.taskDispatcher)

	return fs, nil
}

func (fs *ContainerFunctionService) FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error {
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())
	ctx := stream.Context()

	task, err := fs.invoke(ctx, authInfo, in.StubId, &types.TaskPayload{
		Args: []interface{}{in.Args},
	})
	if err != nil {
		return err
	}

	return fs.stream(ctx, stream, authInfo, task, in.Headless)
}

func (fs *ContainerFunctionService) invoke(ctx context.Context, authInfo *auth.AuthInfo, stubId string, payload *types.TaskPayload) (types.TaskInterface, error) {
	stub, err := fs.backendRepo.GetStubByExternalId(ctx, stubId)
	if err != nil {
		return nil, err
	}

	stubConfig := types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), &stubConfig)
	if err != nil {
		return nil, err
	}

	policy := stubConfig.TaskPolicy
	if policy.TTL == 0 {
		// This is required for backward compatibility since older functions do not have a TTL set which defaults to 0
		policy.TTL = DefaultFunctionTaskTTL
	}
	policy.Expires = time.Now().Add(time.Duration(policy.TTL) * time.Second)

	// Functions called by external workspaces should not be retried if they fail
	if stubConfig.Pricing != nil && stub.Workspace.ExternalId != authInfo.Workspace.ExternalId {
		policy.MaxRetries = 0
	}

	task, err := fs.taskDispatcher.SendAndExecute(ctx, string(types.ExecutorFunction), &auth.AuthInfo{
		Workspace: &stub.Workspace,
	}, stubId, payload, policy, authInfo, stubConfig)
	if err != nil {
		return nil, err
	}

	go fs.eventRepo.PushRunStubEvent(authInfo.Workspace.ExternalId, &stub.Stub)
	return task, err
}

func (fs *ContainerFunctionService) functionTaskFactory(ctx context.Context, msg types.TaskMessage) (types.TaskInterface, error) {
	return &FunctionTask{
		msg: &msg,
		fs:  fs,
	}, nil
}

func (fs *ContainerFunctionService) stream(ctx context.Context, stream pb.FunctionService_FunctionInvokeServer, authInfo *auth.AuthInfo, task types.TaskInterface, headless bool) error {
	taskId := task.Metadata().TaskId
	containerId := task.Metadata().ContainerId
	clientCtx := ctx
	streamDone := make(chan struct{})
	completionObserved := atomic.Bool{}

	sendCallback := func(o common.OutputMsg) error {
		if err := stream.Send(&pb.FunctionInvokeResponse{TaskId: taskId, Output: o.Msg, Done: o.Done}); err != nil {
			return err
		}

		return nil
	}

	exitCallback := func(exitCode int32) error {
		completionObserved.Store(true)
		resultLoadStart := time.Now()
		result, _ := fs.rdb.Get(stream.Context(), Keys.FunctionResult(authInfo.Workspace.Name, taskId)).Bytes()
		if fs.eventRepo != nil {
			fs.eventRepo.PushFunctionResultLoaded(authInfo.Workspace.ExternalId, task, exitCode, len(result))
		}
		if err := stream.Send(&pb.FunctionInvokeResponse{TaskId: taskId, Done: true, Result: result, ExitCode: int32(exitCode)}); err != nil {
			return err
		}
		if fs.eventRepo != nil {
			fs.eventRepo.PushFunctionResultDelivery(authInfo.Workspace.ExternalId, task, resultLoadStart, exitCode, len(result))
			fs.eventRepo.PushFunctionResultSent(authInfo.Workspace.ExternalId, task, exitCode, len(result))
		}
		return nil
	}

	containerStream, err := abstractions.NewContainerStream(abstractions.ContainerStreamOpts{
		SendCallback:    sendCallback,
		ExitCallback:    exitCallback,
		ContainerRepo:   fs.containerRepo,
		Config:          fs.config,
		Tailscale:       fs.tailscale,
		KeyEventManager: fs.keyEventManager,
	})
	if err != nil {
		return err
	}

	go func() {
		if headless {
			return
		}

		select {
		case <-streamDone:
			return
		case <-clientCtx.Done():
		}

		if completionObserved.Load() {
			return
		}

		if fs.eventRepo != nil {
			fs.eventRepo.PushFunctionStreamCancelRequested(authInfo.Workspace.ExternalId, task)
		}
		if err := task.Cancel(context.Background(), types.TaskRequestCancelled); err != nil {
			log.Error().Err(err).Str("task_id", task.Message().TaskId).Str("stub_id", task.Message().StubId).Str("workspace_id", authInfo.Workspace.ExternalId).Msg("error cancelling task")
		} else if fs.eventRepo != nil {
			fs.eventRepo.PushFunctionStreamCancelApplied(authInfo.Workspace.ExternalId, task)
		}

		if err := fs.taskDispatcher.Complete(context.Background(), authInfo.Workspace.Name, task.Message().StubId, task.Message().TaskId); err != nil {
			log.Error().Err(err).Str("task_id", task.Message().TaskId).Str("stub_id", task.Message().StubId).Str("workspace_id", authInfo.Workspace.ExternalId).Msg("error completing task")
		}

		if err := fs.rdb.Publish(context.Background(), common.RedisKeys.TaskCancel(authInfo.Workspace.Name, task.Message().StubId, task.Message().TaskId), task.Message().TaskId).Err(); err != nil {
			log.Error().Err(err).Str("task_id", task.Message().TaskId).Str("stub_id", task.Message().StubId).Str("workspace_id", authInfo.Workspace.ExternalId).Msg("error publishing task cancel event")
		}
	}()

	ctx, cancel := common.MergeContexts(fs.ctx, ctx)
	err = containerStream.Stream(ctx, authInfo, containerId)
	close(streamDone)
	cancel()
	return err
}

func (fs *ContainerFunctionService) FunctionGetArgs(ctx context.Context, in *pb.FunctionGetArgsRequest) (*pb.FunctionGetArgsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	now := time.Now()
	phaseMetrics := task.NewPhaseMetrics(fs.rdb)
	phaseLabels := phaseMetrics.Labels(ctx, authInfo.Workspace.Name, in.TaskId, map[string]string{
		"workspace_id": authInfo.Workspace.ExternalId,
	})

	value, err := fs.rdb.Get(ctx, Keys.FunctionArgs(authInfo.Workspace.Name, in.TaskId)).Bytes()
	if err != nil {
		phaseLabels["success"] = "false"
		phaseMetrics.RecordSince(ctx, authInfo.Workspace.Name, in.TaskId, "start_task_to_get_args", task.FunctionPhaseStartTask, now, phaseLabels)
		return &pb.FunctionGetArgsResponse{Ok: false, Args: nil}, nil
	}

	err = fs.rdb.SetEx(ctx, Keys.FunctionHeartbeat(authInfo.Workspace.Name, in.TaskId), 1, time.Duration(defaultFunctionHeartbeatTimeoutS)*time.Second).Err()
	if err != nil {
		phaseLabels["success"] = "false"
		phaseMetrics.RecordSince(ctx, authInfo.Workspace.Name, in.TaskId, "start_task_to_get_args", task.FunctionPhaseStartTask, now, phaseLabels)
		return &pb.FunctionGetArgsResponse{Ok: false, Args: nil}, nil
	}

	phaseMetrics.RecordSince(ctx, authInfo.Workspace.Name, in.TaskId, "start_task_to_get_args", task.FunctionPhaseStartTask, now, phaseLabels)
	if err := phaseMetrics.Mark(ctx, authInfo.Workspace.Name, in.TaskId, task.FunctionPhaseGetArgs, now); err != nil {
		log.Debug().Err(err).Str("task_id", in.TaskId).Msg("failed to mark function get_args phase")
	}
	if fs.eventRepo != nil {
		if taskWithRelated, err := fs.backendRepo.GetTaskWithRelated(ctx, in.TaskId); err == nil && taskWithRelated != nil {
			fs.eventRepo.PushFunctionGetArgs(ctx, fs.rdb, taskWithRelated, now, len(value))
		}
	}

	return &pb.FunctionGetArgsResponse{
		Ok:   true,
		Args: value,
	}, nil
}

func (fs *ContainerFunctionService) FunctionSetResult(ctx context.Context, in *pb.FunctionSetResultRequest) (*pb.FunctionSetResultResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	now := time.Now()
	phaseMetrics := task.NewPhaseMetrics(fs.rdb)
	phaseLabels := phaseMetrics.Labels(ctx, authInfo.Workspace.Name, in.TaskId, map[string]string{
		"workspace_id": authInfo.Workspace.ExternalId,
	})

	err := fs.rdb.Set(ctx, Keys.FunctionResult(authInfo.Workspace.Name, in.TaskId), in.Result, functionResultExpirationTimeout).Err()
	if err != nil {
		phaseLabels["success"] = "false"
		phaseMetrics.RecordSince(ctx, authInfo.Workspace.Name, in.TaskId, "get_args_to_set_result", task.FunctionPhaseGetArgs, now, phaseLabels)
		phaseMetrics.RecordSince(ctx, authInfo.Workspace.Name, in.TaskId, "start_task_to_set_result", task.FunctionPhaseStartTask, now, phaseLabels)
		return &pb.FunctionSetResultResponse{Ok: false}, nil
	}

	phaseMetrics.RecordSince(ctx, authInfo.Workspace.Name, in.TaskId, "get_args_to_set_result", task.FunctionPhaseGetArgs, now, phaseLabels)
	phaseMetrics.RecordSince(ctx, authInfo.Workspace.Name, in.TaskId, "start_task_to_set_result", task.FunctionPhaseStartTask, now, phaseLabels)
	if err := phaseMetrics.Mark(ctx, authInfo.Workspace.Name, in.TaskId, task.FunctionPhaseSetResult, now); err != nil {
		log.Debug().Err(err).Str("task_id", in.TaskId).Msg("failed to mark function set_result phase")
	}
	if fs.eventRepo != nil {
		if taskWithRelated, err := fs.backendRepo.GetTaskWithRelated(ctx, in.TaskId); err == nil && taskWithRelated != nil {
			fs.eventRepo.PushFunctionSetResult(ctx, fs.rdb, taskWithRelated, now, len(in.Result))
		}
	}

	return &pb.FunctionSetResultResponse{
		Ok: true,
	}, nil
}

func (fs *ContainerFunctionService) FunctionMonitor(req *pb.FunctionMonitorRequest, stream pb.FunctionService_FunctionMonitorServer) error {
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	task, err := fs.backendRepo.GetTaskWithRelated(ctx, req.TaskId)
	if err != nil {
		return err
	}

	var stubConfig types.StubConfigV1 = types.StubConfigV1{}
	err = json.Unmarshal([]byte(task.Stub.Config), &stubConfig)
	if err != nil {
		return err
	}

	timeout := int64(stubConfig.TaskPolicy.Timeout)
	timeoutCallback := func() error {
		task.Status = types.TaskStatusTimeout

		_, err = fs.backendRepo.UpdateTask(
			stream.Context(),
			task.ExternalId,
			task.Task,
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
			messages, errs := fs.rdb.Subscribe(ctx, channelKey)

			for {
				select {
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

				case <-fs.ctx.Done():
					return

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
		case <-fs.ctx.Done():
			return nil

		case <-stream.Context().Done():
			return nil

		case <-cancelFlag:
			err := fs.taskDispatcher.Complete(ctx, authInfo.Workspace.Name, req.StubId, task.ExternalId)
			if err != nil {
				return err
			}

			stream.Send(&pb.FunctionMonitorResponse{Ok: true, Cancelled: true, Complete: false, TimedOut: false})
			return nil

		case <-timeoutFlag:
			err := fs.taskDispatcher.Complete(ctx, authInfo.Workspace.Name, req.StubId, task.ExternalId)
			if err != nil {
				return err
			}

			stream.Send(&pb.FunctionMonitorResponse{Ok: true, Cancelled: false, Complete: false, TimedOut: true})
			return nil

		default:
			if task.Status == types.TaskStatusCancelled {
				stream.Send(&pb.FunctionMonitorResponse{Ok: true, Cancelled: true, Complete: false, TimedOut: false})
				return nil
			}

			err := fs.rdb.SetEx(ctx, Keys.FunctionHeartbeat(authInfo.Workspace.Name, task.ExternalId), 1, time.Duration(defaultFunctionHeartbeatTimeoutS)*time.Second).Err()
			if err != nil {
				return err
			}

			claimed, err := fs.taskRepo.IsClaimed(ctx, authInfo.Workspace.Name, req.StubId, task.ExternalId)
			if err != nil {
				return err
			}

			if !claimed {
				stream.Send(&pb.FunctionMonitorResponse{Ok: true, Cancelled: false, Complete: true, TimedOut: false})
			}

			stream.Send(&pb.FunctionMonitorResponse{Ok: true, Cancelled: false, Complete: false, TimedOut: false})
			time.Sleep(time.Second * 1)
		}
	}
}

func (fs *ContainerFunctionService) genContainerId(taskId, stubType string) string {
	return fmt.Sprintf("%s-%s-%s", stubType, taskId, uuid.New().String()[:8])
}

func (fs *ContainerFunctionService) FunctionSchedule(ctx context.Context, req *pb.FunctionScheduleRequest) (*pb.FunctionScheduleResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	stub, err := fs.backendRepo.GetStubByExternalId(ctx, req.StubId)
	if err != nil {
		return &pb.FunctionScheduleResponse{Ok: false, ErrMsg: "Unable to get stub"}, nil
	}

	currentDeployment, err := fs.backendRepo.GetDeploymentByExternalId(ctx, authInfo.Workspace.Id, req.DeploymentId)
	if err != nil {
		return &pb.FunctionScheduleResponse{Ok: false, ErrMsg: "Unable to find associated deployment"}, nil
	}

	if err := fs.backendRepo.DeletePreviousScheduledJob(ctx, &currentDeployment.Deployment); err != nil {
		return &pb.FunctionScheduleResponse{Ok: false, ErrMsg: "Unable to delete previous scheduled job"}, nil
	}

	job, err := fs.backendRepo.CreateScheduledJob(ctx, &types.ScheduledJob{
		JobName:      fmt.Sprintf("%v-%v", currentDeployment.Name, stub.ExternalId),
		Schedule:     req.When,
		DeploymentId: currentDeployment.Id,
		StubId:       stub.Id,
		Payload: types.ScheduledJobPayload{
			StubId:        stub.ExternalId,
			WorkspaceName: authInfo.Workspace.Name,
		},
	})
	if err != nil {
		return &pb.FunctionScheduleResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	return &pb.FunctionScheduleResponse{
		Ok:             true,
		ScheduledJobId: job.ExternalId,
	}, nil
}

func (fs *ContainerFunctionService) listenForScheduledJobs() {
	log.Info().Str("channel", repository.ScheduledJobsChannel).Msg("listening for scheduled jobs")
	for {
		select {
		case <-fs.ctx.Done():
			return
		default:
			messages, err := fs.backendRepo.ListenToChannel(fs.ctx, repository.ScheduledJobsChannel)
			if err != nil {
				log.Error().Err(err).Msg("failed to listen to scheduled job channel")
				time.Sleep(1 * time.Second)
				continue
			}

			lock := common.NewRedisLock(fs.rdb)
			for message := range messages {
				var payload types.ScheduledJobPayload
				if err := json.Unmarshal([]byte(message), &payload); err != nil {
					log.Error().Err(err).Msg("failed to unmarshal scheduled job payload")
					continue
				}

				if err := lock.Acquire(fs.ctx, Keys.FunctionScheduledJobLock(payload.StubId), common.RedisLockOptions{TtlS: 10, Retries: 0}); err != nil {
					continue
				}

				authInfo := &auth.AuthInfo{Workspace: &types.Workspace{Name: payload.WorkspaceName}}
				if _, err = fs.invoke(fs.ctx, authInfo, payload.StubId, &payload.TaskPayload); err != nil {
					log.Error().Err(err).Msg("failed to invoke scheduled job")
				}

				lock.Release(Keys.FunctionScheduledJobLock(payload.StubId))
			}
		}
	}
}

// Redis keys
var (
	functionPrefix            string = "function"
	functionArgs              string = "function:%s:%s:args"
	functionResult            string = "function:%s:%s:result"
	functionHeartbeat         string = "function:%s:%s:heartbeat"
	functionScheduledJobsLock string = "function:scheduled_jobs_lock:%s"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) FunctionPrefix() string {
	return functionPrefix
}

func (k *keys) FunctionArgs(workspaceName, taskId string) string {
	return fmt.Sprintf(functionArgs, workspaceName, taskId)
}

func (k *keys) FunctionResult(workspaceName, taskId string) string {
	return fmt.Sprintf(functionResult, workspaceName, taskId)
}

func (k *keys) FunctionHeartbeat(workspaceName, taskId string) string {
	return fmt.Sprintf(functionHeartbeat, workspaceName, taskId)
}

func (k *keys) FunctionScheduledJobLock(stubId string) string {
	return fmt.Sprintf(functionScheduledJobsLock, stubId)
}
