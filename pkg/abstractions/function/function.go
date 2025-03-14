package function

import (
	"context"
	"encoding/json"
	"fmt"
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

type RunCFunctionService struct {
	pb.UnimplementedFunctionServiceServer
	ctx             context.Context
	taskDispatcher  *task.Dispatcher
	backendRepo     repository.BackendRepository
	workspaceRepo   repository.WorkspaceRepository
	taskRepo        repository.TaskRepository
	containerRepo   repository.ContainerRepository
	scheduler       *scheduler.Scheduler
	tailscale       *network.Tailscale
	config          types.AppConfig
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
	routeGroup      *echo.Group
	eventRepo       repository.EventRepository
}

type FunctionServiceOpts struct {
	Config         types.AppConfig
	RedisClient    *common.RedisClient
	BackendRepo    repository.BackendRepository
	WorkspaceRepo  repository.WorkspaceRepository
	TaskRepo       repository.TaskRepository
	ContainerRepo  repository.ContainerRepository
	Scheduler      *scheduler.Scheduler
	Tailscale      *network.Tailscale
	RouteGroup     *echo.Group
	TaskDispatcher *task.Dispatcher
	EventRepo      repository.EventRepository
}

func NewRuncFunctionService(ctx context.Context,
	opts FunctionServiceOpts,
) (FunctionService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	fs := &RunCFunctionService{
		ctx:             ctx,
		config:          opts.Config,
		backendRepo:     opts.BackendRepo,
		workspaceRepo:   opts.WorkspaceRepo,
		taskRepo:        opts.TaskRepo,
		containerRepo:   opts.ContainerRepo,
		scheduler:       opts.Scheduler,
		tailscale:       opts.Tailscale,
		rdb:             opts.RedisClient,
		keyEventManager: keyEventManager,
		taskDispatcher:  opts.TaskDispatcher,
		routeGroup:      opts.RouteGroup,
		eventRepo:       opts.EventRepo,
	}

	// Register task dispatcher
	fs.taskDispatcher.Register(string(types.ExecutorFunction), fs.functionTaskFactory)

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(fs.backendRepo, fs.workspaceRepo)
	registerFunctionRoutes(fs.routeGroup.Group(functionRoutePrefix, authMiddleware), fs)
	registerFunctionRoutes(fs.routeGroup.Group(scheduleRoutePrefix, authMiddleware), fs)

	go fs.listenForScheduledJobs()

	return fs, nil
}

func (fs *RunCFunctionService) FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error {
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())
	ctx := stream.Context()

	task, err := fs.invoke(ctx, authInfo, in.StubId, &types.TaskPayload{
		Args: []interface{}{in.Args},
	})
	if err != nil {
		return err
	}

	return fs.stream(ctx, stream, authInfo, task, in.StubId, in.Headless)
}

func (fs *RunCFunctionService) invoke(ctx context.Context, authInfo *auth.AuthInfo, stubId string, payload *types.TaskPayload) (types.TaskInterface, error) {
	stub, err := fs.backendRepo.GetStubByExternalId(ctx, stubId)
	if err != nil {
		return nil, err
	}

	config := types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), &config)
	if err != nil {
		return nil, err
	}

	policy := config.TaskPolicy
	if policy.TTL == 0 {
		// This is required for backward compatibility since older functions do not have a TTL set which defaults to 0
		policy.TTL = DefaultFunctionTaskTTL
	}
	policy.Expires = time.Now().Add(time.Duration(policy.TTL) * time.Second)

	task, err := fs.taskDispatcher.SendAndExecute(ctx, string(types.ExecutorFunction), authInfo, stubId, payload, policy)
	if err != nil {
		return nil, err
	}

	go fs.eventRepo.PushRunStubEvent(authInfo.Workspace.ExternalId, &stub.Stub)
	return task, err
}

func (fs *RunCFunctionService) functionTaskFactory(ctx context.Context, msg types.TaskMessage) (types.TaskInterface, error) {
	return &FunctionTask{
		msg: &msg,
		fs:  fs,
	}, nil
}

func (fs *RunCFunctionService) stream(ctx context.Context, stream pb.FunctionService_FunctionInvokeServer, authInfo *auth.AuthInfo, task types.TaskInterface, stubId string, headless bool) error {
	taskId := task.Metadata().TaskId
	containerId := task.Metadata().ContainerId

	sendCallback := func(o common.OutputMsg) error {
		if err := stream.Send(&pb.FunctionInvokeResponse{TaskId: taskId, Output: o.Msg, Done: o.Done}); err != nil {
			return err
		}

		return nil
	}

	exitCallback := func(exitCode int32) error {
		result, _ := fs.rdb.Get(stream.Context(), Keys.FunctionResult(authInfo.Workspace.Name, taskId)).Bytes()
		if err := stream.Send(&pb.FunctionInvokeResponse{TaskId: taskId, Done: true, Result: result, ExitCode: int32(exitCode)}); err != nil {
			return err
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

		<-ctx.Done() // Wait for the stream to be closed to cancel the task

		err = task.Cancel(context.Background(), types.TaskRequestCancelled)
		if err != nil {
			log.Error().Err(err).Msg("error cancelling task")
		}

		err = fs.rdb.Publish(context.Background(), common.RedisKeys.TaskCancel(authInfo.Workspace.Name, task.Message().StubId, task.Message().TaskId), task.Message().TaskId).Err()
		if err != nil {
			log.Error().Err(err).Msg("error publishing task cancel event")
		}
	}()

	ctx, cancel := common.MergeContexts(fs.ctx, ctx)
	defer cancel()

	return containerStream.Stream(ctx, authInfo, containerId)
}

func (fs *RunCFunctionService) FunctionGetArgs(ctx context.Context, in *pb.FunctionGetArgsRequest) (*pb.FunctionGetArgsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	value, err := fs.rdb.Get(ctx, Keys.FunctionArgs(authInfo.Workspace.Name, in.TaskId)).Bytes()
	if err != nil {
		return &pb.FunctionGetArgsResponse{Ok: false, Args: nil}, nil
	}

	err = fs.rdb.SetEx(ctx, Keys.FunctionHeartbeat(authInfo.Workspace.Name, in.TaskId), 1, time.Duration(defaultFunctionHeartbeatTimeoutS)*time.Second).Err()
	if err != nil {
		return &pb.FunctionGetArgsResponse{Ok: false, Args: nil}, nil
	}

	return &pb.FunctionGetArgsResponse{
		Ok:   true,
		Args: value,
	}, nil
}

func (fs *RunCFunctionService) FunctionSetResult(ctx context.Context, in *pb.FunctionSetResultRequest) (*pb.FunctionSetResultResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	err := fs.rdb.Set(ctx, Keys.FunctionResult(authInfo.Workspace.Name, in.TaskId), in.Result, functionResultExpirationTimeout).Err()
	if err != nil {
		return &pb.FunctionSetResultResponse{Ok: false}, nil
	}

	return &pb.FunctionSetResultResponse{
		Ok: true,
	}, nil
}

func (fs *RunCFunctionService) FunctionMonitor(req *pb.FunctionMonitorRequest, stream pb.FunctionService_FunctionMonitorServer) error {
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

func (fs *RunCFunctionService) genContainerId(taskId, stubType string) string {
	return fmt.Sprintf("%s-%s-%s", stubType, taskId, uuid.New().String()[:8])
}

func (fs *RunCFunctionService) FunctionSchedule(ctx context.Context, req *pb.FunctionScheduleRequest) (*pb.FunctionScheduleResponse, error) {
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

func (fs *RunCFunctionService) listenForScheduledJobs() {
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
