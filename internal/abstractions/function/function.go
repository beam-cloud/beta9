package function

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/task"
	"github.com/beam-cloud/beta9/internal/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

type FunctionService interface {
	pb.FunctionServiceServer
	FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error
	FunctionGetArgs(ctx context.Context, in *pb.FunctionGetArgsRequest) (*pb.FunctionGetArgsResponse, error)
	FunctionSetResult(ctx context.Context, in *pb.FunctionSetResultRequest) (*pb.FunctionSetResultResponse, error)
}

const (
	functionContainerPrefix         string        = "function"
	functionRoutePrefix             string        = "/function"
	defaultFunctionContainerCpu     int64         = 100
	defaultFunctionContainerMemory  int64         = 128
	functionArgsExpirationTimeout   time.Duration = 600 * time.Second
	functionResultExpirationTimeout time.Duration = 600 * time.Second
)

type RunCFunctionService struct {
	pb.UnimplementedFunctionServiceServer
	taskDispatcher  *task.Dispatcher
	backendRepo     repository.BackendRepository
	taskRepo        repository.TaskRepository
	containerRepo   repository.ContainerRepository
	scheduler       *scheduler.Scheduler
	tailscale       *network.Tailscale
	config          types.AppConfig
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
	routeGroup      *echo.Group
}

type FunctionServiceOpts struct {
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

func NewRuncFunctionService(ctx context.Context,
	opts FunctionServiceOpts,
) (FunctionService, error) {
	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	fs := &RunCFunctionService{
		config:          opts.Config,
		backendRepo:     opts.BackendRepo,
		taskRepo:        opts.TaskRepo,
		containerRepo:   opts.ContainerRepo,
		scheduler:       opts.Scheduler,
		tailscale:       opts.Tailscale,
		rdb:             opts.RedisClient,
		keyEventManager: keyEventManager,
		taskDispatcher:  opts.TaskDispatcher,
		routeGroup:      opts.RouteGroup,
	}

	fs.taskDispatcher.Register(string(types.ExecutorFunction), fs.functionTaskFactory)

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(fs.backendRepo)
	registerFunctionRoutes(fs.routeGroup.Group(functionRoutePrefix, authMiddleware), fs)

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

	return fs.stream(ctx, stream, authInfo, task)
}

func (fs *RunCFunctionService) invoke(ctx context.Context, authInfo *auth.AuthInfo, stubId string, payload *types.TaskPayload) (types.TaskInterface, error) {
	task, err := fs.taskDispatcher.Send(ctx, string(types.ExecutorFunction), authInfo.Workspace.Name, stubId, payload, types.DefaultTaskPolicy)
	if err != nil {
		return nil, err
	}

	return task, err
}

func (fs *RunCFunctionService) functionTaskFactory(ctx context.Context, msg types.TaskMessage) (types.TaskInterface, error) {
	return &FunctionTask{
		msg: &msg,
		fs:  fs,
	}, nil
}

func (fs *RunCFunctionService) stream(ctx context.Context, stream pb.FunctionService_FunctionInvokeServer, authInfo *auth.AuthInfo, task types.TaskInterface) error {
	meta := task.Metadata()

	hostname, err := fs.containerRepo.GetWorkerAddress(meta.ContainerId)
	if err != nil {
		return err
	}

	conn, err := network.ConnectToHost(ctx, hostname, time.Second*30, fs.tailscale, fs.config.Tailscale)
	if err != nil {
		return err
	}

	client, err := common.NewRunCClient(hostname, authInfo.Token.Key, conn)
	if err != nil {
		return err
	}

	outputChan := make(chan common.OutputMsg, 1000)
	keyEventChan := make(chan common.KeyEvent, 1000)

	err = fs.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerExitCode(meta.ContainerId), keyEventChan)
	if err != nil {
		return err
	}

	go client.StreamLogs(ctx, meta.ContainerId, outputChan)
	return fs.handleStreams(ctx, stream, authInfo.Workspace.Name, meta.TaskId, meta.ContainerId, outputChan, keyEventChan)
}

func (fs *RunCFunctionService) handleStreams(
	ctx context.Context,
	stream pb.FunctionService_FunctionInvokeServer,
	workspaceName, taskId, containerId string,
	outputChan chan common.OutputMsg,
	keyEventChan chan common.KeyEvent,
) error {

	var lastMessage common.OutputMsg

_stream:
	for {
		select {
		case o := <-outputChan:
			if err := stream.Send(&pb.FunctionInvokeResponse{TaskId: taskId, Output: o.Msg, Done: o.Done}); err != nil {
				lastMessage = o
				break
			}

			if o.Done {
				lastMessage = o
				break _stream
			}
		case <-keyEventChan:
			exitCode, err := fs.containerRepo.GetContainerExitCode(containerId)
			if err != nil {
				exitCode = -1
			}

			result, _ := fs.rdb.Get(stream.Context(), Keys.FunctionResult(workspaceName, taskId)).Bytes()
			if err := stream.Send(&pb.FunctionInvokeResponse{TaskId: taskId, Done: true, Result: result, ExitCode: int32(exitCode)}); err != nil {
				break _stream
			}
		case <-ctx.Done():
			return nil
		}
	}

	if !lastMessage.Success {
		return errors.New("function failed")
	}

	return nil
}

func (fs *RunCFunctionService) FunctionGetArgs(ctx context.Context, in *pb.FunctionGetArgsRequest) (*pb.FunctionGetArgsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	value, err := fs.rdb.Get(ctx, Keys.FunctionArgs(authInfo.Workspace.Name, in.TaskId)).Bytes()
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
		for {
			for {
				select {
				case <-timeoutChan:
					err := timeoutCallback()
					if err != nil {
						log.Printf("error timing out task: %v", err)
					}
					timeoutFlag <- true
					return

				case <-ctx.Done():
					return
				}
			}
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
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

			err := fs.rdb.SetEx(ctx, Keys.FunctionHeartbeat(authInfo.Workspace.Name, task.ExternalId), 1, time.Duration(60)*time.Second).Err()
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

func (fs *RunCFunctionService) genContainerId(taskId string) string {
	return fmt.Sprintf("%s-%s", functionContainerPrefix, taskId)
}

// Redis keys
var (
	functionPrefix    string = "function"
	functionArgs      string = "function:%s:%s:args"
	functionResult    string = "function:%s:%s:result"
	functionHeartbeat string = "function:%s:%s:heartbeat"
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
