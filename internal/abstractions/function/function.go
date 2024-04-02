package function

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/internal/abstractions"
	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/scheduler"
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
	functionContainerPrefix         string        = "function-"
	functionRoutePrefix             string        = "/function"
	defaultFunctionContainerCpu     int64         = 100
	defaultFunctionContainerMemory  int64         = 128
	functionArgsExpirationTimeout   time.Duration = 600 * time.Second
	functionResultExpirationTimeout time.Duration = 600 * time.Second
)

type RunCFunctionService struct {
	pb.UnimplementedFunctionServiceServer
	backendRepo     repository.BackendRepository
	containerRepo   repository.ContainerRepository
	scheduler       *scheduler.Scheduler
	tailscale       *network.Tailscale
	config          types.AppConfig
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
	routeGroup      *echo.Group
}

type FunctionServiceOpts struct {
	Config        types.AppConfig
	RedisClient   *common.RedisClient
	BackendRepo   repository.BackendRepository
	ContainerRepo repository.ContainerRepository
	Scheduler     *scheduler.Scheduler
	Tailscale     *network.Tailscale
	RouteGroup    *echo.Group
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
		containerRepo:   opts.ContainerRepo,
		scheduler:       opts.Scheduler,
		tailscale:       opts.Tailscale,
		rdb:             opts.RedisClient,
		keyEventManager: keyEventManager,
	}

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(fs.backendRepo)
	registerFunctionRoutes(opts.RouteGroup.Group(functionRoutePrefix, authMiddleware), fs)

	return fs, nil
}

func (fs *RunCFunctionService) FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error {
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg, 1000)
	keyEventChan := make(chan common.KeyEvent, 1000)

	task, err := fs.invoke(ctx, authInfo, in.StubId, in.Args, keyEventChan)
	if err != nil {
		return err
	}

	hostname, err := fs.containerRepo.GetWorkerAddress(task.ContainerId)
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

	go client.StreamLogs(ctx, task.ContainerId, outputChan)
	return fs.handleStreams(ctx, stream, authInfo.Workspace.Name, task.ExternalId, task.ContainerId, outputChan, keyEventChan)
}

func (fs *RunCFunctionService) invoke(ctx context.Context, authInfo *auth.AuthInfo, stubId string, args []byte, keyEventChan chan common.KeyEvent) (*types.Task, error) {
	stub, err := fs.backendRepo.GetStubByExternalId(ctx, stubId)
	if err != nil {
		return nil, err
	}

	task, err := fs.createTask(ctx, authInfo, &stub.Stub)
	if err != nil {
		return nil, err
	}

	var stubConfig types.StubConfigV1 = types.StubConfigV1{}
	err = json.Unmarshal([]byte(stub.Config), &stubConfig)
	if err != nil {
		return nil, err
	}

	taskId := task.ExternalId
	containerId := fs.genContainerId(taskId)
	task.ContainerId = containerId

	err = fs.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan)
	if err != nil {
		return nil, err
	}

	_, err = fs.backendRepo.UpdateTask(ctx, task.ExternalId, *task)
	if err != nil {
		return nil, err
	}

	err = fs.rdb.Set(ctx, Keys.FunctionArgs(authInfo.Workspace.Name, taskId), args, functionArgsExpirationTimeout).Err()
	if err != nil {
		return nil, errors.New("unable to store function args")
	}

	// Don't allow negative compute requests
	if stubConfig.Runtime.Cpu <= 0 {
		stubConfig.Runtime.Cpu = defaultFunctionContainerCpu
	}

	if stubConfig.Runtime.Memory <= 0 {
		stubConfig.Runtime.Memory = defaultFunctionContainerMemory
	}

	mounts := abstractions.ConfigureContainerRequestMounts(
		stub.Object.ExternalId,
		authInfo.Workspace.Name,
		stubConfig,
	)

	err = fs.scheduler.Run(&types.ContainerRequest{
		ContainerId: containerId,
		Env: []string{
			fmt.Sprintf("TASK_ID=%s", taskId),
			fmt.Sprintf("HANDLER=%s", stubConfig.Handler),
			fmt.Sprintf("BETA9_TOKEN=%s", authInfo.Token.Key),
			fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
		},
		Cpu:         stubConfig.Runtime.Cpu,
		Memory:      stubConfig.Runtime.Memory,
		Gpu:         string(stubConfig.Runtime.Gpu),
		GpuCount:    1,
		ImageId:     stubConfig.Runtime.ImageId,
		StubId:      stub.ExternalId,
		WorkspaceId: authInfo.Workspace.ExternalId,
		EntryPoint:  []string{stubConfig.PythonVersion, "-m", "beta9.runner.function"},
		Mounts:      mounts,
	})
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (fs *RunCFunctionService) functionTaskFactory() (types.TaskInterface, error) {
	return &FunctionTask{}, nil
}

func (fs *RunCFunctionService) createTask(ctx context.Context, authInfo *auth.AuthInfo, stub *types.Stub) (*types.Task, error) {
	task, err := fs.backendRepo.CreateTask(ctx, &types.TaskParams{
		WorkspaceId: authInfo.Workspace.Id,
		StubId:      stub.Id,
	})
	if err != nil {
		return nil, err
	}

	// TODO: dispatch

	return task, nil
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

func (fs *RunCFunctionService) genContainerId(taskId string) string {
	return fmt.Sprintf("%s%s", functionContainerPrefix, taskId)
}

type FunctionTask struct {
	StubId        string
	WorkspaceName string
	TaskId        string
}

func (ft *FunctionTask) Cancel() error {
	return nil
}

func (ft *FunctionTask) Execute() error {
	return nil
}

func (ft *FunctionTask) Update() error {
	return nil
}

func (ft *FunctionTask) Metadata() types.TaskMetadata {
	return types.TaskMetadata{
		StubId:        ft.StubId,
		WorkspaceName: ft.WorkspaceName,
		TaskId:        ft.TaskId,
	}
}

// Redis keys
var (
	functionPrefix string = "function"
	functionArgs   string = "function:%s:%s:args"
	functionResult string = "function:%s:%s:result"
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
