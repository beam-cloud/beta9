package function

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/beam-cloud/beam/internal/auth"
	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
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
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
	routeGroup      *echo.Group
}

func NewRuncFunctionService(ctx context.Context,
	rdb *common.RedisClient,
	backendRepo repository.BackendRepository,
	containerRepo repository.ContainerRepository,
	scheduler *scheduler.Scheduler,
	baseRouteGroup *echo.Group,
) (FunctionService, error) {
	keyEventManager, err := common.NewKeyEventManager(rdb)
	if err != nil {
		return nil, err
	}

	fs := &RunCFunctionService{
		backendRepo:     backendRepo,
		containerRepo:   containerRepo,
		scheduler:       scheduler,
		rdb:             rdb,
		keyEventManager: keyEventManager,
	}

	// Register HTTP routes
	authMiddleware := auth.AuthMiddleware(backendRepo)
	registerFunctionRoutes(baseRouteGroup.Group(functionRoutePrefix, authMiddleware), fs)

	return fs, nil
}

func (fs *RunCFunctionService) FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error {
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)
	keyEventChan := make(chan common.KeyEvent)

	task, err := fs.invoke(ctx, authInfo, in.StubId, in.Args, keyEventChan)
	if err != nil {
		return err
	}

	hostname, err := fs.containerRepo.GetContainerWorkerHostname(task.ContainerId)
	if err != nil {
		return err
	}

	client, err := common.NewRunCClient(hostname, authInfo.Token.Key)
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

	go fs.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan)

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

	mounts := []types.Mount{
		{
			LocalPath: path.Join(types.DefaultExtractedObjectPath, authInfo.Workspace.Name, stub.Object.ExternalId),
			MountPath: types.WorkerUserCodeVolume,
			ReadOnly:  true,
		},
	}

	for _, v := range stubConfig.Volumes {
		mounts = append(mounts, types.Mount{
			LocalPath: path.Join(types.DefaultVolumesPath, authInfo.Workspace.Name, v.Id),
			LinkPath:  path.Join(types.DefaultExtractedObjectPath, authInfo.Workspace.Name, stub.Object.ExternalId, v.MountPath),
			MountPath: path.Join(types.ContainerVolumePath, v.MountPath),
			ReadOnly:  false,
		})
	}

	err = fs.scheduler.Run(&types.ContainerRequest{
		ContainerId: containerId,
		Env: []string{
			fmt.Sprintf("TASK_ID=%s", taskId),
			fmt.Sprintf("HANDLER=%s", stubConfig.Handler),
			fmt.Sprintf("BEAM_TOKEN=%s", authInfo.Token.Key),
			fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
		},
		Cpu:        stubConfig.Runtime.Cpu,
		Memory:     stubConfig.Runtime.Memory,
		Gpu:        string(stubConfig.Runtime.Gpu),
		ImageId:    stubConfig.Runtime.ImageId,
		EntryPoint: []string{stubConfig.PythonVersion, "-m", "beam.runner.function"},
		Mounts:     mounts,
	})
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (fs *RunCFunctionService) createTask(ctx context.Context, authInfo *auth.AuthInfo, stub *types.Stub) (*types.Task, error) {
	task, err := fs.backendRepo.CreateTask(ctx, "", authInfo.Workspace.Id, stub.Id)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (fs *RunCFunctionService) handleStreams(ctx context.Context,
	stream pb.FunctionService_FunctionInvokeServer,
	workspaceName, taskId, containerId string,
	outputChan chan common.OutputMsg, keyEventChan chan common.KeyEvent) error {

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
				break
			}
		case <-ctx.Done():
			return ctx.Err()
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
