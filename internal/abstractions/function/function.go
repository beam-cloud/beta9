package function

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/beam-cloud/beam/internal/auth"
	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
)

type FunctionService interface {
	FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error
	FunctionGetArgs(ctx context.Context, in *pb.FunctionGetArgsRequest) (*pb.FunctionGetArgsResponse, error)
	FunctionSetResult(ctx context.Context, in *pb.FunctionSetResultRequest) (*pb.FunctionSetResultResponse, error)
}

const (
	functionContainerPrefix         string        = "function-"
	defaultFunctionContainerCpu     int64         = 100
	defaultFunctionContainerMemory  int64         = 128
	functionArgsExpirationTimeout   time.Duration = 600 * time.Second
	functionResultExpirationTimeout time.Duration = 600 * time.Second
)

type RunCFunctionService struct {
	pb.UnimplementedFunctionServiceServer
	backendRepo     repository.BackendRepository
	scheduler       *scheduler.Scheduler
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
}

func NewRuncFunctionService(ctx context.Context, backendRepo repository.BackendRepository, rdb *common.RedisClient, scheduler *scheduler.Scheduler, keyEventManager *common.KeyEventManager) (*RunCFunctionService, error) {
	return &RunCFunctionService{
		backendRepo:     backendRepo,
		scheduler:       scheduler,
		rdb:             rdb,
		keyEventManager: keyEventManager,
	}, nil
}

func (fs *RunCFunctionService) FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error {
	log.Printf("incoming function request: %+v", in)

	authInfo, _ := auth.AuthInfoFromContext(stream.Context())
	task, err := fs.createTask(stream.Context(), in, authInfo)
	if err != nil {
		return err
	}

	taskId := task.ExternalId
	containerId := fs.genContainerId(taskId)
	task.ContainerId = containerId

	_, err = fs.backendRepo.UpdateTask(stream.Context(), task.ExternalId, *task)
	if err != nil {
		return err
	}

	err = fs.rdb.Set(stream.Context(), Keys.FunctionArgs(taskId), in.Args, functionArgsExpirationTimeout).Err()
	if err != nil {
		return errors.New("unable to store function args")
	}

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)
	keyEventChan := make(chan common.KeyEvent)

	go fs.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan)

	// Don't allow negative compute requests
	if in.Cpu <= 0 {
		in.Cpu = defaultFunctionContainerCpu
	}

	if in.Memory <= 0 {
		in.Memory = defaultFunctionContainerMemory
	}

	err = fs.scheduler.Run(&types.ContainerRequest{
		ContainerId: containerId,
		Env: []string{
			fmt.Sprintf("TASK_ID=%s", taskId),
			fmt.Sprintf("HANDLER=%s", in.Handler),
			fmt.Sprintf("BEAM_TOKEN=%s", authInfo.Token.Key),
		},
		Cpu:        in.Cpu,
		Memory:     in.Memory,
		Gpu:        in.Gpu,
		ImageId:    in.ImageId,
		EntryPoint: []string{in.PythonVersion, "-m", "beam.runner.function"},
		Mounts: []types.Mount{
			{LocalPath: path.Join(types.DefaultExtractedObjectPath, authInfo.Workspace.Name, in.ObjectId), MountPath: types.WorkerUserCodeVolume, ReadOnly: true},
		},
	})
	if err != nil {
		return err
	}

	hostname, err := fs.scheduler.ContainerRepo.GetContainerWorkerHostname(containerId)
	if err != nil {
		return err
	}

	client, err := common.NewRunCClient(hostname, authInfo.Token.Key)
	if err != nil {
		return err
	}

	go client.StreamLogs(ctx, containerId, outputChan)
	return fs.handleStreams(ctx, stream, taskId, containerId, outputChan, keyEventChan)
}

func (fs *RunCFunctionService) createTask(ctx context.Context, in *pb.FunctionInvokeRequest, authInfo *auth.AuthInfo) (*types.Task, error) {
	stub, err := fs.backendRepo.GetStubByExternalId(ctx, in.StubId, authInfo.Workspace.Id)
	if err != nil {
		return nil, err
	}

	task, err := fs.backendRepo.CreateTask(ctx, "", authInfo.Workspace.Id, stub.Id)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (fs *RunCFunctionService) handleStreams(ctx context.Context,
	stream pb.FunctionService_FunctionInvokeServer,
	taskId string, containerId string,
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
			result, _ := fs.rdb.Get(stream.Context(), Keys.FunctionResult(taskId)).Bytes()
			if err := stream.Send(&pb.FunctionInvokeResponse{TaskId: taskId, Done: true, Result: result, ExitCode: 0}); err != nil {
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
	value, err := fs.rdb.Get(ctx, Keys.FunctionArgs(in.TaskId)).Bytes()
	if err != nil {
		return &pb.FunctionGetArgsResponse{Ok: false, Args: nil}, nil
	}

	return &pb.FunctionGetArgsResponse{
		Ok:   true,
		Args: value,
	}, nil
}

func (fs *RunCFunctionService) FunctionSetResult(ctx context.Context, in *pb.FunctionSetResultRequest) (*pb.FunctionSetResultResponse, error) {
	err := fs.rdb.Set(ctx, Keys.FunctionResult(in.TaskId), in.Result, functionResultExpirationTimeout).Err()
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
	functionArgs   string = "function:%s:args"
	functionResult string = "function:%s:result"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) FunctionPrefix() string {
	return functionPrefix
}

func (k *keys) FunctionArgs(taskId string) string {
	return fmt.Sprintf(functionArgs, taskId)
}

func (k *keys) FunctionResult(taskId string) string {
	return fmt.Sprintf(functionResult, taskId)
}
