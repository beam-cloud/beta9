package function

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/beam-cloud/beam/internal/auth"
	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
	"github.com/google/uuid"
	"github.com/mholt/archiver/v3"
)

const (
	functionContainerPrefix         string        = "function-"
	defaultFunctionContainerCpu     int64         = 100
	defaultFunctionContainerMemory  int64         = 128
	functionArgsExpirationTimeout   time.Duration = 600 * time.Second
	functionResultExpirationTimeout time.Duration = 600 * time.Second
)

type FunctionService interface {
	FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error
	FunctionGetArgs(ctx context.Context, in *pb.FunctionGetArgsRequest) (*pb.FunctionGetArgsResponse, error)
	FunctionSetResult(ctx context.Context, in *pb.FunctionSetResultRequest) (*pb.FunctionSetResultResponse, error)
}

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
	taskId := fs.genInvocationId()
	containerId := fs.genContainerId(taskId)

	err := fs.rdb.Set(context.TODO(), Keys.FunctionArgs(taskId), in.Args, functionArgsExpirationTimeout).Err()
	if err != nil {
		return errors.New("unable to store function args")
	}

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)
	keyEventChan := make(chan common.KeyEvent)

	go fs.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan)

	// Retrieve object
	_, err = fs.backendRepo.GetObjectByExternalId(ctx, in.ObjectId, authInfo.Context.Id)
	if err != nil {
		return err
	}

	err = fs.extractObjectFile(stream.Context(), in.ObjectId, authInfo.Context.Name, stream)
	if err != nil {
		return err
	}

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
			fmt.Sprintf("TASK_Id=%s", taskId),
			fmt.Sprintf("HANDLER=%s", in.Handler),
			fmt.Sprintf("BEAM_TOKEN=%s", authInfo.Token.Key),
		},
		Cpu:        in.Cpu,
		Memory:     in.Memory,
		Gpu:        in.Gpu,
		ImageId:    in.ImageId,
		EntryPoint: []string{in.PythonVersion, "-m", "beam.runner.function"},
		Mounts: []types.Mount{
			{LocalPath: path.Join(types.DefaultExtractedObjectPath, authInfo.Context.Name, in.ObjectId), MountPath: types.WorkerUserCodeVolume, ReadOnly: true},
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

func (fs *RunCFunctionService) handleStreams(ctx context.Context,
	stream pb.FunctionService_FunctionInvokeServer,
	taskId string, containerId string,
	outputChan chan common.OutputMsg, keyEventChan chan common.KeyEvent) error {

	var lastMessage common.OutputMsg

_stream:
	for {
		select {
		case o := <-outputChan:
			if err := stream.Send(&pb.FunctionInvokeResponse{Output: o.Msg, Done: o.Done}); err != nil {
				lastMessage = o
				break
			}

			if o.Done {
				lastMessage = o
				break _stream
			}
		case <-keyEventChan:
			result, _ := fs.rdb.Get(context.TODO(), Keys.FunctionResult(taskId)).Bytes()
			if err := stream.Send(&pb.FunctionInvokeResponse{Done: true, Result: result, ExitCode: 0}); err != nil {
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

func (fs *RunCFunctionService) extractObjectFile(ctx context.Context, objectId string, contextName string, stream pb.FunctionService_FunctionInvokeServer) error {
	extractedObjectPath := path.Join(types.DefaultExtractedObjectPath, contextName)
	os.MkdirAll(extractedObjectPath, 0644)

	destPath := path.Join(types.DefaultExtractedObjectPath, contextName, objectId)
	if _, err := os.Stat(destPath); !os.IsNotExist(err) {
		// Folder already exists, so skip extraction
		return nil
	}

	// Check if the object file exists
	objectFilePath := path.Join(types.DefaultObjectPath, contextName, objectId)
	if _, err := os.Stat(objectFilePath); os.IsNotExist(err) {
		stream.Send(&pb.FunctionInvokeResponse{Done: true, ExitCode: 1})
		return errors.New("object file does not exist")
	}

	zip := archiver.NewZip()
	if err := zip.Unarchive(objectFilePath, destPath); err != nil {
		stream.Send(&pb.FunctionInvokeResponse{Done: true, ExitCode: 1})
		return fmt.Errorf("failed to unzip object file: %v", err)
	}

	return nil
}

func (fs *RunCFunctionService) FunctionGetArgs(ctx context.Context, in *pb.FunctionGetArgsRequest) (*pb.FunctionGetArgsResponse, error) {
	value, err := fs.rdb.Get(context.TODO(), Keys.FunctionArgs(in.TaskId)).Bytes()
	if err != nil {
		return &pb.FunctionGetArgsResponse{Ok: false, Args: nil}, nil
	}

	return &pb.FunctionGetArgsResponse{
		Ok:   true,
		Args: value,
	}, nil
}

func (fs *RunCFunctionService) FunctionSetResult(ctx context.Context, in *pb.FunctionSetResultRequest) (*pb.FunctionSetResultResponse, error) {
	err := fs.rdb.Set(context.TODO(), Keys.FunctionResult(in.TaskId), in.Result, functionResultExpirationTimeout).Err()
	if err != nil {
		return &pb.FunctionSetResultResponse{Ok: false}, nil
	}

	return &pb.FunctionSetResultResponse{
		Ok: true,
	}, nil
}

func (fs *RunCFunctionService) genInvocationId() string {
	return uuid.New().String()[:8]
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
