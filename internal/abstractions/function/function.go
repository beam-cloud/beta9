package function

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/beam-cloud/beam/internal/common"
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
	scheduler       *scheduler.Scheduler
	keyEventManager *common.KeyEventManager
	rdb             *common.RedisClient
}

func NewRuncFunctionService(ctx context.Context, rdb *common.RedisClient, scheduler *scheduler.Scheduler, keyEventManager *common.KeyEventManager) (*RunCFunctionService, error) {
	return &RunCFunctionService{
		scheduler:       scheduler,
		rdb:             rdb,
		keyEventManager: keyEventManager,
	}, nil
}

func (fs *RunCFunctionService) FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error {
	log.Printf("incoming function run request: %+v", in)

	invocationId := fs.genInvocationId()
	containerId := fs.genContainerId(invocationId)

	err := fs.rdb.Set(context.TODO(), Keys.FunctionArgs(invocationId), in.Args, functionArgsExpirationTimeout).Err()
	if err != nil {
		stream.Send(&pb.FunctionInvokeResponse{Output: "Failed", Done: true, ExitCode: 1})
		return errors.New("unable to store function args")
	}

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)
	keyEventChan := make(chan common.KeyEvent)

	go fs.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId), keyEventChan)

	// Check if object exists & unzip
	objectFilePath := path.Join(types.DefaultObjectPath, in.ObjectId)
	if _, err := os.Stat(objectFilePath); os.IsNotExist(err) {
		stream.Send(&pb.FunctionInvokeResponse{Done: true, ExitCode: 1})
		return errors.New("object file does not exist")
	}

	destPath := fmt.Sprintf("/data/function/%s", invocationId)
	zip := archiver.NewZip()
	if err := zip.Unarchive(objectFilePath, destPath); err != nil {
		stream.Send(&pb.FunctionInvokeResponse{Done: true, ExitCode: 1})
		return fmt.Errorf("failed to unzip object file: %v", err)
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
			fmt.Sprintf("INVOCATION_ID=%s", invocationId),
			fmt.Sprintf("HANDLER=%s", in.Handler),
			"PYTHONUNBUFFERED=1",
		},
		Cpu:        in.Cpu,
		Memory:     in.Memory,
		Gpu:        in.Gpu,
		ImageId:    in.ImageId,
		EntryPoint: []string{in.PythonVersion, "-m", "beam.runner.function"},
		Mounts: []types.Mount{
			{LocalPath: fmt.Sprintf("/data/function/%s", invocationId), MountPath: "/code", ReadOnly: true},
		},
	})
	if err != nil {
		return err
	}

	hostname, err := fs.scheduler.ContainerRepo.GetContainerWorkerHostname(containerId)
	if err != nil {
		return err
	}

	// TODO: replace placeholder service token
	client, err := common.NewRunCClient(hostname, "")
	if err != nil {
		return err
	}

	go client.StreamLogs(ctx, containerId, outputChan)

	return fs.handleStreams(ctx, stream, invocationId, containerId, outputChan, keyEventChan)
}

func (fs *RunCFunctionService) handleStreams(ctx context.Context,
	stream pb.FunctionService_FunctionInvokeServer,
	invocationId string, containerId string,
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
			result, _ := fs.rdb.Get(context.TODO(), Keys.FunctionResult(invocationId)).Bytes()
			if err := stream.Send(&pb.FunctionInvokeResponse{Done: true, Result: result, ExitCode: 0}); err != nil {
				break
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if !lastMessage.Success {
		return errors.New("function invocation failed")
	}

	return nil
}

func (fs *RunCFunctionService) FunctionGetArgs(ctx context.Context, in *pb.FunctionGetArgsRequest) (*pb.FunctionGetArgsResponse, error) {
	value, err := fs.rdb.Get(context.TODO(), Keys.FunctionArgs(in.InvocationId)).Bytes()
	if err != nil {
		return &pb.FunctionGetArgsResponse{Ok: false, Args: nil}, nil
	}

	return &pb.FunctionGetArgsResponse{
		Ok:   true,
		Args: value,
	}, nil
}

func (fs *RunCFunctionService) FunctionSetResult(ctx context.Context, in *pb.FunctionSetResultRequest) (*pb.FunctionSetResultResponse, error) {
	err := fs.rdb.Set(context.TODO(), Keys.FunctionResult(in.InvocationId), in.Result, functionResultExpirationTimeout).Err()
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

func (fs *RunCFunctionService) genContainerId(invocationId string) string {
	return fmt.Sprintf("%s%s", functionContainerPrefix, invocationId)
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

func (k *keys) FunctionArgs(invocationId string) string {
	return fmt.Sprintf(functionArgs, invocationId)
}

func (k *keys) FunctionResult(invocationId string) string {
	return fmt.Sprintf(functionResult, invocationId)
}
