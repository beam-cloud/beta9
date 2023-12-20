package function

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/scheduler"
	"github.com/beam-cloud/beam/internal/types"
	pb "github.com/beam-cloud/beam/proto"
	"github.com/google/uuid"
)

const (
	functionContainerPrefix        string = "function-"
	defaultFunctionContainerCpu    int64  = 1000
	defaultFunctionContainerMemory int64  = 1024
)

type FunctionService interface {
	FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error
	FunctionGetArgs(ctx context.Context, in *pb.FunctionGetArgsRequest) (*pb.FunctionGetArgsResponse, error)
}

type RunCFunctionService struct {
	pb.UnimplementedFunctionServiceServer
	scheduler *scheduler.Scheduler
	rdb       *common.RedisClient
}

func NewRuncFunctionService(ctx context.Context, rdb *common.RedisClient, scheduler *scheduler.Scheduler) (*RunCFunctionService, error) {
	return &RunCFunctionService{
		scheduler: scheduler,
		rdb:       rdb,
	}, nil
}

func (fs *RunCFunctionService) FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error {
	log.Printf("incoming function run request: %+v", in)

	invocationId := fs.genInvocationId()
	containerId := fs.genContainerId(invocationId)

	// TODO: make args expire after 10 minutes
	err := fs.rdb.Set(context.TODO(), Keys.FunctionArgs(invocationId), in.Args, 0).Err()
	if err != nil {
		stream.Send(&pb.FunctionInvokeResponse{Output: "Failed", Done: true, ExitCode: 1})
		return errors.New("unable to store function args")
	}

	// ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)

	err = fs.scheduler.Run(&types.ContainerRequest{
		ContainerId: containerId,
		Env:         []string{},
		Cpu:         defaultFunctionContainerCpu,
		Memory:      defaultFunctionContainerMemory,
		ImageId:     in.ImageId,
		EntryPoint:  []string{"tail", "-f", "/dev/null"},
	})
	if err != nil {
		return err
	}

	var lastMessage common.OutputMsg
	for o := range outputChan {
		if err := stream.Send(&pb.FunctionInvokeResponse{Output: o.Msg, Done: o.Done}); err != nil {
			lastMessage = o
			break
		}

		if o.Done {
			lastMessage = o
			break
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
		return &pb.FunctionGetArgsResponse{Ok: false, Args: nil}, err
	}

	return &pb.FunctionGetArgsResponse{
		Ok:   true,
		Args: value,
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
