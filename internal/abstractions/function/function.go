package function

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/scheduler"
	pb "github.com/beam-cloud/beam/proto"
	"github.com/google/uuid"
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

	// TODO: make args expire after 10 minutes
	err := fs.rdb.Set(context.TODO(), Keys.FunctionArgs(invocationId), in.Args, 0).Err()
	if err != nil {
		stream.Send(&pb.FunctionInvokeResponse{Output: "Failed", Done: true, ExitCode: 1})
		return errors.New("unable to store function args")
	}

	// ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)

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
	return &pb.FunctionGetArgsResponse{}, nil
}

func (fs *RunCFunctionService) genInvocationId() string {
	return uuid.New().String()[:8]
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
