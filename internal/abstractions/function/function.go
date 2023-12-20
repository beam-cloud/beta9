package function

import (
	"context"
	"errors"
	"log"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/scheduler"
	pb "github.com/beam-cloud/beam/proto"
)

type FunctionService interface {
	FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error
	FunctionGetArgs(ctx context.Context, in *pb.FunctionGetArgsRequest) (*pb.FunctionGetArgsResponse, error)
}

type RunCFunctionService struct {
	pb.UnimplementedFunctionServiceServer
	scheduler *scheduler.Scheduler
}

func NewRuncFunctionService(ctx context.Context, scheduler *scheduler.Scheduler) (*RunCFunctionService, error) {
	return &RunCFunctionService{
		scheduler: scheduler,
	}, nil
}

func (fs *RunCFunctionService) FunctionInvoke(in *pb.FunctionInvokeRequest, stream pb.FunctionService_FunctionInvokeServer) error {
	log.Printf("incoming function run request: %+v", in)

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
