package simplequeue

import (
	"context"

	pb "github.com/beam-cloud/beta9/proto"
)

type SimpleQueueService interface {
	pb.SimpleQueueServiceServer
	SimpleQueuePut(ctx context.Context, in *pb.SimpleQueuePutRequest) (*pb.SimpleQueuePutResponse, error)
	SimpleQueuePop(ctx context.Context, in *pb.SimpleQueuePopRequest) (*pb.SimpleQueuePopResponse, error)
	SimpleQueuePeek(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueuePeekResponse, error)
	SimpleQueueEmpty(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueueEmptyResponse, error)
	SimpleQueueSize(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueueSizeResponse, error)
}
