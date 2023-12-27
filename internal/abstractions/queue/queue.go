package dqueue

import (
	"context"

	pb "github.com/beam-cloud/beam/proto"
)

type SimpleQueue interface {
	Put(ctx context.Context, in *pb.SimpleQueuePutRequest) (*pb.SimpleQueuePutResponse, error)
	Pop(ctx context.Context, in *pb.SimpleQueuePopRequest) (*pb.SimpleQueuePopResponse, error)
	Peek(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueuePeekResponse, error)
	Empty(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueueEmptyResponse, error)
	Size(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueueSizeResponse, error)
}
