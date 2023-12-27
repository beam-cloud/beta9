package dqueue

import (
	"context"

	pb "github.com/beam-cloud/beam/proto"
)

type SimpleQueue interface {
	Enqueue(ctx context.Context, in *pb.SimpleQueueEnqueueRequest) (*pb.SimpleQueueEnqueueResponse, error)
	Dequeue(ctx context.Context, in *pb.SimpleQueueDequeueRequest) (*pb.SimpleQueueDequeueResponse, error)
	Peek(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueuePeekResponse, error)
	Empty(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueueEmptyResponse, error)
	Size(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueueSizeResponse, error)
}
