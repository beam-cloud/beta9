package dmap

import (
	"context"

	pb "github.com/beam-cloud/beta9/proto"
)

type MapService interface {
	pb.MapServiceServer
	MapSet(ctx context.Context, in *pb.MapSetRequest) (*pb.MapSetResponse, error)
	MapGet(ctx context.Context, in *pb.MapGetRequest) (*pb.MapGetResponse, error)
	MapDelete(ctx context.Context, in *pb.MapDeleteRequest) (*pb.MapDeleteResponse, error)
	MapCount(ctx context.Context, in *pb.MapCountRequest) (*pb.MapCountResponse, error)
	MapKeys(ctx context.Context, in *pb.MapKeysRequest) (*pb.MapKeysResponse, error)
}
