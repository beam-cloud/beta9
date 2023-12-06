package dmap

import (
	"context"

	pb "github.com/beam-cloud/beam/proto"
)

type RedisMapService struct {
	pb.UnimplementedMapServiceServer
}

func NewRedisMapService() (*RedisMapService, error) {
	return &RedisMapService{}, nil
}

func (m *RedisMapService) MapSet(ctx context.Context, in *pb.MapSetRequest) (*pb.MapSetResponse, error) {
	return &pb.MapSetResponse{Ok: false}, nil
}
