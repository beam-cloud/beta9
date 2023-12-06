package dmap

import (
	"context"

	pb "github.com/beam-cloud/beam/proto"
)

type Map interface {
	Set(string, string) error
	Get(string) (string, error)
}

type RedisMapService struct {
	pb.UnimplementedMapServiceServer
}

func NewRedisMapService() (*RedisMapService, error) {
	return &RedisMapService{}, nil
}

func (m *RedisMapService) MapSet(ctx context.Context, in *pb.MapSetRequest) (*pb.MapSetResponse, error) {
	return &pb.MapSetResponse{Ok: true}, nil
}
