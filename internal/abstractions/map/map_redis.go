package dmap

import (
	"context"

	"github.com/beam-cloud/beam/internal/common"
	pb "github.com/beam-cloud/beam/proto"
)

type RedisMapService struct {
	pb.UnimplementedMapServiceServer

	rdb *common.RedisClient
}

func NewRedisMapService(rdb *common.RedisClient) (*RedisMapService, error) {
	return &RedisMapService{
		rdb: rdb,
	}, nil
}

func (m *RedisMapService) MapSet(ctx context.Context, in *pb.MapSetRequest) (*pb.MapSetResponse, error) {
	return &pb.MapSetResponse{Ok: true}, nil
}

func (m *RedisMapService) MapDelete(ctx context.Context, in *pb.MapDeleteRequest) (*pb.MapDeleteResponse, error) {
	return &pb.MapDeleteResponse{Ok: false}, nil
}
