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

func (m *RedisMapService) MapGet(ctx context.Context, in *pb.MapGetRequest) (*pb.MapGetResponse, error) {
	return &pb.MapGetResponse{Ok: true, Value: []byte{}}, nil
}

func (m *RedisMapService) MapDelete(ctx context.Context, in *pb.MapDeleteRequest) (*pb.MapDeleteResponse, error) {
	return &pb.MapDeleteResponse{Ok: false}, nil
}

func (m *RedisMapService) MapCount(ctx context.Context, in *pb.MapCountRequest) (*pb.MapCountResponse, error) {
	return &pb.MapCountResponse{Ok: false, Count: 0}, nil
}

func (m *RedisMapService) MapKeys(ctx context.Context, in *pb.MapKeysRequest) (*pb.MapKeysResponse, error) {
	return &pb.MapKeysResponse{Ok: false, Keys: []string{"test", "test2"}}, nil
}
