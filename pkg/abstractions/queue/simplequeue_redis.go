package simplequeue

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/redis/go-redis/v9"
)

type RedisSimpleQueueService struct {
	pb.UnimplementedSimpleQueueServiceServer

	rdb *common.RedisClient
}

func NewRedisSimpleQueueService(rdb *common.RedisClient) (SimpleQueueService, error) {
	return &RedisSimpleQueueService{
		rdb: rdb,
	}, nil
}

// Simple queue service implementations
func (s *RedisSimpleQueueService) SimpleQueuePut(ctx context.Context, in *pb.SimpleQueuePutRequest) (*pb.SimpleQueuePutResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	queueName := Keys.SimpleQueueName(authInfo.Workspace.Name, in.Name)

	err := s.rdb.RPush(context.TODO(), queueName, in.Value).Err()
	if err != nil {
		return &pb.SimpleQueuePutResponse{
			Ok: false,
		}, nil
	}

	return &pb.SimpleQueuePutResponse{
		Ok: true,
	}, nil
}

func (s *RedisSimpleQueueService) SimpleQueuePop(ctx context.Context, in *pb.SimpleQueuePopRequest) (*pb.SimpleQueuePopResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	queueName := Keys.SimpleQueueName(authInfo.Workspace.Name, in.Name)

	value, err := s.rdb.LPop(context.TODO(), queueName).Bytes()
	if err == redis.Nil {
		return &pb.SimpleQueuePopResponse{
			Ok:    true,
			Value: []byte{},
		}, nil
	} else if err != nil {
		return &pb.SimpleQueuePopResponse{
			Ok:    false,
			Value: []byte{},
		}, nil
	}

	return &pb.SimpleQueuePopResponse{
		Ok:    true,
		Value: value,
	}, nil
}

func (s *RedisSimpleQueueService) SimpleQueuePeek(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueuePeekResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	queueName := Keys.SimpleQueueName(authInfo.Workspace.Name, in.Name)

	res, err := s.rdb.LRange(context.TODO(), queueName, 0, 0)
	if err != nil {
		return &pb.SimpleQueuePeekResponse{
			Ok:    false,
			Value: []byte{},
		}, nil
	}

	var value []byte
	if len(res) > 0 {
		value = []byte(res[0])
	}

	return &pb.SimpleQueuePeekResponse{
		Ok:    true,
		Value: value,
	}, nil
}

func (s *RedisSimpleQueueService) SimpleQueueEmpty(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueueEmptyResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	queueName := Keys.SimpleQueueName(authInfo.Workspace.Name, in.Name)

	length, err := s.rdb.LLen(context.TODO(), queueName).Result()
	if err != nil {
		return &pb.SimpleQueueEmptyResponse{
			Ok:    false,
			Empty: false,
		}, nil
	}

	if length > 0 {
		return &pb.SimpleQueueEmptyResponse{
			Ok:    true,
			Empty: false,
		}, nil
	}

	return &pb.SimpleQueueEmptyResponse{
		Ok:    true,
		Empty: true,
	}, nil
}

func (s *RedisSimpleQueueService) SimpleQueueSize(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueueSizeResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	queueName := Keys.SimpleQueueName(authInfo.Workspace.Name, in.Name)

	length, err := s.rdb.LLen(context.TODO(), queueName).Result()
	if err != nil {
		return &pb.SimpleQueueSizeResponse{
			Ok:   false,
			Size: 0,
		}, nil
	}

	return &pb.SimpleQueueSizeResponse{
		Ok:   true,
		Size: uint64(length),
	}, nil
}

// Redis keys
var (
	queuePrefix string = "simplequeue"
	queueName   string = "simplequeue:%s:%s"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) SimpleQueuePrefix() string {
	return queuePrefix
}

func (k *keys) SimpleQueueName(workspaceName, name string) string {
	return fmt.Sprintf(queueName, workspaceName, name)
}
