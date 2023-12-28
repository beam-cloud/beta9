package simplequeue

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beam/internal/common"
	pb "github.com/beam-cloud/beam/proto"
	"github.com/redis/go-redis/v9"
)

type RedisSimpleQueueService struct {
	pb.UnimplementedSimpleQueueServiceServer

	rdb *common.RedisClient
}

func NewRedisSimpleQueueService(rdb *common.RedisClient) (*RedisSimpleQueueService, error) {
	return &RedisSimpleQueueService{
		rdb: rdb,
	}, nil
}

// Simple queue service implementations
func (s *RedisSimpleQueueService) Put(ctx context.Context, in *pb.SimpleQueuePutRequest) (*pb.SimpleQueuePutResponse, error) {
	queueName := Keys.QueueName(in.Name)

	err := s.rdb.RPush(context.TODO(), queueName, in.Value).Err()
	if err != nil {
		return &pb.SimpleQueuePutResponse{
			Ok: false,
		}, err
	}

	return &pb.SimpleQueuePutResponse{
		Ok: true,
	}, nil
}

func (s *RedisSimpleQueueService) Pop(ctx context.Context, in *pb.SimpleQueuePopRequest) (*pb.SimpleQueuePopResponse, error) {
	queueName := Keys.QueueName(in.Name)

	value, err := s.rdb.LPop(context.TODO(), queueName).Bytes()
	if err == redis.Nil {
		return &pb.SimpleQueuePopResponse{
			Ok: true,
			Value: []byte{},
		}, nil
	} else if err != nil { 
		return &pb.SimpleQueuePopResponse{
			Ok: false,
			Value: []byte{},
		}, err
	}

	return &pb.SimpleQueuePopResponse{
		Ok: true,
		Value: value,
	}, nil
}

func (s *RedisSimpleQueueService) Peek(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueuePeekResponse, error) {
	queueName := Keys.SimpleQueueName(in.Name)

	res, err := s.rdb.LRange(context.TODO(), queueName, 0, 0)
	if err != nil {
		return &pb.SimpleQueuePeekResponse{
			Ok:    false,
			Value: []byte{},
		}, err
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

func (s *RedisSimpleQueueService) Empty(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueueEmptyResponse, error) {
	queueName := Keys.SimpleQueueName(in.Name)

	length, err := s.rdb.LLen(context.TODO(), queueName).Result()
	if err != nil {
		return &pb.SimpleQueueEmptyResponse{
			Ok:    false,
			Empty: false,
		}, err
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

func (s *RedisSimpleQueueService) Size(ctx context.Context, in *pb.SimpleQueueRequest) (*pb.SimpleQueueSizeResponse, error) {
	queueName := Keys.SimpleQueueName(in.Name)

	length, err := s.rdb.LLen(context.TODO(), queueName).Result()
	if err != nil {
		return &pb.SimpleQueueSizeResponse{
			Ok:   false,
			Size: 0,
		}, err
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

func (k *keys) SimpleQueueName(name string) string {
	return fmt.Sprintf(queueName, name)
}
