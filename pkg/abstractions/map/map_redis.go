package dmap

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	maxMapValueSize = (1024 * 1024) + 13 // 1 MiB + 13 bytes for cloudpickle header
	maxMapValueTtls = 7 * 24 * time.Hour // 1 week
)

type RedisMapService struct {
	pb.UnimplementedMapServiceServer

	rdb *common.RedisClient
}

func NewRedisMapService(rdb *common.RedisClient) (MapService, error) {
	return &RedisMapService{
		rdb: rdb,
	}, nil
}

// Map service implementations
func (m *RedisMapService) MapSet(ctx context.Context, in *pb.MapSetRequest) (*pb.MapSetResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if len(in.Value) > maxMapValueSize {
		return &pb.MapSetResponse{Ok: false, ErrMsg: "Value cannot be larger than 1 MiB"}, nil
	}

	if time.Duration(in.Ttl)*time.Second > maxMapValueTtls {
		return &pb.MapSetResponse{Ok: false, ErrMsg: "TTL cannot be longer than 1 week"}, nil
	}

	err := m.rdb.Set(ctx, Keys.MapEntry(authInfo.Workspace.Name, in.Name, in.Key), in.Value, time.Duration(in.Ttl)*time.Second).Err()
	if err != nil {
		return &pb.MapSetResponse{Ok: false}, nil
	}

	return &pb.MapSetResponse{Ok: true}, nil
}

func (m *RedisMapService) MapGet(ctx context.Context, in *pb.MapGetRequest) (*pb.MapGetResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	value, err := m.rdb.Get(ctx, Keys.MapEntry(authInfo.Workspace.Name, in.Name, in.Key)).Bytes()
	if err != nil {
		return &pb.MapGetResponse{Ok: false, Value: nil}, nil
	}

	return &pb.MapGetResponse{Ok: true, Value: value}, nil
}

func (m *RedisMapService) MapDelete(ctx context.Context, in *pb.MapDeleteRequest) (*pb.MapDeleteResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	err := m.rdb.Del(ctx, Keys.MapEntry(authInfo.Workspace.Name, in.Name, in.Key)).Err()
	if err != nil {
		return &pb.MapDeleteResponse{Ok: false}, err
	}

	return &pb.MapDeleteResponse{Ok: true}, nil
}

func (m *RedisMapService) MapCount(ctx context.Context, in *pb.MapCountRequest) (*pb.MapCountResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	keys, err := m.rdb.Scan(ctx, Keys.MapEntry(authInfo.Workspace.Name, in.Name, "*"))
	if err != nil {
		return &pb.MapCountResponse{Ok: false, Count: 0}, err
	}

	return &pb.MapCountResponse{Ok: true, Count: uint32(len(keys))}, nil
}

func (m *RedisMapService) MapKeys(ctx context.Context, in *pb.MapKeysRequest) (*pb.MapKeysResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	keys, err := m.rdb.Scan(ctx, Keys.MapEntry(authInfo.Workspace.Name, in.Name, "*"))
	if err != nil {
		return &pb.MapKeysResponse{Ok: false, Keys: []string{}}, err
	}

	// Remove the prefix from each key
	for i, key := range keys {
		parts := strings.Split(key, ":")
		keys[i] = parts[len(parts)-1]
	}

	return &pb.MapKeysResponse{Ok: true, Keys: keys}, nil
}

// Redis keys
var (
	mapEntry string = "map:%s:%s:%s"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) MapEntry(workspaceName, name, key string) string {
	return fmt.Sprintf(mapEntry, workspaceName, name, key)
}
