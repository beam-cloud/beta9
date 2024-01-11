package dmap

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/beam/internal/auth"
	"github.com/beam-cloud/beam/internal/common"
	pb "github.com/beam-cloud/beam/proto"
)

type RedisMapService struct {
	pb.UnimplementedMapServiceServer
	lock *common.RedisLock

	rdb *common.RedisClient
}

func NewRedisMapService(rdb *common.RedisClient) (MapService, error) {
	lock := common.NewRedisLock(rdb)

	return &RedisMapService{
		rdb:  rdb,
		lock: lock,
	}, nil
}

// Map service implementations
func (m *RedisMapService) MapSet(ctx context.Context, in *pb.MapSetRequest) (*pb.MapSetResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	err := m.lock.Acquire(ctx, Keys.MapEntryLock(authInfo.Workspace.Name, in.Name, in.Key), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return &pb.MapSetResponse{Ok: false}, nil
	}
	defer m.lock.Release(Keys.MapEntryLock(authInfo.Workspace.Name, in.Name, in.Key))

	err = m.rdb.Set(ctx, Keys.MapEntry(authInfo.Workspace.Name, in.Name, in.Key), in.Value, 0).Err()
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

	// Remove the MapEntry prefix from each key
	for i, key := range keys {
		keys[i] = strings.TrimPrefix(key, fmt.Sprintf(mapEntry, in.Name, ""))
	}

	return &pb.MapKeysResponse{Ok: true, Keys: keys}, nil
}

// Redis keys
var (
	mapPrefix    string = "map"
	mapEntry     string = "map:%s:%s:%s"
	mapEntryLock string = "map:%s:%s:%s:lock"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) MapPrefix() string {
	return mapPrefix
}

func (k *keys) MapEntry(workspaceName, name, key string) string {
	return fmt.Sprintf(mapEntry, workspaceName, name, key)
}

func (k *keys) MapEntryLock(workspaceName, name, key string) string {
	return fmt.Sprintf(mapEntry, workspaceName, name, key)
}
