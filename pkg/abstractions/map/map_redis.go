package dmap

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/redis/go-redis/v9"
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

	entryKey := Keys.MapEntry(authInfo.Workspace.Name, in.Name, in.Key)
	indexKey := Keys.MapIndex(authInfo.Workspace.Name, in.Name)

	pipe := m.rdb.TxPipeline()
	pipe.Set(ctx, entryKey, in.Value, time.Duration(in.Ttl)*time.Second)
	pipe.SAdd(ctx, indexKey, in.Key)
	_, err := pipe.Exec(ctx)
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

	entryKey := Keys.MapEntry(authInfo.Workspace.Name, in.Name, in.Key)
	indexKey := Keys.MapIndex(authInfo.Workspace.Name, in.Name)

	pipe := m.rdb.TxPipeline()
	pipe.Del(ctx, entryKey)
	pipe.SRem(ctx, indexKey, in.Key)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return &pb.MapDeleteResponse{Ok: false}, err
	}

	return &pb.MapDeleteResponse{Ok: true}, nil
}

func (m *RedisMapService) MapCount(ctx context.Context, in *pb.MapCountRequest) (*pb.MapCountResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	keys, err := m.liveMapKeys(ctx, authInfo.Workspace.Name, in.Name)
	if err != nil {
		return &pb.MapCountResponse{Ok: false, Count: 0}, err
	}

	return &pb.MapCountResponse{Ok: true, Count: uint32(len(keys))}, nil
}

func (m *RedisMapService) MapKeys(ctx context.Context, in *pb.MapKeysRequest) (*pb.MapKeysResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	keys, err := m.liveMapKeys(ctx, authInfo.Workspace.Name, in.Name)
	if err != nil {
		return &pb.MapKeysResponse{Ok: false, Keys: []string{}}, err
	}

	return &pb.MapKeysResponse{Ok: true, Keys: keys}, nil
}

func (m *RedisMapService) liveMapKeys(ctx context.Context, workspaceName, name string) ([]string, error) {
	indexKey := Keys.MapIndex(workspaceName, name)
	keys, err := m.rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []string{}, nil
	}

	pipe := m.rdb.TxPipeline()
	exists := make([]*redis.IntCmd, 0, len(keys))
	for _, key := range keys {
		exists = append(exists, pipe.Exists(ctx, Keys.MapEntry(workspaceName, name, key)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	liveKeys := make([]string, 0, len(keys))
	staleKeys := make([]interface{}, 0)
	for i, key := range keys {
		if exists[i].Val() > 0 {
			liveKeys = append(liveKeys, key)
		} else {
			staleKeys = append(staleKeys, key)
		}
	}
	if len(staleKeys) > 0 {
		if err := m.rdb.SRem(ctx, indexKey, staleKeys...).Err(); err != nil {
			return nil, err
		}
	}

	sort.Strings(liveKeys)
	return liveKeys, nil
}

// Redis keys
var (
	mapEntry string = "map:%s:%s:%s"
	mapIndex string = "map:%s:%s:index"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) MapEntry(workspaceName, name, key string) string {
	return fmt.Sprintf(mapEntry, workspaceName, name, key)
}

func (k *keys) MapIndex(workspaceName, name string) string {
	return fmt.Sprintf(mapIndex, workspaceName, name)
}
