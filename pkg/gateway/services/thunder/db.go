package thunder

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	redis "github.com/redis/go-redis/v9"
)

const thunderPoolLockTTLSeconds = 30

var errThunderStateRequired = errors.New("thunder state is required")

type Repository interface {
	WithPoolLock(ctx context.Context, workspaceID, poolName string, fn func(context.Context) error) error

	GetClientEnrollment(ctx context.Context, containerID string) (*ClientEnrollmentState, bool, error)
	SaveClientEnrollment(ctx context.Context, state *ClientEnrollmentState, ttl time.Duration) error
	DeleteClientEnrollment(ctx context.Context, containerID string) error
	ListClientEnrollments(ctx context.Context) ([]*ClientEnrollmentState, error)

	GetNodeEnrollment(ctx context.Context, workspaceID, poolName, machineID string) (*NodeEnrollmentState, bool, error)
	SaveNodeEnrollment(ctx context.Context, state *NodeEnrollmentState, ttl time.Duration) error
	DeleteNodeEnrollment(ctx context.Context, workspaceID, poolName, machineID string) error
	ListNodeEnrollments(ctx context.Context, workspaceID, poolName string) ([]*NodeEnrollmentState, error)

	GetZone(ctx context.Context, workspaceID, poolName string) (*ZoneState, bool, error)
	SaveZone(ctx context.Context, state *ZoneState) error
	DeleteZone(ctx context.Context, workspaceID, poolName string) error
	ListZones(ctx context.Context, workspaceID string) ([]*ZoneState, error)
}

type ClientEnrollmentState struct {
	ContainerID       string `json:"container_id"`
	WorkspaceID       string `json:"workspace_id"`
	WorkerID          string `json:"worker_id"`
	MachineID         string `json:"machine_id"`
	PoolName          string `json:"pool_name"`
	EnrollmentTokenID string `json:"enrollment_token_id"`
}

type NodeEnrollmentState struct {
	WorkspaceID       string `json:"workspace_id"`
	PoolName          string `json:"pool_name"`
	MachineID         string `json:"machine_id"`
	EnrollmentTokenID string `json:"enrollment_token_id"`
}

type ZoneState struct {
	WorkspaceID   string `json:"workspace_id"`
	PoolName      string `json:"pool_name"`
	ThunderZoneID string `json:"thunder_zone_id"`
}

type RedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func NewRedisRepository(rdb *common.RedisClient) *RedisRepository {
	return &RedisRepository{rdb: rdb, lock: common.NewRedisLock(rdb)}
}

func (r *RedisRepository) WithPoolLock(ctx context.Context, workspaceID, poolName string, fn func(context.Context) error) error {
	return r.lock.WithLease(ctx, common.RedisKeys.ThunderPoolLock(workspaceID, poolName), common.RedisLockOptions{
		TtlS:          thunderPoolLockTTLSeconds,
		Retries:       100,
		RetryInterval: 50 * time.Millisecond,
	}, fn)
}

func (r *RedisRepository) GetClientEnrollment(ctx context.Context, containerID string) (*ClientEnrollmentState, bool, error) {
	var state ClientEnrollmentState
	found, err := r.getJSON(ctx, common.RedisKeys.ThunderClientEnrollment(containerID), &state)
	if err != nil || !found {
		return nil, found, err
	}
	return &state, true, nil
}

func (r *RedisRepository) SaveClientEnrollment(ctx context.Context, state *ClientEnrollmentState, ttl time.Duration) error {
	if state == nil || state.ContainerID == "" {
		return errThunderStateRequired
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, common.RedisKeys.ThunderClientEnrollment(state.ContainerID), data, ttl)
		pipe.SAdd(ctx, common.RedisKeys.ThunderClientEnrollmentIndex(), state.ContainerID)
		return nil
	})
	return err
}

func (r *RedisRepository) DeleteClientEnrollment(ctx context.Context, containerID string) error {
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, common.RedisKeys.ThunderClientEnrollment(containerID))
		pipe.SRem(ctx, common.RedisKeys.ThunderClientEnrollmentIndex(), containerID)
		return nil
	})
	return err
}

func (r *RedisRepository) ListClientEnrollments(ctx context.Context) ([]*ClientEnrollmentState, error) {
	containerIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.ThunderClientEnrollmentIndex()).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(containerIDs)
	keys := make([]string, 0, len(containerIDs))
	for _, containerID := range containerIDs {
		keys = append(keys, common.RedisKeys.ThunderClientEnrollment(containerID))
	}
	return listJSON[ClientEnrollmentState](ctx, r.rdb, keys)
}

func (r *RedisRepository) GetNodeEnrollment(ctx context.Context, workspaceID, poolName, machineID string) (*NodeEnrollmentState, bool, error) {
	var state NodeEnrollmentState
	found, err := r.getJSON(ctx, common.RedisKeys.ThunderNodeEnrollment(workspaceID, poolName, machineID), &state)
	if err != nil || !found {
		return nil, found, err
	}
	return &state, true, nil
}

func (r *RedisRepository) SaveNodeEnrollment(ctx context.Context, state *NodeEnrollmentState, ttl time.Duration) error {
	if state == nil || state.WorkspaceID == "" || state.PoolName == "" || state.MachineID == "" {
		return errThunderStateRequired
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, common.RedisKeys.ThunderNodeEnrollment(state.WorkspaceID, state.PoolName, state.MachineID), data, ttl)
		pipe.SAdd(ctx, common.RedisKeys.ThunderNodeEnrollmentIndex(state.WorkspaceID, state.PoolName), state.MachineID)
		return nil
	})
	return err
}

func (r *RedisRepository) DeleteNodeEnrollment(ctx context.Context, workspaceID, poolName, machineID string) error {
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, common.RedisKeys.ThunderNodeEnrollment(workspaceID, poolName, machineID))
		pipe.SRem(ctx, common.RedisKeys.ThunderNodeEnrollmentIndex(workspaceID, poolName), machineID)
		return nil
	})
	return err
}

func (r *RedisRepository) ListNodeEnrollments(ctx context.Context, workspaceID, poolName string) ([]*NodeEnrollmentState, error) {
	machineIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.ThunderNodeEnrollmentIndex(workspaceID, poolName)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(machineIDs)
	keys := make([]string, 0, len(machineIDs))
	for _, machineID := range machineIDs {
		keys = append(keys, common.RedisKeys.ThunderNodeEnrollment(workspaceID, poolName, machineID))
	}
	return listJSON[NodeEnrollmentState](ctx, r.rdb, keys)
}

func (r *RedisRepository) GetZone(ctx context.Context, workspaceID, poolName string) (*ZoneState, bool, error) {
	var state ZoneState
	found, err := r.getJSON(ctx, common.RedisKeys.ThunderZone(workspaceID, poolName), &state)
	if err != nil || !found {
		return nil, found, err
	}
	return &state, true, nil
}

func (r *RedisRepository) SaveZone(ctx context.Context, state *ZoneState) error {
	if state == nil || state.WorkspaceID == "" || state.PoolName == "" {
		return errThunderStateRequired
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, common.RedisKeys.ThunderZone(state.WorkspaceID, state.PoolName), data, 0)
		pipe.SAdd(ctx, common.RedisKeys.ThunderZoneIndex(state.WorkspaceID), state.PoolName)
		return nil
	})
	return err
}

func (r *RedisRepository) DeleteZone(ctx context.Context, workspaceID, poolName string) error {
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, common.RedisKeys.ThunderZone(workspaceID, poolName))
		pipe.SRem(ctx, common.RedisKeys.ThunderZoneIndex(workspaceID), poolName)
		return nil
	})
	return err
}

func (r *RedisRepository) ListZones(ctx context.Context, workspaceID string) ([]*ZoneState, error) {
	poolNames, err := r.rdb.SMembers(ctx, common.RedisKeys.ThunderZoneIndex(workspaceID)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(poolNames)
	keys := make([]string, 0, len(poolNames))
	for _, poolName := range poolNames {
		keys = append(keys, common.RedisKeys.ThunderZone(workspaceID, poolName))
	}
	return listJSON[ZoneState](ctx, r.rdb, keys)
}

func (r *RedisRepository) getJSON(ctx context.Context, key string, out any) (bool, error) {
	data, err := r.rdb.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	return true, json.Unmarshal(data, out)
}

func listJSON[T any](ctx context.Context, rdb *common.RedisClient, keys []string) ([]*T, error) {
	if len(keys) == 0 {
		return []*T{}, nil
	}
	values, err := rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	states := make([]*T, 0, len(values))
	for _, value := range values {
		data, ok := jsonBytes(value)
		if !ok {
			continue
		}
		var state T
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		states = append(states, &state)
	}
	return states, nil
}

func jsonBytes(value any) ([]byte, bool) {
	switch v := value.(type) {
	case string:
		return []byte(v), true
	case []byte:
		return v, true
	default:
		return nil, false
	}
}

var _ Repository = (*RedisRepository)(nil)
