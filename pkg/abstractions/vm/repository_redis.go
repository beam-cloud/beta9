package vm

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/redis/go-redis/v9"
)

const (
	activityReconnectGrace = 150 * time.Second
	activityKeyPrefix      = "vm:activity:"
	iceRegistrationPrefix  = "vm:ice-rate:"
	iceSessionPrefix       = "vm:ice:"
	iceLeasePrefix         = "vm:ice-open:"
)

type VMRedisRepository struct {
	rdb *common.RedisClient
}

func NewVMRedisRepository(rdb *common.RedisClient) VMRepository {
	if rdb == nil {
		return nil
	}
	return &VMRedisRepository{rdb: rdb}
}

func (r *VMRedisRepository) RenewActivity(ctx context.Context, id, lease string) error {
	key := activityKeyPrefix + id
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(time.Now().UnixMilli(), 10))
		pipe.ZAdd(ctx, key, redis.Z{Member: lease, Score: float64(time.Now().Add(activityReconnectGrace).UnixMilli())})
		pipe.Expire(ctx, key, activityReconnectGrace)
		return nil
	})
	return err
}

func (r *VMRedisRepository) HasActivity(ctx context.Context, id string) (bool, error) {
	count, err := r.rdb.ZCount(ctx, activityKeyPrefix+id, strconv.FormatInt(time.Now().UnixMilli(), 10), "+inf").Result()
	return count > 0, err
}

func (r *VMRedisRepository) ReleaseActivity(ctx context.Context, id, lease string) error {
	return r.rdb.ZRem(ctx, activityKeyPrefix+id, lease).Err()
}

func (r *VMRedisRepository) IncrementICERegistrations(ctx context.Context, id string) (int, error) {
	return r.rdb.Eval(ctx, `
		local count = redis.call('INCR', KEYS[1])
		if count == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
		return count
	`, []string{iceRegistrationPrefix + id}, time.Minute.Milliseconds()).Int()
}

func (r *VMRedisRepository) RegisterICESession(ctx context.Context, fragment string, session ICESession, ttl time.Duration) (bool, error) {
	encoded, err := json.Marshal(session)
	if err != nil {
		return false, err
	}
	// Renew identical routes without replacing colliding sessions.
	stored, err := r.rdb.Eval(ctx, `
		local old = redis.call('GET', KEYS[1])
		if old and old ~= ARGV[1] then return 0 end
		redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
		return 1
	`, []string{iceSessionPrefix + fragment}, string(encoded), ttl.Milliseconds()).Int()
	return stored == 1, err
}

func (r *VMRedisRepository) GetICESession(ctx context.Context, fragment string) (*ICESession, error) {
	encoded, err := r.rdb.Get(ctx, iceSessionPrefix+fragment).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var session ICESession
	if err := json.Unmarshal(encoded, &session); err != nil {
		return nil, err
	}
	return &session, nil
}

func (r *VMRedisRepository) AcquireICELease(ctx context.Context, fragment, lease string, ttl time.Duration) (int, error) {
	now := time.Now().UnixMilli()
	return r.rdb.Eval(ctx, `
		redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
		redis.call('ZADD', KEYS[1], ARGV[2], ARGV[3])
		redis.call('PEXPIRE', KEYS[1], ARGV[4])
		return redis.call('ZCARD', KEYS[1])
	`, []string{iceLeasePrefix + fragment}, now, now+ttl.Milliseconds(), lease, ttl.Milliseconds()).Int()
}

func (r *VMRedisRepository) RenewICELease(ctx context.Context, fragment, lease string, ttl time.Duration) error {
	renewed, err := r.rdb.Eval(ctx, `
		if not redis.call('ZSCORE', KEYS[1], ARGV[2]) then return 0 end
		redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
		redis.call('PEXPIRE', KEYS[1], ARGV[3])
		return 1
	`, []string{iceLeasePrefix + fragment}, time.Now().UnixMilli()+ttl.Milliseconds(), lease, ttl.Milliseconds()).Int()
	if err == nil && renewed != 1 {
		return errors.New("ICE lease expired")
	}
	return err
}

func (r *VMRedisRepository) ReleaseICELease(ctx context.Context, fragment, lease string) error {
	return r.rdb.ZRem(ctx, iceLeasePrefix+fragment, lease).Err()
}
