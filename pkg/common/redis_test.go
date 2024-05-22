package common

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func NewRedisClientForTest() (*RedisClient, error) {
	s, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	return NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
}

func TestRedisLock(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	lock := NewRedisLock(rdb)

	key := "test_key"
	opts := RedisLockOptions{TtlS: 10, Retries: 2}

	// Test acquiring lock
	err = lock.Acquire(context.Background(), key, opts)
	assert.NoError(t, err)

	// Test acquiring lock again without releasing
	err = lock.Acquire(context.Background(), key, opts)
	assert.Error(t, err)
	assert.Equal(t, "redislock: not obtained", err.Error())

	// Test releasing lock
	err = lock.Release(key)
	assert.NoError(t, err)

	// Test acquiring lock after releasing
	err = lock.Acquire(context.Background(), key, RedisLockOptions{TtlS: 2, Retries: 0})
	assert.NoError(t, err)
}

func TestRedisLockWithTTLAndRetry(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	firstLock := NewRedisLock(rdb)
	secondLock := NewRedisLock(rdb)

	key := "test_key"
	err = firstLock.Acquire(context.Background(), key, RedisLockOptions{TtlS: 1, Retries: 0})
	assert.NoError(t, err)

	// Create a channel to pass the result
	resultCh := make(chan error)

	// Start a new goroutine that will attempt to acquire the lock
	// This will initially fail since the lock is still held by 'firstLock'
	// After 500mS, we release it, so the secondLock should be able to acquire it
	// NOTE: I wanted to test this using a TTL, but unfortunately miniredis doesn't support true TTL like redis
	// TTL'd keys technically still exist. Instead we're relying on manually releasing the lock (which deletes the key)
	go func() {
		err := secondLock.Acquire(context.Background(), key, RedisLockOptions{TtlS: 10, Retries: 5})
		resultCh <- err
	}()

	time.Sleep(time.Millisecond * 500)

	// Release lock so the secondLock can acquire it
	err = firstLock.Release(key)
	assert.NoError(t, err)

	// Get the result from the channel and check it
	err = <-resultCh
	assert.NoError(t, err)
}

func TestCopyStruct(t *testing.T) {
	options1 := &types.RedisConfig{ClientName: "hello", PoolSize: 10, ConnMaxLifetime: time.Second}
	options2 := &types.RedisConfig{}

	CopyStruct(options1, options2)

	assert.Equal(t, options1, options2)
}

func TestToSlice(t *testing.T) {
	type Test struct {
		Int32    int32   `redis:"int32"`
		Int64    int64   `redis:"int64"`
		Float32  float32 `redis:"float32"`
		Float64  float64 `redis:"float64"`
		Bool     bool    `redis:"bool"`
		String   string  `redis:"string"`
		Ingored  string  `redis:"-"`
		Ignored2 string
		Duration time.Duration `redis:"duration"`
	}

	expect := []interface{}{"int32", "2147483647", "int64", "9223372036854775807", "float32", "343.2", "float64", "3.141592653589793", "bool", "true", "string", "hello, world", "duration", "2m0s"}
	actual := ToSlice(&Test{Int32: math.MaxInt32, Int64: math.MaxInt64, Float32: 343.2, Float64: 3.141592653589793, Bool: true, String: "hello, world", Duration: 2 * time.Minute})
	assert.Equal(t, expect, actual)
}

func TestToStruct(t *testing.T) {
	type Test struct {
		Int32    int32   `redis:"int32"`
		Int64    int64   `redis:"int64"`
		Float32  float32 `redis:"float32"`
		Float64  float64 `redis:"float64"`
		Bool     bool    `redis:"bool"`
		String   string  `redis:"string"`
		Ingored  string  `redis:"-"`
		Ignored2 string
		Duration time.Duration `redis:"duration"`
	}

	// Keys map to redis tags e.g. redis:"id" == "id"
	userMap := map[string]string{
		"int32":    "2147483647",
		"int64":    "9223372036854775807",
		"float32":  "343.2",
		"float64":  "3.141592653589793",
		"bool":     "true",
		"string":   "hello, world",
		"duration": "2m0s",
	}
	actual := &Test{}

	// Copy userMap values into userStruct, but only those with redis tags
	err := ToStruct(userMap, actual)
	assert.Nil(t, err)

	expect := &Test{Int32: math.MaxInt32, Int64: math.MaxInt64, Float32: 343.2, Float64: 3.141592653589793, Bool: true, String: "hello, world", Duration: 2 * time.Minute}
	assert.Equal(t, expect, actual)
}

func TestRedisClientSetAndGet(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)
	defer rdb.Close()

	ctx := context.Background()
	key := uuid.New().String()
	val := uuid.New().String()

	err = rdb.Set(ctx, key, val, 0).Err()
	assert.NoError(t, err)

	res, err := rdb.Get(ctx, key).Result()
	assert.NoError(t, err)
	assert.Equal(t, val, res)
}

func TestRedisClientScan(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)
	defer rdb.Close()

	ctx := context.Background()

	// Generate 1000 items, add to map var, add to redis
	keys := map[string]string{}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprint(i)
		keys[key] = uuid.New().String()
		err := rdb.Set(ctx, key, keys[key], 0).Err()
		assert.NoError(t, err)
	}

	// Make sure 1000 items were added to redis
	items, err := rdb.Scan(ctx, "*")
	assert.NoError(t, err)
	assert.Len(t, items, 1000)

	// Scan/search for 100 items e.g. 800 - 899
	items, err = rdb.Scan(ctx, "8??")
	assert.NoError(t, err)
	assert.Len(t, items, 100)

	// Get values for all items, compare their values to map
	for _, item := range items {
		expect := keys[item]
		actual, err := rdb.Get(ctx, item).Result()
		assert.NoError(t, err)
		assert.Equal(t, expect, actual)
	}
}

// TODO: Need real redis service for pubsub testing
// func TestRedisClientSubscribe(t *testing.T) {}
