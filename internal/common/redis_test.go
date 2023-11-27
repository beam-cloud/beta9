package common

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func NewRedisClientForTest() (*RedisClient, error) {
	s, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	return NewRedisClient(WithAddress(s.Addr()))
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
	assert.Equal(t, "redislock: lock not released", err.Error())

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

func TestWithClientName(t *testing.T) {
	opts1 := &RedisOptions{}
	WithClientName("beam")(opts1)
	assert.Equal(t, "beam", opts1.ClientName)

	opts2 := &RedisOptions{}
	WithClientName("My ^ App $")(opts2)
	assert.Equal(t, "MyApp", opts2.ClientName)

	opts3 := &RedisOptions{}
	WithClientName("  g /.\\ []o]  o  ! @   #d---n$% ^&a*()m-_=e+ ")(opts3)
	assert.Equal(t, "goodname", opts3.ClientName)
}

func TestWithAddress(t *testing.T) {
	addr := "redis.beam.cloud:3452"

	opts1 := &RedisOptions{}
	WithAddress(addr)(opts1)
	assert.Equal(t, addr, opts1.Addr)
	assert.Equal(t, []string{addr}, opts1.Addrs())

	addr = "redis1.beam.cloud:3452,redis2.beam.cloud:3452"
	opts2 := &RedisOptions{}
	WithAddress(addr)(opts2)
	assert.Equal(t, addr, opts2.Addr)
	assert.Equal(t, strings.Split(addr, ","), opts2.Addrs())
}

func TestCopyStruct(t *testing.T) {
	options1 := &RedisOptions{ClientName: "hello", PoolSize: 10, ConnMaxLifetime: time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13}}
	options2 := &RedisOptions{}

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
