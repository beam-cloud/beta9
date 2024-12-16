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
	tests := []struct {
		name     string
		src      any
		dst      any
		expected any
	}{
		{
			name: "copy struct basic",
			src: &types.RedisConfig{
				ClientName:      "hello",
				PoolSize:        10,
				ConnMaxLifetime: time.Second,
			},
			dst: &types.RedisConfig{},
			expected: &types.RedisConfig{
				ClientName:      "hello",
				PoolSize:        10,
				ConnMaxLifetime: time.Second,
			},
		},
		{
			name: "copy struct overwriting fields on destination on redisconfig struct",
			src: &types.RedisConfig{
				ClientName:      "hello",
				PoolSize:        10,
				ConnMaxLifetime: time.Second,
			},
			dst: &types.RedisConfig{
				ClientName: "world",
				PoolSize:   20,
			},
			expected: &types.RedisConfig{
				ClientName:      "hello",
				PoolSize:        10,
				ConnMaxLifetime: time.Second,
			},
		},
		{
			name: "copy struct overwriting fields on destination on worker struct",
			src: &types.Worker{
				Id:                   "123",
				Status:               types.WorkerStatusAvailable,
				TotalCpu:             10,
				TotalMemory:          1000,
				TotalGpuCount:        4,
				FreeCpu:              1,
				FreeMemory:           500,
				FreeGpuCount:         2,
				Gpu:                  "A10G",
				PoolName:             "pool1",
				MachineId:            "machine1",
				ResourceVersion:      1,
				RequiresPoolSelector: true,
				Priority:             -1,
				BuildVersion:         "0.1.0",
			},
			dst: &types.Worker{
				Id:                   "456",
				Status:               types.WorkerStatusDisabled,
				TotalCpu:             100000,
				TotalMemory:          100000,
				TotalGpuCount:        100000,
				FreeCpu:              100000,
				FreeMemory:           100000,
				FreeGpuCount:         100000,
				Gpu:                  "to be overwritten",
				PoolName:             "to be overwritten",
				MachineId:            "to be overwritten",
				ResourceVersion:      50000,
				RequiresPoolSelector: false,
				Priority:             -99,
				BuildVersion:         "dev",
			},
			expected: &types.Worker{
				Id:                   "123",
				Status:               types.WorkerStatusAvailable,
				TotalCpu:             10,
				TotalMemory:          1000,
				TotalGpuCount:        4,
				FreeCpu:              1,
				FreeMemory:           500,
				FreeGpuCount:         2,
				Gpu:                  "A10G",
				PoolName:             "pool1",
				MachineId:            "machine1",
				ResourceVersion:      1,
				RequiresPoolSelector: true,
				Priority:             -1,
				BuildVersion:         "0.1.0",
			},
		},
		{
			name: "copy struct overwriting fields on destination on providermachinestate struct",
			src: &types.ProviderMachineState{
				MachineId:         "123",
				PoolName:          "pool1",
				Status:            types.MachineStatusRegistered,
				HostName:          "host1",
				Token:             "token1",
				Cpu:               10,
				Memory:            1000,
				Gpu:               "A10G",
				GpuCount:          4,
				RegistrationToken: "regtoken1",
				Created:           "2037-01-01",
				LastWorkerSeen:    "2037-01-01",
				LastKeepalive:     "",
				AutoConsolidate:   true,
				AgentVersion:      "0.1.0",
			},
			dst: &types.ProviderMachineState{
				MachineId:         "456",
				PoolName:          "to be overwritten",
				Status:            types.MachineStatusPending,
				HostName:          "to be overwritten",
				Token:             "to be overwritten",
				Cpu:               100000,
				Memory:            100000,
				Gpu:               "to be overwritten",
				GpuCount:          100000,
				RegistrationToken: "to be overwritten",
				Created:           "to be overwritten",
				LastWorkerSeen:    "to be overwritten",
				LastKeepalive:     "to be overwritten",
				AutoConsolidate:   false,
				AgentVersion:      "to be overwritten",
			},
			expected: &types.ProviderMachineState{
				MachineId:         "123",
				PoolName:          "pool1",
				Status:            types.MachineStatusRegistered,
				HostName:          "host1",
				Token:             "token1",
				Cpu:               10,
				Memory:            1000,
				Gpu:               "A10G",
				GpuCount:          4,
				RegistrationToken: "regtoken1",
				Created:           "2037-01-01",
				LastWorkerSeen:    "2037-01-01",
				LastKeepalive:     "",
				AutoConsolidate:   true,
				AgentVersion:      "0.1.0",
			},
		},
		{
			name: "copy struct validate default values",
			src: &types.Worker{
				Id:       "123",
				Status:   types.WorkerStatusAvailable,
				TotalCpu: 10,
			},
			dst: &types.Worker{},
			expected: &types.Worker{
				Id:                   "123",
				Status:               types.WorkerStatusAvailable,
				TotalCpu:             10,
				TotalMemory:          0,
				TotalGpuCount:        0,
				FreeCpu:              0,
				FreeMemory:           0,
				FreeGpuCount:         0,
				Gpu:                  "",
				PoolName:             "",
				MachineId:            "",
				ResourceVersion:      0,
				RequiresPoolSelector: false,
				Priority:             0,
				BuildVersion:         "",
			},
		},
		{
			name: "copy struct validate nested struct, nil values, and slices",
			src: &types.ContainerRequest{
				ContainerId: "123",
				Env: []string{
					"key1=val1",
				},
				Workspace: types.Workspace{
					Id: 5,
				},
			},
			dst: &types.ContainerRequest{},
			expected: &types.ContainerRequest{
				ContainerId: "123",
				Env: []string{
					"key1=val1",
				},
				Workspace: types.Workspace{
					Id: 5,
				},
				BuildOptions: types.BuildOptions{
					SourceImage: nil,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			CopyStruct(test.src, test.dst)

			assert.Equal(t, test.expected, test.dst)
		})
	}
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
