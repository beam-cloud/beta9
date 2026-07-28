package common

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestKeyEventManagerParsesExplicitPubSubMessage(t *testing.T) {
	kem := &KeyEventManager{}
	event := kem.messageToKeyEvent("scheduler:backend_route_machine_rev:", "scheduler:backend_route_machine_rev:workspace:pool:machine", KeyOperationSet)

	if event.Key != "workspace:pool:machine" {
		t.Fatalf("key = %q, want workspace:pool:machine", event.Key)
	}
	if event.Operation != KeyOperationSet {
		t.Fatalf("operation = %q, want %q", event.Operation, KeyOperationSet)
	}
}

func TestKeyEventManagerListensForExistingKey(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	const key = "scheduler:container:exit-code:test"
	require.NoError(t, rdb.Set(context.Background(), key, 0, time.Minute).Err())

	events := make(chan KeyEvent, 1)
	manager := NewKeyEventManager(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, manager.ListenForKey(ctx, key, events))

	select {
	case event := <-events:
		require.Equal(t, KeyEvent{Operation: KeyOperationSet}, event)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for existing key event")
	}
}

func TestKeyEventManagerParsesKeyspaceMessage(t *testing.T) {
	kem := &KeyEventManager{}
	event := kem.messageToKeyEvent("scheduler:backend_route_machine_rev:", "__keyspace@0__:scheduler:backend_route_machine_rev:workspace:pool:machine", "incr")

	if event.Key != "workspace:pool:machine" {
		t.Fatalf("key = %q, want workspace:pool:machine", event.Key)
	}
	if event.Operation != "incr" {
		t.Fatalf("operation = %q, want incr", event.Operation)
	}
}

func TestKeyEventManagerReplaysIndexedContainerState(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	active := RedisKeys.SchedulerContainerState("pod-active")
	expired := RedisKeys.SchedulerContainerState("pod-expired")
	now := time.Now().Unix()
	require.NoError(t, rdb.ZAdd(context.Background(), RedisKeys.SchedulerContainerStateIndex(),
		redis.Z{Score: float64(now + 60), Member: active},
		redis.Z{Score: float64(now - 60), Member: expired},
	).Err())

	events := make(chan KeyEvent, 1)
	manager := NewKeyEventManager(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, manager.ListenForContainerPattern(ctx, "pod-", events))
	require.Equal(t, KeyEvent{Key: "active", Operation: KeyOperationSet}, <-events)
	require.Equal(t, float64(0), rdb.ZScore(context.Background(), RedisKeys.SchedulerContainerStateIndex(), expired).Val())
}
