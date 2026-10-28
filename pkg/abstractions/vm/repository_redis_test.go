package vm

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func redisRepositoryFixture(t *testing.T) (*miniredis.Miniredis, VMRepository) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	return server, NewVMRedisRepository(rdb)
}

func TestVMRedisRepositoryActivityLeasesAreSharedAndExpire(t *testing.T) {
	server, repo := redisRepositoryFixture(t)
	other := NewVMRedisRepository(repo.(*VMRedisRepository).rdb)
	ctx := context.Background()
	require.NoError(t, repo.RenewActivity(ctx, "vm", "first"))
	require.NoError(t, other.RenewActivity(ctx, "vm", "second"))
	require.NoError(t, repo.ReleaseActivity(ctx, "vm", "first"))
	active, err := other.HasActivity(ctx, "vm")
	require.NoError(t, err)
	require.True(t, active)
	server.FastForward(activityReconnectGrace)
	active, err = repo.HasActivity(ctx, "vm")
	require.NoError(t, err)
	require.False(t, active)
}

func TestVMRedisRepositoryICERegistrationWindowDoesNotSlide(t *testing.T) {
	server, repo := redisRepositoryFixture(t)
	ctx := context.Background()
	count, err := repo.IncrementICERegistrations(ctx, "vm")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	server.FastForward(59 * time.Second)
	count, err = repo.IncrementICERegistrations(ctx, "vm")
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, time.Second, server.TTL("vm:ice-rate:vm"))
	server.FastForward(time.Second)
	count, err = repo.IncrementICERegistrations(ctx, "vm")
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestVMRedisRepositoryICESessionCompatibilityAndCollision(t *testing.T) {
	server, repo := redisRepositoryFixture(t)
	ctx := context.Background()
	const saved = `{"VMID":"vm","ContainerID":"container","TokenHash":"token","Password":"password","WorkspaceID":7,"Generation":2}`
	require.NoError(t, server.Set("vm:ice:fragment", saved))
	server.SetTTL("vm:ice:fragment", time.Minute)
	session, err := repo.GetICESession(ctx, "fragment")
	require.NoError(t, err)
	require.Equal(t, &ICESession{VMID: "vm", ContainerID: "container", TokenHash: "token", Password: "password", WorkspaceID: 7, Generation: 2}, session)
	stored, err := repo.RegisterICESession(ctx, "fragment", *session, 2*time.Minute)
	require.NoError(t, err)
	require.True(t, stored)
	encoded, err := server.Get("vm:ice:fragment")
	require.NoError(t, err)
	require.Equal(t, saved, encoded, "existing gateways compare serialized routes byte for byte")
	require.Equal(t, 2*time.Minute, server.TTL("vm:ice:fragment"))
	changed := *session
	changed.Generation++
	stored, err = repo.RegisterICESession(ctx, "fragment", changed, 3*time.Minute)
	require.NoError(t, err)
	require.False(t, stored)
	require.Equal(t, 2*time.Minute, server.TTL("vm:ice:fragment"))
	loaded, err := repo.GetICESession(ctx, "fragment")
	require.NoError(t, err)
	require.Equal(t, session, loaded)
	server.FastForward(2 * time.Minute)
	loaded, err = repo.GetICESession(ctx, "fragment")
	require.NoError(t, err)
	require.Nil(t, loaded)
	require.NoError(t, server.Set("vm:ice:fragment", "invalid JSON"))
	_, err = repo.GetICESession(ctx, "fragment")
	require.Error(t, err)
}

func TestVMRedisRepositoryICELeasesCountAcrossGatewaysAndNeverResurrect(t *testing.T) {
	server, repo := redisRepositoryFixture(t)
	other := NewVMRedisRepository(repo.(*VMRedisRepository).rdb)
	ctx := context.Background()
	const connections = 16
	counts := make(chan int, connections)
	failures := make(chan error, connections)
	var wg sync.WaitGroup
	for i := range connections {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			count, err := other.AcquireICELease(ctx, "fragment", fmt.Sprint(i), time.Minute)
			counts <- count
			failures <- err
		}(i)
	}
	wg.Wait()
	close(counts)
	close(failures)
	seen := map[int]bool{}
	for err := range failures {
		require.NoError(t, err)
	}
	for count := range counts {
		seen[count] = true
	}
	require.Len(t, seen, connections)
	require.True(t, seen[connections])
	require.NoError(t, repo.ReleaseICELease(ctx, "fragment", "0"))
	require.ErrorContains(t, other.RenewICELease(ctx, "fragment", "0", time.Minute), "expired")
	count, err := repo.AcquireICELease(ctx, "fragment", "new", time.Minute)
	require.NoError(t, err)
	require.Equal(t, connections, count)
	server.FastForward(time.Minute)
	require.ErrorContains(t, repo.RenewICELease(ctx, "fragment", "new", time.Minute), "expired")
	require.False(t, server.Exists("vm:ice-open:fragment"))
}

func TestVMRedisRepositoryRedisFailuresAreNotMissingState(t *testing.T) {
	server, repo := redisRepositoryFixture(t)
	server.SetError("unavailable")
	ctx := context.Background()
	require.Error(t, repo.RenewActivity(ctx, "vm", "lease"))
	_, err := repo.HasActivity(ctx, "vm")
	require.Error(t, err)
	require.Error(t, repo.ReleaseActivity(ctx, "vm", "lease"))
	_, err = repo.IncrementICERegistrations(ctx, "vm")
	require.Error(t, err)
	_, err = repo.RegisterICESession(ctx, "fragment", ICESession{}, time.Minute)
	require.Error(t, err)
	_, err = repo.GetICESession(ctx, "fragment")
	require.Error(t, err)
	_, err = repo.AcquireICELease(ctx, "fragment", "lease", time.Minute)
	require.Error(t, err)
	require.Error(t, repo.RenewICELease(ctx, "fragment", "lease", time.Minute))
	require.Error(t, repo.ReleaseICELease(ctx, "fragment", "lease"))
}
