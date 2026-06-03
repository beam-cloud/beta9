package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	rendezvous "github.com/beam-cloud/rendezvous"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSelectedActiveHostExcludesLogicalOnly(t *testing.T) {
	active1 := &Host{HostId: "host-a", Addr: "10.0.0.1:2049"}
	active2 := &Host{HostId: "host-b", PrivateAddr: "10.0.0.2:2049"}
	logicalOnly := &Host{HostId: "host-c"} // no endpoint

	c := &Client{
		ctx:      context.Background(),
		locality: "default",
		hostDirectory: testHostDirectoryFunc(func(context.Context, string) ([]*Host, error) {
			return []*Host{active1, active2, logicalOnly}, nil
		}),
	}

	for i := 0; i < 50; i++ {
		host, err := c.SelectedActiveHost(fmt.Sprintf("key-%d", i))
		require.NoError(t, err)
		require.True(t, host.HasEndpoint())
		require.NotEqual(t, "host-c", host.HostId)
	}
}

func TestSelectedActiveHostMatchesRendezvousOverActiveSet(t *testing.T) {
	active1 := &Host{HostId: "host-a", Addr: "10.0.0.1:2049"}
	active2 := &Host{HostId: "host-b", Addr: "10.0.0.2:2049"}

	c := &Client{
		ctx: context.Background(),
		hostDirectory: testHostDirectoryFunc(func(context.Context, string) ([]*Host, error) {
			return []*Host{active1, active2}, nil
		}),
	}

	hasher := rendezvous.New[*Host]()
	hasher.Add(active1)
	hasher.Add(active2)

	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("k-%d", i)
		want, _ := hasher.Get(key)
		got, err := c.SelectedActiveHost(key)
		require.NoError(t, err)
		require.Equal(t, want.HostId, got.HostId)
	}
}

func TestSelectedActiveHostErrorsWithoutActiveHosts(t *testing.T) {
	c := &Client{
		ctx: context.Background(),
		hostDirectory: testHostDirectoryFunc(func(context.Context, string) ([]*Host, error) {
			return []*Host{{HostId: "host-c"}}, nil // no endpoint
		}),
	}

	_, err := c.SelectedActiveHost("key")
	require.Error(t, err)
}

func newTestMetadata(t *testing.T) *Metadata {
	t.Helper()
	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewMetadataWithRedisClient(client)
}

func TestRecentStubsListNewestFirst(t *testing.T) {
	ctx := context.Background()
	m := newTestMetadata(t)

	require.NoError(t, m.AddRecentStub(ctx, "default", "ws", "stub-a", time.Hour))
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, m.AddRecentStub(ctx, "default", "ws", "stub-b", time.Hour))

	stubs, err := m.ListRecentStubs(ctx, "default", time.Hour, 10)
	require.NoError(t, err)
	require.Len(t, stubs, 2)
	require.Equal(t, "stub-b", stubs[0].StubID)
	require.Equal(t, "stub-a", stubs[1].StubID)
	require.Equal(t, "ws", stubs[0].WorkspaceID)
}

func TestRecentStubsExcludesExpiredByTTL(t *testing.T) {
	ctx := context.Background()
	m := newTestMetadata(t)

	require.NoError(t, m.AddRecentStub(ctx, "default", "ws", "stub-a", time.Hour))
	time.Sleep(3 * time.Millisecond)

	// A 1ns TTL window excludes the entry recorded a few ms ago.
	stubs, err := m.ListRecentStubs(ctx, "default", time.Nanosecond, 10)
	require.NoError(t, err)
	require.Empty(t, stubs)
}

func TestReconcileStatusRoundTrip(t *testing.T) {
	ctx := context.Background()
	m := newTestMetadata(t)

	status, err := m.GetReconcileStatus(ctx, "default", "stub", "hash", "rk")
	require.NoError(t, err)
	require.Empty(t, status)

	require.NoError(t, m.SetReconcileStatus(ctx, "default", "stub", "hash", "rk", "materialized", time.Hour))
	status, err = m.GetReconcileStatus(ctx, "default", "stub", "hash", "rk")
	require.NoError(t, err)
	require.Equal(t, "materialized", status)
}

func TestMarkStubReportedClaimsOnce(t *testing.T) {
	ctx := context.Background()
	m := newTestMetadata(t)

	claimed, err := m.MarkStubReported(ctx, "default", "stub", time.Hour)
	require.NoError(t, err)
	require.True(t, claimed)

	claimed, err = m.MarkStubReported(ctx, "default", "stub", time.Hour)
	require.NoError(t, err)
	require.False(t, claimed)

	// A different stub can still be claimed.
	claimed, err = m.MarkStubReported(ctx, "default", "other", time.Hour)
	require.NoError(t, err)
	require.True(t, claimed)
}

func TestReconcileLockIsExclusive(t *testing.T) {
	ctx := context.Background()
	m := newTestMetadata(t)

	acquired, err := m.AcquireReconcileLock(ctx, "default", "host-a", "hash", 30)
	require.NoError(t, err)
	require.True(t, acquired)

	// Second acquire for the same key should not be granted while held.
	acquired, err = m.AcquireReconcileLock(ctx, "default", "host-a", "hash", 30)
	require.NoError(t, err)
	require.False(t, acquired)

	require.NoError(t, m.ReleaseReconcileLock("default", "host-a", "hash"))

	// After release it can be acquired again.
	acquired, err = m.AcquireReconcileLock(ctx, "default", "host-a", "hash", 30)
	require.NoError(t, err)
	require.True(t, acquired)
}
