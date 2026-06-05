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

func newTestReadClient(nTopHosts int, hosts ...*Host) *Client {
	c := &Client{
		ctx:          context.Background(),
		clientConfig: ClientConfig{NTopHosts: nTopHosts},
		hasher:       rendezvous.New[*Host](),
	}
	for _, host := range hosts {
		c.hasher.Add(host)
	}
	return c
}

func TestPrimaryReadHostMatchesReadRankZero(t *testing.T) {
	active1 := &Host{HostId: "host-a", Addr: "10.0.0.1:2049"}
	active2 := &Host{HostId: "host-b", Addr: "10.0.0.2:2049"}

	c := newTestReadClient(3, active1, active2)

	// With all hosts reachable, the primary read host is exactly the read path's
	// rank-0 host, so reconciliation materializes where reads look first.
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%d", i)
		want := c.hasher.GetN(c.readReplicaHostCount(), key)[0]
		got, err := c.PrimaryReadHost(key)
		require.NoError(t, err)
		require.Equal(t, want.HostId, got.HostId)
	}
}

func TestPrimaryReadHostSkipsUnreachableInWindow(t *testing.T) {
	active := &Host{HostId: "host-a", Addr: "10.0.0.1:2049"}
	logicalOnly := &Host{HostId: "host-b"} // no endpoint

	c := newTestReadClient(3, active, logicalOnly)

	// The primary read host is always reachable, mirroring how reads skip
	// unavailable hosts and fall back in rank order within the window.
	for i := 0; i < 50; i++ {
		got, err := c.PrimaryReadHost(fmt.Sprintf("key-%d", i))
		require.NoError(t, err)
		require.True(t, got.HasEndpoint())
		require.Equal(t, "host-a", got.HostId)
	}
}

func TestPrimaryReadHostRespectsReadWindow(t *testing.T) {
	active := &Host{HostId: "host-a", Addr: "10.0.0.1:2049"}
	logicalOnly := &Host{HostId: "host-b"} // no endpoint

	// With a window of 1, only the single highest-ranked host is considered, so
	// for keys where the unreachable host ranks first there is no in-window
	// target. Reconciliation must not place content outside the read window.
	c := newTestReadClient(1, active, logicalOnly)

	sawError := false
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%d", i)
		host, err := c.PrimaryReadHost(key)
		if err != nil {
			sawError = true
			continue
		}
		// When a host is returned it is the in-window rank-0 host and reachable.
		require.True(t, host.HasEndpoint())
		require.Equal(t, c.hasher.GetN(1, key)[0].HostId, host.HostId)
	}
	require.True(t, sawError, "expected at least one key whose in-window rank-0 host is unreachable")
}

func TestPrimaryReadHostErrorsWithoutReachableHosts(t *testing.T) {
	c := newTestReadClient(3, &Host{HostId: "host-c"}) // no endpoint

	_, err := c.PrimaryReadHost("key")
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

func TestRecentStubsAnyLocalityDedupesByNewestSeen(t *testing.T) {
	ctx := context.Background()
	m := newTestMetadata(t)

	require.NoError(t, m.AddRecentStub(ctx, "locality-a", "ws", "stub-a", time.Hour))
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, m.AddRecentStub(ctx, "locality-b", "ws", "stub-a", time.Hour))
	require.NoError(t, m.AddRecentStub(ctx, "locality-b", "ws", "stub-b", time.Hour))

	indexMembers, err := m.rdb.ZRange(ctx, MetadataKeys.MetadataReconcileRecentAll(), 0, -1).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		RecentStubKey("ws", "stub-a"),
		RecentStubKey("ws", "stub-b"),
	}, indexMembers)

	stubs, err := m.ListRecentStubsAnyLocality(ctx, time.Hour)
	require.NoError(t, err)

	require.Len(t, stubs, 2)
	byKey := map[string]RecentStub{}
	for _, stub := range stubs {
		byKey[RecentStubKey(stub.WorkspaceID, stub.StubID)] = stub
	}
	require.Contains(t, byKey, RecentStubKey("ws", "stub-a"))
	require.Contains(t, byKey, RecentStubKey("ws", "stub-b"))
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
