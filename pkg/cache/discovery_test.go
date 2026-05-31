package cache

import (
	"context"
	"errors"
	"testing"

	proto "github.com/beam-cloud/beta9/proto"
	rendezvous "github.com/beam-cloud/rendezvous"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type testHostDirectoryFunc func(context.Context, string) ([]*Host, error)

func (f testHostDirectoryFunc) GetAvailableHosts(ctx context.Context, locality string) ([]*Host, error) {
	return f(ctx, locality)
}

func TestCacheHostCandidateGroupsPreserveLogicalHostAndCandidateOrder(t *testing.T) {
	hosts := []*Host{
		{HostId: "host-a", PrivateAddr: "10.0.0.1:2049"},
		{HostId: "host-b", PrivateAddr: "10.0.0.2:2049"},
		{HostId: "host-a", PrivateAddr: "10.0.0.3:2049"},
		nil,
		{PrivateAddr: "10.0.0.4:2049"},
		{HostId: "host-b", PrivateAddr: "10.0.0.5:2049"},
	}

	groups := cacheHostCandidateGroups(hosts)

	require.Len(t, groups, 2)
	require.Equal(t, "host-a", groups[0].hostID)
	require.Equal(t, "10.0.0.1:2049", groups[0].candidates[0].PrivateAddr)
	require.Equal(t, "10.0.0.3:2049", groups[0].candidates[1].PrivateAddr)
	require.Equal(t, "host-b", groups[1].hostID)
	require.Equal(t, "10.0.0.2:2049", groups[1].candidates[0].PrivateAddr)
	require.Equal(t, "10.0.0.5:2049", groups[1].candidates[1].PrivateAddr)
}

func TestCacheHostCandidateGroupHasEndpoint(t *testing.T) {
	group := cacheHostCandidateGroup{
		hostID: "logical-host",
		candidates: []*Host{
			{HostId: "logical-host", Addr: "public-a:2049", PrivateAddr: "10.0.0.1:2049"},
			{HostId: "logical-host", Addr: "public-b:2049", PrivateAddr: "10.0.0.2:2049"},
		},
	}

	require.True(t, group.hasEndpoint(&Host{
		HostId:      "logical-host",
		Addr:        "public-a:2049",
		PrivateAddr: "10.0.0.1:2049",
	}))
	require.True(t, group.hasEndpoint(&Host{
		HostId:      "logical-host",
		Addr:        "public-b:2049",
		PrivateAddr: "10.0.0.2:2049",
	}))
	require.False(t, group.hasEndpoint(&Host{
		HostId:      "logical-host",
		Addr:        "public-c:2049",
		PrivateAddr: "10.0.0.3:2049",
	}))
}

func TestCacheHostCandidateGroupFirstReachableFallsBackInOrder(t *testing.T) {
	group := cacheHostCandidateGroup{
		hostID: "logical-host",
		candidates: []*Host{
			{HostId: "logical-host", PrivateAddr: "10.0.0.1:2049"},
			{HostId: "logical-host", PrivateAddr: "10.0.0.2:2049"},
		},
	}

	called := make([]string, 0, 2)
	host, ok := group.firstReachable(nil, func(_ context.Context, candidate *Host) (*Host, error) {
		called = append(called, candidate.PrivateAddr)
		if candidate.PrivateAddr == "10.0.0.1:2049" {
			return nil, errors.New("unreachable")
		}
		return candidate, nil
	})

	require.True(t, ok)
	require.Equal(t, "10.0.0.2:2049", host.PrivateAddr)
	require.Equal(t, []string{"10.0.0.1:2049", "10.0.0.2:2049"}, called)
}

func TestCacheHostCandidateGroupLogicalHostIgnoresEndpointFields(t *testing.T) {
	group := cacheHostCandidateGroup{
		hostID: "logical-host",
		candidates: []*Host{
			{
				HostId:         "logical-host",
				RegistrationID: "worker-a",
				PoolName:       "default",
				Locality:       "default",
				NodeID:         "node-a",
				CachePathID:    "path",
				PrivateAddr:    "10.0.0.1:2049",
			},
		},
	}

	host := group.logicalHost()

	require.NotNil(t, host)
	require.Equal(t, "logical-host", host.HostId)
	require.Equal(t, "node-a", host.NodeID)
	require.Equal(t, "path", host.CachePathID)
	require.Empty(t, host.RegistrationID)
	require.Empty(t, host.PrivateAddr)
	require.False(t, host.HasEndpoint())
}

func TestDiscoveryKeepsKnownRegisteredEndpoint(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        4,
			ObjectTtlS:           300,
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}
	server, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("logical-host"))
	require.NoError(t, err)
	addr, err := server.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	knownStale := &Host{
		HostId:      "logical-host",
		Addr:        "public-old:2049",
		PrivateAddr: "10.0.0.1:2049",
	}
	active := &Host{
		HostId:      "logical-host",
		Addr:        addr,
		PrivateAddr: addr,
	}
	stale := &Host{
		HostId:      "logical-host",
		Addr:        "public-old:2049",
		PrivateAddr: "10.0.0.1:2049",
	}

	hostMap := NewHostMap(GlobalConfig{}, nil)
	hostMap.Set(knownStale)
	discovery := &DiscoveryClient{
		cfg:     cfg.Global,
		hostMap: hostMap,
		hostDirectory: testHostDirectoryFunc(func(context.Context, string) ([]*Host, error) {
			return []*Host{active, stale}, nil
		}),
	}

	hosts, err := discovery.discoverHosts(context.Background())

	require.NoError(t, err)
	require.Empty(t, hosts)
}

func TestRefreshRoutableHostsReactivatesLogicalOnlyHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        4,
			ObjectTtlS:           300,
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}
	server, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("logical-host"))
	require.NoError(t, err)
	addr, err := server.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	active := &Host{
		HostId:      "logical-host",
		PrivateAddr: addr,
	}
	client := &Client{
		ctx:            ctx,
		clientConfig:   ClientConfig{NTopHosts: 1},
		globalConfig:   cfg.Global,
		grpcClients:    make(map[string]proto.CacheClient),
		grpcConns:      make(map[string]*grpc.ClientConn),
		rawReadPools:   make(map[string]*rawReadConnPool),
		localHostCache: make(map[string]*localClientCache),
		hasher:         rendezvous.New[*Host](),
		hostMap:        NewHostMap(cfg.Global, nil),
		hostDirectory: testHostDirectoryFunc(func(context.Context, string) ([]*Host, error) {
			return []*Host{active}, nil
		}),
		maxGetContentAttempts: 1,
	}
	client.hostMap.onHostAdded = client.addHost
	defer client.Cleanup()

	client.hostMap.Set(active.LogicalOnly())
	require.False(t, client.hostMap.Get("logical-host").HasEndpoint())

	require.NoError(t, client.refreshRoutableHosts(ctx))

	refreshed := client.hostMap.Get("logical-host")
	require.NotNil(t, refreshed)
	require.True(t, refreshed.HasEndpoint())
	require.Equal(t, addr, refreshed.PrivateAddr)
	require.True(t, client.hasCacheClient("logical-host"))
}

func TestRefreshRoutableHostsRemovesUndiscoveredLogicalHost(t *testing.T) {
	ctx := context.Background()
	oldHost := (&Host{
		HostId:      "cache-host-default-node-old-path",
		Locality:    "default",
		NodeID:      "node-old",
		CachePathID: "path",
	}).LogicalOnly()
	stillHost := (&Host{
		HostId:      "cache-host-default-node-live-path",
		Locality:    "default",
		NodeID:      "node-live",
		CachePathID: "path",
	}).LogicalOnly()
	client := &Client{
		ctx:            ctx,
		clientConfig:   ClientConfig{NTopHosts: 1},
		grpcClients:    make(map[string]proto.CacheClient),
		grpcConns:      make(map[string]*grpc.ClientConn),
		rawReadPools:   make(map[string]*rawReadConnPool),
		localHostCache: make(map[string]*localClientCache),
		hasher:         &orderedTestHasher{},
		hostMap:        NewHostMap(GlobalConfig{}, nil),
		hostDirectory: testHostDirectoryFunc(func(context.Context, string) ([]*Host, error) {
			return []*Host{stillHost}, nil
		}),
		maxGetContentAttempts: 1,
	}
	client.hostMap.onHostAdded = client.addHost
	client.hostMap.Set(oldHost)
	client.hostMap.Set(stillHost)

	selected, err := client.getHostForRequest(&ClientRequest{
		rt:        ClientRequestTypeRetrieval,
		hash:      "hash",
		key:       "hash",
		hostIndex: 0,
	})
	require.NoError(t, err)
	require.Equal(t, oldHost.HostId, selected.HostId)

	require.NoError(t, client.refreshRoutableHosts(ctx))
	client.removeLocalHostCache("hash")

	require.Nil(t, client.hostMap.Get(oldHost.HostId))
	selected, err = client.getHostForRequest(&ClientRequest{
		rt:        ClientRequestTypeRetrieval,
		hash:      "hash",
		key:       "hash",
		hostIndex: 0,
	})
	require.NoError(t, err)
	require.Equal(t, stillHost.HostId, selected.HostId)
}
