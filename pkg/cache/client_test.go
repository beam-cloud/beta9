package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	proto "github.com/beam-cloud/beta9/proto"
	"github.com/beam-cloud/rendezvous"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type orderedTestHasher struct {
	hosts []*Host
}

func (h *orderedTestHasher) Add(hosts ...*Host) {
	h.hosts = append(h.hosts, hosts...)
}

func (h *orderedTestHasher) Remove(host *Host) {
	filtered := h.hosts[:0]
	for _, existing := range h.hosts {
		if existing == nil || host == nil || existing.HostId != host.HostId {
			filtered = append(filtered, existing)
		}
	}
	h.hosts = filtered
}

func (h *orderedTestHasher) GetN(n int, key string) []*Host {
	if n > len(h.hosts) {
		n = len(h.hosts)
	}
	return h.hosts[:n]
}

func TestReadContentIntoUsesAttachedLocalServerOnlyForSelectedHost(t *testing.T) {
	store := newTestStore(t, 5)
	content := []byte("local-cache-content")
	hash, _, err := store.AddReader(context.Background(), bytes.NewReader(content))
	require.NoError(t, err)

	localHost := &Host{HostId: "local-host"}
	store.currentHost = localHost
	client := &Client{
		ctx:                   context.Background(),
		clientConfig:          ClientConfig{NTopHosts: 1},
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{{HostId: "remote-host"}}},
		maxGetContentAttempts: 1,
	}
	client.AttachLocalServer(&Server{cas: store})

	dst := make([]byte, len(content))
	_, err = client.ReadContentInto(context.Background(), hash, 0, dst, ClientOptions{})
	require.ErrorIs(t, err, ErrSelectedHostUnavailable)

	client.removeLocalHostCache(hash)
	client.hasher = &orderedTestHasher{hosts: []*Host{localHost}}
	n, err := client.ReadContentInto(context.Background(), hash, 0, dst, ClientOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)
}

func TestReadContentIntoKeepsLogicalHostUnavailableDistinctFromMiss(t *testing.T) {
	logicalHost := &Host{
		HostId:      "logical-host",
		PoolName:    "default",
		Locality:    "test",
		NodeID:      "node-a",
		CachePathID: "path",
	}
	client := &Client{
		ctx:                   context.Background(),
		clientConfig:          ClientConfig{NTopHosts: 1},
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{logicalHost}},
		maxGetContentAttempts: 1,
	}

	dst := make([]byte, 8)
	_, err := client.ReadContentInto(context.Background(), "hash", 0, dst, ClientOptions{RoutingKey: "hash"})

	require.ErrorIs(t, err, ErrSelectedHostUnavailable)
}

func TestFallbackHostSelectionDoesNotPoisonPrimaryCache(t *testing.T) {
	primaryHost := &Host{HostId: "primary-host"}
	fallbackHost := &Host{HostId: "fallback-host", PrivateAddr: "fallback-host:2049"}
	client := &Client{
		ctx:            context.Background(),
		clientConfig:   ClientConfig{NTopHosts: 2},
		localHostCache: make(map[string]*localClientCache),
		hasher:         &orderedTestHasher{hosts: []*Host{primaryHost, fallbackHost}},
	}

	selected, err := client.getHostForRequest(&ClientRequest{
		rt:        ClientRequestTypeRetrieval,
		hash:      "hash",
		key:       "hash",
		hostIndex: 1,
	})
	require.NoError(t, err)
	require.Equal(t, fallbackHost.HostId, selected.HostId)
	require.Empty(t, client.localHostCache)

	selected, err = client.getHostForRequest(&ClientRequest{
		rt:        ClientRequestTypeRetrieval,
		hash:      "hash",
		key:       "hash",
		hostIndex: 0,
	})
	require.NoError(t, err)
	require.Equal(t, primaryHost.HostId, selected.HostId)
}

func TestClientEndpointFailureKeepsLogicalHostInHRW(t *testing.T) {
	hostA := &Host{
		HostId:         "cache-host-default-node-a-path",
		RegistrationID: "worker-a",
		NodeID:         "node-a",
		CachePathID:    "path",
		PrivateAddr:    "10.0.0.1:2049",
	}
	hostB := &Host{
		HostId:         "cache-host-default-node-b-path",
		RegistrationID: "worker-b",
		NodeID:         "node-b",
		CachePathID:    "path",
		PrivateAddr:    "10.0.0.2:2049",
	}
	hostMap := NewHostMap(GlobalConfig{}, nil)
	hostMap.Set(hostA)
	hostMap.Set(hostB)
	hasher := rendezvous.New[*Host]()
	hasher.Add(hostA, hostB)
	keyForHostA := ""
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		host, _ := hasher.Get(key)
		if host != nil && host.HostId == hostA.HostId {
			keyForHostA = key
			break
		}
	}
	require.NotEmpty(t, keyForHostA)

	client := &Client{
		ctx:            context.Background(),
		clientConfig:   ClientConfig{NTopHosts: 2},
		hostMap:        hostMap,
		grpcClients:    make(map[string]proto.CacheClient),
		grpcConns:      make(map[string]*grpc.ClientConn),
		localServers:   make(map[string]*Server),
		rawReadPools:   make(map[string]*rawReadConnPool),
		localHostCache: make(map[string]*localClientCache),
		hasher:         hasher,
	}

	client.removeHost(hostA)
	client.removeLocalHostCache(keyForHostA)

	deactivated := hostMap.Get(hostA.HostId)
	require.NotNil(t, deactivated)
	require.False(t, deactivated.HasEndpoint())

	selected, err := client.getHostForRequest(&ClientRequest{
		rt:        ClientRequestTypeRetrieval,
		hash:      keyForHostA,
		key:       keyForHostA,
		hostIndex: 0,
	})
	require.NoError(t, err)
	require.Equal(t, hostA.HostId, selected.HostId)
	require.False(t, selected.HasEndpoint())

	_, _, err = client.getGRPCClientForHostIndex(context.Background(), ClientRequestTypeRetrieval, keyForHostA, keyForHostA, 0)
	require.ErrorIs(t, err, ErrSelectedHostUnavailable)
}

func TestCheckHostEndpointPrunesUnreachableEndpoint(t *testing.T) {
	host := &Host{
		HostId:         "cache-host-default-node-a-path",
		RegistrationID: "worker-a",
		NodeID:         "node-a",
		CachePathID:    "path",
		PrivateAddr:    "10.0.0.1:2049",
	}
	hostMap := NewHostMap(GlobalConfig{}, nil)
	hostMap.Set(host)

	client := &Client{
		ctx:            context.Background(),
		clientConfig:   ClientConfig{NTopHosts: 1},
		hostMap:        hostMap,
		grpcClients:    map[string]proto.CacheClient{host.HostId: &fakeStoreCacheClient{stateErr: errors.New("connection refused")}},
		grpcConns:      make(map[string]*grpc.ClientConn),
		localServers:   make(map[string]*Server),
		rawReadPools:   make(map[string]*rawReadConnPool),
		localHostCache: make(map[string]*localClientCache),
		hasher:         &orderedTestHasher{hosts: []*Host{host}},
	}

	require.False(t, client.checkHostEndpoint(host))

	deactivated := hostMap.Get(host.HostId)
	require.NotNil(t, deactivated)
	require.False(t, deactivated.HasEndpoint())

	selected, err := client.getHostForRequest(&ClientRequest{
		rt:        ClientRequestTypeRetrieval,
		hash:      "hash",
		key:       "hash",
		hostIndex: 0,
	})
	require.NoError(t, err)
	require.Equal(t, host.HostId, selected.HostId)
	require.False(t, selected.HasEndpoint())

	_, _, err = client.getGRPCClientForHostIndex(context.Background(), ClientRequestTypeRetrieval, "hash", "hash", 0)
	require.ErrorIs(t, err, ErrSelectedHostUnavailable)
}

func TestReadContentIntoDoesNotMaskPrimaryUnavailableWithReplicaMiss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        4,
			ObjectTtlS:           300,
		},
		Client: ClientConfig{NTopHosts: 2},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}

	replicaServer, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("replica-host"))
	require.NoError(t, err)
	replicaAddr, err := replicaServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	defer func() { require.NoError(t, replicaServer.Close()) }()

	primaryHost := &Host{
		HostId:      "primary-host",
		PoolName:    "default",
		Locality:    "test",
		NodeID:      "node-a",
		CachePathID: "path",
	}
	replicaHost := replicaServer.Host()
	require.NotNil(t, replicaHost)
	replicaHost.Addr = replicaAddr
	replicaHost.PrivateAddr = replicaAddr

	client := &Client{
		ctx:                   ctx,
		clientConfig:          cfg.Client,
		globalConfig:          cfg.Global,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{primaryHost, replicaHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(replicaHost))
	defer client.Cleanup()

	dst := make([]byte, 8)
	_, err = client.ReadContentInto(ctx, "missing-hash", 0, dst, ClientOptions{RoutingKey: "missing-hash"})
	require.ErrorIs(t, err, ErrSelectedHostUnavailable)
}

func TestLogicalOnlyHostStaysInHRWButHasNoEndpoint(t *testing.T) {
	client := &Client{
		ctx:                   context.Background(),
		clientConfig:          ClientConfig{NTopHosts: 1},
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                rendezvous.New[*Host](),
		maxGetContentAttempts: 1,
	}

	logicalHost := &Host{
		HostId:      "logical-host",
		PoolName:    "default",
		Locality:    "test",
		NodeID:      "node-a",
		CachePathID: "path",
	}
	require.NoError(t, client.addHost(logicalHost))

	selected, err := client.getHostForRequest(&ClientRequest{
		rt:        ClientRequestTypeStorage,
		hash:      "hash",
		key:       "hash",
		hostIndex: 0,
	})
	require.NoError(t, err)
	require.Equal(t, logicalHost.HostId, selected.HostId)
	require.False(t, selected.HasEndpoint())

	_, _, err = client.getGRPCClientForHostIndex(context.Background(), ClientRequestTypeStorage, "hash", "hash", 0)
	require.ErrorIs(t, err, ErrSelectedHostUnavailable)
}

func TestReadContentIntoDoesNotUseNonSelectedLocalReplica(t *testing.T) {
	store := newTestStore(t, 5)
	content := []byte("local-cache-content")
	hash, _, err := store.AddReader(context.Background(), bytes.NewReader(content))
	require.NoError(t, err)

	localHost := &Host{HostId: "local-host"}
	store.currentHost = localHost
	client := &Client{
		ctx:                   context.Background(),
		clientConfig:          ClientConfig{NTopHosts: 1},
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{{HostId: "remote-host"}}},
		hostMap:               NewHostMap(GlobalConfig{}, nil),
		maxGetContentAttempts: 1,
	}
	client.hostMap.Set(localHost)
	client.AttachLocalServer(&Server{cas: store})

	dst := make([]byte, len(content))
	_, err = client.ReadContentInto(context.Background(), hash, 0, dst, ClientOptions{})
	require.ErrorIs(t, err, ErrSelectedHostUnavailable)

	_, err = client.ClientLocalPageFileViews(hash, 3, 6, ClientOptions{})
	require.ErrorIs(t, err, ErrContentNotFound)
}

func TestReadContentIntoDoesNotMaskSelectedHostMissWithDifferentHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        4,
			ObjectTtlS:           300,
			ReadTransport:        ServerReadTransportConfig{Enabled: true, Sendfile: true},
		},
		Client: ClientConfig{
			NTopHosts:     1,
			ReadTransport: ClientReadTransportConfig{Enabled: true, MaxActiveConnsPerHost: 2, MaxIdleConnsPerHost: 1},
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}

	localServer, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("local-host"))
	require.NoError(t, err)
	localAddr, err := localServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	defer func() { require.NoError(t, localServer.Close()) }()

	remoteCfg := cfg
	remoteCfg.Server.DiskCacheDir = t.TempDir()
	remoteServer, err := NewServerWithOptions(ctx, remoteCfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("remote-host"))
	require.NoError(t, err)
	remoteAddr, err := remoteServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	defer func() { require.NoError(t, remoteServer.Close()) }()

	content := []byte("content-appears-after-host-refresh")
	hash, _, err := remoteServer.cas.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)

	localHost := localServer.Host()
	require.NotNil(t, localHost)
	localHost.Addr = localAddr
	localHost.PrivateAddr = localAddr
	remoteHost := remoteServer.Host()
	require.NotNil(t, remoteHost)
	remoteHost.Addr = remoteAddr
	remoteHost.PrivateAddr = remoteAddr

	metadataStore := NewMockCacheMetadataStore()
	require.NoError(t, metadataStore.AddHostToIndex(ctx, "test", localHost))
	require.NoError(t, metadataStore.AddHostToIndex(ctx, "test", remoteHost))

	client := &Client{
		ctx:                   ctx,
		locality:              "test",
		clientConfig:          cfg.Client,
		globalConfig:          cfg.Global,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{},
		hostDirectory:         metadataStore,
		maxGetContentAttempts: 1,
	}
	client.hostMap = NewHostMap(cfg.Global, client.addHost)
	client.hostMap.Set(localHost)
	defer client.Cleanup()

	readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
	defer readCancel()
	dst := make([]byte, len(content))
	_, err = client.ReadContentInto(readCtx, hash, 0, dst, ClientOptions{RoutingKey: hash})
	require.ErrorIs(t, err, ErrContentNotFound)
}

func TestReadContentIntoFallsBackToRankedReplicaHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        4,
			ObjectTtlS:           300,
		},
		Client: ClientConfig{NTopHosts: 2},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}

	primaryServer, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("primary-host"))
	require.NoError(t, err)
	primaryAddr, err := primaryServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	defer func() { require.NoError(t, primaryServer.Close()) }()

	replicaCfg := cfg
	replicaCfg.Server.DiskCacheDir = t.TempDir()
	replicaServer, err := NewServerWithOptions(ctx, replicaCfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("replica-host"))
	require.NoError(t, err)
	replicaAddr, err := replicaServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	defer func() { require.NoError(t, replicaServer.Close()) }()

	content := []byte("content-on-ranked-replica")
	hash, _, err := replicaServer.cas.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)

	primaryHost := primaryServer.Host()
	require.NotNil(t, primaryHost)
	primaryHost.Addr = primaryAddr
	primaryHost.PrivateAddr = primaryAddr
	replicaHost := replicaServer.Host()
	require.NotNil(t, replicaHost)
	replicaHost.Addr = replicaAddr
	replicaHost.PrivateAddr = replicaAddr

	client := &Client{
		ctx:                   ctx,
		locality:              "test",
		clientConfig:          cfg.Client,
		globalConfig:          cfg.Global,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{primaryHost, replicaHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(primaryHost))
	require.NoError(t, client.addHost(replicaHost))
	defer client.Cleanup()

	dst := make([]byte, len(content))
	n, err := client.ReadContentInto(ctx, hash, 0, dst, ClientOptions{RoutingKey: hash})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)
}

func TestReadContentIntoUsesSelectedLocalServer(t *testing.T) {
	store := newTestStore(t, 5)
	content := []byte("local-cache-content")
	hash, _, err := store.AddReader(context.Background(), bytes.NewReader(content))
	require.NoError(t, err)

	localHost := &Host{HostId: "local-host"}
	store.currentHost = localHost
	client := &Client{
		ctx:                   context.Background(),
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{localHost}},
		maxGetContentAttempts: 1,
	}
	client.AttachLocalServer(&Server{cas: store})

	dst := make([]byte, len(content))
	n, err := client.ReadContentInto(context.Background(), hash, 0, dst, ClientOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)
}

func TestContentReadHotPathDoesNotUseCacheFSMetadata(t *testing.T) {
	store := newTestStore(t, 5)
	content := []byte("local-cache-content")
	hash, _, err := store.AddReader(context.Background(), bytes.NewReader(content))
	require.NoError(t, err)

	localHost := &Host{HostId: "local-host"}
	store.currentHost = localHost
	metadataStore := &failOnGetFsNodeMetadataStore{MockCacheMetadataStore: NewMockCacheMetadataStore(), t: t}
	client := &Client{
		ctx:                   context.Background(),
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		metadataStore:         metadataStore,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{localHost}},
		maxGetContentAttempts: 1,
	}
	client.AttachLocalServer(&Server{cas: store})

	dst := make([]byte, len(content))
	n, err := client.ReadContentInto(context.Background(), hash, 0, dst, ClientOptions{RoutingKey: "/images/test.clip"})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)

	views, err := client.ClientLocalPageFileViews(hash, 0, int64(len(content)), ClientOptions{RoutingKey: "/images/test.clip"})
	require.NoError(t, err)
	require.NotEmpty(t, views)
}

func TestClientLocalPageFileViewsReturnsSelectedClientLocalPageFiles(t *testing.T) {
	store := newTestStore(t, 5)
	content := []byte("abcdefghijkl")
	hash, _, err := store.AddReader(context.Background(), bytes.NewReader(content))
	require.NoError(t, err)

	localHost := &Host{HostId: "local-host"}
	store.currentHost = localHost
	client := &Client{
		ctx:            context.Background(),
		clientConfig:   ClientConfig{NTopHosts: 1},
		localServers:   make(map[string]*Server),
		rawReadPools:   make(map[string]*rawReadConnPool),
		localHostCache: make(map[string]*localClientCache),
		hasher:         &orderedTestHasher{hosts: []*Host{localHost}},
	}
	client.AttachLocalServer(&Server{cas: store})

	regions, err := client.ClientLocalPageFileViews(hash, 3, 6, ClientOptions{})
	require.NoError(t, err)
	require.Len(t, regions, 2)
	require.Equal(t, int64(3), regions[0].Offset)
	require.Equal(t, 2, regions[0].Length)
	require.Equal(t, int64(0), regions[1].Offset)
	require.Equal(t, 4, regions[1].Length)
	require.FileExists(t, regions[0].Path)
	require.FileExists(t, regions[1].Path)
}

func TestClientLocalPageFileViewsUsesSelectedLocalServer(t *testing.T) {
	store := newTestStore(t, 5)
	content := []byte("abcdefghijkl")
	hash, _, err := store.AddReader(context.Background(), bytes.NewReader(content))
	require.NoError(t, err)

	localHost := &Host{HostId: "local-host"}
	store.currentHost = localHost
	client := &Client{
		ctx:            context.Background(),
		clientConfig:   ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		localServers:   make(map[string]*Server),
		rawReadPools:   make(map[string]*rawReadConnPool),
		localHostCache: make(map[string]*localClientCache),
		hasher:         &orderedTestHasher{hosts: []*Host{localHost}},
	}
	client.AttachLocalServer(&Server{cas: store})

	regions, err := client.ClientLocalPageFileViews(hash, 3, 6, ClientOptions{})
	require.NoError(t, err)
	require.Len(t, regions, 2)
	require.Equal(t, int64(3), regions[0].Offset)
	require.Equal(t, 2, regions[0].Length)
	require.Equal(t, int64(0), regions[1].Offset)
	require.Equal(t, 4, regions[1].Length)
}

func TestWithStoreFromContentLockReturnsUnlockErrorAndRetriesDeferredRelease(t *testing.T) {
	registry := &failFirstUnlockRegistry{MockCacheMetadataStore: NewMockCacheMetadataStore()}
	client := &Client{
		ctx:           context.Background(),
		locality:      "test",
		metadataStore: registry,
	}

	hash, err := client.withStoreFromContentLock(context.Background(), "/source", true, func() (string, error) {
		return "hash", nil
	})

	require.Equal(t, "hash", hash)
	require.ErrorContains(t, err, "unlock failed")
	require.Equal(t, 2, registry.removeCalls)
	require.False(t, registry.locks["store-lock:test:/source"])
}

func TestWithStoreFromContentLockIgnoresAlreadyReleasedLock(t *testing.T) {
	registry := &lockNotHeldOnUnlockRegistry{MockCacheMetadataStore: NewMockCacheMetadataStore()}
	client := &Client{
		ctx:           context.Background(),
		locality:      "test",
		metadataStore: registry,
	}

	hash, err := client.withStoreFromContentLock(context.Background(), "/source", true, func() (string, error) {
		return "hash", nil
	})

	require.NoError(t, err)
	require.Equal(t, "hash", hash)
	require.False(t, registry.locks["store-lock:test:/source"])
}

func TestAddHostClearsCachedRoutingForReplacedHost(t *testing.T) {
	client := &Client{
		ctx:          context.Background(),
		clientConfig: ClientConfig{NTopHosts: 1},
		globalConfig: GlobalConfig{
			GRPCMessageSizeBytes: 4 * 1024 * 1024,
		},
		grpcClients:    make(map[string]proto.CacheClient),
		grpcConns:      make(map[string]*grpc.ClientConn),
		rawReadPools:   make(map[string]*rawReadConnPool),
		localHostCache: make(map[string]*localClientCache),
		hasher:         &orderedTestHasher{},
	}
	defer client.Cleanup()

	oldHost := &Host{HostId: "host-a", Addr: "127.0.0.1:1"}
	client.localHostCache["content-hash"] = &localClientCache{
		host:      oldHost,
		timestamp: time.Now(),
	}

	err := client.addHost(&Host{HostId: "host-a", Addr: "127.0.0.1:1"})
	require.NoError(t, err)

	_, exists := client.localHostCache["content-hash"]
	require.False(t, exists)
}

func TestStoreContentFromLocalFileUsesMatchingCacheMetadata(t *testing.T) {
	ctx := context.Background()
	content := []byte("already cached local content")

	sourcePath := filepath.Join(t.TempDir(), "source.txt")
	require.NoError(t, os.WriteFile(sourcePath, content, 0644))
	info, err := os.Stat(sourcePath)
	require.NoError(t, err)

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        5,
			ObjectTtlS:           300,
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 4 * 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}
	remoteCfg := cfg
	remoteCfg.Server.DiskCacheDir = t.TempDir()
	remoteServer, err := NewServerWithOptions(ctx, remoteCfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("remote-host"))
	require.NoError(t, err)
	addr, err := remoteServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remoteServer.Close()) })

	remoteHost := remoteServer.Host()
	require.NotNil(t, remoteHost)
	remoteHost.Addr = addr
	remoteHost.PrivateAddr = addr

	hash, _, err := remoteServer.cas.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)

	cachePath := "/workspace/source.txt"
	metadataStore := NewMockCacheMetadataStore()
	require.NoError(t, metadataStore.SetFsNode(ctx, GenerateFsID(cachePath), &FSMetadata{
		Path:      cachePath,
		Hash:      hash,
		Size:      uint64(info.Size()),
		Mtime:     uint64(info.ModTime().Unix()),
		Mtimensec: uint32(info.ModTime().Nanosecond()),
	}))

	client := &Client{
		ctx:                   ctx,
		locality:              "test",
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		globalConfig:          cfg.Global,
		metadataStore:         metadataStore,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{remoteHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(remoteHost))
	defer client.Cleanup()

	got, err := client.StoreContentFromLocalFile(LocalContentSource{
		Path:      sourcePath,
		CachePath: cachePath,
	}, StoreContentOptions{
		RoutingKey: cachePath,
		Lock:       true,
	})
	require.NoError(t, err)
	require.Equal(t, hash, got)
	require.Empty(t, metadataStore.locks)
}

func TestStoreContentFromLocalFileWaitsForLockedHashAlreadyPublished(t *testing.T) {
	ctx := context.Background()
	content := []byte("published by competing cache writer")

	sourcePath := filepath.Join(t.TempDir(), "source.txt")
	require.NoError(t, os.WriteFile(sourcePath, content, 0644))

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        5,
			ObjectTtlS:           300,
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 4 * 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}
	remoteCfg := cfg
	remoteCfg.Server.DiskCacheDir = t.TempDir()
	remoteServer, err := NewServerWithOptions(ctx, remoteCfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("remote-host"))
	require.NoError(t, err)
	addr, err := remoteServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remoteServer.Close()) })

	remoteHost := remoteServer.Host()
	require.NotNil(t, remoteHost)
	remoteHost.Addr = addr
	remoteHost.PrivateAddr = addr

	hash, _, err := remoteServer.cas.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)

	cachePath := "/workspace/source.txt"
	metadataStore := NewMockCacheMetadataStore()
	require.NoError(t, metadataStore.SetStoreFromContentLock(ctx, "test", storeContentHashLockKey(hash)))

	client := &Client{
		ctx:                   ctx,
		locality:              "test",
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		globalConfig:          cfg.Global,
		metadataStore:         metadataStore,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{remoteHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(remoteHost))
	defer client.Cleanup()

	got, err := client.StoreContentFromLocalFile(LocalContentSource{
		Path:      sourcePath,
		CachePath: cachePath,
	}, StoreContentOptions{
		RoutingKey: hash,
		Lock:       true,
	})
	require.NoError(t, err)
	require.Equal(t, hash, got)
}

func TestStoreContentFromLocalFileWithHashWritesSelectedRemoteHost(t *testing.T) {
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
	remoteCfg := cfg
	remoteCfg.Server.DiskCacheDir = t.TempDir()
	remoteServer, err := NewServerWithOptions(ctx, remoteCfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("remote-host"))
	require.NoError(t, err)
	addr, err := remoteServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remoteServer.Close()) })

	remoteHost := remoteServer.Host()
	require.NotNil(t, remoteHost)
	remoteHost.Addr = addr
	remoteHost.PrivateAddr = addr

	metadataStore := NewMockCacheMetadataStore()
	client := &Client{
		ctx:                   ctx,
		locality:              "test",
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		globalConfig:          cfg.Global,
		metadataStore:         metadataStore,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{remoteHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(remoteHost))
	defer client.Cleanup()

	content := []byte("cache-through-local-file")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	sourcePath := filepath.Join(t.TempDir(), "source.bin")
	require.NoError(t, os.WriteFile(sourcePath, content, 0644))
	cachePath := "/workspace/source.bin"

	got, err := client.StoreContentFromLocalFile(LocalContentSource{
		Path:      sourcePath,
		CachePath: cachePath,
	}, StoreContentOptions{
		RoutingKey: expectedHash,
		Lock:       true,
	})
	require.NoError(t, err)
	require.Equal(t, expectedHash, got)
	require.Empty(t, metadataStore.locks)

	require.Eventually(t, func() bool {
		remote := make([]byte, len(content))
		n, err := remoteServer.cas.ReadAt(expectedHash, 0, remote)
		return err == nil && n == int64(len(content)) && bytes.Equal(content, remote)
	}, time.Second, 10*time.Millisecond)
}

func TestStoreContentFromLocalFileRepairsSelectedHostWhenFallbackHasContent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        4,
			ObjectTtlS:           300,
		},
		Client: ClientConfig{
			NTopHosts:            2,
			PreferLocalCacheHost: true,
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}
	selectedServer, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("selected-host"))
	require.NoError(t, err)
	selectedAddr, err := selectedServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, selectedServer.Close()) })

	fallbackCfg := cfg
	fallbackCfg.Server.DiskCacheDir = t.TempDir()
	fallbackServer, err := NewServerWithOptions(ctx, fallbackCfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("fallback-host"))
	require.NoError(t, err)
	fallbackAddr, err := fallbackServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fallbackServer.Close()) })

	selectedHost := selectedServer.Host()
	require.NotNil(t, selectedHost)
	selectedHost.Addr = selectedAddr
	selectedHost.PrivateAddr = selectedAddr

	fallbackHost := fallbackServer.Host()
	require.NotNil(t, fallbackHost)
	fallbackHost.Addr = fallbackAddr
	fallbackHost.PrivateAddr = fallbackAddr

	content := []byte("cache-through-must-repair-selected-host")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	sourcePath := filepath.Join(t.TempDir(), "source.bin")
	require.NoError(t, os.WriteFile(sourcePath, content, 0644))
	info, err := os.Stat(sourcePath)
	require.NoError(t, err)

	_, _, err = fallbackServer.cas.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)
	require.False(t, selectedServer.cas.Exists(expectedHash))
	require.True(t, fallbackServer.cas.Exists(expectedHash))

	cachePath := "/workspace/source.bin"
	metadataStore := NewMockCacheMetadataStore()
	require.NoError(t, metadataStore.SetFsNode(ctx, GenerateFsID(cachePath), &FSMetadata{
		Path:      cachePath,
		Hash:      expectedHash,
		Size:      uint64(info.Size()),
		Mtime:     uint64(info.ModTime().Unix()),
		Mtimensec: uint32(info.ModTime().Nanosecond()),
	}))

	client := &Client{
		ctx:                   ctx,
		locality:              "test",
		clientConfig:          cfg.Client,
		globalConfig:          cfg.Global,
		metadataStore:         metadataStore,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{selectedHost, fallbackHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(selectedHost))
	require.NoError(t, client.addHost(fallbackHost))
	defer client.Cleanup()

	got, err := client.StoreContentFromLocalFile(LocalContentSource{
		Path:      sourcePath,
		CachePath: cachePath,
	}, StoreContentOptions{
		RoutingKey: expectedHash,
		Lock:       true,
	})
	require.NoError(t, err)
	require.Equal(t, expectedHash, got)

	require.Eventually(t, func() bool {
		selected := make([]byte, len(content))
		n, err := selectedServer.cas.ReadAt(expectedHash, 0, selected)
		return err == nil && n == int64(len(content)) && bytes.Equal(content, selected)
	}, time.Second, 10*time.Millisecond)
}

func TestStoreContentFromLocalFileRepairsPartialSelectedHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        4,
			ObjectTtlS:           300,
		},
		Client: ClientConfig{
			NTopHosts: 1,
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}
	selectedServer, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("selected-host"))
	require.NoError(t, err)
	selectedAddr, err := selectedServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, selectedServer.Close()) })

	selectedHost := selectedServer.Host()
	require.NotNil(t, selectedHost)
	selectedHost.Addr = selectedAddr
	selectedHost.PrivateAddr = selectedAddr

	content := []byte("partial-selected-host-content")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	sourcePath := filepath.Join(t.TempDir(), "source.bin")
	require.NoError(t, os.WriteFile(sourcePath, content, 0644))
	info, err := os.Stat(sourcePath)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(selectedServer.cas.pageDir(expectedHash), 0755))
	require.NoError(t, os.WriteFile(selectedServer.cas.pagePath(expectedHash, 0), content[:int(cfg.Server.PageSizeBytes)], 0644))
	require.False(t, selectedServer.cas.Exists(expectedHash))
	require.False(t, selectedServer.cas.Exists(expectedHash, info.Size()))

	cachePath := "/images/cache/" + expectedHash
	metadataStore := NewMockCacheMetadataStore()
	require.NoError(t, metadataStore.SetFsNode(ctx, GenerateFsID(cachePath), &FSMetadata{
		Path:      cachePath,
		Hash:      expectedHash,
		Size:      uint64(info.Size()),
		Mtime:     uint64(info.ModTime().Unix()),
		Mtimensec: uint32(info.ModTime().Nanosecond()),
	}))

	client := &Client{
		ctx:                   ctx,
		locality:              "test",
		clientConfig:          cfg.Client,
		globalConfig:          cfg.Global,
		metadataStore:         metadataStore,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{selectedHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(selectedHost))
	defer client.Cleanup()

	got, err := client.StoreContentFromLocalFile(LocalContentSource{
		Path:      sourcePath,
		CachePath: cachePath,
	}, StoreContentOptions{
		RoutingKey: expectedHash,
		Lock:       true,
	})
	require.NoError(t, err)
	require.Equal(t, expectedHash, got)
	require.True(t, selectedServer.cas.Exists(expectedHash, info.Size()))

	selected := make([]byte, len(content))
	n, err := selectedServer.cas.ReadAt(expectedHash, 0, selected)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, selected)
}

func TestStoreContentFromReaderDoesNotFanOutAfterSelectedHostSuccess(t *testing.T) {
	ctx := context.Background()
	firstHost := &Host{HostId: "first-host", PrivateAddr: "first-host:2049"}
	secondHost := &Host{HostId: "second-host", PrivateAddr: "second-host:2049"}
	thirdHost := &Host{HostId: "third-host", PrivateAddr: "third-host:2049"}
	firstStream := &fakeStoreContentStream{}
	secondStream := &fakeStoreContentStream{}
	thirdStream := &fakeStoreContentStream{}
	client := &Client{
		ctx:                   ctx,
		clientConfig:          ClientConfig{NTopHosts: 3},
		grpcClients:           map[string]proto.CacheClient{"first-host": &fakeStoreCacheClient{stream: firstStream}, "second-host": &fakeStoreCacheClient{stream: secondStream}, "third-host": &fakeStoreCacheClient{stream: thirdStream}},
		grpcConns:             make(map[string]*grpc.ClientConn),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{firstHost, secondHost, thirdHost}},
		maxGetContentAttempts: 2,
	}

	content := bytes.Repeat([]byte("x"), writeBufferSizeBytes+17)
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])

	got, err := client.storeContentFromReaderWithContext(ctx, bytes.NewReader(content), expectedHash, "/cache/file.bin", nil)
	require.NoError(t, err)
	require.Equal(t, expectedHash, got)
	require.Equal(t, content, firstStream.sent.Bytes())
	require.Empty(t, secondStream.sent.Bytes())
	require.Empty(t, thirdStream.sent.Bytes())
}

func TestStoreContentFromReaderDoesNotUseRankedReplicaWhenSelectedHostUnavailable(t *testing.T) {
	ctx := context.Background()
	selectedHost := &Host{HostId: "selected-host"}
	fallbackHost := &Host{HostId: "fallback-host", PrivateAddr: "fallback-host:2049"}
	fallbackStream := &fakeStoreContentStream{}
	client := &Client{
		ctx:                   ctx,
		clientConfig:          ClientConfig{NTopHosts: 2},
		grpcClients:           map[string]proto.CacheClient{"fallback-host": &fakeStoreCacheClient{stream: fallbackStream}},
		grpcConns:             make(map[string]*grpc.ClientConn),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{selectedHost, fallbackHost}},
		maxGetContentAttempts: 1,
	}

	content := []byte("fallback-cache-through-when-primary-endpoint-is-gone")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])

	got, err := client.storeContentFromReaderWithContext(ctx, bytes.NewReader(content), expectedHash, "/cache/file.bin", nil)
	require.Error(t, err)
	require.Empty(t, got)
	require.Empty(t, fallbackStream.sent.Bytes())
}

type fakeStoreCacheClient struct {
	stream   proto.Cache_StoreContentClient
	state    *proto.CacheGetStateResponse
	stateErr error
}

func (f *fakeStoreCacheClient) GetContent(ctx context.Context, in *proto.CacheGetContentRequest, opts ...grpc.CallOption) (*proto.CacheGetContentResponse, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeStoreCacheClient) HasContent(ctx context.Context, in *proto.CacheHasContentRequest, opts ...grpc.CallOption) (*proto.CacheHasContentResponse, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeStoreCacheClient) GetContentStream(ctx context.Context, in *proto.CacheGetContentRequest, opts ...grpc.CallOption) (proto.Cache_GetContentStreamClient, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeStoreCacheClient) StoreContent(ctx context.Context, opts ...grpc.CallOption) (proto.Cache_StoreContentClient, error) {
	return f.stream, nil
}

func (f *fakeStoreCacheClient) StoreContentFromSource(ctx context.Context, in *proto.CacheStoreContentFromSourceRequest, opts ...grpc.CallOption) (*proto.CacheStoreContentFromSourceResponse, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeStoreCacheClient) StoreContentFromSourceWithLock(ctx context.Context, in *proto.CacheStoreContentFromSourceRequest, opts ...grpc.CallOption) (*proto.CacheStoreContentFromSourceWithLockResponse, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeStoreCacheClient) GetState(ctx context.Context, in *proto.CacheGetStateRequest, opts ...grpc.CallOption) (*proto.CacheGetStateResponse, error) {
	if f.stateErr != nil {
		return nil, f.stateErr
	}
	if f.state != nil {
		return f.state, nil
	}
	return nil, errors.New("not implemented")
}

type fakeStoreContentStream struct {
	grpc.ClientStream
	failOnSend bool
	sent       bytes.Buffer
}

func (s *fakeStoreContentStream) Send(req *proto.CacheStoreContentRequest) error {
	if len(req.GetContent()) > 0 {
		_, _ = s.sent.Write(req.GetContent())
	}
	if s.failOnSend {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (s *fakeStoreContentStream) CloseAndRecv() (*proto.CacheStoreContentResponse, error) {
	sum := sha256.Sum256(s.sent.Bytes())
	return &proto.CacheStoreContentResponse{
		Ok:   true,
		Hash: hex.EncodeToString(sum[:]),
	}, nil
}

func TestStoreContentUsesSelectedRemoteHost(t *testing.T) {
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
	remoteServer, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("remote-host"))
	require.NoError(t, err)
	addr, err := remoteServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remoteServer.Close()) })

	remoteHost := remoteServer.Host()
	require.NotNil(t, remoteHost)
	remoteHost.Addr = addr
	remoteHost.PrivateAddr = addr

	client := &Client{
		ctx:                   ctx,
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		globalConfig:          cfg.Global,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{remoteHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(remoteHost))
	defer client.Cleanup()

	content := []byte("cache-through-selected-remote")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	chunks := make(chan []byte, 2)
	chunks <- content[:9]
	chunks <- content[9:]
	close(chunks)

	got, err := client.StoreContent(chunks, expectedHash, struct{ RoutingKey string }{RoutingKey: "/cache/object"})
	require.NoError(t, err)
	require.Equal(t, expectedHash, got)

	require.Eventually(t, func() bool {
		remote := make([]byte, len(content))
		n, err := remoteServer.cas.ReadAt(expectedHash, 0, remote)
		return err == nil && n == int64(len(content)) && bytes.Equal(content, remote)
	}, time.Second, 10*time.Millisecond)
}

func TestStoreContentDoesNotSilentlyFallbackToLocalWhenSelectedHostUnavailable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	remoteHost := &Host{HostId: "remote-host", Addr: "127.0.0.1:1", PrivateAddr: "127.0.0.1:1"}
	client := &Client{
		ctx:                   ctx,
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		globalConfig:          GlobalConfig{GRPCMessageSizeBytes: 1024 * 1024, GRPCDialTimeoutS: 1},
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{remoteHost}},
		maxGetContentAttempts: 1,
	}
	defer client.Cleanup()

	content := []byte("local-cache-through-survives-remote-failure")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	chunks := make(chan []byte, 3)
	chunks <- content[:10]
	chunks <- content[10:25]
	chunks <- content[25:]
	close(chunks)

	_, err := client.StoreContent(chunks, expectedHash, struct{ RoutingKey string }{RoutingKey: expectedHash})
	require.Error(t, err)
}

func TestStoreContentStreamWritesSelectedRemoteHost(t *testing.T) {
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
	remoteServer, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("remote-host"))
	require.NoError(t, err)
	addr, err := remoteServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remoteServer.Close()) })

	remoteHost := remoteServer.Host()
	require.NotNil(t, remoteHost)
	remoteHost.Addr = addr
	remoteHost.PrivateAddr = addr

	client := &Client{
		ctx:                   ctx,
		locality:              "test",
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		globalConfig:          cfg.Global,
		metadataStore:         NewMockCacheMetadataStore(),
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{remoteHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(remoteHost))
	defer client.Cleanup()

	content := []byte("stream-cache-through-selected-remote")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	chunks := make(chan []byte, 1)
	chunks <- content
	close(chunks)

	got, err := client.StoreContent(chunks, expectedHash, struct{ RoutingKey string }{RoutingKey: expectedHash})
	require.NoError(t, err)
	require.Equal(t, expectedHash, got)

	require.Eventually(t, func() bool {
		remote := make([]byte, len(content))
		n, err := remoteServer.cas.ReadAt(expectedHash, 0, remote)
		return err == nil && n == int64(len(content)) && bytes.Equal(content, remote)
	}, time.Second, 10*time.Millisecond)
}

func TestStoreContentFromS3SourceWaitsForLockedHashAlreadyPublished(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metadataStore := NewMockCacheMetadataStore()
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
	remoteServer, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(metadataStore), WithServerHostID("remote-host"))
	require.NoError(t, err)
	addr, err := remoteServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remoteServer.Close()) })

	remoteHost := remoteServer.Host()
	require.NotNil(t, remoteHost)
	remoteHost.Addr = addr
	remoteHost.PrivateAddr = addr

	content := []byte("published by source cache writer")
	hash, _, err := remoteServer.cas.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)
	require.NoError(t, metadataStore.SetStoreFromContentLock(ctx, "test", storeContentHashLockKey(hash)))

	client := &Client{
		ctx:                   ctx,
		locality:              "test",
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		globalConfig:          cfg.Global,
		metadataStore:         metadataStore,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{remoteHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(remoteHost))
	defer client.Cleanup()

	got, err := client.StoreContentFromS3Source(S3ContentSource{Path: "objects/source.bin"}, StoreContentOptions{RoutingKey: hash, Lock: true})
	require.NoError(t, err)
	require.Equal(t, hash, got)
}

func TestStoreContentUsesSelectedLocalLogicalHost(t *testing.T) {
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
	localServer, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("local-host"))
	require.NoError(t, err)
	localAddr, err := localServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, localServer.Close()) })

	remoteCfg := cfg
	remoteCfg.Server.DiskCacheDir = t.TempDir()
	remoteServer, err := NewServerWithOptions(ctx, remoteCfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("remote-host"))
	require.NoError(t, err)
	addr, err := remoteServer.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remoteServer.Close()) })

	remoteHost := remoteServer.Host()
	require.NotNil(t, remoteHost)
	remoteHost.Addr = addr
	remoteHost.PrivateAddr = addr

	localHost := localServer.Host()
	require.NotNil(t, localHost)
	localHost.Addr = localAddr
	localHost.PrivateAddr = localAddr

	client := &Client{
		ctx:                   ctx,
		clientConfig:          ClientConfig{NTopHosts: 2, PreferLocalCacheHost: true},
		globalConfig:          cfg.Global,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{localHost, remoteHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(localHost))
	require.NoError(t, client.addHost(remoteHost))
	defer client.Cleanup()
	client.AttachLocalServer(localServer)

	content := []byte("cache-through-local-primary")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	chunks := make(chan []byte, 2)
	chunks <- content[:11]
	chunks <- content[11:]
	close(chunks)

	got, err := client.StoreContent(chunks, expectedHash, struct{ RoutingKey string }{RoutingKey: "/cache/local-primary"})
	require.NoError(t, err)
	require.Equal(t, expectedHash, got)

	local := make([]byte, len(content))
	n, err := localServer.cas.ReadAt(expectedHash, 0, local)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, local)
	require.False(t, remoteServer.cas.Exists(expectedHash))

	postChurnClient := &Client{
		ctx:                   ctx,
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		globalConfig:          cfg.Global,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{localHost}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, postChurnClient.addHost(localHost))
	postChurnClient.AttachLocalServer(localServer)
	defer postChurnClient.Cleanup()

	afterLocalChurn := make([]byte, len(content))
	n, err = postChurnClient.ReadContentInto(ctx, expectedHash, 0, afterLocalChurn, ClientOptions{RoutingKey: "/cache/local-primary"})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, afterLocalChurn)
}

func TestReadContentIntoSurvivesLogicalHostRegistrationReplacement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cacheDir := t.TempDir()
	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         cacheDir,
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        4,
			ObjectTtlS:           300,
			ReadTransport:        ServerReadTransportConfig{Enabled: true, Sendfile: true},
		},
		Client: ClientConfig{
			NTopHosts:     1,
			ReadTransport: ClientReadTransportConfig{Enabled: true, MaxActiveConnsPerHost: 4, MaxIdleConnsPerHost: 2},
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}
	logicalHostID := "logical-node-cache-host"

	server1, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID(logicalHostID))
	require.NoError(t, err)
	addr1, err := server1.Serve("127.0.0.1:0", "")
	require.NoError(t, err)

	content := []byte("cache-survives-registration-replacement")
	hash, _, err := server1.cas.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)
	direct := make([]byte, len(content))
	n, err := server1.cas.ReadAt(hash, 0, direct)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, direct)

	client := &Client{
		ctx:                   ctx,
		clientConfig:          cfg.Client,
		globalConfig:          cfg.Global,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{},
		maxGetContentAttempts: 1,
	}
	defer client.Cleanup()

	host1 := server1.Host()
	require.NotNil(t, host1)
	host1.Addr = addr1
	host1.PrivateAddr = addr1
	require.NoError(t, client.addHost(host1))

	firstRead := make([]byte, len(content))
	n, err = client.ReadContentInto(ctx, hash, 0, firstRead, ClientOptions{RoutingKey: hash})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, firstRead)

	require.NoError(t, server1.Close())

	server2, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID(logicalHostID))
	require.NoError(t, err)
	defer func() { require.NoError(t, server2.Close()) }()
	addr2, err := server2.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	require.NotEqual(t, addr1, addr2)

	host2 := server2.Host()
	require.NotNil(t, host2)
	host2.Addr = addr2
	host2.PrivateAddr = addr2
	require.NoError(t, client.addHost(host2))

	afterReplacement := make([]byte, len(content))
	n, err = client.ReadContentInto(ctx, hash, 0, afterReplacement, ClientOptions{RoutingKey: hash})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, afterReplacement)
}

func TestStoreContentSourceLockKeyUsesExpectedHash(t *testing.T) {
	source := &proto.CacheSource{
		Path:         "objects/path",
		CachePath:    "cache/path",
		ExpectedHash: strings.Repeat("a", sha256.Size*2),
	}
	require.Equal(t, storeContentHashLockKey(source.ExpectedHash), storeContentSourceLockKey(source))

	source.ExpectedHash = ""
	require.Equal(t, "/cache/path", storeContentSourceLockKey(source))

	source.CachePath = ""
	require.Equal(t, "objects/path", storeContentSourceLockKey(source))
}

type failFirstUnlockRegistry struct {
	*MockCacheMetadataStore
	removeCalls int
}

func (r *failFirstUnlockRegistry) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	r.removeCalls++
	if r.removeCalls == 1 {
		return errors.New("unlock failed")
	}
	return r.MockCacheMetadataStore.RemoveStoreFromContentLock(ctx, locality, sourcePath)
}

type lockNotHeldOnUnlockRegistry struct {
	*MockCacheMetadataStore
}

func (r *lockNotHeldOnUnlockRegistry) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	_ = r.MockCacheMetadataStore.RemoveStoreFromContentLock(ctx, locality, sourcePath)
	return errors.New("redislock: lock not held")
}

type failOnGetFsNodeMetadataStore struct {
	*MockCacheMetadataStore
	t *testing.T
}

func (m *failOnGetFsNodeMetadataStore) GetFsNode(ctx context.Context, id string) (*FSMetadata, error) {
	m.t.Fatalf("content read hot path must not use cachefs metadata: %s", id)
	return nil, errors.New("unexpected cachefs metadata lookup")
}
