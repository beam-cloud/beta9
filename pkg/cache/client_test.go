package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	proto "github.com/beam-cloud/beta9/proto"
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
	require.ErrorIs(t, err, ErrContentNotFound)

	client.removeLocalHostCache(hash)
	client.hasher = &orderedTestHasher{hosts: []*Host{localHost}}
	n, err := client.ReadContentInto(context.Background(), hash, 0, dst, ClientOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)
}

func TestReadContentIntoFallsBackToReachableReplicaOutsideTopHosts(t *testing.T) {
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
	n, err := client.ReadContentInto(context.Background(), hash, 0, dst, ClientOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)

	regions, err := client.ClientLocalPageFileViews(hash, 3, 6, ClientOptions{})
	require.NoError(t, err)
	require.Len(t, regions, 2)
}

func TestReadContentIntoPrefersAttachedLocalReplica(t *testing.T) {
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
		hasher:                &orderedTestHasher{hosts: []*Host{{HostId: "remote-host"}}},
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
	client.AttachLocalStore(store)

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

func TestClientLocalPageFileViewsPrefersAttachedLocalReplica(t *testing.T) {
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
		hasher:         &orderedTestHasher{hosts: []*Host{{HostId: "remote-host"}}},
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

	store := newTestStore(t, 5)
	hash, _, err := store.AddReader(ctx, bytes.NewReader(content))
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
		globalConfig:          GlobalConfig{GRPCMessageSizeBytes: 4 * 1024 * 1024},
		metadataStore:         metadataStore,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		localPromotionSem:     make(chan struct{}, 1),
		hasher:                &orderedTestHasher{},
		maxGetContentAttempts: 1,
	}
	defer client.Cleanup()
	client.AttachLocalStore(store)

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

	store := newTestStore(t, 5)
	hash, _, err := store.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)

	cachePath := "/workspace/source.txt"
	metadataStore := NewMockCacheMetadataStore()
	require.NoError(t, metadataStore.SetStoreFromContentLock(ctx, "test", cachePath))

	client := &Client{
		ctx:                   ctx,
		locality:              "test",
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		globalConfig:          GlobalConfig{GRPCMessageSizeBytes: 4 * 1024 * 1024},
		metadataStore:         metadataStore,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		localPromotionSem:     make(chan struct{}, 1),
		hasher:                &orderedTestHasher{},
		maxGetContentAttempts: 1,
	}
	defer client.Cleanup()
	client.AttachLocalStore(store)

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

func TestStoreContentWithLocalReplicaWritesRemoteAndLocal(t *testing.T) {
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

	localStore := newTestStore(t, 4)
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
	client.AttachLocalStore(localStore)

	content := []byte("cache-through-local-replica")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	chunks := make(chan []byte, 2)
	chunks <- content[:9]
	chunks <- content[9:]
	close(chunks)

	got, err := client.StoreContentWithLocalReplica(chunks, expectedHash, StoreContentOptions{RoutingKey: "/cache/object"})
	require.NoError(t, err)
	require.Equal(t, expectedHash, got)

	remote := make([]byte, len(content))
	n, err := remoteServer.cas.ReadAt(expectedHash, 0, remote)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, remote)

	local := make([]byte, len(content))
	n, err = localStore.ReadAt(expectedHash, 0, local)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, local)
}

func TestStoreContentWithLocalReplicaSkipsLocalPrimaryForRemoteReplica(t *testing.T) {
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
	t.Cleanup(func() { require.NoError(t, localServer.Close()) })

	remoteServer, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("remote-host"))
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
	require.NoError(t, client.addHost(remoteHost))
	defer client.Cleanup()
	client.AttachLocalServer(localServer)
	client.AttachLocalStore(localServer.cas)

	content := []byte("cache-through-local-primary")
	sum := sha256.Sum256(content)
	expectedHash := hex.EncodeToString(sum[:])
	chunks := make(chan []byte, 2)
	chunks <- content[:11]
	chunks <- content[11:]
	close(chunks)

	got, err := client.StoreContentWithLocalReplica(chunks, expectedHash, StoreContentOptions{RoutingKey: "/cache/local-primary"})
	require.NoError(t, err)
	require.Equal(t, expectedHash, got)

	remote := make([]byte, len(content))
	n, err := remoteServer.cas.ReadAt(expectedHash, 0, remote)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, remote)

	local := make([]byte, len(content))
	n, err = localServer.cas.ReadAt(expectedHash, 0, local)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, local)

	postChurnClient := &Client{
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
	require.NoError(t, postChurnClient.addHost(remoteHost))
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
