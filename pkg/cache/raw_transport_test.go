package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"os"
	"testing"
	"time"

	proto "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRawReadMethodSelection(t *testing.T) {
	server := &Server{
		serverConfig: ServerConfig{
			PageSizeBytes:                4 * 1024 * 1024,
			SmallRangeCopyThresholdBytes: 128 * 1024,
			ReadTransport: ServerReadTransportConfig{
				Sendfile: true,
			},
		},
	}

	require.True(t, server.rawReadUsesSmallRangeCopy(16*1024))
	require.True(t, server.rawReadUsesSmallRangeCopy(128*1024))
	require.False(t, server.rawReadUsesSmallRangeCopy(128*1024+1))
	require.False(t, server.rawReadUsesSendfile(128*1024))
	require.True(t, server.rawReadUsesSendfile(128*1024+1))
	require.False(t, server.rawReadShouldFadviseWillneed(16*1024, 16*1024))
	require.True(t, server.rawReadShouldFadviseWillneed(256*1024, 256*1024))
	require.True(t, server.rawReadShouldFadviseWillneed(16*1024, 4*1024*1024))
}

func TestRawReadTraceSource(t *testing.T) {
	client := &Client{
		clientConfig: ClientConfig{
			ReadTransport: ClientReadTransportConfig{
				SmallRangeCopyThresholdBytes: 128 * 1024,
			},
		},
	}

	require.Equal(t, "raw_copy", client.rawReadTraceSource(16*1024))
	require.Equal(t, "raw_copy", client.rawReadTraceSource(128*1024))
	require.Equal(t, "raw_sendfile", client.rawReadTraceSource(128*1024+1))
}

func TestSamePortRawReadTransportAndGRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        5,
			ObjectTtlS:           300,
			ReadTransport: ServerReadTransportConfig{
				Enabled:  true,
				Sendfile: false,
			},
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}
	server, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("raw-host"))
	require.NoError(t, err)
	addr, err := server.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	require.Equal(t, addr, server.listener.Addr().String())
	require.Equal(t, addr, server.Host().Addr)
	require.Equal(t, addr, server.Host().PrivateAddr)

	content := []byte("hello raw cache transport")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	require.NoError(t, server.cas.Add(context.Background(), hash, content))

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte(rawReadMagic))
	require.NoError(t, err)
	require.NoError(t, writeRawReadRequest(conn, hash, 1, 3))
	status, length, err := readRawReadResponseHeader(conn)
	require.NoError(t, err)
	require.Equal(t, rawReadStatusOK, status)
	require.Equal(t, int64(3), length)
	body := make([]byte, length)
	_, err = io.ReadFull(conn, body)
	require.NoError(t, err)
	require.Equal(t, []byte("ell"), body)

	dialCtx, dialCancel := context.WithTimeout(ctx, time.Second)
	defer dialCancel()
	grpcConn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	defer grpcConn.Close()
	state, err := proto.NewCacheClient(grpcConn).GetState(dialCtx, &proto.CacheGetStateRequest{})
	require.NoError(t, err)
	require.Equal(t, Version, state.GetVersion())
}

func TestClientLocalPageFileViewsDoesNotPromoteRemotePageRegion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        4,
			ObjectTtlS:           300,
			ReadTransport: ServerReadTransportConfig{
				Enabled:  true,
				Sendfile: false,
			},
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

	content := []byte("abcdefgh")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	require.NoError(t, remoteServer.cas.Add(context.Background(), hash, content))

	localStore := newTestStore(t, 4)
	localHost := &Host{HostId: "local-host"}
	localStore.currentHost = localHost

	remoteHost := remoteServer.Host()
	require.NotNil(t, remoteHost)
	remoteHost.Addr = addr
	remoteHost.PrivateAddr = addr

	client := &Client{
		ctx:                   ctx,
		clientConfig:          ClientConfig{NTopHosts: 1, ReadTransport: ClientReadTransportConfig{Enabled: true}},
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[localHostCacheKey]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{remoteHost}},
		maxGetContentAttempts: 1,
	}
	client.AttachLocalServer(&Server{cas: localStore})

	regions, err := client.ClientLocalPageFileViews(hash, 0, int64(len(content)), ClientOptions{})
	require.ErrorIs(t, err, ErrContentNotFound)
	require.Empty(t, regions)

	dst := make([]byte, len(content))
	n, err := localStore.ReadAt(hash, 0, dst)
	require.ErrorIs(t, err, ErrContentNotFound)
	require.Zero(t, n)

	n, err = client.ReadContentInto(ctx, hash, 0, dst, ClientOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)
}

func TestClientLocalPageFileViewsReturnsLocalFinalPartialPage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	content := []byte("abcdefghij")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])

	localHost := &Host{HostId: "local-host"}
	localStore := newTestStore(t, 4)
	localStore.currentHost = localHost
	require.NoError(t, localStore.Add(ctx, hash, content))

	client := &Client{
		ctx:                   ctx,
		clientConfig:          ClientConfig{NTopHosts: 1, PreferLocalCacheHost: true},
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[localHostCacheKey]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{localHost}},
		maxGetContentAttempts: 1,
	}
	client.AttachLocalServer(&Server{cas: localStore})

	regions, err := client.ClientLocalPageFileViews(hash, 0, int64(len(content)), ClientOptions{})
	require.NoError(t, err)
	require.Len(t, regions, 3)
	require.Equal(t, 4, regions[0].Length)
	require.Equal(t, 4, regions[1].Length)
	require.Equal(t, 2, regions[2].Length)
	require.Contains(t, regions[0].Path, localStore.serverConfig.DiskCacheDir)
}

func TestRawReadSmallRangeCopyUsesCompletePageFDCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, _, hash, content := newRawReadTestClient(t, ctx, 128*1024, 4)
	before := snapshotCachePathStats()

	dst := make([]byte, 16)
	n, err := client.ReadContentInto(ctx, hash, 0, dst, ClientOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(dst)), n)
	require.Equal(t, content[:len(dst)], dst)

	n, err = client.ReadContentInto(ctx, hash, 0, dst, ClientOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(dst)), n)
	require.Equal(t, content[:len(dst)], dst)

	diff := diffCachePathStats(snapshotCachePathStats(), before)
	require.GreaterOrEqual(t, diff.serverRawCopyHits, int64(2))
	require.GreaterOrEqual(t, diff.serverRawFDCacheMiss, int64(1))
	require.GreaterOrEqual(t, diff.serverRawFDCacheHits, int64(1))
	require.Zero(t, diff.serverRawSendfileHits)
}

func TestRawReadFDCacheRejectsStalePageFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, server, hash, content := newRawReadTestClient(t, ctx, 128*1024, 4)

	dst := make([]byte, 4)
	n, err := client.ReadContentInto(ctx, hash, 0, dst, ClientOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(dst)), n)
	require.Equal(t, content[:len(dst)], dst)

	path, _, _, ok, err := server.cas.PageRegion(hash, 0, int64(len(dst)))
	require.NoError(t, err)
	require.True(t, ok)
	replacement := []byte("ABCD")
	tmp := path + ".replacement"
	require.NoError(t, os.WriteFile(tmp, replacement, 0644))
	require.NoError(t, os.Rename(tmp, path))

	before := snapshotCachePathStats()
	dst = make([]byte, len(replacement))
	n, err = client.ReadContentInto(ctx, hash, 0, dst, ClientOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(dst)), n)
	require.Equal(t, replacement, dst)

	diff := diffCachePathStats(snapshotCachePathStats(), before)
	require.GreaterOrEqual(t, diff.serverRawFDCacheStale, int64(1))
}

func newRawReadTestClient(t *testing.T, ctx context.Context, smallRangeThreshold int64, pageFDCacheSize int) (*Client, *Server, string, []byte) {
	t.Helper()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:                 t.TempDir(),
			DiskCacheMaxUsagePct:         90,
			PageSizeBytes:                4,
			SmallRangeCopyThresholdBytes: smallRangeThreshold,
			ObjectTtlS:                   300,
			ReadTransport: ServerReadTransportConfig{
				Enabled:         true,
				Sendfile:        true,
				PageFDCacheSize: pageFDCacheSize,
			},
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}
	server, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("raw-host"))
	require.NoError(t, err)
	addr, err := server.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	content := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	require.NoError(t, server.cas.Add(context.Background(), hash, content))

	host := server.Host()
	require.NotNil(t, host)
	host.Addr = addr
	host.PrivateAddr = addr

	client := &Client{
		ctx: context.Background(),
		clientConfig: ClientConfig{
			NTopHosts: 1,
			ReadTransport: ClientReadTransportConfig{
				Enabled:                      true,
				MaxActiveConnsPerHost:        4,
				MaxIdleConnsPerHost:          2,
				SmallRangeCopyThresholdBytes: smallRangeThreshold,
			},
		},
		globalConfig:          cfg.Global,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[localHostCacheKey]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{host}},
		maxGetContentAttempts: 1,
	}

	return client, server, hash, content
}
