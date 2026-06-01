package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"testing"
	"time"

	proto "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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
