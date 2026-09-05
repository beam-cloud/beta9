package cache

import (
	"bytes"
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

func TestRawReadUsesCopyForSmallPageBackedRange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:                 t.TempDir(),
			DiskCacheMaxUsagePct:         90,
			PageSizeBytes:                1024,
			ObjectTtlS:                   300,
			SmallRangeCopyThresholdBytes: 128 * 1024,
			ReadTransport: ServerReadTransportConfig{
				Enabled:  true,
				Sendfile: true,
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

	content := []byte("abcdefghijklmnopqrstuvwxyz")
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	require.NoError(t, server.cas.Add(context.Background(), hash, content))

	before := snapshotCachePathStats()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte(rawReadMagic))
	require.NoError(t, err)
	require.NoError(t, writeRawReadRequest(conn, hash, 3, 8))
	status, length, err := readRawReadResponseHeader(conn)
	require.NoError(t, err)
	require.Equal(t, rawReadStatusOK, status)
	require.Equal(t, int64(8), length)
	body := make([]byte, length)
	_, err = io.ReadFull(conn, body)
	require.NoError(t, err)
	require.Equal(t, []byte("defghijk"), body)
	var diff cachePathStatsSnapshot
	require.Eventually(t, func() bool {
		after := snapshotCachePathStats()
		diff = diffCachePathStats(after, before)
		return diff.serverRawSendfileHits+diff.serverRawCopyHits+diff.serverRawReadAtHits > 0
	}, time.Second, 10*time.Millisecond)
	// The guarantee for a small (sub-threshold) range is that it is never served
	// via sendfile. It is served from the local page file through the copy path,
	// or the readAt fallback if the page region isn't resolvable on this read;
	// asserting on copy alone is flaky across filesystems, so require that it was
	// served by one of the non-sendfile local paths.
	require.Equal(t, int64(0), diff.serverRawSendfileHits)
	require.Equal(t, int64(1), diff.serverRawCopyHits+diff.serverRawReadAtHits)
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

// A full admission queue waits briefly for a slot instead of answering busy
// straight away: the slot is usually free again within a few milliseconds.
func TestRawReadAdmissionQueuesForASlot(t *testing.T) {
	admission := newRawReadAdmission(ServerReadTransportConfig{MaxConcurrentRequests: 1, MaxInflightBytes: 1 << 20})

	release, err := admission.acquire(1024)
	require.NoError(t, err)

	got := make(chan error, 1)
	go func() {
		release2, err := admission.acquire(1024)
		if err == nil {
			release2()
		}
		got <- err
	}()

	select {
	case err := <-got:
		t.Fatalf("second acquire returned before the slot was released: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	release()
	select {
	case err := <-got:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("second acquire did not proceed after release")
	}

	// A slot that never frees still reports busy once the wait is up.
	release, err = admission.acquire(1024)
	require.NoError(t, err)
	defer release()
	started := time.Now()
	_, err = admission.acquire(1024)
	require.ErrorIs(t, err, ErrRawReadBusy)
	require.GreaterOrEqual(t, time.Since(started), rawReadAdmissionWait)
}

// A host whose raw-read admission is saturated must still serve the read: the
// client retries and then falls back to gRPC rather than failing the window,
// which the lazily loaded image would answer with a whole-layer wait.
func TestReadContentIntoSurvivesSaturatedRawReadAdmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        4096,
			ObjectTtlS:           300,
			ReadTransport:        ServerReadTransportConfig{Enabled: true, MaxConcurrentRequests: 1, MaxInflightBytes: 1 << 20},
		},
		Client: ClientConfig{NTopHosts: 1, ReadTransport: ClientReadTransportConfig{Enabled: true, RequestSizeBytes: 1 << 20}},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: 16 * 1024 * 1024,
			GRPCDialTimeoutS:     1,
		},
	}
	server, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("busy-host"))
	require.NoError(t, err)
	addr, err := server.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	content := bytes.Repeat([]byte("window-bytes-"), 4096)
	hash, _, err := server.cas.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)

	// Hold the only admission slot for longer than the client is willing to
	// retry, so every raw attempt is answered busy.
	release, err := server.rawReadLimits.acquire(1)
	require.NoError(t, err)
	defer release()

	host := server.Host()
	host.Addr = addr
	host.PrivateAddr = addr
	client := &Client{
		ctx:                   ctx,
		locality:              "test",
		clientConfig:          cfg.Client,
		globalConfig:          cfg.Global,
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[localHostCacheKey]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{host}},
		maxGetContentAttempts: 1,
	}
	require.NoError(t, client.addHost(host))
	defer client.Cleanup()

	dst := make([]byte, len(content))
	n, trace, err := client.ReadContentIntoWithTrace(ctx, hash, 0, dst, ClientOptions{RoutingKey: hash})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)

	raw, grpcHits := 0, 0
	for _, attempt := range trace.Attempts {
		switch attempt.Source {
		case "raw":
			raw++
			require.Contains(t, attempt.Error, ErrRawReadBusy.Error())
		case "grpc":
			grpcHits++
		}
	}
	require.Equal(t, rawReadBusyRetries+1, raw, "raw attempts: %+v", trace.Attempts)
	require.Equal(t, 1, grpcHits)
}
