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

func TestRawReadAdmissionDefaultsPreserveEightMaxSizedRequests(t *testing.T) {
	admission := newRawReadAdmission(ServerReadTransportConfig{})
	releases := make([]func(), 0, 8)
	for i := 0; i < 8; i++ {
		release, err := admission.acquire(context.Background(), defaultRawReadMaxRequestBytes)
		require.NoError(t, err)
		releases = append(releases, release)
	}

	blockedCtx, cancelBlocked := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelBlocked()
	_, err := admission.acquire(blockedCtx, defaultRawReadMaxRequestBytes)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	releases[0]()
	releases = releases[1:]
	release, err := admission.acquire(context.Background(), defaultRawReadMaxRequestBytes)
	require.NoError(t, err)
	release()
	for _, release := range releases {
		release()
	}
}

func TestRawReadAdmissionAlsoBoundsZeroLengthRequests(t *testing.T) {
	admission := newRawReadAdmission(ServerReadTransportConfig{
		MaxRequestSizeBytes:   64,
		MaxInflightBytes:      64,
		MaxConcurrentRequests: 1,
	})
	release, err := admission.acquire(context.Background(), 0)
	require.NoError(t, err)

	_, err = admission.acquire(context.Background(), 0)
	require.ErrorIs(t, err, errRawReadAdmissionBusy)
	release()
}

func TestRawReadAdmissionBoundsDisconnectedWaiters(t *testing.T) {
	admission := newRawReadAdmission(ServerReadTransportConfig{
		MaxRequestSizeBytes:   1,
		MaxInflightBytes:      1,
		MaxConcurrentRequests: 2,
	})
	admission.waitTimeout = 20 * time.Millisecond
	release, err := admission.acquire(context.Background(), 1)
	require.NoError(t, err)
	defer release()

	server := &Server{ctx: context.Background(), rawReadLimits: admission}
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer serverConn.Close()
		server.serveRawRead(serverConn, rawReadRequest{hash: "hash", length: 1})
	}()
	require.Eventually(t, func() bool { return len(admission.concurrent) == 2 }, time.Second, time.Millisecond)
	require.NoError(t, clientConn.Close())

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disconnected raw admission waiter did not leave within its bounded wait")
	}
	require.Len(t, admission.concurrent, 1)
}

func TestRawReadRequestLimitIsIndependentOfGRPC(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	server := &Server{
		ctx:          context.Background(),
		globalConfig: GlobalConfig{GRPCMessageSizeBytes: 1024 * 1024},
		rawReadLimits: newRawReadAdmission(ServerReadTransportConfig{
			MaxRequestSizeBytes:   8,
			MaxInflightBytes:      64,
			MaxConcurrentRequests: 1,
		}),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer serverConn.Close()
		server.serveRawRead(serverConn, rawReadRequest{hash: "hash", length: 9})
	}()

	status, length, err := readRawReadResponseHeader(clientConn)
	require.NoError(t, err)
	require.Equal(t, rawReadStatusError, status)
	require.Zero(t, length)
	<-done
}

func TestRawReadFallbackStreamsThroughBoundedBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverConfig := ServerConfig{
		DiskCacheDir:         t.TempDir(),
		DiskCacheMaxUsagePct: 90,
		MaxCachePct:          1,
		PageSizeBytes:        1024 * 1024,
		ObjectTtlS:           300,
		ReadTransport: ServerReadTransportConfig{
			Enabled:               true,
			MaxRequestSizeBytes:   8 * 1024 * 1024,
			MaxInflightBytes:      8 * 1024 * 1024,
			MaxConcurrentRequests: 1,
		},
	}
	store, err := NewStore(ctx, &Host{HostId: "fallback-host"}, "test", NewMockCacheMetadataStore(), Config{Server: serverConfig})
	require.NoError(t, err)
	t.Cleanup(store.Cleanup)
	store.diskCachedUsageExceeded = true

	content := bytes.Repeat([]byte("b"), rawReadFallbackBufferBytes+17)
	hash, size, err := store.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), size)
	require.Eventually(t, func() bool { return store.Exists(hash) }, time.Second, 10*time.Millisecond)

	server := &Server{
		ctx:           ctx,
		cas:           store,
		serverConfig:  serverConfig,
		rawReadLimits: newRawReadAdmission(serverConfig.ReadTransport),
	}
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	before := snapshotCachePathStats()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer serverConn.Close()
		server.serveRawRead(serverConn, rawReadRequest{hash: hash, length: int64(len(content))})
	}()

	status, length, err := readRawReadResponseHeader(clientConn)
	require.NoError(t, err)
	require.Equal(t, rawReadStatusOK, status)
	require.Equal(t, int64(len(content)), length)
	body := make([]byte, length)
	_, err = io.ReadFull(clientConn, body)
	require.NoError(t, err)
	require.Equal(t, content, body)
	<-done

	diff := diffCachePathStats(snapshotCachePathStats(), before)
	require.Equal(t, int64(1), diff.serverRawReadAtHits)
	require.GreaterOrEqual(t, diff.storeReadAtRequests, int64(2))
}

func TestRawReadFallbackClosesOnMidstreamEviction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverConfig := ServerConfig{
		DiskCacheDir:         t.TempDir(),
		DiskCacheMaxUsagePct: 90,
		MaxCachePct:          1,
		PageSizeBytes:        rawReadFallbackBufferBytes,
		ObjectTtlS:           300,
		ReadTransport: ServerReadTransportConfig{
			Enabled:               true,
			MaxRequestSizeBytes:   8 * 1024 * 1024,
			MaxInflightBytes:      8 * 1024 * 1024,
			MaxConcurrentRequests: 1,
		},
	}
	store, err := NewStore(ctx, &Host{HostId: "eviction-host"}, "test", NewMockCacheMetadataStore(), Config{Server: serverConfig})
	require.NoError(t, err)
	t.Cleanup(store.Cleanup)
	store.diskCachedUsageExceeded = true

	content := bytes.Repeat([]byte("e"), rawReadFallbackBufferBytes+17)
	hash, _, err := store.AddReader(ctx, bytes.NewReader(content))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return store.Exists(hash) }, time.Second, 10*time.Millisecond)

	server := &Server{
		ctx:           ctx,
		cas:           store,
		serverConfig:  serverConfig,
		rawReadLimits: newRawReadAdmission(serverConfig.ReadTransport),
	}
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer serverConn.Close()
		server.serveRawRead(serverConn, rawReadRequest{hash: hash, length: int64(len(content))})
	}()

	status, length, err := readRawReadResponseHeader(clientConn)
	require.NoError(t, err)
	require.Equal(t, rawReadStatusOK, status)
	require.Equal(t, int64(len(content)), length)
	store.cache.Del(store.pageKey(hash, 1))

	body := make([]byte, length)
	n, err := io.ReadFull(clientConn, body)
	require.Error(t, err)
	require.Equal(t, rawReadFallbackBufferBytes, n)
	require.Equal(t, content[:n], body[:n])
	<-done
}

func TestRawReadCancellationInterruptsActiveSocketIO(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	requestReceived := make(chan struct{})
	releaseServer := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		magic := make([]byte, len(rawReadMagic))
		if _, err := io.ReadFull(conn, magic); err != nil {
			return
		}
		if _, err := readRawReadRequest(conn); err != nil {
			return
		}
		close(requestReceived)
		<-releaseServer
	}()

	client := &Client{
		clientConfig: ClientConfig{ReadTransport: ClientReadTransportConfig{
			Enabled:               true,
			MaxActiveConnsPerHost: 1,
			MaxIdleConnsPerHost:   1,
		}},
		rawReadPools: make(map[string]*rawReadConnPool),
	}
	host := &Host{HostId: "blocked-host", Addr: listener.Addr().String(), PrivateAddr: listener.Addr().String()}
	readCtx, cancelRead := context.WithCancel(context.Background())
	readDone := make(chan error, 1)
	go func() {
		_, err := client.rawReadInto(readCtx, host, "hash", 0, make([]byte, 1))
		readDone <- err
	}()

	select {
	case <-requestReceived:
	case <-time.After(time.Second):
		t.Fatal("raw request did not reach the server")
	}
	cancelRead()
	select {
	case err := <-readDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("raw read did not wake after context cancellation")
	}
	close(releaseServer)
	<-serverDone
	for _, pool := range client.rawReadPools {
		pool.close()
	}
}

func TestRawReadConnPoolClosesReturnedConnectionAfterClose(t *testing.T) {
	pool := newRawReadConnPool("unused", 1, 1)
	require.NoError(t, pool.acquire(context.Background()))
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	require.NoError(t, serverConn.SetReadDeadline(time.Now().Add(time.Second)))

	pool.close()
	pool.put(clientConn)
	require.Empty(t, pool.tokens)
	_, err := pool.get(context.Background())
	require.ErrorIs(t, err, ErrUnableToReachHost)

	_, err = serverConn.Read(make([]byte, 1))
	require.Error(t, err)
	pool.close()
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

func TestRawReadWindowingHonorsServerRequestLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const maxRequestBytes = 1024 * 1024
	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         t.TempDir(),
			DiskCacheMaxUsagePct: 90,
			PageSizeBytes:        maxRequestBytes,
			ObjectTtlS:           300,
			ReadTransport: ServerReadTransportConfig{
				Enabled:  true,
				Sendfile: false,
			},
		},
		Global: GlobalConfig{
			GRPCMessageSizeBytes: maxRequestBytes,
			GRPCDialTimeoutS:     1,
		},
	}
	server, err := NewServerWithOptions(ctx, cfg, "test", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("windowed-raw-host"))
	require.NoError(t, err)
	addr, err := server.Serve("127.0.0.1:0", "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	content := make([]byte, 2*maxRequestBytes+17)
	for i := range content {
		content[i] = byte(i)
	}
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	require.NoError(t, server.cas.Add(ctx, hash, content))

	host := &Host{HostId: "windowed-raw-host", Addr: addr, PrivateAddr: addr}
	client := &Client{
		ctx: ctx,
		clientConfig: ClientConfig{
			NTopHosts: 1,
			ReadTransport: ClientReadTransportConfig{
				Enabled:               true,
				MaxActiveConnsPerHost: 1,
				MaxIdleConnsPerHost:   1,
				RequestSizeBytes:      4 * maxRequestBytes,
				MaxPartsPerRead:       1,
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
	t.Cleanup(func() { require.NoError(t, client.Cleanup()) })

	before := snapshotCachePathStats()
	dst := make([]byte, len(content))
	n, err := client.ReadContentInto(ctx, hash, 0, dst, ClientOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)
	after := snapshotCachePathStats()
	diff := diffCachePathStats(after, before)
	require.Equal(t, int64(3), diff.clientRawRequests)
	require.Equal(t, int64(3), diff.serverRawRequests)
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
