package cache

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	mathrand "math/rand"
	"testing"

	proto "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
)

func BenchmarkGetContentDiskCache(b *testing.B) {
	InitLogger(false, false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := b.TempDir()

	config := Config{
		Server: ServerConfig{
			DiskCacheDir:         tmpDir,
			DiskCacheMaxUsagePct: 90,
			MaxCachePct:          0,
			PageSizeBytes:        4 * 1024 * 1024,
			ObjectTtlS:           300,
		},
		Global: GlobalConfig{
			DebugMode: false,
		},
		Metrics: MetricsConfig{
			URL: "",
		},
	}

	currentHost := &Host{
		HostId: "bench-host",
		Addr:   "localhost:50051",
		RTT:    0,
	}

	mockCoordinator := NewMockCacheMetadataStore()
	cas, err := NewStore(ctx, currentHost, "local", mockCoordinator, config)
	if err != nil {
		b.Fatalf("Failed to create CAS: %v", err)
	}
	defer cas.Cleanup()

	// Store 16MB file
	fileSize := int64(16 * 1024 * 1024)
	content := make([]byte, fileSize)
	rand.Read(content)
	hash := "test-file"

	err = cas.Add(context.Background(), hash, content)
	if err != nil {
		b.Fatalf("Failed to add content: %v", err)
	}

	// Read in 4MB chunks (realistic usage)
	chunkSize := int64(4 * 1024 * 1024)
	dst := make([]byte, chunkSize)

	b.ResetTimer()
	b.SetBytes(fileSize)

	for i := 0; i < b.N; i++ {
		offset := int64(0)
		totalRead := int64(0)

		for offset < fileSize {
			readSize := chunkSize
			if offset+readSize > fileSize {
				readSize = fileSize - offset
			}

			n, err := cas.Get(hash, offset, readSize, dst)
			if err != nil {
				b.Fatalf("GetContent failed at offset %d: %v", offset, err)
			}
			if n != readSize {
				b.Fatalf("Expected %d bytes, got %d", readSize, n)
			}

			if offset == 0 && i == 0 {
				for j := int64(0); j < 1024 && j < n; j++ {
					if dst[j] != content[j] {
						b.Fatalf("Data mismatch at byte %d", j)
					}
				}
			}

			offset += n
			totalRead += n
		}

		if totalRead != fileSize {
			b.Fatalf("Read %d bytes, expected %d", totalRead, fileSize)
		}
	}
}

func benchmarkStoreWithContent(b *testing.B, pageSize int64, fileSize int64) (*Store, string, []byte) {
	b.Helper()
	InitLogger(false, false)

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	store, err := NewStore(ctx, &Host{HostId: "bench-host"}, "local", NewMockCacheMetadataStore(), Config{
		Server: ServerConfig{
			DiskCacheDir:         b.TempDir(),
			DiskCacheMaxUsagePct: 90,
			MaxCachePct:          0,
			PageSizeBytes:        pageSize,
			ObjectTtlS:           300,
		},
	})
	if err != nil {
		b.Fatalf("new store: %v", err)
	}
	b.Cleanup(store.Cleanup)

	content := make([]byte, fileSize)
	if _, err := rand.Read(content); err != nil {
		b.Fatalf("random content: %v", err)
	}
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	if err := store.Add(context.Background(), hash, content); err != nil {
		b.Fatalf("add content: %v", err)
	}

	return store, hash, content
}

func BenchmarkStoreReadAtDiskRange_1MiB(b *testing.B) {
	store, hash, _ := benchmarkStoreWithContent(b, 1024*1024, 64*1024*1024)
	dst := make([]byte, 1024*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(dst)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i%64) * int64(len(dst))
		n, err := store.ReadAt(hash, offset, dst)
		if err != nil || n != int64(len(dst)) {
			b.Fatalf("read at offset %d: n=%d err=%v", offset, n, err)
		}
	}
}

func BenchmarkStoreReadAtDiskRange_64KiBRandom(b *testing.B) {
	store, hash, _ := benchmarkStoreWithContent(b, 1024*1024, 64*1024*1024)
	dst := make([]byte, 64*1024)
	rng := mathrand.New(mathrand.NewSource(1))
	offsets := make([]int64, b.N)
	for i := range offsets {
		offsets[i] = int64(rng.Intn((64 * 1024 * 1024) - len(dst)))
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(dst)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := store.ReadAt(hash, offsets[i], dst)
		if err != nil || n != int64(len(dst)) {
			b.Fatalf("read at offset %d: n=%d err=%v", offsets[i], n, err)
		}
	}
}

func BenchmarkClientLocalReadInto(b *testing.B) {
	store, hash, _ := benchmarkStoreWithContent(b, 1024*1024, 64*1024*1024)
	localHost := &Host{HostId: "local-bench-host"}
	store.currentHost = localHost
	client := &Client{
		ctx:                   context.Background(),
		clientConfig:          ClientConfig{NTopHosts: 1},
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[localHostCacheKey]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{localHost}},
		maxGetContentAttempts: 1,
	}
	client.AttachLocalServer(&Server{cas: store})
	dst := make([]byte, 1024*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(dst)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i%64) * int64(len(dst))
		n, err := client.ReadContentInto(context.Background(), hash, offset, dst, ClientOptions{})
		if err != nil || n != int64(len(dst)) {
			b.Fatalf("read into offset %d: n=%d err=%v", offset, n, err)
		}
	}
}

func BenchmarkClientSharedLocalDiskReadInto(b *testing.B) {
	store, hash, _ := benchmarkStoreWithContent(b, 1024*1024, 64*1024*1024)
	host := &Host{HostId: "daemon-bench-host", NodeID: "node-a", CachePathID: "cache-path-a"}
	client := newSharedLocalDiskClient(store, host)
	dst := make([]byte, 1024*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(dst)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i%64) * int64(len(dst))
		n, err := client.ReadContentInto(context.Background(), hash, offset, dst, ClientOptions{RoutingKey: hash})
		if err != nil || n != int64(len(dst)) {
			b.Fatalf("shared local disk read offset %d: n=%d err=%v", offset, n, err)
		}
	}
}

func BenchmarkClientSharedLocalDiskPageViews(b *testing.B) {
	store, hash, _ := benchmarkStoreWithContent(b, 1024*1024, 64*1024*1024)
	host := &Host{HostId: "daemon-bench-host", NodeID: "node-a", CachePathID: "cache-path-a"}
	client := newSharedLocalDiskClient(store, host)

	b.ReportAllocs()
	b.SetBytes(1024 * 1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i%64) * 1024 * 1024
		views, err := client.ClientLocalPageFileViews(hash, offset, 1024*1024, ClientOptions{RoutingKey: hash})
		if err != nil || len(views) != 1 || views[0].Length != 1024*1024 {
			b.Fatalf("shared local disk page views offset %d: views=%d err=%v", offset, len(views), err)
		}
	}
}

func BenchmarkClientRawReadInto(b *testing.B) {
	InitLogger(false, false)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:         b.TempDir(),
			DiskCacheMaxUsagePct: 90,
			MaxCachePct:          0,
			PageSizeBytes:        1024 * 1024,
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
	server, err := NewServerWithOptions(ctx, cfg, "local", WithServerMetadataStore(NewMockCacheMetadataStore()), WithServerHostID("raw-bench-host"))
	if err != nil {
		b.Fatalf("new server: %v", err)
	}
	b.Cleanup(func() { _ = server.Close() })
	addr, err := server.Serve("127.0.0.1:0", "")
	if err != nil {
		b.Fatalf("serve: %v", err)
	}

	content := make([]byte, 64*1024*1024)
	if _, err := rand.Read(content); err != nil {
		b.Fatalf("random content: %v", err)
	}
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	if err := server.cas.Add(context.Background(), hash, content); err != nil {
		b.Fatalf("add content: %v", err)
	}

	host := &Host{HostId: "raw-bench-host", Addr: addr, PrivateAddr: addr}
	client := &Client{
		ctx: context.Background(),
		clientConfig: ClientConfig{
			NTopHosts: 1,
			ReadTransport: ClientReadTransportConfig{
				Enabled:               true,
				MaxActiveConnsPerHost: 64,
				MaxIdleConnsPerHost:   16,
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
	dst := make([]byte, 1024*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(dst)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i%64) * int64(len(dst))
		n, err := client.ReadContentInto(context.Background(), hash, offset, dst, ClientOptions{})
		if err != nil || n != int64(len(dst)) {
			b.Fatalf("raw read offset %d: n=%d err=%v", offset, n, err)
		}
	}
}
