package cache

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	mathrand "math/rand"
	"sync"
	"sync/atomic"
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
	client, hash := newRawReadBenchmarkSetup(b, rawReadBenchmarkSetup{
		pageSize:                  1024 * 1024,
		smallRangeThreshold:       128 * 1024,
		clientSmallRangeThreshold: 128 * 1024,
	})
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

type rawReadBenchmarkSetup struct {
	pageSize                  int64
	smallRangeThreshold       int64
	clientSmallRangeThreshold int64
	sendfile                  bool
	pageFDCacheSize           int
}

func BenchmarkClientRawReadIntoMatrix(b *testing.B) {
	sizes := []int{16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024}
	patterns := []string{"sequential", "random"}
	concurrencyLevels := []int{1, 8, 32, 64}
	sendfileModes := []bool{false, true}
	thresholdModes := []struct {
		name      string
		threshold int64
	}{
		{name: "threshold_off", threshold: 0},
		{name: "threshold_128KiB", threshold: 128 * 1024},
	}

	for _, size := range sizes {
		size := size
		for _, pattern := range patterns {
			pattern := pattern
			for _, concurrency := range concurrencyLevels {
				concurrency := concurrency
				for _, sendfile := range sendfileModes {
					sendfile := sendfile
					for _, thresholdMode := range thresholdModes {
						thresholdMode := thresholdMode
						name := fmt.Sprintf("size_%s/%s/concurrency_%d/sendfile_%t/%s", rawBenchSizeName(size), pattern, concurrency, sendfile, thresholdMode.name)
						b.Run(name, func(b *testing.B) {
							client, hash := newRawReadBenchmarkClient(b, sendfile, thresholdMode.threshold)
							runRawReadBenchmark(b, client, hash, size, pattern, concurrency)
						})
					}
				}
			}
		}
	}
}

func newRawReadBenchmarkClient(b *testing.B, sendfile bool, smallRangeThreshold int64) (*Client, string) {
	return newRawReadBenchmarkSetup(b, rawReadBenchmarkSetup{
		pageSize:            4 * 1024 * 1024,
		smallRangeThreshold: smallRangeThreshold,
		sendfile:            sendfile,
		pageFDCacheSize:     1024,
	})
}

func newRawReadBenchmarkSetup(b *testing.B, setup rawReadBenchmarkSetup) (*Client, string) {
	b.Helper()
	InitLogger(false, false)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	if setup.pageSize <= 0 {
		setup.pageSize = 4 * 1024 * 1024
	}
	cfg := Config{
		Server: ServerConfig{
			DiskCacheDir:                 b.TempDir(),
			DiskCacheMaxUsagePct:         90,
			MaxCachePct:                  0,
			PageSizeBytes:                setup.pageSize,
			SmallRangeCopyThresholdBytes: setup.smallRangeThreshold,
			ObjectTtlS:                   300,
			ReadTransport: ServerReadTransportConfig{
				Enabled:         true,
				Sendfile:        setup.sendfile,
				PageFDCacheSize: setup.pageFDCacheSize,
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
				Enabled:                      true,
				MaxActiveConnsPerHost:        64,
				MaxIdleConnsPerHost:          16,
				SmallRangeCopyThresholdBytes: setup.clientSmallRangeThreshold,
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
	return client, hash
}

func runRawReadBenchmark(b *testing.B, client *Client, hash string, size int, pattern string, concurrency int) {
	b.Helper()
	if concurrency <= 0 {
		concurrency = 1
	}
	const fileSize = int64(64 * 1024 * 1024)
	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()

	var next atomic.Int64
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dst := make([]byte, size)
			for {
				i := next.Add(1) - 1
				if i >= int64(b.N) {
					return
				}
				offset := rawBenchOffset(i, int64(size), fileSize, pattern)
				n, err := client.ReadContentInto(context.Background(), hash, offset, dst, ClientOptions{})
				if err != nil || n != int64(size) {
					b.Errorf("raw read offset %d: n=%d err=%v", offset, n, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func rawBenchOffset(i int64, size int64, fileSize int64, pattern string) int64 {
	limit := fileSize - size
	if limit <= 0 {
		return 0
	}
	switch pattern {
	case "random":
		return int64((uint64(i)*11400714819323198485 + 0x9e3779b97f4a7c15) % uint64(limit+1))
	default:
		return (int64(i) * size) % (limit + 1)
	}
}

func rawBenchSizeName(size int) string {
	switch size {
	case 16 * 1024:
		return "16KiB"
	case 32 * 1024:
		return "32KiB"
	case 64 * 1024:
		return "64KiB"
	case 128 * 1024:
		return "128KiB"
	case 1024 * 1024:
		return "1MiB"
	default:
		return fmt.Sprintf("%dB", size)
	}
}
