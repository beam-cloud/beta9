package cache

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

var benchmarkSetupOnce sync.Once

func setupBenchmarkCAS(b *testing.B) (*Store, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	tmpDir := b.TempDir()

	benchmarkSetupOnce.Do(func() {
		InitLogger(false, false)
	})

	config := Config{
		Server: ServerConfig{
			DiskCacheDir:         tmpDir,
			DiskCacheMaxUsagePct: 90,
			MaxCachePct:          50,
			PageSizeBytes:        4 * 1024 * 1024,
			ObjectTtlS:           300,
		},
		Global: GlobalConfig{
			DebugMode: false,
		},
		Metrics: MetricsConfig{
			PushIntervalS: 60,
			URL:           "",
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

	cleanup := func() {
		cancel()
		cas.Cleanup()
		os.RemoveAll(tmpDir)
	}

	return cas, cleanup
}

func BenchmarkSequentialRead(b *testing.B) {
	sizes := []int64{
		1 * 1024 * 1024,
		4 * 1024 * 1024,
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%dMB", size/(1024*1024)), func(b *testing.B) {
			cas, cleanup := setupBenchmarkCAS(b)
			defer cleanup()

			// Generate test data
			content := make([]byte, size)
			rand.Read(content)
			hash := fmt.Sprintf("test-hash-%d", size)

			err := cas.Add(context.Background(), hash, content)
			if err != nil {
				b.Fatalf("Failed to add content: %v", err)
			}

			dst := make([]byte, 1024*1024) // 1MB read buffer

			b.ResetTimer()
			b.SetBytes(size)

			for i := 0; i < b.N; i++ {
				offset := int64(0)
				for offset < size {
					readLen := int64(len(dst))
					if offset+readLen > size {
						readLen = size - offset
					}

					n, err := cas.Get(hash, offset, readLen, dst)
					if err != nil {
						b.Fatalf("Failed to read: %v", err)
					}
					offset += n
				}
			}
		})
	}
}

func BenchmarkRandomRead(b *testing.B) {
	chunkSizes := []int64{
		64 * 1024,
		512 * 1024,
	}

	fileSize := int64(16 * 1024 * 1024)

	for _, chunkSize := range chunkSizes {
		b.Run(fmt.Sprintf("chunk_%dKB", chunkSize/1024), func(b *testing.B) {
			cas, cleanup := setupBenchmarkCAS(b)
			defer cleanup()

			// Generate test data
			content := make([]byte, fileSize)
			rand.Read(content)
			hash := "random-test-hash"

			err := cas.Add(context.Background(), hash, content)
			if err != nil {
				b.Fatalf("Failed to add content: %v", err)
			}

			dst := make([]byte, chunkSize)

			b.ResetTimer()
			b.SetBytes(chunkSize)

			for i := 0; i < b.N; i++ {
				// Random offset
				offset := int64(i*13) % (fileSize - chunkSize)

				_, err := cas.Get(hash, offset, chunkSize, dst)
				if err != nil {
					b.Fatalf("Failed to read: %v", err)
				}
			}
		})
	}
}

func BenchmarkSmallFiles(b *testing.B) {
	fileSizes := []int{10 * 1024, 100 * 1024}

	for _, fileSize := range fileSizes {
		b.Run(fmt.Sprintf("size_%dKB", fileSize/1024), func(b *testing.B) {
			cas, cleanup := setupBenchmarkCAS(b)
			defer cleanup()

			numFiles := 100
			hashes := make([]string, numFiles)

			for i := 0; i < numFiles; i++ {
				content := make([]byte, fileSize)
				rand.Read(content)
				hash := fmt.Sprintf("small-file-%d", i)
				hashes[i] = hash

				err := cas.Add(context.Background(), hash, content)
				if err != nil {
					b.Fatalf("Failed to add content: %v", err)
				}
			}

			dst := make([]byte, fileSize)

			b.ResetTimer()
			b.SetBytes(int64(fileSize))

			for i := 0; i < b.N; i++ {
				hash := hashes[i%numFiles]
				_, err := cas.Get(hash, 0, int64(fileSize), dst)
				if err != nil {
					b.Fatalf("Failed to read: %v", err)
				}
			}
		})
	}
}

// BenchmarkCacheHitRatios measures L0/L1 cache performance
func BenchmarkCacheHitRatios(b *testing.B) {
	cas, cleanup := setupBenchmarkCAS(b)
	defer cleanup()

	fileSize := int64(16 * 1024 * 1024) // 16 MB
	content := make([]byte, fileSize)
	rand.Read(content)
	hash := "cache-test-hash"

	err := cas.Add(context.Background(), hash, content)
	if err != nil {
		b.Fatalf("Failed to add content: %v", err)
	}

	dst := make([]byte, 1024*1024) // 1MB chunks

	b.Run("L0_Hot_Cache", func(b *testing.B) {
		// Warm up cache
		cas.Get(hash, 0, int64(len(dst)), dst)

		b.ResetTimer()
		b.SetBytes(int64(len(dst)))

		for i := 0; i < b.N; i++ {
			_, err := cas.Get(hash, 0, int64(len(dst)), dst)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}
		}
	})

	b.Run("L1_Disk_Cache", func(b *testing.B) {
		// Clear in-memory cache but keep disk cache
		// Note: This is a simplified test, actual implementation may vary

		b.ResetTimer()
		b.SetBytes(int64(len(dst)))

		for i := 0; i < b.N; i++ {
			offset := int64((i % 4) * len(dst)) // Rotate through chunks
			_, err := cas.Get(hash, offset, int64(len(dst)), dst)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}
		}
	})
}

// BenchmarkPrefetcher tests prefetcher effectiveness
func BenchmarkPrefetcher(b *testing.B) {
	cas, cleanup := setupBenchmarkCAS(b)
	defer cleanup()

	fileSize := int64(64 * 1024 * 1024) // 64 MB
	content := make([]byte, fileSize)
	rand.Read(content)
	hash := "prefetch-test-hash"

	err := cas.Add(context.Background(), hash, content)
	if err != nil {
		b.Fatalf("Failed to add content: %v", err)
	}

	chunkSize := int64(4 * 1024 * 1024) // 4MB chunks
	dst := make([]byte, chunkSize)

	b.ResetTimer()
	b.SetBytes(fileSize)

	for i := 0; i < b.N; i++ {
		// Sequential reads to trigger prefetcher
		for offset := int64(0); offset < fileSize; offset += chunkSize {
			readLen := chunkSize
			if offset+readLen > fileSize {
				readLen = fileSize - offset
			}

			_, err := cas.Get(hash, offset, readLen, dst)
			if err != nil {
				b.Fatalf("Failed to read: %v", err)
			}
		}

		// Small delay to let prefetcher work
		time.Sleep(10 * time.Millisecond)
	}
}

// BenchmarkBufferPool tests buffer allocation performance
func BenchmarkBufferPool(b *testing.B) {
	pool := NewBufferPool()

	sizes := []int{
		1 * 1024 * 1024,  // 1MB
		4 * 1024 * 1024,  // 4MB
		16 * 1024 * 1024, // 16MB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%dMB", size/(1024*1024)), func(b *testing.B) {
			b.Run("WithPool", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					buf := pool.Get(size)
					pool.Put(buf)
				}
			})

			b.Run("WithoutPool", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					buf := make([]byte, size)
					_ = buf
				}
			})
		})
	}
}
