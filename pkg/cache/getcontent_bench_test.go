package cache

import (
	"context"
	"crypto/rand"
	"testing"
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

	mockCoordinator := NewMockRegistry()
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
