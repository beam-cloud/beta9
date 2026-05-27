package worker

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestNormalizeCacheConfigAppliesPoolDiskOverrides(t *testing.T) {
	config := types.AppConfig{
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:     true,
				HostPath:    "/var/lib/beta9/cache",
				MountPath:   "/var/lib/beta9/cache",
				MaxUsagePct: 0.95,
			},
		},
	}
	poolConfig := types.WorkerPoolConfig{
		Cache: types.WorkerPoolCacheConfig{
			Disk: types.WorkerPoolCacheDiskConfig{
				HostPath:    "/mnt/a100-cache",
				MountPath:   "/cache-disk",
				MaxUsagePct: 0.85,
			},
		},
	}

	got := normalizeCacheConfig(config, poolConfig, "node-a", "gpu")

	require.Equal(t, "/mnt/a100-cache", got.Disk.HostPath)
	require.Equal(t, "/cache-disk", got.Disk.MountPath)
	require.Equal(t, 0.85, got.Disk.MaxUsagePct)
	require.Equal(t, filepath.Join("/cache-disk", "gpu", "node-a"), got.Server.DiskCacheDir)
	require.Equal(t, 0.85, got.Server.DiskCacheMaxUsagePct)
}

func TestWorkerCacheManagerDisabledWhenPoolDiskCacheDisabled(t *testing.T) {
	disabled := false
	manager := &WorkerCacheManager{
		config: types.AppConfig{
			Worker: types.WorkerConfig{CacheEnabled: true},
			Cache: cache.Config{
				Enabled: true,
				Disk:    cache.DiskConfig{Enabled: true},
			},
		},
		poolConfig: types.WorkerPoolConfig{
			Cache: types.WorkerPoolCacheConfig{
				Disk: types.WorkerPoolCacheDiskConfig{Enabled: &disabled},
			},
		},
	}

	require.False(t, manager.enabled())
}

func TestWorkerCacheManagerDrainStopsRegistrationLoopOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancelCount := 0
	manager := &WorkerCacheManager{
		registrationCancel: func() {
			cancelCount++
			cancel()
		},
	}

	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		<-ctx.Done()
	}()

	require.NoError(t, manager.Drain())
	require.NoError(t, manager.Drain())
	require.Equal(t, 1, cancelCount)
}

func TestCacheLogicalHostIDDeduplicatesSharedNodeCachePath(t *testing.T) {
	first := cacheLogicalHostID("default", "default", "node-a", "/var/lib/beta9/cache/default/node-a")
	second := cacheLogicalHostID("default", "default", "node-a", "/var/lib/beta9/cache/default/node-a")
	otherPath := cacheLogicalHostID("default", "default", "node-a", "/mnt/cache/default/node-a")

	require.Equal(t, first, second)
	require.NotEqual(t, first, otherPath)
}
