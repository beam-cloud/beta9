package scheduler

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestWorkerCachePoolDiskOverrides(t *testing.T) {
	config := types.AppConfig{
		Worker: types.WorkerConfig{CacheEnabled: true},
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:   true,
				HostPath:  "/var/lib/beta9/cache",
				MountPath: "/var/lib/beta9/cache",
			},
		},
	}
	poolConfig := types.WorkerPoolConfig{
		Cache: types.WorkerPoolCacheConfig{
			Disk: types.WorkerPoolCacheDiskConfig{
				HostPath:  "/mnt/nvme/beta9/cache",
				MountPath: "/cache-disk",
			},
		},
	}

	require.True(t, workerCacheEnabled(config, poolConfig))
	require.Equal(t, "/mnt/nvme/beta9/cache", workerCacheHostPath(config, poolConfig))
	require.Equal(t, "/cache-disk", workerCacheMountPath(config, poolConfig))
}

func TestWorkerCachePoolCanDisableDiskCache(t *testing.T) {
	disabled := false
	config := types.AppConfig{
		Worker: types.WorkerConfig{CacheEnabled: true},
		Cache: cache.Config{
			Enabled: true,
			Disk:    cache.DiskConfig{Enabled: true},
		},
	}
	poolConfig := types.WorkerPoolConfig{
		Cache: types.WorkerPoolCacheConfig{
			Disk: types.WorkerPoolCacheDiskConfig{Enabled: &disabled},
		},
	}

	require.False(t, workerCacheEnabled(config, poolConfig))
}

func TestWorkerCacheHostPathIsCreatedForLocalPools(t *testing.T) {
	config, poolConfig := cacheVolumeTestConfig()
	controller := &LocalKubernetesWorkerPoolController{
		config:           config,
		workerPoolConfig: poolConfig,
	}

	volume := requireCacheVolume(t, controller.getWorkerVolumes(1024))
	require.Equal(t, "/mnt/nvme/beta9/cache", volume.HostPath.Path)
	require.NotNil(t, volume.HostPath.Type)
	require.Equal(t, corev1.HostPathDirectoryOrCreate, *volume.HostPath.Type)
}

func TestWorkerCacheHostPathIsCreatedForExternalPools(t *testing.T) {
	config, poolConfig := cacheVolumeTestConfig()
	controller := &LegacyExternalWorkerPoolController{
		config:           config,
		workerPoolConfig: poolConfig,
	}

	volume := requireCacheVolume(t, controller.getWorkerVolumes(1024))
	require.Equal(t, "/mnt/nvme/beta9/cache", volume.HostPath.Path)
	require.NotNil(t, volume.HostPath.Type)
	require.Equal(t, corev1.HostPathDirectoryOrCreate, *volume.HostPath.Type)
}

func cacheVolumeTestConfig() (types.AppConfig, types.WorkerPoolConfig) {
	config := types.AppConfig{
		Worker: types.WorkerConfig{CacheEnabled: true},
		Cache: cache.Config{
			Enabled: true,
			Disk: cache.DiskConfig{
				Enabled:   true,
				HostPath:  "/var/lib/beta9/cache",
				MountPath: "/var/lib/beta9/cache",
			},
		},
	}
	poolConfig := types.WorkerPoolConfig{
		Cache: types.WorkerPoolCacheConfig{
			Disk: types.WorkerPoolCacheDiskConfig{
				HostPath: "/mnt/nvme/beta9/cache",
			},
		},
	}

	return config, poolConfig
}

func requireCacheVolume(t *testing.T, volumes []corev1.Volume) corev1.Volume {
	t.Helper()

	for _, volume := range volumes {
		if volume.Name == cacheVolumeName {
			require.NotNil(t, volume.HostPath)
			return volume
		}
	}

	t.Fatalf("expected cache volume to be attached")
	return corev1.Volume{}
}
