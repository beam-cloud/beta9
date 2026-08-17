package scheduler

import (
	"fmt"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestCalculateMemoryQuantity(t *testing.T) {
	tests := []struct {
		percentStr string
		memory     int64
		expected   string
	}{
		{"10%", 1024, "102Mi"},
		{"25%", 1024, "256Mi"},
		{"50%", 1024, "512Mi"},
		{"75", 1024, "768Mi"},
		{"100", 1024, "1Gi"},
		{"-1", 1024, "512Mi"},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%s of %d is %v", test.percentStr, test.memory, test.expected)
		t.Run(name, func(t *testing.T) {
			quantity := calculateMemoryQuantity(test.percentStr, test.memory)
			assert.Equal(t, test.expected, quantity.String())
		})
	}
}

func TestGetPercentageWithDefault(t *testing.T) {
	tests := []struct {
		percentStr string
		expected   float32
	}{
		{"100%", 1},
		{"99%", 0.99},
		{"1%", 0.01},
		{"33", 0.33},
		{"0", 0},
		{"-1", 0},
		{"xx", 0},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%s becomes or defaults to %.2f", test.percentStr, test.expected)
		t.Run(name, func(t *testing.T) {
			value, err := parseMemoryPercentage(test.percentStr)
			if test.expected == 0 {
				assert.Error(t, err)
				assert.Equal(t, test.expected, value)
			}

			assert.Equal(t, test.expected, value)
		})
	}
}

func TestWorkerPodTerminationGracePeriodAllowsNestedCleanup(t *testing.T) {
	tests := []struct {
		name            string
		workerStopGrace int64
		want            int64
	}{
		{name: "default", workerStopGrace: 0, want: 240},
		{name: "short", workerStopGrace: 10, want: 240},
		{name: "long", workerStopGrace: 90, want: 240},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workerPodTerminationGracePeriod(tt.workerStopGrace); got != tt.want {
				t.Fatalf("workerPodTerminationGracePeriod(%d) = %d, want %d", tt.workerStopGrace, got, tt.want)
			}
		})
	}
}

func TestWorkerPodCommandUsesInitReaper(t *testing.T) {
	assert.Equal(t, []string{"/usr/bin/tini", "-g", "--", "/usr/local/bin/worker"}, workerPodCommand())
}

func TestWorkerDurableDisksHostPathUsesPoolOverride(t *testing.T) {
	assert.Equal(t, types.DefaultDurableDisksPath, workerDurableDisksHostPath(types.WorkerPoolConfig{}))
	assert.Equal(t, "/mnt/nvme/beta9/storage/durable-disks", workerDurableDisksHostPath(types.WorkerPoolConfig{
		StoragePath: "/mnt/nvme/beta9/storage",
	}))
	assert.Equal(t, "/mnt/nvme/beta9/disks", workerDurableDisksHostPath(types.WorkerPoolConfig{
		StoragePath:      "/mnt/nvme/beta9/storage",
		DurableDisksPath: "/mnt/nvme/beta9/disks",
	}))
}

func TestWorkerStateVolumesUseDedicatedSiblingHostPath(t *testing.T) {
	assert.Equal(t, types.DefaultStateVolumesPath, workerStateVolumesHostPath(types.WorkerPoolConfig{}))
	assert.Equal(t, "/mnt/nvme/beta9/storage/state-volumes", workerStateVolumesHostPath(types.WorkerPoolConfig{
		StoragePath: "/mnt/nvme/beta9/storage",
	}))
	assert.Equal(t, "/mnt/nvme/beta9/state-volumes", workerStateVolumesHostPath(types.WorkerPoolConfig{
		DurableDisksPath: "/mnt/nvme/beta9/disks",
	}))
}

func TestWorkerStateVolumeLocksUseOneNodeGlobalHostPathAcrossPools(t *testing.T) {
	first := types.WorkerPoolConfig{StoragePath: "/mnt/nvme-a/beta9"}
	second := types.WorkerPoolConfig{StoragePath: "/mnt/nvme-b/beta9"}
	assert.NotEqual(t, workerStateVolumesHostPath(first), workerStateVolumesHostPath(second))
	assert.Equal(t, types.DefaultStateVolumeLocksPath, workerStateVolumeLocksHostPath())

	local := &LocalKubernetesWorkerPoolController{workerPoolConfig: first}
	provider := &ProviderWorkerPoolController{workerPoolConfig: second}
	for name, volumes := range map[string][]corev1.Volume{
		"local": local.getWorkerVolumes(1024), "provider": provider.getWorkerVolumes(1024),
	} {
		volume := requireNamedHostPathVolume(t, volumes, stateVolumeLocksVolumeName)
		require.Equal(t, types.DefaultStateVolumeLocksPath, volume.HostPath.Path, name)
		require.NotNil(t, volume.HostPath.Type, name)
		require.Equal(t, corev1.HostPathDirectoryOrCreate, *volume.HostPath.Type, name)
	}
	for name, mounts := range map[string][]corev1.VolumeMount{
		"local": local.getWorkerVolumeMounts(), "provider": provider.getWorkerVolumeMounts(),
	} {
		mount := requireNamedVolumeMount(t, mounts, stateVolumeLocksVolumeName)
		require.Equal(t, types.DefaultStateVolumeLocksPath, mount.MountPath, name)
		require.False(t, mount.ReadOnly, name)
	}
}

func requireNamedHostPathVolume(t *testing.T, volumes []corev1.Volume, name string) corev1.Volume {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			require.NotNil(t, volume.HostPath)
			return volume
		}
	}
	t.Fatalf("volume %q is missing", name)
	return corev1.Volume{}
}

func requireNamedVolumeMount(t *testing.T, mounts []corev1.VolumeMount, name string) corev1.VolumeMount {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name == name {
			return mount
		}
	}
	t.Fatalf("volume mount %q is missing", name)
	return corev1.VolumeMount{}
}
