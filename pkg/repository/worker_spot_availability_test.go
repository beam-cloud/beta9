package repository

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestImplicitProvisioningSpotIsAvailableWithZeroWorkers(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	config := types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{}}
	want := []string{}
	for _, gpu := range []types.GpuType{types.GPU_RTX_PRO6000, types.GPU_H100, types.GPU_H200, types.GPU_B200, types.GPU_B300} {
		config.Pools["spot-"+gpu.String()] = types.WorkerPoolConfig{
			GPUType: gpu.String(), Provisioning: types.WorkerPoolProvisioningConfig{Providers: []string{"nebius"}, Spot: true, MaxNodes: 16, MaxGPUs: 16},
		}
		want = append(want, gpu.String())
	}
	config.Pools["on-demand"] = types.WorkerPoolConfig{GPUType: "A6000", Provisioning: types.WorkerPoolProvisioningConfig{Providers: []string{"shadeform"}, MaxNodes: 1}}
	repo := NewWorkerRedisRepository(rdb, config)
	workers, err := repo.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)
	require.ElementsMatch(t, want, repo.GetPreemptibleGpus())
	available, err := repo.GetGpuAvailability()
	require.NoError(t, err)
	for _, gpu := range want {
		require.True(t, available[gpu], "serve admission recognizes the configured scale-from-zero Spot pool")
	}
	require.False(t, available["A6000"], "on-demand capacity is not advertised from an empty pool")
	for _, pool := range config.Pools {
		require.False(t, pool.Preemptable, "derivation only normalizes a local copy, not the shared raw config")
		require.Empty(t, pool.Mode)
	}
}
