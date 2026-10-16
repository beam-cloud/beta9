package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCPUShareMultiplier(t *testing.T) {
	for _, multiplier := range []float64{0, 1, 1.5, 2} {
		config := RuntimeConfig{CPUShareMultiplier: multiplier}
		require.NoError(t, config.Validate())
		require.Equal(t, int64(7500*max(multiplier, 1)), config.CPUCapacity(7500))
		physical := config.PhysicalCPURequest(4001)
		require.GreaterOrEqual(t, config.CPUCapacity(physical), int64(4001))
		require.Less(t, config.CPUCapacity(physical-1), int64(4001))
	}
	for _, invalid := range []float64{-1, .5, math.NaN(), math.Inf(1)} {
		config := RuntimeConfig{CPUShareMultiplier: invalid}
		require.Error(t, config.Validate())
		require.EqualValues(t, 7500, config.CPUCapacity(7500), "invalid ratios cannot oversubscribe")
	}
	require.EqualValues(t, math.MaxInt64, (RuntimeConfig{CPUShareMultiplier: 2}).CPUCapacity(math.MaxInt64))
}

func TestCPUSharingDoesNotMultiplyStartupConcurrency(t *testing.T) {
	worker := &Worker{PoolName: "shared", TotalCpu: 8000, PhysicalCpu: 4000}
	require.EqualValues(t, 4000, worker.PhysicalCPU())
	require.Equal(t, 8, WorkerStartConcurrency(WorkerConfig{}, worker))
	worker.PhysicalCpu = 0
	require.EqualValues(t, 8000, worker.PhysicalCPU(), "older workers use their unshared total")
}
