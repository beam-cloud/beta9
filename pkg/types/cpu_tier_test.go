package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCPUTierPolicy(t *testing.T) {
	shared := WorkerPoolConfig{CPUClass: CPUTierShared, CPUShareMultiplier: 2.5}
	performance := WorkerPoolConfig{CPUClass: CPUTierPerformance, CPUShareMultiplier: 1}
	legacy := WorkerPoolConfig{}
	for _, tc := range []struct {
		tier CPUTier
		pool WorkerPoolConfig
		want bool
	}{
		{"", legacy, true}, {"", shared, false}, {CPUTierShared, legacy, false},
		{CPUTierShared, shared, true}, {CPUTierShared, performance, false},
		{CPUTierPerformance, performance, true}, {CPUTierPerformance, shared, false},
		{CPUTierSpot, shared, true}, {CPUTierSpot, performance, true},
		{CPUTierSpot, WorkerPoolConfig{CPUClass: CPUTierShared, CPUShareMultiplier: 1}, false},
		{CPUTierShared, WorkerPoolConfig{CPUClass: CPUTierShared, CPUShareMultiplier: 2.5, Mode: PoolModePrivate}, false},
		{CPUTierPerformance, WorkerPoolConfig{CPUClass: CPUTierPerformance, GPUType: "A100"}, false},
	} {
		require.Equal(t, tc.want, tc.tier.MatchesPool(tc.pool), "%s %+v", tc.tier, tc.pool)
	}
	require.NoError(t, CPUTierShared.Validate(100, 128, false, false))
	require.Error(t, CPUTierShared.Validate(999, 1024, true, false))
	require.Error(t, CPUTierShared.Validate(1000, 1023, true, false))
	require.Error(t, CPUTierShared.Validate(8001, 1024, false, false))
	require.Error(t, CPUTierShared.Validate(1000, 16385, false, false))
	require.Error(t, CPUTierSpot.Validate(1000, 1024, true, true))
}

func TestCPUPriceBindingHourlyCost(t *testing.T) {
	for _, tc := range []struct {
		cpu, ram string
		want     float64
	}{{"0.63", "0.135", 3.6}, {"1.1", "0.55", 8.8}, {"5", "1", 28}} {
		b := &CPUPriceBinding{Version: "immutable", CPUPriceID: "price_cpu", MemoryPriceID: "price_ram", CPUCentsPerHour: tc.cpu, MemoryCentsPerGiBHour: tc.ram}
		cost, err := b.CostPerMs(4000, 8192)
		require.NoError(t, err)
		require.InDelta(t, tc.want, cost*3600000, 1e-12)
	}
}

func TestBoundCPUResourcesNeedANewRevision(t *testing.T) {
	before := StubConfigV1{CPUTier: CPUTierShared, Runtime: Runtime{Cpu: 4000, Memory: 8192}, CPUPricing: &CPUPriceBinding{Version: "stored"}}
	edited := before
	edited.Env = []string{"KEY=value"}
	require.NoError(t, edited.ValidateCPURevision(before))
	edited.Runtime.Cpu = 8000
	require.ErrorContains(t, edited.ValidateCPURevision(before), "redeploy")
	edited = before
	edited.CPUTier = CPUTierPerformance
	require.ErrorContains(t, edited.ValidateCPURevision(before), "redeploy")
}
