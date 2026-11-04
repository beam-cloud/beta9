package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCPUTierPlacement(t *testing.T) {
	shared := WorkerPoolConfig{CPUClass: CPUTierShared}
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
		{CPUTierSpot, WorkerPoolConfig{CPUClass: CPUTierSpot}, false},
		{CPUTierShared, WorkerPoolConfig{CPUClass: CPUTierShared, Mode: PoolModePrivate}, false},
		{CPUTierShared, WorkerPoolConfig{CPUClass: CPUTierShared, Mode: PoolModeExternal}, false},
		{CPUTierPerformance, WorkerPoolConfig{CPUClass: CPUTierPerformance, GPUType: "A100"}, false},
	} {
		require.Equal(t, tc.want, tc.tier.MatchesPool(tc.pool), "%s %+v", tc.tier, tc.pool)
	}
}

func TestCPUTierPoolsScheduleAtTheirTiersMultiplier(t *testing.T) {
	pool, err := WorkerPoolConfig{CPUClass: CPUTierShared}.NormalizeCPU()
	require.NoError(t, err)
	require.Equal(t, CPUShareMultiplier(2.5), pool.CPUShareMultiplier)
	pool, err = WorkerPoolConfig{CPUShareMultiplier: 1.5}.NormalizeCPU()
	require.NoError(t, err)
	require.Equal(t, CPUShareMultiplier(1.5), pool.CPUShareMultiplier, "untagged pools keep their own multiplier")
	for _, invalid := range []WorkerPoolConfig{
		{CPUClass: CPUTierShared, CPUShareMultiplier: 1},
		{CPUClass: CPUTierSpot},
		{CPUClass: "turbo"},
		{CPUClass: CPUTierShared, Mode: PoolModePrivate},
		{CPUClass: CPUTierPerformance, GPUType: "A100"},
		{CPUShareMultiplier: 20},
	} {
		_, err := invalid.NormalizeCPU()
		require.Error(t, err, "%+v", invalid)
	}
}

func TestCPUTierWorkloadLimits(t *testing.T) {
	require.NoError(t, CPUTier("").Validate(64000, 1<<20, false, true))
	require.NoError(t, CPUTierShared.Validate(100, 128, false, false))
	require.NoError(t, CPUTierPerformance.Validate(64000, 1<<18, false, false))
	require.Error(t, CPUTierShared.Validate(999, 1024, true, false))
	require.Error(t, CPUTierShared.Validate(1000, 1023, true, false))
	require.ErrorContains(t, CPUTierShared.Validate(8001, 1024, false, false), "The shared tier supports at most 8 vCPU and 16 GiB RAM")
	require.Error(t, CPUTierShared.Validate(1000, 16385, false, false))
	require.ErrorIs(t, CPUTierShared.Validate(1000, 1024, false, true), ErrCPUTierIneligible)
	require.ErrorIs(t, CPUTierSpot.Validate(1000, 1024, true, true), ErrCPUTierIneligible)
	require.ErrorContains(t, CPUTier("turbo").Validate(1000, 1024, false, false), "unknown CPU tier")
}

func TestCPUTierPolicyResolve(t *testing.T) {
	enrolled := CPUTierPolicy{DefaultCPUTier: CPUTierShared}
	for _, tc := range []struct {
		policy    CPUTierPolicy
		requested CPUTier
		want      CPUTier
	}{
		{CPUTierPolicy{}, "", ""},
		{enrolled, "", CPUTierShared},
		{enrolled, CPUTierShared, CPUTierShared},
		{CPUTierPolicy{Enabled: true}, CPUTierSpot, CPUTierSpot},
	} {
		tier, err := tc.policy.Resolve(tc.requested)
		require.NoError(t, err)
		require.Equal(t, tc.want, tier)
	}
	_, err := enrolled.Resolve(CPUTierSpot)
	require.ErrorContains(t, err, "not enabled")
	_, err = CPUTierPolicy{}.Resolve(CPUTierPerformance)
	require.ErrorContains(t, err, "not enabled")

	// Spot is opt-in: a policy may allow it but never make it the default.
	spotDefault := CPUTierPolicy{Enabled: true, DefaultCPUTier: CPUTierSpot}
	_, err = spotDefault.Resolve("")
	require.ErrorContains(t, err, "must be chosen explicitly")
	tier, err := spotDefault.Resolve(CPUTierSpot)
	require.NoError(t, err)
	require.Equal(t, CPUTierSpot, tier)
}
