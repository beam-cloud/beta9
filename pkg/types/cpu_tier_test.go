package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCPUTierPlacement(t *testing.T) {
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
}
