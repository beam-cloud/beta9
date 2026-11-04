package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// testCPUTiers matches the worker.cpuTiers defaults in config.default.yaml.
var testCPUTiers = CPUTiers{
	CPUTierShared:      {CPUShareMultiplier: 2.5, MaxCpu: 8000, MaxMemory: 16384},
	CPUTierPerformance: {CPUShareMultiplier: 1},
	CPUTierSpot:        {BorrowsFrom: []CPUTier{CPUTierShared, CPUTierPerformance}, DrainSeconds: 30},
}

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
		require.Equal(t, tc.want, testCPUTiers.MatchesPool(tc.tier, tc.pool), "%s %+v", tc.tier, tc.pool)
	}
}

func TestCPUTierPoolsScheduleAtTheirTiersMultiplier(t *testing.T) {
	pool, err := testCPUTiers.NormalizePool(WorkerPoolConfig{CPUClass: CPUTierShared})
	require.NoError(t, err)
	require.Equal(t, CPUShareMultiplier(2.5), pool.CPUShareMultiplier)
	pool, err = testCPUTiers.NormalizePool(WorkerPoolConfig{CPUShareMultiplier: 1.5})
	require.NoError(t, err)
	require.Equal(t, CPUShareMultiplier(1.5), pool.CPUShareMultiplier, "untagged pools keep their own multiplier")
	for _, invalid := range []WorkerPoolConfig{
		{CPUClass: CPUTierShared, CPUShareMultiplier: 1},
		{CPUClass: CPUTierSpot},
		{CPUClass: "turbo"},
		{CPUClass: CPUTierShared, Mode: PoolModePrivate},
		{CPUShareMultiplier: 20},
	} {
		_, err := testCPUTiers.NormalizePool(invalid)
		require.Error(t, err, "%+v", invalid)
	}

	require.NoError(t, testCPUTiers.Validate())
	require.Error(t, CPUTiers{CPUTierSpot: {BorrowsFrom: []CPUTier{CPUTierShared}}}.Validate())
	require.Error(t, CPUTiers{CPUTierShared: {}, "a": {BorrowsFrom: []CPUTier{"b"}}, "b": {BorrowsFrom: []CPUTier{CPUTierShared}}}.Validate())
	require.Error(t, CPUTiers{CPUTierShared: {CPUShareMultiplier: 0.5}}.Validate())
}

func TestCPUTierWorkloadLimits(t *testing.T) {
	require.NoError(t, testCPUTiers.ValidateWorkload("", 64000, 1<<20, false, true))
	require.NoError(t, testCPUTiers.ValidateWorkload(CPUTierShared, 100, 128, false, false))
	require.NoError(t, testCPUTiers.ValidateWorkload(CPUTierPerformance, 64000, 1<<18, false, false))
	require.Error(t, testCPUTiers.ValidateWorkload(CPUTierShared, 999, 1024, true, false))
	require.Error(t, testCPUTiers.ValidateWorkload(CPUTierShared, 1000, 1023, true, false))
	require.ErrorContains(t, testCPUTiers.ValidateWorkload(CPUTierShared, 8001, 1024, false, false), "The shared tier supports at most 8 vCPU")
	require.ErrorContains(t, testCPUTiers.ValidateWorkload(CPUTierShared, 1000, 16385, false, false), "at most 16 GiB RAM")
	require.ErrorIs(t, testCPUTiers.ValidateWorkload(CPUTierSpot, 1000, 1024, true, true), ErrCPUTierIneligible)
	require.ErrorContains(t, testCPUTiers.ValidateWorkload("turbo", 1000, 1024, false, false), "unknown CPU tier")
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
