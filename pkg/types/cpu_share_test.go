package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCPUShareMultiplierValidate(t *testing.T) {
	for _, valid := range []CPUShareMultiplier{0, 1, 1.01, 1.1, 1.25, 1.4, 2.3, 4, 15.99, 16} {
		require.NoError(t, valid.Validate(), "%v", valid)
	}
	for _, invalid := range []CPUShareMultiplier{-1, 0.5, 0.99, 1.005, 1.333, 16.01, 100, CPUShareMultiplier(math.NaN()), CPUShareMultiplier(math.Inf(1))} {
		require.Error(t, invalid.Validate(), "%v", invalid)
	}
}

func TestCPUShareMultiplierConversions(t *testing.T) {
	for _, unshared := range []CPUShareMultiplier{0, 1} {
		require.EqualValues(t, 16000, unshared.Schedulable(16000))
		require.EqualValues(t, 15750, unshared.Physical(15750))
	}

	shared := CPUShareMultiplier(1.5)
	require.EqualValues(t, 24000, shared.Schedulable(16000))
	require.EqualValues(t, 16000, shared.Physical(24000))
	require.EqualValues(t, 16001, shared.Physical(24001))
	require.EqualValues(t, 0, shared.Physical(0))
}

// A worker sized by Physical must always hold the request that sized it, and
// be no larger than needed. Float math missed by a millicore for requests like
// 15750m at 1.4x, so the scheduler would provision a worker it could never use.
func TestCPUShareMultiplierPhysicalAlwaysHoldsRequest(t *testing.T) {
	check := func(multiplier CPUShareMultiplier, cpu int64) {
		physical := multiplier.Physical(cpu)
		if multiplier.Schedulable(physical) < cpu || multiplier.Schedulable(physical-1) >= cpu {
			t.Fatalf("multiplier=%v cpu=%d: physical %d schedules %d", multiplier, cpu, physical, multiplier.Schedulable(physical))
		}
	}
	for hundredths := int64(100); hundredths <= 1600; hundredths++ {
		multiplier := CPUShareMultiplier(float64(hundredths) / 100)
		require.NoError(t, multiplier.Validate())
		for cpu := int64(250); cpu <= 256000; cpu += 250 {
			check(multiplier, cpu)
		}
		for cpu := int64(1); cpu <= 20000; cpu += 7 {
			check(multiplier, cpu)
		}
	}
}
