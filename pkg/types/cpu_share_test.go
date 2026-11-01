package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCPUShareMultiplier(t *testing.T) {
	for _, valid := range []CPUShareMultiplier{0, 1, 1.1, 1.25, 16} {
		require.NoError(t, valid.Validate(), "%v", valid)
	}
	for _, invalid := range []CPUShareMultiplier{-1, 0.5, 1.005, 16.01, CPUShareMultiplier(math.NaN())} {
		require.Error(t, invalid.Validate(), "%v", invalid)
	}
	require.EqualValues(t, 16000, CPUShareMultiplier(0).Schedulable(16000))
	require.EqualValues(t, 24000, CPUShareMultiplier(1.5).Schedulable(16000))
	require.EqualValues(t, 22050, CPUShareMultiplier(1.4).Schedulable(15750))
}
