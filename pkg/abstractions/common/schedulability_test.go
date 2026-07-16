package abstractions

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type fakeGPUPoolChecker struct {
	supported map[string]bool
}

func (f fakeGPUPoolChecker) HasManagedPoolForGPU(gpuType string, allowMarketplace bool) bool {
	if allowMarketplace {
		return f.supported[gpuType] || f.supported[gpuType+":marketplace"]
	}
	return f.supported[gpuType]
}

func TestStubSchedulableCPUOnly(t *testing.T) {
	ok, reason := StubSchedulable(fakeGPUPoolChecker{}, &types.StubConfigV1{})
	require.True(t, ok)
	require.Empty(t, reason)
}

func TestStubSchedulableWithSupportedGPU(t *testing.T) {
	checker := fakeGPUPoolChecker{supported: map[string]bool{"T4": true}}
	config := &types.StubConfigV1{
		Runtime: types.Runtime{Gpus: []types.GpuType{types.GpuType("T4")}},
	}

	ok, reason := StubSchedulable(checker, config)
	require.True(t, ok)
	require.Empty(t, reason)
}

func TestStubSchedulableFailsForUnsupportedGPU(t *testing.T) {
	checker := fakeGPUPoolChecker{supported: map[string]bool{}}
	config := &types.StubConfigV1{
		Runtime: types.Runtime{Gpus: []types.GpuType{types.GpuType("A6000")}},
	}

	ok, reason := StubSchedulable(checker, config)
	require.False(t, ok)
	require.Contains(t, reason, "A6000")
	require.Contains(t, reason, "beta9 machine reserve --gpu A6000")
}

func TestStubSchedulableSkipsPoolSelectorBoundStubs(t *testing.T) {
	checker := fakeGPUPoolChecker{supported: map[string]bool{}}
	config := &types.StubConfigV1{
		Runtime: types.Runtime{Gpus: []types.GpuType{types.GpuType("A6000")}},
		Pool:    &types.PoolConfig{Selector: "my-private-pool"},
	}

	ok, _ := StubSchedulable(checker, config)
	require.True(t, ok, "pool-selector-bound stubs must never fail fast")
}

func TestStubSchedulableSkipsMachinePinnedStubs(t *testing.T) {
	checker := fakeGPUPoolChecker{supported: map[string]bool{}}
	config := &types.StubConfigV1{
		Runtime:   types.Runtime{Gpus: []types.GpuType{types.GpuType("A6000")}},
		MachineID: "machine-1",
	}

	ok, _ := StubSchedulable(checker, config)
	require.True(t, ok, "machine-pinned stubs must never fail fast")
}

func TestStubSchedulableMarketplaceOptIn(t *testing.T) {
	checker := fakeGPUPoolChecker{supported: map[string]bool{"A6000:marketplace": true}}
	config := &types.StubConfigV1{
		Runtime: types.Runtime{Gpus: []types.GpuType{types.GpuType("A6000")}},
	}

	ok, _ := StubSchedulable(checker, config)
	require.False(t, ok)

	config.AllowMarketplace = true
	ok, _ = StubSchedulable(checker, config)
	require.True(t, ok)
}
