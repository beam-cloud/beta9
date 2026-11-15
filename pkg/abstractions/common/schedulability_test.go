package abstractions

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

const podStub = types.StubType(types.StubTypePodDeployment)

// fakeGPUPoolChecker has pools for the supported GPU types, and pools whose
// capacity can be taken back for the spot ones.
type fakeGPUPoolChecker struct {
	supported map[string]bool
	spot      map[string]bool
}

func (f fakeGPUPoolChecker) HasManagedPoolForGPU(gpuType string, preemptable bool) bool {
	return f.supported[gpuType] || preemptable && f.spot[gpuType]
}

func TestStubSchedulableCPUOnly(t *testing.T) {
	ok, reason := StubSchedulable(fakeGPUPoolChecker{}, podStub, &types.StubConfigV1{})
	require.True(t, ok)
	require.Empty(t, reason)
}

func TestStubSchedulableWithSupportedGPU(t *testing.T) {
	checker := fakeGPUPoolChecker{supported: map[string]bool{"T4": true}}
	config := &types.StubConfigV1{
		Runtime: types.Runtime{Gpus: []types.GpuType{types.GpuType("T4")}},
	}

	ok, reason := StubSchedulable(checker, podStub, config)
	require.True(t, ok)
	require.Empty(t, reason)
}

func TestStubSchedulableFailsForUnsupportedGPU(t *testing.T) {
	checker := fakeGPUPoolChecker{supported: map[string]bool{}}
	config := &types.StubConfigV1{
		Runtime: types.Runtime{Gpus: []types.GpuType{types.GpuType("A6000")}},
	}

	ok, reason := StubSchedulable(checker, podStub, config)
	require.False(t, ok)
	require.Contains(t, reason, "A6000")
	require.Contains(t, reason, "beta9 machine reserve --gpu A6000")
}

// Capacity that can be taken back runs only work that may be stopped at any
// time: an endpoint, whose requests go to another container.
func TestStubSchedulableOnSpotCapacityOnlyIfItMayBeInterrupted(t *testing.T) {
	checker := fakeGPUPoolChecker{spot: map[string]bool{"H100": true}}
	config := &types.StubConfigV1{
		Runtime: types.Runtime{Gpus: []types.GpuType{types.GpuType("H100")}},
	}

	for _, stubType := range []string{types.StubTypeEndpointDeployment, types.StubTypeASGIDeployment, types.StubTypeEndpointServe} {
		ok, _ := StubSchedulable(checker, types.StubType(stubType), config)
		require.True(t, ok, stubType)
	}
	for _, stubType := range []string{types.StubTypePodDeployment, types.StubTypeFunction, types.StubTypeTaskQueueDeployment, types.StubTypeSandbox} {
		ok, reason := StubSchedulable(checker, types.StubType(stubType), config)
		require.False(t, ok, stubType)
		require.Contains(t, reason, "H100")
	}
}

func TestStubSchedulableSkipsPoolSelectorBoundStubs(t *testing.T) {
	checker := fakeGPUPoolChecker{supported: map[string]bool{}}
	config := &types.StubConfigV1{
		Runtime: types.Runtime{Gpus: []types.GpuType{types.GpuType("A6000")}},
		Pool:    &types.PoolConfig{Selector: "my-private-pool"},
	}

	ok, _ := StubSchedulable(checker, podStub, config)
	require.True(t, ok, "pool-selector-bound stubs must never fail fast")
}

func TestStubSchedulableSkipsMachinePinnedStubs(t *testing.T) {
	checker := fakeGPUPoolChecker{supported: map[string]bool{}}
	config := &types.StubConfigV1{
		Runtime:   types.Runtime{Gpus: []types.GpuType{types.GpuType("A6000")}},
		MachineID: "machine-1",
	}

	ok, _ := StubSchedulable(checker, podStub, config)
	require.True(t, ok, "machine-pinned stubs must never fail fast")
}
