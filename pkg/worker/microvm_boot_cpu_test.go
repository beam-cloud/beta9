package worker

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestMicroVMBootCPUShares(t *testing.T) {
	for _, value := range []string{"", "bad", "-1", "0", "1", "262145"} {
		t.Setenv("BETA9_MICROVM_BOOT_CPU_SHARES", value)
		require.Nil(t, microVMBootCPUShares())
	}
	for value, weight := range map[string]uint64{"2": 1, "29": 2, "81": 4, "262144": 10000} {
		t.Setenv("BETA9_MICROVM_BOOT_CPU_SHARES", value)
		shares := microVMBootCPUShares()
		require.NotNil(t, shares)
		require.Equal(t, weight, 1+(*shares-2)*9999/262142)
	}
}

func TestMicroVMBootSharesKeepDeferredTenantBudgetIntact(t *testing.T) {
	t.Setenv("BETA9_MICROVM_BOOT_CPU_SHARES", "81")
	for _, name := range []string{"runc", types.ContainerRuntimeMicroVM.String()} {
		rt := &mockResourceRuntime{mockRuntime: mockRuntime{name: name}}
		instances := common.NewSafeMap[*ContainerInstance]()
		instances.Set("container-1", &ContainerInstance{Id: "container-1", Runtime: rt})
		worker := &Worker{runtime: rt, containerInstances: instances,
			config: types.AppConfig{Worker: types.WorkerConfig{ContainerResourceLimits: types.ContainerResourceLimitsConfig{CPUEnforced: true, MemoryEnforced: true}}}}
		spec, err := worker.specFromRequest(&types.ContainerRequest{ContainerId: "container-1", EntryPoint: []string{"sleep", "60"}, Cpu: 100, Memory: 128,
			Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}}}, &ContainerOptions{BindPorts: []int{8001}})
		require.NoError(t, err)
		require.Nil(t, spec.Linux.Resources.CPU.Quota)
		if name == types.ContainerRuntimeMicroVM.String() {
			require.EqualValues(t, 81, *spec.Linux.Resources.CPU.Shares)
		} else {
			require.EqualValues(t, 102, *spec.Linux.Resources.CPU.Shares)
		}
		instance, ok := instances.Get("container-1")
		require.True(t, ok)
		require.EqualValues(t, 102, *instance.DeferredCPUQuota.Shares)
		require.EqualValues(t, 10000, *instance.DeferredCPUQuota.Quota)
	}
}
