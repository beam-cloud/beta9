package worker

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestMicroVMStartupRetainsReservedHostCPUs(t *testing.T) {
	for _, test := range []struct {
		runtime   string
		blockRoot bool
		forced    bool
		want      string
	}{
		{runtime: "microvm", blockRoot: true, want: "2-3"},
		{runtime: "gvisor", want: ""},
		{runtime: "runc", want: ""},
		{runtime: "another-block-runtime", blockRoot: true, want: ""},
		{runtime: "gvisor", forced: true, want: "2-3"},
	} {
		t.Run(test.runtime+test.want, func(t *testing.T) {
			rt := &mockResourceRuntime{mockRuntime: mockRuntime{name: test.runtime, capabilities: runtime.Capabilities{BlockRoot: test.blockRoot}}}
			instances := common.NewSafeMap[*ContainerInstance]()
			instances.Set("microvm-cpu-affinity", &ContainerInstance{Id: "microvm-cpu-affinity", Runtime: rt, CPUSet: "2-3"})
			worker := &Worker{runtime: rt, containerInstances: instances, config: types.AppConfig{Worker: types.WorkerConfig{ContainerResourceLimits: types.ContainerResourceLimitsConfig{CPUEnforced: true, CPUAffinityEnforced: true}}}}
			request := &types.ContainerRequest{ContainerId: "microvm-cpu-affinity", EntryPoint: []string{"true"}, Cpu: 2000, Memory: 128, Stub: types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)}}}
			if test.forced {
				request.Stub.Config = `{"_beta9_force_resource_limits":true}`
			}
			spec, err := worker.specFromRequest(request, &ContainerOptions{BindPorts: []int{8001}})
			require.NoError(t, err)
			require.NotNil(t, spec.Linux.Resources.CPU)
			require.Equal(t, test.want, spec.Linux.Resources.CPU.Cpus)
			require.Nil(t, spec.Linux.Resources.CPU.Quota, "CPU quota remains deferred during boot")
			require.Nil(t, spec.Linux.Resources.CPU.Period)
			instance, ok := instances.Get(request.ContainerId)
			require.True(t, ok)
			require.Equal(t, "2-3", instance.DeferredCPUQuota.Cpus)
			require.NoError(t, worker.applyDeferredCPUThrottle(request, instance))
			require.Equal(t, "2-3", rt.updatedResources.CPU.Cpus)
			require.Equal(t, int64(200000), *rt.updatedResources.CPU.Quota)
		})
	}
}
