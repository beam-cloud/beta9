package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerlessGPUPreemptable(t *testing.T) {
	for _, gpu := range []GpuType{GPU_RTX_PRO6000, GPU_H100, GPU_H200, GPU_B200, GPU_B300} {
		t.Run(gpu.String(), func(t *testing.T) {
			config := &StubConfigV1{Runtime: Runtime{Gpu: gpu, GpuCount: 1}}
			require.True(t, config.ServerlessGPUPreemptable())
		})
	}

	gpu := func() *StubConfigV1 { return &StubConfigV1{Runtime: Runtime{Gpu: GPU_H100, GpuCount: 1}} }
	for _, test := range []struct {
		name   string
		config *StubConfigV1
	}{
		{"nil", nil},
		{"CPU", &StubConfigV1{}},
		{"explicit no GPU", &StubConfigV1{Runtime: Runtime{Gpu: NO_GPU}}},
		{"named pool", func() *StubConfigV1 { c := gpu(); c.Pool = &PoolConfig{Name: "shared-gpu"}; return c }()},
		{"private selector", func() *StubConfigV1 { c := gpu(); c.Pool = &PoolConfig{Selector: "private-gpu"}; return c }()},
		{"machine pin", func() *StubConfigV1 { c := gpu(); c.MachineID = "machine-1"; return c }()},
		{"reservation before selector assignment", func() *StubConfigV1 { c := gpu(); c.Pool = &PoolConfig{Nodes: 1}; return c }()},
		{"provider reservation", func() *StubConfigV1 { c := gpu(); c.Pool = &PoolConfig{Providers: []string{"nebius"}}; return c }()},
		{"VM", func() *StubConfigV1 { c := gpu(); c.UseVM = true; return c }()},
		{"managed endpoint", func() *StubConfigV1 { c := gpu(); c.ManagedEndpoint = &ManagedEndpointStubConfig{}; return c }()},
	} {
		t.Run(test.name, func(t *testing.T) { require.False(t, test.config.ServerlessGPUPreemptable()) })
	}

	t.Run("ordinary GPU preference list", func(t *testing.T) {
		config := &StubConfigV1{Runtime: Runtime{Gpus: []GpuType{NO_GPU, GPU_H200, GPU_H100}, GpuCount: 1}}
		require.True(t, config.ServerlessGPUPreemptable())
	})
}
