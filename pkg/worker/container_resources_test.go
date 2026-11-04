package worker

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCPUWeightFollowsReservedCPU(t *testing.T) {
	resources := NewStandardResources()
	cpu := resources.GetCPU(&types.ContainerRequest{Cpu: 2000, ReservedCpu: 5000})
	require.Equal(t, uint64(5120), *cpu.Shares)
	require.Equal(t, int64(200_000), *cpu.Quota)
	require.Equal(t, maxCPUShares, *resources.GetCPU(&types.ContainerRequest{Cpu: 512_000}).Shares)
}

func TestGvisorMemoryIncludesRuntimeHeadroom(t *testing.T) {
	request := &types.ContainerRequest{Memory: 1024}

	runcMemory := NewRuncResources().GetMemory(request)
	gvisorMemory := NewGvisorResources().GetMemory(request)

	require.Equal(t, *runcMemory.Limit+gvisorRuntimeMemoryHeadroomBytes, *gvisorMemory.Limit)
	require.Equal(t, *gvisorMemory.Limit, *gvisorMemory.Swap)
	require.Equal(t, *runcMemory.Reservation, *gvisorMemory.Reservation)
}
