package worker

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestGvisorMemoryIncludesRuntimeHeadroom(t *testing.T) {
	request := &types.ContainerRequest{Memory: 1024}

	runcMemory := NewRuncResources().GetMemory(request)
	gvisorMemory := NewGvisorResources().GetMemory(request)

	require.Equal(t, *runcMemory.Limit+gvisorRuntimeMemoryHeadroomBytes, *gvisorMemory.Limit)
	require.Equal(t, *gvisorMemory.Limit, *gvisorMemory.Swap)
	require.Equal(t, *runcMemory.Reservation, *gvisorMemory.Reservation)
}
