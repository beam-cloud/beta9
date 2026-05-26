package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkerStartConcurrencyUsesPoolAndRuntime(t *testing.T) {
	worker := &Worker{
		TotalCpu: 16000,
		PoolName: "default",
		Runtime:  ContainerRuntimeRunc.String(),
	}
	config := WorkerConfig{
		Pools: map[string]WorkerPoolConfig{
			"default": {
				ContainerRuntime:          ContainerRuntimeRunc.String(),
				ContainerStartConcurrency: 32,
			},
		},
	}

	require.Equal(t, 32, WorkerStartConcurrency(config, worker))
}

func TestWorkerStartConcurrencyCapsByWorkerCPU(t *testing.T) {
	worker := &Worker{
		TotalCpu: 1000,
		PoolName: "default",
		Runtime:  ContainerRuntimeRunc.String(),
	}
	config := WorkerConfig{
		Pools: map[string]WorkerPoolConfig{
			"default": {
				ContainerRuntime:          ContainerRuntimeRunc.String(),
				ContainerStartConcurrency: 32,
			},
		},
	}

	require.Equal(t, 2, WorkerStartConcurrency(config, worker))
}

func TestWorkerProtoRoundTripPreservesRuntime(t *testing.T) {
	worker := &Worker{
		Id:      "worker-1",
		Runtime: ContainerRuntimeRunc.String(),
	}

	require.Equal(t, ContainerRuntimeRunc.String(), NewWorkerFromProto(worker.ToProto()).Runtime)
}
