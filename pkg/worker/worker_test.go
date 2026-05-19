package worker

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCalculateCPUShares(t *testing.T) {
	tests := []struct {
		name       string
		millicores int64
		wantShares uint64
		wantQuota  int64
	}{
		{
			name:       "100m",
			millicores: 100,
			wantShares: 102,
			wantQuota:  10_000,
		},
		{
			name:       "250m",
			millicores: 250,
			wantShares: 256,
			wantQuota:  25_000,
		},
		{
			name:       "1000m",
			millicores: 1000,
			wantShares: 1024,
			wantQuota:  100_000,
		},
		{
			name:       "2000m",
			millicores: 2000,
			wantShares: 2048,
			wantQuota:  200_000,
		},
		{
			name:       "32000m",
			millicores: 32_000,
			wantShares: 32_768,
			wantQuota:  3_200_000,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := calculateCPUShares(test.millicores)
			if got != test.wantShares {
				t.Errorf("calculateCPUShares(%d) = %d, want %d", test.millicores, got, test.wantShares)
			}

			gotQuota := calculateCPUQuota(test.millicores)
			if gotQuota != test.wantQuota {
				t.Errorf("calculateCPUQuota(%d) = %d, want %d", test.millicores, gotQuota, test.wantQuota)
			}
		})
	}
}

func TestContainerStartLimitForRuntimeUsesRuntimeName(t *testing.T) {
	t.Setenv("WORKER_CONTAINER_START_CONCURRENCY", "")

	require.Equal(t, 16, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeRunc.String(), 16, 2))
	require.Equal(t, 2, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeGvisor.String(), 16, 2))
	require.Equal(t, 16, containerStartLimitForRuntimeWithDefaults("unknown", 16, 2))
}

func TestContainerStartLimitForRuntimeAllowsExplicitOverride(t *testing.T) {
	t.Setenv("WORKER_CONTAINER_START_CONCURRENCY", "4")

	require.Equal(t, 4, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeRunc.String(), 16, 2))
	require.Equal(t, 4, containerStartLimitForRuntimeWithDefaults(types.ContainerRuntimeGvisor.String(), 16, 2))
}
