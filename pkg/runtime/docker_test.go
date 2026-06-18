package runtime

import (
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestAddDockerInDockerCapabilities(t *testing.T) {
	spec := &specs.Spec{
		Process: &specs.Process{
			Capabilities: &specs.LinuxCapabilities{
				Bounding:  []string{"CAP_CHOWN"},
				Effective: []string{"CAP_CHOWN"},
				Permitted: []string{"CAP_CHOWN"},
			},
		},
	}

	AddDockerInDockerCapabilities(spec)

	for _, cap := range DockerInDockerCapabilities() {
		require.Contains(t, spec.Process.Capabilities.Bounding, cap)
		require.Contains(t, spec.Process.Capabilities.Effective, cap)
		require.Contains(t, spec.Process.Capabilities.Permitted, cap)
		require.Contains(t, spec.Process.Capabilities.Inheritable, cap)
	}
	require.Equal(t, 1, countCapability(spec.Process.Capabilities.Bounding, "CAP_CHOWN"))
}

func countCapability(values []string, cap string) int {
	count := 0
	for _, value := range values {
		if value == cap {
			count++
		}
	}
	return count
}

func TestSelectDockerPacketWriteFlag(t *testing.T) {
	require.Equal(t, "--allow-packet-socket-write", selectDockerPacketWriteFlag("-allow-packet-socket-write\n"))
	require.Equal(t, "--TESTONLY-allow-packet-endpoint-write", selectDockerPacketWriteFlag("-TESTONLY-allow-packet-endpoint-write\n"))
	require.Empty(t, selectDockerPacketWriteFlag("-net-raw\n"))
}
