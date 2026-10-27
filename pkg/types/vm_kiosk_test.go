package types

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestVMKioskCloneIsolatesApplicationAndMCPState(t *testing.T) {
	parent := &VMKioskSpec{Command: []string{"app"}, Env: map[string]string{"KEY": "saved"}, MCP: &VMKioskMCP{Command: []string{"adapter"}}}
	child := parent.Clone()
	child.Command[0] = "other"
	child.Env["KEY"] = "changed"
	child.MCP.Command[0] = "other-adapter"
	child.MCP.Path = "/changed"
	require.Equal(t, "app", parent.Command[0])
	require.Equal(t, "saved", parent.Env["KEY"])
	require.Equal(t, "adapter", parent.MCP.Command[0])
	require.Empty(t, parent.MCP.Path)
	require.Nil(t, (*VMKioskSpec)(nil).Clone())
}

func TestVMWebRTCPolicyDefaultsAndRejectsUnsafeTiming(t *testing.T) {
	var policy VMWebRTCConfig
	require.NoError(t, policy.ApplyDefaults())
	require.Equal(t, 256, policy.MaxConnections)
	require.Equal(t, 8, policy.MaxConnectionsPerSession)
	require.Greater(t, policy.LeaseTTL, policy.CheckInterval+policy.HandshakeTimeout)
	policy.MaxConnections = 32
	require.NoError(t, policy.ApplyDefaults())
	require.Equal(t, 32, policy.MaxConnections)
	for _, invalid := range []VMWebRTCConfig{
		{MaxConnections: -1}, {MaxConnectionsPerSession: -1}, {RegistrationsPerMinute: -1},
		{HandshakeTimeout: -time.Second}, {SessionTTL: -time.Second}, {LeaseTTL: -time.Second}, {CheckInterval: -time.Second},
		{LeaseTTL: time.Second}, {SessionTTL: time.Second},
	} {
		require.Error(t, invalid.ApplyDefaults())
	}
}
