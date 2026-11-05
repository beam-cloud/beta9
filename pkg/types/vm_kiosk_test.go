package types

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestVMKioskCloneIsolatesApplicationAndMCPState(t *testing.T) {
	parent := &VMKioskSpec{Command: []string{"app"}, Env: map[string]string{"KEY": "saved"}, MCP: map[string]VMKioskMCPServer{"app": {Command: "adapter", Args: []string{"original"}, Env: map[string]string{"TOKEN": "saved"}}}}
	child := parent.Clone()
	child.Command[0] = "other"
	child.Env["KEY"] = "changed"
	child.MCP["app"].Args[0] = "changed"
	child.MCP["app"].Env["TOKEN"] = "changed"
	require.Equal(t, "app", parent.Command[0])
	require.Equal(t, "saved", parent.Env["KEY"])
	require.Equal(t, "original", parent.MCP["app"].Args[0])
	require.Equal(t, "saved", parent.MCP["app"].Env["TOKEN"])
	require.Nil(t, (*VMKioskSpec)(nil).Clone())
}

func TestPublicTemplateRemovesWorkspaceAdaptersAndSSHWithoutMutatingSource(t *testing.T) {
	source := VMSpec{SSH: true, SSHPublicKey: "source-key", Ports: []uint32{VMSSHPort, VMDesktopPort}, Kiosk: &VMKioskSpec{
		Command: []string{"editor"}, Env: map[string]string{"TOKEN": "secret"},
		MCP: map[string]VMKioskMCPServer{"remote": {URL: "https://server.test/mcp", Headers: map[string]string{"Authorization": "secret"}}},
	}}
	shared := source.PublicTemplate()
	require.False(t, shared.SSH)
	require.Empty(t, shared.SSHPublicKey)
	require.Equal(t, []uint32{VMDesktopPort}, shared.Ports)
	require.Nil(t, shared.Kiosk.Env)
	require.Nil(t, shared.Kiosk.MCP)
	require.Equal(t, source.Kiosk.Command, shared.Kiosk.Command)
	require.True(t, source.SSH)
	require.Contains(t, source.Ports, uint32(VMSSHPort))
	require.NotEmpty(t, source.Kiosk.MCP["remote"].URL)
	require.NotEmpty(t, source.Kiosk.Env)
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

func TestVMDesktopPrivatePorts(t *testing.T) {
	for _, test := range []struct {
		name  string
		spec  VMSpec
		ports []uint32
	}{
		{"headless", VMSpec{}, nil},
		{"legacy desktop", VMSpec{Desktop: true}, nil},
		{"desktop websockets", VMSpec{Desktop: true, DesktopTransport: VMDesktopTransportWebSockets}, []uint32{VMKioskStatusPort}},
		{"desktop webrtc", VMSpec{Desktop: true, DesktopTransport: VMDesktopTransportWebRTC}, []uint32{VMKioskMediaPort, VMKioskStatusPort}},
		{"kiosk websockets", VMSpec{Kiosk: &VMKioskSpec{Transport: VMKioskTransportWebSockets}}, []uint32{VMKioskStatusPort, VMKioskMCPPort}},
		{"kiosk webrtc", VMSpec{Kiosk: &VMKioskSpec{Transport: VMKioskTransportWebRTC}}, []uint32{VMKioskMediaPort, VMKioskStatusPort, VMKioskMCPPort}},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.ports, test.spec.DesktopPrivatePorts())
			if test.spec.Kiosk != nil {
				require.Equal(t, test.ports, test.spec.Kiosk.PrivatePorts())
			}
		})
	}
	require.Nil(t, (*VMKioskSpec)(nil).PrivatePorts())
}
