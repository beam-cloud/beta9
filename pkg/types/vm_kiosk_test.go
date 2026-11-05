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
