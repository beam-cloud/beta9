package compute

import (
	"net/netip"
	"testing"

	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"tailscale.com/ipn/ipnstate"
)

func TestMachineNetworkFallsBackToMatchingTailnetPeer(t *testing.T) {
	machines := []*pb.Machine{
		{Id: "missing"},
		{Id: "reported", PublicIp: "203.0.113.10", TailnetHostname: "custom-agent", TailnetIp: "100.64.0.10", TailnetSsh: true},
		{Id: "unrelated"},
		{Id: "collision"},
	}
	peers := []*ipnstate.PeerStatus{
		{
			HostName: "beam-agent-missing", DNSName: "beam-agent-missing.tailnet.ts.net.",
			TailscaleIPs: []netip.Addr{netip.MustParseAddr("fd7a:115c:a1e0::1"), netip.MustParseAddr("100.64.0.7")},
			CurAddr:      "203.0.113.7:41641",
		},
		{
			HostName: "custom-agent", DNSName: "custom-agent.tailnet.ts.net.",
			TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.11")}, CurAddr: "203.0.113.11:41641",
		},
		{
			HostName: "beam-agent-collision", DNSName: "beam-agent-collision-1.tailnet.ts.net.",
			CurAddr: "203.0.113.12:41641",
		},
		nil,
	}

	applyMachinePeerNetwork(machines, peers)

	require.Equal(t, "203.0.113.7", machines[0].PublicIp)
	require.Equal(t, "100.64.0.7", machines[0].TailnetIp)
	require.Equal(t, "beam-agent-missing", machines[0].TailnetHostname)
	require.False(t, machines[0].TailnetSsh, "SSH permission cannot be inferred from peer presence")
	require.Equal(t, "203.0.113.10", machines[1].PublicIp)
	require.Equal(t, "100.64.0.10", machines[1].TailnetIp)
	require.Equal(t, "custom-agent", machines[1].TailnetHostname)
	require.True(t, machines[1].TailnetSsh)
	require.Empty(t, machines[2].PublicIp)
	require.Empty(t, machines[2].TailnetHostname)
	require.Empty(t, machines[3].PublicIp, "a suffixed DNS name must not match another node")
}

func TestPeerPublicIPRejectsPrivateAndRelayAddresses(t *testing.T) {
	tests := []struct {
		name string
		peer ipnstate.PeerStatus
		want string
	}{
		{name: "direct endpoint", peer: ipnstate.PeerStatus{CurAddr: "203.0.113.7:41641", Addrs: []string{"203.0.113.8:41641"}}, want: "203.0.113.7"},
		{name: "advertised endpoint", peer: ipnstate.PeerStatus{CurAddr: "10.0.0.7:41641", Addrs: []string{"100.64.0.7:41641", "203.0.113.8:41641"}}, want: "203.0.113.8"},
		{name: "IPv6 endpoint", peer: ipnstate.PeerStatus{CurAddr: "[2606:4700::1111]:41641"}, want: "2606:4700::1111"},
		{name: "private only", peer: ipnstate.PeerStatus{Addrs: []string{"127.0.0.1:41641", "10.0.0.1:41641", "192.168.0.1:41641", "100.100.0.1:41641", "[fd7a:115c:a1e0::1]:41641"}}},
		{name: "relay only", peer: ipnstate.PeerStatus{Relay: "nyc", CurAddr: "derp-1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, peerPublicIP(&tt.peer))
		})
	}
}
