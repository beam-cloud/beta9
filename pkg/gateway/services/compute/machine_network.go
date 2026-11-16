package compute

import (
	"context"
	"net/netip"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"tailscale.com/ipn/ipnstate"
)

// Older agents may be connected without reporting AgentNetworkInfo. Use the
// gateway's existing netmap to fill missing display fields without changing
// stored agent state or inferring whether tailnet SSH is enabled.
func (s *Service) fillMissingMachineNetwork(ctx context.Context, machines []*pb.Machine) {
	if s.tailscale == nil || !s.appConfig.Tailscale.Enabled {
		return
	}
	missing := false
	for _, machine := range machines {
		if machine.PublicIp == "" || machine.TailnetIp == "" || machine.TailnetHostname == "" {
			missing = true
			break
		}
	}
	if !missing {
		return
	}
	lookupCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	peers, err := s.tailscale.PeerStatuses(lookupCtx)
	if err != nil {
		return // Inventory remains available when tailnet status is unavailable.
	}
	applyMachinePeerNetwork(machines, peers)
}

func applyMachinePeerNetwork(machines []*pb.Machine, peers []*ipnstate.PeerStatus) {
	byHost := make(map[string]*ipnstate.PeerStatus, len(peers))
	for _, peer := range peers {
		if peer == nil {
			continue
		}
		// Use the actual MagicDNS label, which may have a collision suffix.
		host, _, _ := strings.Cut(peer.DNSName, ".")
		if host == "" {
			host = peer.HostName
		}
		byHost[host] = peer
	}
	for _, machine := range machines {
		host := machine.TailnetHostname
		if host == "" {
			host = types.AgentTailnetHostnamePrefix + machine.Id
		}
		peer := byHost[host]
		if peer == nil {
			continue
		}
		if machine.TailnetHostname == "" {
			machine.TailnetHostname = host
		}
		if machine.TailnetIp == "" {
			for _, ip := range peer.TailscaleIPs {
				if machine.TailnetIp == "" || ip.Is4() {
					machine.TailnetIp = ip.String()
				}
			}
		}
		if machine.PublicIp == "" {
			machine.PublicIp = peerPublicIP(peer)
		}
	}
}

func peerPublicIP(peer *ipnstate.PeerStatus) string {
	endpoints := append([]string{peer.CurAddr}, peer.Addrs...)
	for _, endpoint := range endpoints {
		addrPort, err := netip.ParseAddrPort(endpoint)
		if err != nil {
			continue
		}
		addr := addrPort.Addr().Unmap()
		// CGNAT addresses are global-unicast according to netip, but are not
		// public endpoints (and include the tailnet's own address range).
		if netip.MustParsePrefix("100.64.0.0/10").Contains(addr) {
			continue
		}
		if ip := normalizePublicIP(addr.String()); ip != "" {
			return ip
		}
	}
	return ""
}
