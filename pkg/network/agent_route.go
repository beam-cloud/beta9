package network

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"golang.org/x/net/dns/dnsmessage"
	"tailscale.com/tailcfg"
)

type agentPeerLookup func(context.Context, string) (netip.Addr, *tailcfg.Node, error)

// agentRouteTarget treats reported proxy addresses as untrusted. WhoIs uses
// Tailscale's authenticated address ownership rather than the agent's network
// telemetry. Pin the resulting IP so DNS cannot change after the
// ownership check. No connection to the reported destination precedes this.
func (t *Tailscale) agentRouteTarget(ctx context.Context, machineID, target string) (string, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return "", fmt.Errorf("invalid agent proxy target: %w", err)
	}
	if port != strconv.Itoa(types.DefaultAgentTSNetRouteProxyPort) || strings.TrimSpace(machineID) == "" {
		return "", fmt.Errorf("agent route requires a machine identity and proxy port %d", types.DefaultAgentTSNetRouteProxyPort)
	}
	lookup := t.agentPeerFunc
	if lookup == nil {
		lookup = t.lookupAgentPeer
	}
	ip, peer, err := lookup(ctx, host)
	if err != nil {
		return "", fmt.Errorf("verify agent route destination: %w", err)
	}
	if peer == nil || !peer.Hostinfo.Valid() || peer.Hostinfo.Hostname() != types.AgentTailnetHostnamePrefix+machineID {
		return "", fmt.Errorf("agent proxy destination does not belong to machine %s", machineID)
	}
	for _, address := range peer.Addresses {
		if address.IsSingleIP() && address.Addr().Unmap() == ip.Unmap() {
			return net.JoinHostPort(ip.String(), port), nil
		}
	}
	return "", fmt.Errorf("agent proxy destination is not an address of machine %s", machineID)
}

func (t *Tailscale) lookupAgentPeer(ctx context.Context, host string) (netip.Addr, *tailcfg.Node, error) {
	if err := t.ensureUp(ctx); err != nil {
		return netip.Addr{}, nil, err
	}
	client, err := t.currentServer().LocalClient()
	if err != nil {
		return netip.Addr{}, nil, err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		// Legacy agents can report a MagicDNS name. Resolve through this tsnet
		// node, then authenticate and pin the answer before opening a socket.
		if !strings.Contains(host, ".") {
			status, statusErr := client.StatusWithoutPeers(ctx)
			if statusErr != nil {
				return netip.Addr{}, nil, statusErr
			}
			if status.CurrentTailnet == nil || status.CurrentTailnet.MagicDNSSuffix == "" {
				return netip.Addr{}, nil, fmt.Errorf("tailnet DNS suffix is unavailable")
			}
			host += "." + status.CurrentTailnet.MagicDNSSuffix
		}
		for _, query := range []string{"A", "AAAA"} {
			packet, _, queryErr := client.QueryDNS(ctx, host, query)
			if queryErr != nil {
				err = queryErr
				continue
			}
			var message dnsmessage.Message
			if err = message.Unpack(packet); err != nil {
				continue
			}
			for _, answer := range message.Answers {
				switch value := answer.Body.(type) {
				case *dnsmessage.AResource:
					ip = netip.AddrFrom4(value.A)
				case *dnsmessage.AAAAResource:
					ip = netip.AddrFrom16(value.AAAA)
				}
				if ip.IsValid() {
					break
				}
			}
			if ip.IsValid() {
				break
			}
		}
		if !ip.IsValid() {
			return netip.Addr{}, nil, fmt.Errorf("resolve agent proxy destination: %v", err)
		}
	}
	identity, err := client.WhoIs(ctx, ip.String())
	if err != nil {
		return netip.Addr{}, nil, err
	}
	if identity == nil || identity.Node == nil {
		return netip.Addr{}, nil, fmt.Errorf("tailnet node identity is unavailable")
	}
	return ip, identity.Node, nil
}
