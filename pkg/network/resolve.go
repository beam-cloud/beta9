package network

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"tailscale.com/client/tailscale"
)

func ConnectToHost(ctx context.Context, host string, timeout time.Duration, tailscale *Tailscale, tsConfig types.TailscaleConfig) (net.Conn, error) {
	var conn net.Conn = nil

	if tsConfig.Enabled && tailscale != nil && strings.Contains(host, tsConfig.HostName) {
		// Advisory netmap check: a MagicDNS dial for a peer missing from the
		// netmap falls back to the system resolver and fails with a misleading
		// NXDOMAIN, but tsnet.Status can also omit peers that tsnet.Dial can
		// still reach — so enrich the error rather than gate the dial.
		var peerErr error
		if peerHost := tailnetHostFromAddr(host); peerHost != "" {
			peerWait := tailnetPeerAdvisoryTimeout
			if timeout > 0 {
				peerWait = min(peerWait, timeout)
			}
			peerErr = tailscale.WaitForPeer(ctx, peerHost, peerWait)
		}

		conn, err := tailscale.DialContextTimeout(ctx, "tcp", host, timeout)
		if err != nil {
			if peerErr != nil {
				return nil, fmt.Errorf("%w; tsnet dial failed: %w", peerErr, err)
			}
			return nil, err
		}

		return conn, err
	}

	dialCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", host)
	if err != nil {
		return conn, err
	}
	return conn, nil
}

func GetDialer(host string, tailscale *Tailscale, tsConfig types.TailscaleConfig) func(ctx context.Context, network, address string) (net.Conn, error) {
	if tsConfig.Enabled && strings.Contains(host, tsConfig.HostName) {
		return tailscale.Dial
	}

	dialer := &net.Dialer{DualStack: true}
	return dialer.DialContext
}

func ResolveTailscaleService(serviceName string, timeout time.Duration) (string, error) {
	client := tailscale.LocalClient{}
	interval := time.Second * 1
	startTime := time.Now()

	for time.Since(startTime) < timeout {
		// Get the status from Tailscale
		status, err := client.Status(context.Background())
		if err != nil {
			return "", err
		}

		// Iterate through the peers to find a matching service
		for _, peer := range status.Peer {
			if !peer.Online {
				continue
			}

			if strings.Contains(peer.HostName, serviceName) {
				return peer.HostName, nil
			}
		}

		time.Sleep(interval)
	}

	return "", fmt.Errorf("no valid service found for <%s>", serviceName)
}
